#include "postgres.h"
#include "fmgr.h"

#include <math.h>
#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "MurmurHash3.h"
#include "utils/array.h"
#include "utils/bytea.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/typcache.h"


#define DEFAULT_ERROR_BOUND 0.001
#define DEFAULT_CONFIDENCE_INTERVAL 0.99
#define MURMUR_SEED 304837963
#define MAX_FREQUENCY ULONG_MAX
#define DEFAULT_TOPN_ITEM_SIZE 16
#define TOPN_ARRAY_OVERHEAD ARR_OVERHEAD_NONULLS(1)


/*
 * Frequency can be set to different types to specify limit and size of the
 * counters. If we change type of Frequency, we also need to update MAX_FREQUENCY
 * above. If Frequency is defined as uint32, MAX_FREQUENCY has to be UINT_MAX.
 */
typedef uint64 Frequency;

/*
 * CmsTopn is the main struct for the count-min sketch top-n implementation and
 * it has two variable length fields. First one is an array for keeping the sketch
 * which is pointed by "sketch" of the struct and second one is an ArrayType for
 * keeping the most frequent n items. It is not possible to lay out two variable
 * width fields consecutively in memory. So we are using pointer arithmetic to
 * reach and handle ArrayType for the most frequent n items.
 */
typedef struct CmsTopn
{
	char length[4];
	uint32 sketchDepth;
	uint32 sketchWidth;
	uint32 topnItemCount;
	uint32 sizeForTopnItem;
	Frequency minFrequencyOfTopnItems;
	Frequency sketch[1];
} CmsTopn;

/*
 * TopnItem is the struct to keep frequent items and their frequencies together.
 * It is useful to sort the top-n items before returning in "topn" function.
 */
typedef struct TopnItem
{
	Datum item;
	Frequency frequency;
} TopnItem;


/* local functions forward declarations */
static CmsTopn * CreateCmsTopn(int32 topnItemCount, float8 errorBound,
							   float8 confidenceInterval);
static CmsTopn * UpdateCmsTopn(CmsTopn *currentCmsTopn, Datum newItem, Oid itemType);
static ArrayType * TopnArray(CmsTopn *cmsTopn);
static Frequency UpdateSketchInPlace(CmsTopn *cmsTopn, Datum newItem, Oid newItemType);
static void DatumToBytes(Datum datum, Oid datumType, StringInfo datumString);
static Frequency CmsTopnEstimateHashedItemFrequency(CmsTopn *cmsTopn,
													uint64 *hashValueArray);
static ArrayType * UpdateTopnArray(CmsTopn *cmsTopn, Datum candidateItem,
								   Frequency itemFrequency, Oid itemType,
								   bool *topnArrayUpdated);
static Frequency CmsTopnEstimateItemFrequency(CmsTopn *cmsTopn, Datum item, Oid itemType);
static CmsTopn * FormCmsTopn(CmsTopn *cmsTopn, ArrayType *newTopnArray);
static CmsTopn * CmsTopnUnion(CmsTopn *firstCmsTopn, CmsTopn *secondCmsTopn);


/* declarations for dynamic loading */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(cms_topn_in);
PG_FUNCTION_INFO_V1(cms_topn_out);
PG_FUNCTION_INFO_V1(cms_topn_recv);
PG_FUNCTION_INFO_V1(cms_topn_send);
PG_FUNCTION_INFO_V1(cms_topn);
PG_FUNCTION_INFO_V1(cms_topn_add);
PG_FUNCTION_INFO_V1(cms_topn_add_agg);
PG_FUNCTION_INFO_V1(cms_topn_add_agg_with_parameters);
PG_FUNCTION_INFO_V1(cms_topn_union);
PG_FUNCTION_INFO_V1(cms_topn_union_agg);
PG_FUNCTION_INFO_V1(cms_topn_frequency);
PG_FUNCTION_INFO_V1(cms_topn_info);
PG_FUNCTION_INFO_V1(topn);


/*
 * cms_topn_in converts from printable representation
 */
Datum
cms_topn_in(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteain, PG_GETARG_DATUM(0));

	return datum;
}


/*
 *  cms_topn_out converts to printable representation
 */
Datum
cms_topn_out(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteaout, PG_GETARG_DATUM(0));

	PG_RETURN_CSTRING(datum);
}


/*
 * cms_topn_recv converts external binary format to bytea
 */
Datum
cms_topn_recv(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(bytearecv, PG_GETARG_DATUM(0));

	return datum;
}


/*
 * cms_topn_send converts bytea to binary format
 */
Datum
cms_topn_send(PG_FUNCTION_ARGS)
{
	Datum datum = DirectFunctionCall1(byteasend, PG_GETARG_DATUM(0));

	return datum;
}


/*
 * cms_topn is a user-facing UDF which creates new cms_topn with given parameters.
 * The first parameter is for number of top items which will be kept, the others
 * are for error bound(e) and confidence interval(p) respectively. Given e and p,
 * estimated frequency can be at most (e*||a||) more than real frequency with the
 * probability p while ||a|| is the sum of frequencies of all items according to
 * this paper: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf.
 */
Datum
cms_topn(PG_FUNCTION_ARGS)
{
	int32 topnItemCount = PG_GETARG_UINT32(0);
	float8 errorBound = PG_GETARG_FLOAT8(1);
	float8 confidenceInterval =  PG_GETARG_FLOAT8(2);

	CmsTopn *cmsTopn = CreateCmsTopn(topnItemCount, errorBound, confidenceInterval);

	PG_RETURN_POINTER(cmsTopn);
}


/*
 * CmsTopnCreate creates CmsTopn structure with given parameters. The first parameter
 * is for the number of frequent items, other two specifies error bound and confidence
 * interval. Size of the sketch is determined with the given error bound and confidence
 * interval according to formula in this paper:
 * http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf.
 *
 * Note that here we set only number of frequent items and don't allocate memory
 * for keeping them. Memory allocation for frequent items array is done when the
 * first insertion occurs.
 */
static CmsTopn *
CreateCmsTopn(int32 topnItemCount, float8 errorBound, float8 confidenceInterval)
{
	CmsTopn *cmsTopn = NULL;
	uint32 sketchWidth = 0;
	uint32 sketchDepth = 0;
	Size staticStructSize = 0;
	Size sketchSize = 0;
	Size reservedSizeForItems = 0;
	Size topnArrayReservedSize = 0;
	Size totalCmsTopnSize = 0;

	if (topnItemCount <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid parameters for cms_topn"),
						errhint("Number of top items has to be positive")));
	}
	else if (errorBound <= 0 || errorBound >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid parameters for cms_topn"),
						errhint("Error bound has to be between 0 and 1")));
	}
	else if (confidenceInterval <= 0 || confidenceInterval >= 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid parameters for cms_topn"),
						errhint("Confidence interval has to be between 0 and 1")));
	}

	sketchWidth = (uint32) ceil(exp(1) / errorBound);
	sketchDepth = (uint32) ceil(log(1 / (1 - confidenceInterval)));
	sketchSize =  sizeof(Frequency) * sketchDepth * sketchWidth;
	staticStructSize = sizeof(CmsTopn);
	reservedSizeForItems = topnItemCount * DEFAULT_TOPN_ITEM_SIZE;
	topnArrayReservedSize = TOPN_ARRAY_OVERHEAD + reservedSizeForItems;
	totalCmsTopnSize = staticStructSize + sketchSize + topnArrayReservedSize;

	cmsTopn = palloc0(totalCmsTopnSize);
	cmsTopn->sketchDepth = sketchDepth;
	cmsTopn->sketchWidth = sketchWidth;
	cmsTopn->topnItemCount = topnItemCount;
	cmsTopn->sizeForTopnItem = DEFAULT_TOPN_ITEM_SIZE;
	cmsTopn->minFrequencyOfTopnItems = 0;

	SET_VARSIZE(cmsTopn, totalCmsTopnSize);

	return cmsTopn;
}


/*
 * cms_topn_add is a user-facing UDF which inserts new item to the given CmsTopn.
 * The first parameter is for the CmsTopn to add the new item and second is for
 * the new item.
 */
Datum
cms_topn_add(PG_FUNCTION_ARGS)
{
	CmsTopn *currentCmsTopn = NULL;
	CmsTopn *updatedCmsTopn = NULL;
	Datum newItem = 0;
	Oid newItemType = InvalidOid;

	/* check whether cms_topn is null */
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}
	else
	{
		currentCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	}

	/* if new item is null, then return current CmsTopn */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(currentCmsTopn);
	}

	newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	if (newItemType == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("could not determine input data type")));
	}

	newItem = PG_GETARG_DATUM(1);
	updatedCmsTopn = UpdateCmsTopn(currentCmsTopn, newItem, newItemType);

	PG_RETURN_POINTER(updatedCmsTopn);
}


/*
 * UpdateCmsTopn is helper function to add new item to cmsTopn structure. It first
 * adds the item to the sketch, calculates its frequency, then updates the top-n
 * array. Finally it forms new CmsTopn from updated sketch and updated top-n array.
 */
static CmsTopn *
UpdateCmsTopn(CmsTopn *currentCmsTopn, Datum newItem, Oid itemType)
{
	CmsTopn *updatedCmsTopn = NULL;
	ArrayType *currentTopnArray = NULL;
	ArrayType *updatedTopnArray = NULL;
	TypeCacheEntry *itemTypeCacheEntry = NULL;
	Datum detoastedItem = 0;
	Frequency newItemFrequency = 0;
	bool topnArrayUpdated = false;
	Size currentTopnArrayLength = 0;

	currentTopnArray = TopnArray(currentCmsTopn);
	currentTopnArrayLength = ARR_DIMS(currentTopnArray)[0];

	if (currentTopnArrayLength != 0 && itemType != ARR_ELEMTYPE(currentTopnArray))
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("not proper type for this cms_topn")));
	}

	/* make sure the datum is not toasted */
	itemTypeCacheEntry = lookup_type_cache(itemType, 0);
	if (itemTypeCacheEntry->typlen == -1)
	{
		detoastedItem = PointerGetDatum(PG_DETOAST_DATUM(newItem));
	}
	else
	{
		detoastedItem = newItem;
	}

	newItemFrequency = UpdateSketchInPlace(currentCmsTopn, detoastedItem, itemType);
	updatedTopnArray = UpdateTopnArray(currentCmsTopn, detoastedItem, newItemFrequency,
									   itemType, &topnArrayUpdated);
	if (topnArrayUpdated)
	{
		updatedCmsTopn = FormCmsTopn(currentCmsTopn, updatedTopnArray);
	}
	else
	{
		updatedCmsTopn = currentCmsTopn;
	}

	return updatedCmsTopn;
}


/*
 * TopnArray returns pointer for the ArrayType which is kept in CmsTopn structure
 * by calculating its place with pointer arithmetic.
 */
static ArrayType *
TopnArray(CmsTopn *cmsTopn)
{
	Size staticSize = sizeof(CmsTopn);
	Size sketchSize = cmsTopn->sketchDepth * cmsTopn->sketchWidth * sizeof(Frequency);
	Size sizeWithoutTopnArray = staticSize + sketchSize;
	ArrayType *topnArray = (ArrayType *) (((char *) cmsTopn) + sizeWithoutTopnArray);

	return topnArray;
}


/*
 * UpdateSketchInPlace updates skecth in CmsTopn with given item in place and
 * returns new estimated frequency for the given item.
 */
static Frequency
UpdateSketchInPlace(CmsTopn *cmsTopn, Datum newItem, Oid newItemType)
{
	uint32 hashIndex = 0;
	uint64 hashValueArray[2] = {0, 0};
	StringInfo newItemString = makeStringInfo();
	Frequency newFrequency = 0;
	Frequency minFrequency = MAX_FREQUENCY;

	DatumToBytes(newItem, newItemType, newItemString);
	MurmurHash3_x64_128(newItemString->data, newItemString->len, MURMUR_SEED,
						&hashValueArray);
	minFrequency = CmsTopnEstimateHashedItemFrequency(cmsTopn, hashValueArray);
	newFrequency = minFrequency + 1;

	/* XXX Here we need to explain why we can create an independent hash function */
	for (hashIndex = 0; hashIndex < cmsTopn->sketchDepth; hashIndex++)
	{
		uint32 depthOffset = hashIndex * cmsTopn->sketchWidth;
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 widthIndex = hashValue	% cmsTopn->sketchWidth;
		uint32 counterIndex = depthOffset + widthIndex;

		/* selective update to decrease effect of collisions */
		Frequency counterFrequency = cmsTopn->sketch[counterIndex];
		if (newFrequency > counterFrequency)
		{
			cmsTopn->sketch[counterIndex] = newFrequency;
		}
	}

	return newFrequency;
}


/*
 * DatumToBytes converts datum to its bytes according to its type
 */
static void
DatumToBytes(Datum datum, Oid datumType, StringInfo datumString)
{
	TypeCacheEntry *datumTypeCacheEntry = lookup_type_cache(datumType, 0);

	if (datumTypeCacheEntry->type_id != RECORDOID
		&& datumTypeCacheEntry->typtype != TYPTYPE_COMPOSITE)
	{
		Size size;

		if (datumTypeCacheEntry->typlen == -1)
		{
			size = VARSIZE_ANY_EXHDR(DatumGetPointer(datum));
		}
		else
		{
			size = datumGetSize(datum, datumTypeCacheEntry->typbyval,
								datumTypeCacheEntry->typlen);
		}

		if (datumTypeCacheEntry->typbyval)
		{
			appendBinaryStringInfo(datumString, (char *) &datum, size);
		}
		else
		{
			appendBinaryStringInfo(datumString, VARDATA_ANY(datum), size);
		}
	}
	else
	{
		/* For composite types, we need to serialize all attributes */
		HeapTupleHeader record = DatumGetHeapTupleHeader(datum);
		TupleDesc tupleDescriptor = NULL;
		int attributeIndex = 0;
		HeapTupleData tmptup;

		tupleDescriptor = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(record),
													  HeapTupleHeaderGetTypMod(record));
		tmptup.t_len = HeapTupleHeaderGetDatumLength(record);
		tmptup.t_data = record;

		for (attributeIndex = 0; attributeIndex < tupleDescriptor->natts;
			 attributeIndex++)
		{
			Form_pg_attribute att = tupleDescriptor->attrs[attributeIndex];
			bool isnull;
			Datum tmp = heap_getattr(&tmptup, attributeIndex + 1, tupleDescriptor,
									 &isnull);

			if (isnull)
			{
				appendStringInfoChar(datumString, '0');
				continue;
			}

			appendStringInfoChar(datumString, '1');
			DatumToBytes(tmp, att->atttypid, datumString);
		}
	}
}


/*
 * CmsTopnEstimateHashedItemFrequency is a helper function to get frequency of
 * an item's hashed values.
 */
static Frequency
CmsTopnEstimateHashedItemFrequency(CmsTopn *cmsTopn, uint64 *hashValueArray)
{
	uint32 hashIndex = 0;
	Frequency minFrequency = MAX_FREQUENCY;

	for (hashIndex = 0; hashIndex < cmsTopn->sketchDepth; hashIndex++)
	{
		uint32 depthOffset = hashIndex * cmsTopn->sketchWidth;
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 widthIndex = hashValue	% cmsTopn->sketchWidth;
		uint32 counterIndex = depthOffset + widthIndex;

		Frequency counterFrequency = cmsTopn->sketch[counterIndex];
		if (counterFrequency < minFrequency)
		{
			minFrequency = counterFrequency;
		}
	}

	return minFrequency;
}


/*
 * UpdateTopnArray is a helper function for the unions and inserts. It takes
 * given item and its frequency. If the item is not in the top-n array, it tries
 * to insert new item. If there is place in the top-n array, it insert directly.
 * Otherwise, it compares its frequency with minimum of current items in the array
 * and updates top-n array if new frequency is bigger.
 */
static ArrayType *
UpdateTopnArray(CmsTopn *cmsTopn, Datum candidateItem, Frequency itemFrequency,
				Oid itemType, bool *topnArrayUpdated)
{
	ArrayType *currentTopnArray = TopnArray(cmsTopn);
	ArrayType *updatedTopnArray = NULL;
	int candidateIndex = -1;
	TypeCacheEntry *itemTypeCacheEntry = lookup_type_cache(itemType, 0);
	int16 itemTypeLength = itemTypeCacheEntry->typlen;
	bool itemTypeByValue = itemTypeCacheEntry->typbyval;
	char itemTypeAlignment = itemTypeCacheEntry->typalign;
	int currentArrayLength = ARR_DIMS(currentTopnArray)[0];
	Frequency minOfNewTopnItems = MAX_FREQUENCY;
	bool candidateAlreadyInArray = false;

	if (currentArrayLength == 0)
	{
		currentTopnArray = construct_empty_array(itemType);
	}

	/*
	 * If item frequency is smaller than min frequency of old top-n items,
	 * it cannot be in the top-n items and we insert it if there is place.
	 * Otherwise, we need to check new minimum and whether it is in the top-n.
	 */
	if (itemFrequency <= cmsTopn->minFrequencyOfTopnItems)
	{
		if (currentArrayLength < cmsTopn->topnItemCount)
		{
			candidateIndex =  currentArrayLength + 1;
			minOfNewTopnItems = itemFrequency;
		}
	}
	else
	{
		ArrayIterator iterator = array_create_iterator(currentTopnArray, 0);
		Datum topnItem = 0;
		int topnItemIndex = 1;
		int minIndex = 1;
		bool hasMoreItem = false;
		bool isNull = false;

		/*
		 * Find the top-n item with minimum frequency to replace it with candidate
		 * top-n item.
		 */
		hasMoreItem = array_iterate(iterator, &topnItem, &isNull);
		while (hasMoreItem)
		{
			Frequency topnItemFrequency = CmsTopnEstimateItemFrequency(cmsTopn, topnItem,
																	   itemType);
			if (topnItemFrequency < minOfNewTopnItems)
			{
				minOfNewTopnItems = topnItemFrequency;
				minIndex = topnItemIndex;
			}

			candidateAlreadyInArray = datumIsEqual(topnItem, candidateItem,
												   itemTypeByValue, itemTypeLength);
			if (candidateAlreadyInArray)
			{
				minIndex = -1;
				break;
			}

			hasMoreItem = array_iterate(iterator, &topnItem, &isNull);
			topnItemIndex++;
		}

		/* if new item is not in the top-n and there is place, insert the item */
		if (!candidateAlreadyInArray && currentArrayLength < cmsTopn->topnItemCount)
		{
			minIndex = currentArrayLength + 1;
			minOfNewTopnItems = Min(minOfNewTopnItems, itemFrequency);
		}

		candidateIndex = minIndex;
	}

	/*
	 * If it is not in the top-n structure and its frequency bigger than minimum
	 * put into top-n instead of the item which has minimum frequency. If it is in
	 * top-n or not frequent items, do not change anything.
	 */
	if (!candidateAlreadyInArray && minOfNewTopnItems <= itemFrequency)
	{
		updatedTopnArray = array_set(currentTopnArray, 1, &candidateIndex, candidateItem,
									 false, -1, itemTypeLength, itemTypeByValue,
									 itemTypeAlignment);
		cmsTopn->minFrequencyOfTopnItems = minOfNewTopnItems;
		*topnArrayUpdated = true;
	}
	else
	{
		updatedTopnArray = currentTopnArray;
		*topnArrayUpdated = false;
	}

	return updatedTopnArray;
}


/*
 * CmsTopnEstimateItemFrequency is a helper function which uses
 * CmsTopnEstimateHashedItemFrequency after calculating hash values.
 */
static Frequency
CmsTopnEstimateItemFrequency(CmsTopn *cmsTopn, Datum item, Oid itemType)
{
	TypeCacheEntry *typeCacheEntry = lookup_type_cache(itemType, 0);
	uint64 hashValueArray[2] = {0, 0};
	StringInfo itemString = makeStringInfo();
	Frequency count = 0;

	/* make sure the datum is not toasted */
	if (typeCacheEntry->typlen == -1)
	{
		Datum detoastedItem =  PointerGetDatum(PG_DETOAST_DATUM(item));
		DatumToBytes(detoastedItem, itemType, itemString);
	}
	else
	{
		DatumToBytes(item, itemType, itemString);
	}

	MurmurHash3_x64_128(itemString->data, itemString->len, MURMUR_SEED, &hashValueArray);
	count = CmsTopnEstimateHashedItemFrequency(cmsTopn, hashValueArray);

	return count;
}


/*
 * FormCmsTopn copies current count-min sketch and new top-n array into a new
 * CmsTopn.
 */
static CmsTopn *
FormCmsTopn(CmsTopn *cmsTopn, ArrayType *newTopnArray)
{
	Size staticSize = sizeof(CmsTopn);
	Size sketchSize = cmsTopn->sketchDepth * cmsTopn->sketchWidth * sizeof(Frequency);
	Size sizeWithoutTopnArray = staticSize + sketchSize;
	Size topnArrayReservedSize = VARSIZE(cmsTopn) - sizeWithoutTopnArray;
	Size newTopnArraySize = ARR_SIZE(newTopnArray);
	char *newCmsTopn = NULL;
	char *topnArrayOffset = NULL;

	/* check whether we have enough memory for new top-n array */
	if (newTopnArraySize > topnArrayReservedSize)
	{
		Size newReservedSizeForItems = 0;
		Size newTopnArrayReservedSize = 0;
		Size sizeForTopnItem = cmsTopn->sizeForTopnItem * 2;
		uint32 topnItemCount = cmsTopn->topnItemCount;

		cmsTopn->sizeForTopnItem = sizeForTopnItem;
		newReservedSizeForItems = topnItemCount * sizeForTopnItem;
		newTopnArrayReservedSize = TOPN_ARRAY_OVERHEAD + newReservedSizeForItems;
		newCmsTopn = palloc0(newTopnArrayReservedSize);

		/* first copy until to top-n array */
		memcpy(newCmsTopn, (char *)cmsTopn, sizeWithoutTopnArray);

		/* set size of new CmsTopn */
		SET_VARSIZE(newCmsTopn, newTopnArrayReservedSize);
	}
	else
	{
		newCmsTopn = (char *)cmsTopn;
	}

	/* finally copy new top-n array*/
	topnArrayOffset = ((char *) newCmsTopn) + sizeWithoutTopnArray;
	memcpy(topnArrayOffset, newTopnArray, ARR_SIZE(newTopnArray));


	return (CmsTopn *) newCmsTopn;
}


/*
 * cms_topn_add_agg is aggregate function to add items. It uses default values
 * for error bound and confidence interval. The first parameter for the CmsTopn
 * which is updated during the aggregation, the second one is for the items to add
 * and third one specifies number of items to be kept in top-n array.
 */
Datum
cms_topn_add_agg(PG_FUNCTION_ARGS)
{
	CmsTopn *currentCmsTopn = NULL;
	CmsTopn *updatedCmsTopn = NULL;
	uint32 topnItemCount = PG_GETARG_UINT32(2);
	float8 errorBound = DEFAULT_ERROR_BOUND;
	float8 confidenceInterval = DEFAULT_CONFIDENCE_INTERVAL;
	Datum newItem = 0;
	Oid newItemType = InvalidOid;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		elog(ERROR, "cms_topn_add_agg called in non-aggregate context");
	}

	/* check whether cms_topn is null and create if it is */
	if (PG_ARGISNULL(0))
	{
		currentCmsTopn = CreateCmsTopn(topnItemCount, errorBound, confidenceInterval);
	}
	else
	{
		currentCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	}

	/* if new item is null, return current CmsTopn */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(currentCmsTopn);
	}

	newItem = PG_GETARG_DATUM(1);
	newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	updatedCmsTopn = UpdateCmsTopn(currentCmsTopn, newItem, newItemType);

	PG_RETURN_POINTER(updatedCmsTopn);
}


/*
 * cms_topn_add_agg_with_parameters is aggregate function to add items. It allows
 * to specify parameters of created CmsTopn structure. In addition to cms_topn_add_agg
 * function, it takes error bound and confidence interval parameters as the forth and
 * fifth parameters.
 */
Datum
cms_topn_add_agg_with_parameters(PG_FUNCTION_ARGS)
{
	CmsTopn *currentCmsTopn = NULL;
	CmsTopn *updatedCmsTopn = NULL;
	uint32 topnItemCount = PG_GETARG_UINT32(2);
	float8 errorBound = PG_GETARG_FLOAT8(3);
	float8 confidenceInterval = PG_GETARG_FLOAT8(4);
	Datum newItem = 0;
	Oid newItemType = InvalidOid;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		elog(ERROR, "cms_topn_add_agg called in non-aggregate context");
	}

	/* check whether cms_topn is null and create if it is */
	if (PG_ARGISNULL(0))
	{
		currentCmsTopn = CreateCmsTopn(topnItemCount, errorBound, confidenceInterval);
	}
	else
	{
		currentCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	}

	/* if new item is null, return current CmsTopn */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(currentCmsTopn);
	}

	newItem = PG_GETARG_DATUM(1);
	newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	updatedCmsTopn = UpdateCmsTopn(currentCmsTopn, newItem, newItemType);

	PG_RETURN_POINTER(updatedCmsTopn);
}


/*
 * cms_topn_union is a user-facing UDF which takes two cms_topn and returns
 * their union.
 */
Datum
cms_topn_union(PG_FUNCTION_ARGS)
{
	CmsTopn *firstCmsTopn = NULL;
	CmsTopn *secondCmsTopn = NULL;
	CmsTopn *newCmsTopn = NULL;
	ArrayType *firstTopnArray = NULL;
	ArrayType *secondTopnArray = NULL;
	Size firstTopnArrayLength = 0;
	Size secondTopnArrayLength = 0;

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();
	}
	else if (PG_ARGISNULL(0))
	{
		secondCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);
		PG_RETURN_POINTER(secondCmsTopn);
	}
	else if (PG_ARGISNULL(1))
	{
		firstCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
		PG_RETURN_POINTER(firstCmsTopn);
	}

	firstCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	secondCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);

	if (firstCmsTopn->sketchDepth != secondCmsTopn->sketchDepth ||
		firstCmsTopn->sketchWidth != secondCmsTopn->sketchWidth ||
		firstCmsTopn->topnItemCount != secondCmsTopn->topnItemCount)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 	errmsg("cannot merge cms_topns with different parameters")));
	}

	firstTopnArray = TopnArray(firstCmsTopn);
	secondTopnArray = TopnArray(secondCmsTopn);
	firstTopnArrayLength = ARR_DIMS(firstTopnArray)[0];
	secondTopnArrayLength = ARR_DIMS(secondTopnArray)[0];

	if (firstTopnArrayLength == 0)
	{
		PG_RETURN_POINTER(secondCmsTopn);
	}

	if (secondTopnArrayLength == 0)
	{
		PG_RETURN_POINTER(firstCmsTopn);
	}

	if (ARR_ELEMTYPE(firstTopnArray) != ARR_ELEMTYPE(secondTopnArray))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 	errmsg("cannot merge cms_topns of different types")));
	}

	newCmsTopn = CmsTopnUnion(firstCmsTopn, secondCmsTopn);

	PG_RETURN_POINTER(newCmsTopn);
}


/*
 * CmsTopnUnion is helper function for union operations. It first sums two
 * sketchs up and iterates through the top-n of the second to update the top-n
 * of union.
 */
static CmsTopn *
CmsTopnUnion(CmsTopn *firstCmsTopn, CmsTopn *secondCmsTopn)
{
	ArrayType *firstTopnArray = TopnArray(firstCmsTopn);
	ArrayType *secondTopnArray = TopnArray(secondCmsTopn);
	ArrayType *newTopnArray = NULL;
	Oid itemType = ARR_ELEMTYPE(firstTopnArray);
	uint32 topnItemIndex = 0;
	Datum topnItem = 0;
	ArrayIterator topnIterator = NULL;
	Size sketchSize = 0;
	bool isNull = false;
	bool hasMoreItem = false;

	sketchSize = firstCmsTopn->sketchDepth * firstCmsTopn->sketchWidth;
	for (topnItemIndex = 0; topnItemIndex < sketchSize; topnItemIndex++)
	{
		firstCmsTopn->sketch[topnItemIndex] += secondCmsTopn->sketch[topnItemIndex];
	}

	topnIterator = array_create_iterator(secondTopnArray, 0);
	newTopnArray = firstTopnArray;

	hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
	while (hasMoreItem)
	{
		Frequency newItemFrequency = 0;
		bool topnArrayUpdated = false;

		newItemFrequency = CmsTopnEstimateItemFrequency(firstCmsTopn, topnItem,
														itemType);
		newTopnArray = UpdateTopnArray(firstCmsTopn, topnItem, newItemFrequency,
									   itemType, &topnArrayUpdated);
		if (topnArrayUpdated)
		{
			firstCmsTopn = FormCmsTopn(firstCmsTopn, newTopnArray);
		}

		hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
	}

	return firstCmsTopn;
}


/*
 * cms_topn_union_agg is aggregate function to create union of cms_topns
 */
Datum
cms_topn_union_agg(PG_FUNCTION_ARGS)
{
	CmsTopn *firstCmsTopn = NULL;
	CmsTopn *secondCmsTopn = NULL;
	CmsTopn *newCmsTopn = NULL;
	ArrayType *firstTopnArray = NULL;
	ArrayType *secondTopnArray = NULL;
	Size firstTopnArrayLength = 0;
	Size secondTopnArrayLength = 0;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		elog(ERROR, "cmsketch_merge_agg_trans called in non-aggregate context");
	}

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();
	}
	else if (PG_ARGISNULL(0))
	{
		secondCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);
		PG_RETURN_POINTER(secondCmsTopn);
	}
	else if (PG_ARGISNULL(1))
	{
		firstCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
		PG_RETURN_POINTER(firstCmsTopn);
	}

	firstCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	secondCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);

	if (firstCmsTopn->sketchDepth != secondCmsTopn->sketchDepth
		|| firstCmsTopn->sketchWidth != secondCmsTopn->sketchWidth
		|| firstCmsTopn->topnItemCount != secondCmsTopn->topnItemCount)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 	errmsg("cannot merge cms_topns with different parameters")));
	}

	firstTopnArray = TopnArray(firstCmsTopn);
	secondTopnArray = TopnArray(secondCmsTopn);
	firstTopnArrayLength = ARR_DIMS(firstTopnArray)[0];
	secondTopnArrayLength = ARR_DIMS(secondTopnArray)[0];

	if (firstTopnArrayLength == 0)
	{
		PG_RETURN_POINTER(secondCmsTopn);
	}

	if (secondTopnArrayLength == 0)
	{
		PG_RETURN_POINTER(firstCmsTopn);
	}

	if (ARR_ELEMTYPE(firstTopnArray) != ARR_ELEMTYPE(secondTopnArray))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 	errmsg("cannot merge cms_topns of different types")));
	}

	newCmsTopn = CmsTopnUnion(firstCmsTopn, secondCmsTopn);

	PG_RETURN_POINTER(newCmsTopn);
}


/*
 * cms_topn_frequency is a user-facing UDF which returns the estimated frequency of
 * an item. The first parameter is for CmsTopn and second is for the item to return
 * the frequency.
 */
Datum
cms_topn_frequency(PG_FUNCTION_ARGS)
{
	CmsTopn *cmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	ArrayType *topnArray = TopnArray(cmsTopn);
	Datum item = PG_GETARG_DATUM(1);
	Oid	itemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	Frequency count = 0;

	if (itemType == InvalidOid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data types")));
	}

	if (topnArray != NULL && itemType != ARR_ELEMTYPE(topnArray))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("Not proper type for this cms_topn")));
	}

	count = CmsTopnEstimateItemFrequency(cmsTopn, item, itemType);

	PG_RETURN_INT32(count);
}


/*
 * cms_topn_info returns information about the CmsTopn structure
 */
Datum
cms_topn_info(PG_FUNCTION_ARGS)
{
	CmsTopn *cmsTopn = NULL;
	StringInfo cmsTopnInfoString = makeStringInfo();

	cmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	appendStringInfo(cmsTopnInfoString, "Sketch depth = %d, Sketch width = %d, "
					 "Size = %ukB", cmsTopn->sketchDepth, cmsTopn->sketchWidth,
					 VARSIZE(cmsTopn) / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(cmsTopnInfoString->data));
}


/*
 * topn is a user-facing UDF which returns the top items and their frequencies.
 * It first gets the top-n structure and converts it into the ordered array of
 * FrequentItems which keeps Datums and the frequencies in the first call. Then,
 * it returns an item and its frequency according to call counter. This function
 * requires a parameter for the type because PostgreSQL has strongly typed system
 * and the type of frequent items in returning rows has to be given.
 */
Datum
topn(PG_FUNCTION_ARGS)
{
    FuncCallContext *functionCallContext = NULL;
    TupleDesc tupleDescriptor = NULL;
    TupleDesc completeDescriptor = NULL;
	Oid resultTypeId = get_fn_expr_argtype(fcinfo->flinfo, 1);
    CmsTopn *cmsTopn = NULL;
    ArrayType *topnArray = NULL;
    Size topnArrayLength = 0;
    Datum topnItem = 0;
    bool isNull = false;
    ArrayIterator topnIterator = NULL;
    TopnItem *orderedTopn = NULL;
    bool hasMoreItem = false;
    int callCounter = 0;
    int maxCalls = 0;

    if (SRF_IS_FIRSTCALL())
    {
    	MemoryContext oldcontext = NULL;
    	Size topnArraySize = 0;
    	int currentArrayLength = 0;
    	int topnIndex = 0;

    	functionCallContext = SRF_FIRSTCALL_INIT();
    	oldcontext = MemoryContextSwitchTo(functionCallContext->multi_call_memory_ctx);

    	if (PG_ARGISNULL(0))
    	{
    		PG_RETURN_NULL();
    	}

    	cmsTopn = (CmsTopn *)  PG_GETARG_VARLENA_P(0);
    	topnArray = TopnArray(cmsTopn);
    	topnArrayLength = ARR_DIMS(topnArray)[0];

    	if (topnArrayLength == 0)
    	{
    		elog(ERROR, "there is not any items in the cms_topn");
    	}

    	if (ARR_ELEMTYPE(topnArray) != resultTypeId)
    	{
    		elog(ERROR, "not proper cms_topn for the result type");
    	}

    	currentArrayLength = ARR_DIMS(topnArray)[0];
    	functionCallContext->max_calls = currentArrayLength;
    	topnArraySize = currentArrayLength * sizeof(TopnItem);
    	orderedTopn = palloc0(topnArraySize);
    	topnIterator = array_create_iterator(topnArray, 0);

    	hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
    	while (hasMoreItem)
    	{
    		TopnItem f;
    		Oid itemType = ARR_ELEMTYPE(topnArray);

			f.item = topnItem;
			f.frequency = CmsTopnEstimateItemFrequency(cmsTopn, topnItem, itemType);
			orderedTopn[topnIndex] = f;
			hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
			topnIndex++;
		}

		/* improvable part by using different sort algorithms */
		for (topnIndex = 0; topnIndex < currentArrayLength; topnIndex++)
		{
			Frequency max = orderedTopn[topnIndex].frequency;
			TopnItem tmp;
			int maxIndex = topnIndex;
			int j = 0;

			for (j = topnIndex + 1; j < currentArrayLength; j++)
			{
				if(orderedTopn[j].frequency > max)
				{
					max = orderedTopn[j].frequency;
					maxIndex = j;
				}
			}

			tmp = orderedTopn[maxIndex];
			orderedTopn[maxIndex] = orderedTopn[topnIndex];
			orderedTopn[topnIndex] = tmp;
		}

		functionCallContext->user_fctx = orderedTopn;
		get_call_result_type(fcinfo, &resultTypeId, &tupleDescriptor);

		completeDescriptor = BlessTupleDesc(tupleDescriptor);
		functionCallContext->tuple_desc = completeDescriptor;
		MemoryContextSwitchTo(oldcontext);
    }

    functionCallContext = SRF_PERCALL_SETUP();
    callCounter = functionCallContext->call_cntr;
    maxCalls = functionCallContext->max_calls;
    completeDescriptor = functionCallContext->tuple_desc;
    orderedTopn = (TopnItem *) functionCallContext->user_fctx;

    if (callCounter < maxCalls)
    {
    	Datum       *values = (Datum *) palloc(2*sizeof(Datum));
    	HeapTuple    tuple;
    	Datum        result = 0;
    	char *nulls = palloc0(2*sizeof(char));

    	values[0] = orderedTopn[callCounter].item;
    	values[1] = orderedTopn[callCounter].frequency;
    	tuple = heap_formtuple(completeDescriptor, values,nulls);
    	result = HeapTupleGetDatum(tuple);
    	SRF_RETURN_NEXT(functionCallContext, result);
    }
    else
    {
    	SRF_RETURN_DONE(functionCallContext);
    }
}
