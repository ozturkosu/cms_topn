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
#define MURMUR_SEED 953945395346
#define MAX_FREQUENCY ULONG_MAX;
#define DatumPointer(datum, byVal)  (byVal ? (void *)&datum : DatumGetPointer(datum))


typedef uint64 Frequency;

/*
 * CmsTopn is the main struct for the count-min sketch top n implementation and it
 * has two variable length fields. First one is for keeping the sketch which is
 * pointed by "sketch" of the struct and other is for keeping the most frequent
 * n items. It is not possible to point this by adding another field to the struct
 * so we are using pointer arithmetic to reach and handle with these items.
 */
typedef struct CmsTopn
{
	char length[4];
	uint32 sketchDepth;
	uint32 sketchWidth;
	uint32 topnItemCount;
	Frequency minFrequencyOfTopnItems;
	Frequency sketch[1];
} CmsTopn;

/*
 * TopnItem is the struct to keep frequent items and their frequencies together.
 * It is useful to sort the top n items before returning in "topn" function.
 */
typedef struct TopnItem
{
	Datum item;
	Frequency frequency;
} TopnItem;


/* local functions forward declarations */
static CmsTopn * CreateCmsTopn(int32 topnItemCount, float8 errorBound,
							   float8 confidenceInterval);
static CmsTopn * CmsTopnAddItem(CmsTopn *cmsTopn, Datum newItem, Oid newItemType);
static Frequency CountMinSketchAdd(CmsTopn *cmsTopn, uint64 *hashValueArray);
static CmsTopn * CmsTopnUnion(CmsTopn *firstCmsTopn, CmsTopn *secondCmsTopn);
static CmsTopn * InsertItemToTopn(TypeCacheEntry *newItemTypeCacheEntry,
								  CmsTopn *cmsTopn, Datum elem,
								  Frequency newItemFrequency);
static CmsTopn * FormCmsTopn(CmsTopn *cmsTopn, ArrayType *newTopn);
static ArrayType * TopnArray(CmsTopn *cmsTopn);
static Size CmsTopnEmptySize(CmsTopn *cmsTopn);
static Frequency CmsTopnEstimateFrequency(CmsTopn *cmsTopn, Datum item, Oid itemType);
static Frequency CountMinSketchEstimateFrequency(CmsTopn *cmsTopn,
												 uint64 *hashValueArray);
static void DatumToBytes(Datum d, TypeCacheEntry *typ, StringInfo buf);


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
 * The first parameter is for number of top items which will be kept, the others are
 * for error bound(e) and confidence interval(p) respectively. Given e and p,
 * estimated frequency can be at most (e*||a||) more than real frequency with the
 * probability p while ||a|| is the sum of frequencies of all items according to the
 * paper: http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf.
 */
Datum
cms_topn(PG_FUNCTION_ARGS)
{
	int32 topnItemCount = PG_GETARG_UINT32(0);
	float8 errorBound = PG_GETARG_FLOAT8(1);
	float8 confidenceInterval =  PG_GETARG_FLOAT8(2);
	CmsTopn *cmsTopn = NULL;

	cmsTopn = CreateCmsTopn(topnItemCount, errorBound, confidenceInterval);

	PG_RETURN_POINTER(cmsTopn);
}


/*
 * cms_topn_add is a user-facing UDF which inserts new item to the given cms_topn.
 * The first parameter is for the cms_topn to add the new item and second is for the
 * new item.
 */
Datum
cms_topn_add(PG_FUNCTION_ARGS)
{
	CmsTopn *cmsTopn = NULL;
	ArrayType *topnArray = NULL;
	Datum newItem = PG_GETARG_DATUM(1);
	Oid newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);

	/* check whether whether cms_topn is null */
	if (PG_ARGISNULL(0))
	{
		PG_RETURN_NULL();
	}

	cmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	/* if new item is null, return old cms_Topn */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(cmsTopn);
	}

	if (newItemType == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));
	}

	topnArray = TopnArray(cmsTopn);
	/* check item type consistency, if any item is inserted before */
	if (topnArray != NULL && newItemType != topnArray->elemtype)
	{
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("not proper type for this cms_topn")));
	}

	cmsTopn = CmsTopnAddItem(cmsTopn, newItem, newItemType);

	PG_RETURN_POINTER(cmsTopn);
}


/*
 * cms_topn_add_agg is aggregate function to add items. It uses default values
 * for error bound and confidence interval. The first parameter for the cmsTopn
 * which is updated during the aggregation, the second is for the items and third
 * specifies number of top n items.
 */
Datum
cms_topn_add_agg(PG_FUNCTION_ARGS)
{
	CmsTopn *cmsTopn = NULL;
	Datum newItem = PG_GETARG_DATUM(1);
	Oid newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	uint32 topnItemCount = PG_GETARG_UINT32(2);
	float8 errorBound = DEFAULT_ERROR_BOUND;
	float8 confidenceInterval =  DEFAULT_CONFIDENCE_INTERVAL;

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		elog(ERROR, "cms_topn_add_agg called in non-aggregate context");
	}

	/* check whether cms_topn is null and create if it is */
	if (PG_ARGISNULL(0))
	{
		cmsTopn = CreateCmsTopn(topnItemCount, errorBound, confidenceInterval);
	}
	else
	{
		cmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	}

	/* if new item is null, return old cms_Topn */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(cmsTopn);
	}

	cmsTopn = CmsTopnAddItem(cmsTopn, newItem, newItemType);

	PG_RETURN_POINTER(cmsTopn);
}


/*
 * cms_topn_add_agg_with_parameters is aggregate function to add items. It allows
 * to specify parameters of created cms_topn structure. In addition to cms_topn_add_agg
 * function, it takes error bound and confidence interval parameters as the forth and
 * fifth parameters.
 */
Datum
cms_topn_add_agg_with_parameters(PG_FUNCTION_ARGS)
{
	CmsTopn *cmsTopn = NULL;
	Datum newItem = PG_GETARG_DATUM(1);
	Oid newItemType = get_fn_expr_argtype(fcinfo->flinfo, 1);
	uint32 topnItemCount = PG_GETARG_UINT32(2);
	float8 errorBound = PG_GETARG_FLOAT8(3);
	float8 confidenceInterval =  PG_GETARG_FLOAT8(4);

	if (!AggCheckCallContext(fcinfo, NULL))
	{
		elog(ERROR, "cms_topn_add_agg called in non-aggregate context");
	}

	/* check whether cms_topn is null and create if it is */
	if (PG_ARGISNULL(0))
	{
		cmsTopn = CreateCmsTopn(topnItemCount, errorBound, confidenceInterval);
	}
	else
	{
		cmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	}

	/* if new item is null, return old cms_Topn */
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(cmsTopn);
	}

	cmsTopn = CmsTopnAddItem(cmsTopn, newItem, newItemType);

	PG_RETURN_POINTER(cmsTopn);
}


/*
 * CmsTopnCreate creates cmsTopn structure with given parameters.The first
 * parameter is for the number of frequent items, other two specifies error
 * bound and confidence interval. Size of the sketch is determined with the given
 * error bound and confidence interval according to formula in the paper:
 * http://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf. Here, we set only
 * number of frequent items and do'nt allocate memory for keeping them. Memory
 * allocation for frequent items is done when the first insertion occurs.
 */
static CmsTopn *
CreateCmsTopn(int32 topnItemCount, float8 errorBound, float8 confidenceInterval)
{
	CmsTopn *cmsTopn = NULL;
	uint32 sketchWidth = 0;
	uint32 sketchDepth = 0;
	Size staticStructSize = 0;
	Size sketchSize = 0;
	Size totalSize = 0;

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
	staticStructSize = sizeof(CmsTopn);
	sketchSize =  sizeof(Frequency) * sketchDepth * sketchWidth;

	totalSize = staticStructSize + sketchSize;
	cmsTopn = palloc0(totalSize);
	cmsTopn->sketchDepth = sketchDepth;
	cmsTopn->sketchWidth = sketchWidth;
	cmsTopn->topnItemCount = topnItemCount;
	cmsTopn->minFrequencyOfTopnItems = MAX_FREQUENCY;
	SET_VARSIZE(cmsTopn, totalSize);

	return cmsTopn;
}

/*
 * CmsTopnAdd is helper function to add new item to cmsTopn structure.
 * It first adds the item to the sketch, calculates its frequency and
 * then updates the top n structure.
 */
static CmsTopn *
CmsTopnAddItem(CmsTopn *cmsTopn, Datum newItem, Oid newItemType)
{
	StringInfo newItemString = NULL;
	CmsTopn *newCmsTopn = NULL;
	Frequency newItemFrequency = 0;
	TypeCacheEntry *newItemTypeCacheEntry = lookup_type_cache(newItemType, 0);
	uint64 hashValueArray[2] = {0, 0};

	/* make sure the datum is not toasted */
	if (newItemTypeCacheEntry->typlen == -1)
	{
		newItem = PointerGetDatum(PG_DETOAST_DATUM(newItem));
	}

	newItemString = makeStringInfo();
	DatumToBytes(newItem, newItemTypeCacheEntry, newItemString);
	MurmurHash3_x64_128(newItemString->data, newItemString->len,
						MURMUR_SEED, &hashValueArray);

	/* add new item and get the frequency */
	newItemFrequency = CountMinSketchAdd(cmsTopn, hashValueArray);

	/* update the top n array of cms_topn */
	newCmsTopn = InsertItemToTopn(newItemTypeCacheEntry, cmsTopn, newItem,
								  newItemFrequency);

	return newCmsTopn;
}


/*
 * CountMinSketchAdd is helper function to add the new item to the sketch and return
 * its new estimated frequency.
 */
static Frequency
CountMinSketchAdd(CmsTopn *cmsTopn, uint64 *hashValueArray)
{
	uint32 hashIndex = 0;
	Frequency minFrequency = CountMinSketchEstimateFrequency(cmsTopn, hashValueArray);
	Frequency newFrequency = minFrequency + 1;

	for (hashIndex = 0; hashIndex < cmsTopn->sketchDepth; hashIndex++)
	{
		uint32 hashOffset = hashIndex * cmsTopn->sketchWidth;
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 counterIndex = hashValue	% cmsTopn->sketchWidth;
		Frequency counterFrequency = cmsTopn->sketch[hashOffset + counterIndex];+

		cmsTopn->sketch[hashOffset + counterIndex] = Max(counterFrequency, newFrequency);
	}

	return newFrequency;
}


/*
 * cms_topn_union is a user-facing UDF which takes two cms_topn and returns their union
 */
Datum
cms_topn_union(PG_FUNCTION_ARGS)
{
	CmsTopn *firstCmsTopn = NULL;
	CmsTopn *secondCmsTopn = NULL;
	CmsTopn *newCmsTopn = NULL;

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();
	}
	else if (PG_ARGISNULL(0))
	{
		secondCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);
		PG_RETURN_POINTER(secondCmsTopn);
	}

	firstCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(firstCmsTopn);
	}

	secondCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);
	newCmsTopn = CmsTopnUnion(firstCmsTopn, secondCmsTopn);

	PG_RETURN_POINTER(newCmsTopn);
}


/*
 * cms_topn_union_agg is aggregate function to create union of cms_topns
 */
Datum
cms_topn_union_agg(PG_FUNCTION_ARGS)
{
	CmsTopn *cmsTopn = NULL;
	CmsTopn *newCmsTopn = NULL;


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
		newCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);
		PG_RETURN_POINTER(newCmsTopn);
	}

	cmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	if (PG_ARGISNULL(1))
	{
		PG_RETURN_POINTER(cmsTopn);
	}

	newCmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(1);
	cmsTopn = CmsTopnUnion(cmsTopn, newCmsTopn);

	PG_RETURN_POINTER(cmsTopn);
}


/*
 * CmsTopnUnion is helper function for union operations. It first sums two
 * sketchs up and iterates through the top n of the second to update the top n
 * of union.
 */
static CmsTopn *
CmsTopnUnion(CmsTopn *firstCmsTopn, CmsTopn *secondCmsTopn)
{
	ArrayType *firstTopn = TopnArray(firstCmsTopn);
	ArrayType *secondTopn = TopnArray(secondCmsTopn);
	CmsTopn *newCmsTopn = NULL;
	uint32 topnItemIndex = 0;

	if (firstCmsTopn->sketchDepth != secondCmsTopn->sketchDepth
		|| firstCmsTopn->sketchWidth != secondCmsTopn->sketchWidth
		|| firstCmsTopn->topnItemCount != secondCmsTopn->topnItemCount)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 	 	errmsg("cannot merge cms_topns with different parameters")));
	}

	if (firstTopn == NULL)
	{
		newCmsTopn = secondCmsTopn;
	}
	else if (secondTopn == NULL)
	{
		newCmsTopn = firstCmsTopn;
	}
	else
	{
		Datum topnItem = 0;
		ArrayIterator topnIterator = NULL;
		bool isNull = false;
		TypeCacheEntry *newItemTypeCacheEntry = NULL;
		bool hasMoreItem = false;
		Size sketchSize = 0;
		uint64 hashValueArray[2] = {0, 0};

		if (firstTopn->elemtype != secondTopn->elemtype)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 	 	errmsg("cannot merge cms_topns of different types")));
		}

		sketchSize = firstCmsTopn->sketchDepth * firstCmsTopn->sketchWidth;
		for (topnItemIndex = 0; topnItemIndex < sketchSize; topnItemIndex++)
		{
			firstCmsTopn->sketch[topnItemIndex] += secondCmsTopn->sketch[topnItemIndex];
		}

		newItemTypeCacheEntry = lookup_type_cache(secondTopn->elemtype, 0);
		topnIterator = array_create_iterator(secondTopn, 0);

		hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
		while (hasMoreItem)
		{
			StringInfo topnItemString = makeStringInfo();
			Frequency newItemFrequency;

			DatumToBytes(topnItem, newItemTypeCacheEntry, topnItemString);
			MurmurHash3_x64_128(topnItemString->data, topnItemString->len,
								MURMUR_SEED, &hashValueArray);
			newItemFrequency = CountMinSketchEstimateFrequency(firstCmsTopn,
															   hashValueArray);
			firstCmsTopn = InsertItemToTopn(newItemTypeCacheEntry, firstCmsTopn,
											topnItem, newItemFrequency);
			hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
		}

		newCmsTopn = firstCmsTopn;
	}

	return newCmsTopn;
}


/*
 * InsertItemToTopn is helper function for the unions and inserts.
 * It takes new item and its frequency. If the item is not in the top n
 * structure, it tries to insert new item. If there is place in the top n array, it
 * insert directly. Otherwise, it compares its frequency with minimum of old
 * frequent items and updates if new frequency is bigger.
 */
static CmsTopn *
InsertItemToTopn(TypeCacheEntry *newItemTypeCacheEntry, CmsTopn *cmsTopn, Datum newItem,
				 Frequency newItemFrequency)
{
	CmsTopn *newCmsTopn = NULL;
	ArrayType *oldTopn = NULL;
	ArrayType *newTopn = NULL;
	int newItemIndex = -1; /* do not insert if it stays -1 */
	Oid newItemType = newItemTypeCacheEntry->type_id;
	int16 newItemTypeLength = newItemTypeCacheEntry->typlen;
	bool newItemTypeByValue = newItemTypeCacheEntry->typbyval;
	char newItemTypeAlignment = newItemTypeCacheEntry->typalign;
	uint32 topnItemCount = 0;
	Frequency newMinOfTopnItems = MAX_FREQUENCY;

	/* if the new item is the first one, create the top n structure */
	oldTopn = TopnArray(cmsTopn);
	if (oldTopn == NULL)
	{
		oldTopn = construct_empty_array(newItemType);
	}
	/* otherwise get the old topn from cmsTopn */
	else
	{
		topnItemCount = ARR_DIMS(oldTopn)[0];
	}

	/* !!!improvable logic espceially for aggregates with hash and ordered array */
	/* new item is not in the topn */
	if (newItemFrequency <= cmsTopn->minFrequencyOfTopnItems)
	{
		if (topnItemCount < cmsTopn->topnItemCount)
		{
			newItemIndex =  1 + topnItemCount;
			newMinOfTopnItems = newItemFrequency;
		}
	}
	/* new item can be in top n, check by iterating */
	else
	{
		ArrayIterator iterator = array_create_iterator(oldTopn, 0);
		Datum topnItem = 0;
		bool isNull = false;
		uint32 datumLength = 0;
		int minIndex = 1;
		int topnItemIndex = 1;
		Frequency topnItemFrequency = 0;
		bool hasMoreItem = false;
		bool sameLength = false;
		uint64 hashValueArray[2] = {0, 0};

		/* iterate through array */
		hasMoreItem = array_iterate(iterator, &topnItem, &isNull);
		while (hasMoreItem)
		{
			StringInfo topnItemString =  makeStringInfo();

			DatumToBytes(topnItem, newItemTypeCacheEntry, topnItemString);
			MurmurHash3_x64_128(topnItemString->data, topnItemString->len,
								MURMUR_SEED, &hashValueArray);
			topnItemFrequency = CountMinSketchEstimateFrequency(cmsTopn, hashValueArray);
			if (topnItemFrequency < newMinOfTopnItems)
			{
				newMinOfTopnItems = topnItemFrequency;
				minIndex = topnItemIndex;
			}

			datumLength = datumGetSize(topnItem, newItemTypeByValue, newItemTypeLength);
			sameLength = (datumLength == datumGetSize(newItem, newItemTypeByValue,
													  newItemTypeLength));
			if (sameLength)
			{
				void *newItemPointer = DatumPointer(topnItem, newItemTypeByValue);
				void *topnItemPointer = DatumPointer(newItem, newItemTypeByValue);

				if (!memcmp(topnItemPointer, newItemPointer, datumLength))
				{
					minIndex = -1;
					break;
				}
			}

			hasMoreItem = array_iterate(iterator, &topnItem, &isNull);
			topnItemIndex++;
		}

		/* new item is not in the top n and there is place */
		if (minIndex != -1 && topnItemCount < cmsTopn->topnItemCount)
		{
			minIndex = 1 + topnItemCount;
			newMinOfTopnItems = Min(newMinOfTopnItems, newItemFrequency);
		}

		newItemIndex = minIndex;
		array_free_iterator(iterator);
	}

	/*
	 * If it is not in the top n structure and its frequency bigger than minimum
	 * put into top n instead of the item which has minimum frequency
	 */
	if (newItemIndex != -1 && newMinOfTopnItems <= newItemFrequency)
	{
		newTopn = array_set(oldTopn, 1, &newItemIndex, newItem, false, -1,
							newItemTypeLength, newItemTypeByValue, newItemTypeAlignment);
		newCmsTopn = FormCmsTopn(cmsTopn, newTopn);
		newCmsTopn->minFrequencyOfTopnItems = newMinOfTopnItems;
	}
	/* if it is in top n or not frequent items, do not change anything */
	else
	{
		newCmsTopn = cmsTopn;
	}

	return newCmsTopn;
}


/*
 * FormCmsTopn copies old count-min sketch and new top n into new CmsTopn
 */
static CmsTopn *
FormCmsTopn(CmsTopn *cmsTopn, ArrayType *newTopn)
{
	Size newSizeWithoutTopn = CmsTopnEmptySize(cmsTopn);
	Size newSize =  newSizeWithoutTopn + VARSIZE(newTopn);
	char *newCmsTopn = palloc0(newSize);
	char *arrayTypeOffset = NULL;

	memcpy(newCmsTopn, (char *)cmsTopn, newSizeWithoutTopn);
	arrayTypeOffset = ((char *) newCmsTopn) + newSizeWithoutTopn;
	memcpy(arrayTypeOffset, newTopn, VARSIZE(newTopn));
	SET_VARSIZE(newCmsTopn, newSize);

	return (CmsTopn *) newCmsTopn;
}


/*
 * GetCmsTopnItem returns pointer for the ArrayType which is kept in CmsTopn
 * structure by calculating its place with pointer arithmetic.
 */
static ArrayType *
TopnArray(CmsTopn *cmsTopn)
{
	Size emptyCmsTopnSize = CmsTopnEmptySize(cmsTopn);
	ArrayType *topnArray = NULL;

	if (emptyCmsTopnSize != VARSIZE(cmsTopn))
	{
		topnArray = (ArrayType *) (((char *) cmsTopn) + emptyCmsTopnSize);
	}

	return topnArray;
}


/*
 * CmsTopnEmptySize returns empty CmsTopn size. Empty CmsTopn does not include
 * frequent item array.
 */
static Size
CmsTopnEmptySize(CmsTopn *cmsTopn)
{
	Size staticSize = sizeof(CmsTopn);
	Size sketchSize = sizeof(Frequency) * cmsTopn->sketchDepth * cmsTopn->sketchWidth;

	Size emptyCmsTopnSize = staticSize + sketchSize;

	return emptyCmsTopnSize;
}


/*
 * cms_topn_frequency is a user-facing UDF which returns the estimated frequency of
 * an item. The first parameter is for cms_topn and second is for the item to return
 * the frequency.
 */
Datum
cms_topn_frequency(PG_FUNCTION_ARGS)
{
	CmsTopn *cmsTopn = (CmsTopn *) PG_GETARG_VARLENA_P(0);
	ArrayType *topnArray = TopnArray(cmsTopn);
	Datum item = PG_GETARG_DATUM(1);
	Frequency count = 0;
	Oid	itemType = get_fn_expr_argtype(fcinfo->flinfo, 1);

	if (itemType == InvalidOid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data types")));
	}

	if (topnArray != NULL && itemType != topnArray->elemtype)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("Not proper type for this cms_topn")));
	}

	count = CmsTopnEstimateFrequency(cmsTopn, item, itemType);

	PG_RETURN_INT32(count);
}

/*
 * CmsTopnEstimateFrequency is helper function which uses CountMinSketchEstimateFrequency
 * after calculating necessary information.
 */
static Frequency
CmsTopnEstimateFrequency(CmsTopn *cmsTopn, Datum item, Oid itemType)
{
	TypeCacheEntry *typeCacheEntry = lookup_type_cache(itemType, 0);
	StringInfo itemString = makeStringInfo();
	Frequency count = 0;
	uint64 hashValueArray[2] = {0, 0};

	/* make sure the datum is not toasted */
	if (typeCacheEntry->typlen == -1)
	{
		item = PointerGetDatum(PG_DETOAST_DATUM(item));
	}

	DatumToBytes(item, typeCacheEntry, itemString);
	MurmurHash3_x64_128(itemString->data, itemString->len, MURMUR_SEED, &hashValueArray);
	count = CountMinSketchEstimateFrequency(cmsTopn, hashValueArray);

	return count;
}


/*
 * CountMinSketchEstimateFrequency is helper function to get frequency of an item
 */
static Frequency
CountMinSketchEstimateFrequency(CmsTopn *cmsTopn, uint64 *hashValueArray)
{
	uint32 hashIndex = 0;
	Frequency minFrequency = MAX_FREQUENCY;

	for (hashIndex = 0; hashIndex < cmsTopn->sketchDepth; hashIndex++)
	{
		uint32 hashOffset = hashIndex * cmsTopn->sketchWidth;
		uint64 hashValue = hashValueArray[0] + (hashIndex * hashValueArray[1]);
		uint32 counterIndex = hashValue % cmsTopn->sketchWidth;
		Frequency counterFrequency = cmsTopn->sketch[hashOffset + counterIndex];

		minFrequency = Min(minFrequency, counterFrequency);
	}

	return minFrequency;
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
 * It firsts get the top n structure and convert it into the ordered array of
 * FrequentItems which keeps Datums and the frequencies in the first call. Then,
 * it returns an item and its frequency according to call counter.
 */
Datum
topn(PG_FUNCTION_ARGS)
{
    FuncCallContext *functionCallContext = NULL;
    TupleDesc tupleDescriptor = NULL;
    TupleDesc completeDescriptor = NULL;
	Oid resultTypeId = get_fn_expr_argtype(fcinfo->flinfo, 1);
    CmsTopn *cmsTopn = NULL;
    ArrayType *topn = NULL;
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
    	uint32 topnItemCount = 0;
        int topnIndex = 0;

        functionCallContext = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(functionCallContext->multi_call_memory_ctx);
        if (PG_ARGISNULL(0))
        {
        	PG_RETURN_NULL();
        }

        cmsTopn = (CmsTopn *)  PG_GETARG_VARLENA_P(0);
        topn = TopnArray(cmsTopn);
        if (topn == NULL)
        {
    		elog(ERROR, "there is not any items in the cms_topn");
        }

        topn = TopnArray(cmsTopn);
        if (topn->elemtype != resultTypeId)
        {
        	elog(ERROR, "not proper cms_topn for the result type");
        }

        topnItemCount = ARR_DIMS(topn)[0];
        functionCallContext->max_calls = topnItemCount;
        topnArraySize = topnItemCount * sizeof(TopnItem);
        orderedTopn = palloc0(topnArraySize);
        topnIterator = array_create_iterator(topn, 0);
		hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
		while (hasMoreItem)
		{
			TopnItem f;
			Oid itemType = topn->elemtype;

			f.item = topnItem;
			f.frequency = CmsTopnEstimateFrequency(cmsTopn, topnItem, itemType);
			orderedTopn[topnIndex] = f;
			hasMoreItem = array_iterate(topnIterator, &topnItem, &isNull);
			topnIndex++;
		}

		/* improvable part by using different sort algorithms */
		for (topnIndex = 0; topnIndex < topnItemCount; topnIndex++)
		{
			Frequency max = orderedTopn[topnIndex].frequency;
			TopnItem tmp;
			int maxIndex = topnIndex;
			int j = 0;

			for (j = topnIndex + 1; j < topnItemCount; j++)
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
        char *nulls;

        values[0] = orderedTopn[callCounter].item;
        values[1] = orderedTopn[callCounter].frequency;
        nulls = palloc0(2*sizeof(char));
        tuple = heap_formtuple(completeDescriptor, values,nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(functionCallContext, result);
    }
    else
    {
        SRF_RETURN_DONE(functionCallContext);
    }
}


/*
 * DatumToBytes converts datum to its bytes according to its type
 */
static void
DatumToBytes(Datum datum, TypeCacheEntry *datumTypeCacheEntry, StringInfo datumString)
{
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
			DatumToBytes(tmp, lookup_type_cache(att->atttypid, 0), datumString);
		}
	}
}
