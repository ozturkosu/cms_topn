# cms_topn
The project creates a new Postgres type which is called cms_topn to implement the count-min sketch and additional structures as Postgres extension to provide some functionalities:
	
#### 1. Point Query
The point query is to get frequency estimation of an item in the stream with the help of count-min sketch(cms) structure.
	
#### 2. Top n Query
This query is helpful to find the most frequent items in the stream.
	
#### 3. Union
Union is the process of merging more than one cms_topn structure for cumulative results of the point and top n query.
	
# Count-Min Sketch and The Top n implementation
The count-min sketch is a summary structure for the frequencies of events in a data stream. It is simply a matrix which has n rows and m columns. In addition, one hash function which maps an item to a number from 1 to m is required for each row. The matrix is updated for each item in the stream with the help of these hash functions and the resulting table can be used for the queries.
	
The CM sketch structure is enough to calculate the frequency of a tuple, however we need more to find the top n items. After updates, it is not possible to get the top n tuples from values but we can update another structure which keeps the most frequent n items during the iteration. The additional structure keeps the top n at the end of the iteration.
	
In addition, the CM sketch and the additional structure allows us to combine the separately computed results of subsets. After collecting sub-results, we can add the matrices and evaluate each different top n candidate from the subsets again according to the total matrix. This gives a good approximation for the top n of all data because each matrix also contains information about  the items in the corresponding subset even if the items is not included in the top n for this subset of data.

#Usage
We provide user defined Postgres types and functions with the extension:

###Data Type
######cms_topn
User defined PostgreSQL type to keep the count min sketch structure and the top n list.

##Function to create empty cms_topn structure
######cms_topn(any type, integer n, double precision epsilon default 0.001, double precision p default 0.99)
This creates empty cms_topn type for the given type(integer, text etc.). It has parameters for the precision and top-n count. Second parameter specifies top-n count. Third parameter specifies error bound for the approximation of the frequencies and the fourth one specifies 
confidence of the error bound. Such as these default values give us an error bound of 0.1% with a confidence of 99%.

##Functions to insert items
######cms_topn_add(cms_topn, value) 
Adds the given item to the given cms_topn structure.

######cms_topn_add_agg(value,  integer n, double precision epsilon default 0.001, double precision p default 0.99)
This is the aggregate add function. It creates an empty cms_topn with given parameters and inserts series of item from given column to create aggregate summary of these items.

##Functions to get results
######topn(cms_topn, value)
Gives the top n elements which have the same type with the second parameter and their frequencies as set of rows.

######cms_topn_frequency(cms_topn, value)
Gives frequency estimation of an item.

######cms_topn_info(cms_topn)
Gives some information about size of cms_topn structure.

##Functions to combine different summaries
######cms_topn_union(cms_topn, cms_topn)
Creates new structure by combining two count min sketch structures and evaluating their frequencies again.

######cms_topn_union_agg(value)
This is the aggregate for union operation.

#Build
Once you have PostgreSQL, you're ready to build cms_topn. For this, you will need to include the pg_config directory path in your make command. This path is typically the same as your PostgreSQL installation's bin/ directory path. For example:

	PATH=/usr/local/pgsql/bin/:$PATH make
	sudo PATH=/usr/local/pgsql/bin/:$PATH make install
	
#Use Case
We made this example use case similar to hll extension readme.

Let's assume I've got a fact table that records users' visits to my site, what they did, and where they came from. It's got hundreds of millions of rows. Table scans take minutes (or at least lots and lots of seconds.)

```sql
CREATE TABLE facts (
   date                date,
   user_id           integer,
   activity_type   smallint,
   referrer           varchar(255)
);
```

I'd like to get the ten users are visiting the website most frequently for each day. Let's set up an aggregate table:
```sql
--Create Extension
CREATE EXTENSION cms_topn;
```

```sql
-- Create the table
CREATE TABLE daily_hits (
   date            date UNIQUE,
   users          cms_topn
);
```

```sql
-- Fill it with the aggregated summaries
INSERT INTO daily_hits(date, users)
   SELECT 
       date, 
       cms_topn_add_agg(user_id, 10)
   FROM 
       facts
   GROUP BY
       date;
```

We're inserting users into one cms_topn and keeping the top 10 users per day. Now we can ask for the top 10 users for each day:

```sql
SELECT 
     date, 
     topn(users)
FROM 
     daily_hits;
```

What if you wanted to this week's top-10 users?

```sql
SELECT
     topn(cms_topn_union_agg(users))
FROM
     daily_hits 
WHERE
     date >= '2012-01-02'::date AND 
     date <= '2012-01-08'::date;
```

On same week for every date, get frequency of user with id 1234.
```sql
SELECT
     date,
     cms_topn_frequency(users, 1234)
FROM
     daily_hits 
WHERE
     date >= '2012-01-02'::date AND 
     date <= '2012-01-08'::date;
```
