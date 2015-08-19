# cms_topn
`cms_topn` is a count-min sketch top-n extension for PostgreSQL. This project creates a new Postgres type which is called cms_topn to implement the count-min sketch and additional structures as Postgres extension to provide some functionalities:

#### 1. Point Query
The point query is to get frequency estimate of an item in the count-min sketch(cms) structure.
	
#### 2. Top-N Query
This query is helpful to find the most frequent items in the count-min sketch(cms) structure.

#### 3. Union
Union is the process of merging more than one cms_topn structure for cumulative results of the point and top-n query.

# Count-Min Sketch and The Top-N implementation
The count-min sketch is a summary structure for the frequencies of events in a data stream. It is simply a matrix which has n rows and m columns. One independent hash function which maps an item to a number from 1 to m is required for each row. The matrix is updated for every item added with the help of these hash functions and the resulting table can be used for the queries.

The CM sketch structure is enough to calculate the frequency of a tuple. However we need more to find the top-n items. After updates, it is not possible to get the top-n tuples from sketch but we can update another structure which keeps the most frequent n items during the iteration. The additional structure is used to keep top n items at the end of the each iteration.

The CM sketch and the additional structure allows us to combine the separately computed results of subsets. After collecting sub-results, we can add the matrices and evaluate each different top-n candidate from the subsets again according to the total matrix. This gives a good approximation for the top-n of all data because each matrix also contains information about  the items in the corresponding subset even if the items is not included in the top-n for this subset of data.

In research part of this project, we noticed that [PipelineDB](https://github.com/pipelinedb/pipelinedb) already has a [count-min sketch implementation](https://github.com/pipelinedb/pipelinedb/blob/db70946eef8a781b93ebc270be86546f357a1286/src/backend/pipeline/cmsketch.c). We inspired from it and implemented a similar count-min sketch logic as an extension. Then we added top-n logic on the top of it.

#Usage
We provide user defined Postgres types and functions with the extension:

###Data Type
######cms_topn
User defined PostgreSQL type to keep the count-min sketch structure and the top-n list.

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
	
#Use Case Example
We made this example use case similar to [hll extension](https://github.com/aggregateknowledge/postgresql-hll) data warehouse use case example.

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
