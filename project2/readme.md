# Project: Data Modeling with Cassandra

## Project Description

Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

Create queries to ask the following three questions of the data

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# Key points about cassandra

NoSQL = Not only SQL.

- Think about your queries first (denormalization is required, theres no join in apache cassandra)

## Data Modeling in Apache Cassandra:

- Denormalization is not just okay -- it's a must (it's required)
- Denormalization must be done for fast reads
- __Apache Cassandra has been optimized for fast writes__
- ALWAYS think Queries first and focus needs to be on the where clause
- __One table per query is a great strategy__
- __Apache Cassandra does not allow: JOINs, GROUP BY or subqueries__
- CQL (Cassandra query language) is the way to interact with the database
- Primary key is used to partition and sort your data. Primary key is made up of partition key (first element) and clustering columns. PRIMARY KEY ([PARTITION KEY],[CLUSTERING COLUMNS]). __The clustering columns will determine the sort order within a partition__.
- Where clause is required in all queries for perfomance reason. __The primary key must be included in your query and any clustering columns can be used in the order they appear in your primary key__ (All columns used in where clause needs to be in primary key (partition or clustering keys)
- There are no duplicates in Apache Cassandra, so the primary key must be unique.


I see certain downsides of this approach, since in a production application, requirements change quickly and I may need to improve my queries later. Isn't that a downside of Apache Cassandra?

In Apache Cassandra, you want to model your data to your queries, and if your business need calls for quickly changing requirements, you need to create a new table to process the data. That is a requirement of Apache Cassandra. If your business needs calls for ad-hoc queries, these are not a strength of Apache Cassandra. However keep in mind that it is easy to create a new table that will fit your new query.

### Primary keys:

- __The query that you need to do will determine the primary key__
- __Must be unique__
- The PRIMARY KEY is made up of either just the PARTITION KEY or may also include additional CLUSTERING COLUMNS
- A Simple PRIMARY KEY is just one column that is also the PARTITION KEY. A Composite PRIMARY KEY is made up of more than one column and will assist in creating a unique value and in your retrieval queries
- The PARTITION KEY will determine the distribution of data across the system (in what node the data will live -node1, node2 and so on). __Essentially, we want to spread the data equally across our nodes__.

### Clustering keys:

- __The clustering column will sort the data in sorted ascending order (by default)__, e.g., alphabetical order.
- __You cannot use the clustering columns out of order in the SELECT statement__
- More than one clustering column can be added (or none!)
- From there the clustering columns will sort in order of how they were added to the primary key
- You may choose to omit using a clustering column in your SELECT statement. That's OK. Just remember to use them in order when you are using the SELECT statement.

### Where clause:

- Data Modeling in Apache Cassandra is query focused, and that focus needs to be on the WHERE clause
- Failure to include a WHERE clause will result in an error
- AVOID using "ALLOW FILTERING": [why you should not use it](https://www.datastax.com/blog/allow-filtering-explained)
- The WHERE statement is allowing us to do the fast reads. With Apache Cassandra, we are talking about big data -- think terabytes of data -- so we are making it fast for read purposes. Data is spread across all the nodes. By using the WHERE statement, we know which node to go to, from which node to get that data and serve it back. For example, imagine we have 10 years of data on 10 nodes or servers. So 1 year's data is on a separate node. By using the WHERE year = 1 statement we know which node to visit fast to pull the data from.


### Create statement good practice

We should create the tables with keys and clustering keys organized in this way:

```sql
CREATE TABLE mytable (
   key int,
   col_1 int,
   col_2 int,
   col_3 int,
   col_4 int,
-- ...
   PRIMARY KEY ((key), col_1, col_2, col_3, col_4));
```
Tip: A well-designed table uses clustering columns to allow a query to return ranges of data

Because the database uses the clustering columns to determine the location of the data on the partition, you must identify the higher level clustering columns definitively using the equals (=) or IN operators. In a query, you can only restrict the lowest level using the range operators (>, >=, <, or <=).

__To avoid full scans of the partition__ and to make queries more efficient, the database requires that the higher level columns in the sort order (col_1, col_2, and col_3) are identified using the equals or IN operators. Ranges are allowed on the last column (col_4).

seealso: https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/whereClustering.html

### Questions

In apache cassandra, every column that i want to filter needs to be in partition key?

Since Cassandra uses partition key to determine the node where the data exists, you should always filter on the partition key first. In the above example, filtering on ```user_id``` and ```session_id``` is definitely a must.

You can further filter on the other clustering columns, but the order in which you filter should be same as how the clustering key is setup. For example, the following filters are valid:

user_id, session_id
user_id, session_id, item_in_session
user_id, session_id, item_in_session, first_name
user_id, session_id, item_in_session, first_name, last_name
However, the following filters are invalid:

user_id, session_id, first_name
user_id, session_id, last_name
user_id, session_id, item_in_session, last_name
Hope this helps!

PS: In extreme situations, you can query by using any of the filters but it will be highly inefficient and in worst cases break the system. Have a look at [Allow Clustering](https://www.datastax.com/blog/allow-filtering-explained) to learn more about it

[Ghanshyam Y](https://knowledge.udacity.com/questions/443427)

# ReferenceS:

- https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/a7801de4-ee3f-4531-b887-82dea67f47a6/lessons/73fd6e35-3319-4520-94b5-9651437235d7/concepts/65cecac3-8728-4fe1-b06e-3950c123185b

### Sample queries

```sql
CREATE TABLE IF NOT EXISTS artist_history (
    user_id int,
    session_id int,
    artist text,
    song text,
    item_in_session int,
    first_name text,
    last_name text,
    PRIMARY KEY ((user_id,session_id),item_in_session, first_name, last_name)
);

select item_in_session, artist, song from artist_history
where song='Kilometer'
order by item_in_session, first_name, last_name

/* output:

Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING
*/

select item_in_session, artist, song  from artist_history
where user_id=10 and session_id=182 and last_name='Cruz'

/*
Error from server: code=2200 [Invalid query] message="PRIMARY KEY column "last_name" cannot be restricted as preceding column "item_in_session" is not restricted"
*/

select item_in_session, artist, song  from artist_history
where user_id=10 and session_id=182 and first_name='Cruz'

/*
Error from server: code=2200 [Invalid query] message="PRIMARY KEY column "first_name" cannot be restricted as preceding column "item_in_session" is not restricted"
*/

select item_in_session, artist, song from artist_history
where user_id=10 and session_id=182 and item_in_session=1

/*
1 Three Drives Greece 2000
*/

select item_in_session, artist, song from artist_history
where user_id=10 and session_id=182 and item_in_session=1 and first_name='Sylvie'

/*
1 Three Drives Greece 2000
*/

select item_in_session, artist, song  from artist_history
where user_id=10 and session_id=182 and item_in_session=1 and first_name='Sylvie' and last_name='Cruz'

/*
1 Three Drives Greece 2000
*/

-- Note that all primary key columns were filter but the order in where clause does not matter, since all primary key columns were filtered.

select item_in_session, artist, song  from artist_history
where session_id=182 and user_id=10 and last_name='Cruz' and first_name='Sylvie' and item_in_session = 1

/*
1 Three Drives Greece 2000
*/

select item_in_session, artist, song  from artist_history
where user_id=10 and session_id=182 and item_in_session > 1 and first_name='Sylvie' and last_name='Cruz'

-- Note that only the last clustering column can use diferent operators that "="

/*
Error from server: code=2200 [Invalid query] message="Clustering column "first_name" cannot be restricted (preceding column "item_in_session" is restricted by a non-EQ relation)"
*/

select item_in_session, artist, song from artist_history
where user_id=10

-- Note that the where clause needs to include all partition columns.

/*
Error from server: code=2200 [Invalid query] message="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
*/

select item_in_session, artist, song  from artist_history
where user_id=10 and session_id=182

/*
0 Down To The Bone Keep On Keepin' On
1 Three Drives Greece 2000
2 Sebastien Tellier Kilometer
3 Lonnie Gordon Catch You Baby (Steve Pitron & Max Sanna Radio Edit)
*/
```

# Checklist

ETL Pipeline Processing

- [ok] Creates event_data_new.csv file.
- [ok] Uses the appropriate datatype within the CREATE statement, for each Cassandra CREATE statement.

Data Modeling

- [ok] Creates the correct Apache Cassandra tables for each of the three queries. The CREATE TABLE statement should include the appropriate table.
- [ok] Demonstrates good understanding of data modeling by generating correct SELECT statements to generate the result being asked for in the question. The SELECT statement should NOT use ALLOW FILTERING to generate the results.
- [ok] should use table names that reflect the query and the result it will generate. Table names should include alphanumeric characters and underscores, and table names must start with a letter.
- [ok] Student has given careful thought to how the data is modeled in the table and the sequence and order in which data is partitioned, inserted and retrieved from the table.

PRIMARY KEYS

- [ok] The combination of the PARTITION KEY alone or with the addition of CLUSTERING COLUMNS should be used appropriately to uniquely identify each row.

Presentation

- [ok] The notebooks should include a description of the query the data is modeled after.
- [ok] Code should be organized well into the different queries. Any in-line comments that were clearly part of the project instructions should be removed so the notebook provides a professional look.
