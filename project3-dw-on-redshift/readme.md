# Project: Data Warehouses on Redshift

# Schema for Song Play Analysis

Using the song and event datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

# Fact Table

1. __songplays__ - records in event data associated with song plays i.e. __records with page NextSong__
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

```sql
DISTSTYLE KEY
DISTKEY (song_id)
COMPOUND SORTKEY (start_time, song_id, user_id, session_id)
```

This table use the above distkey and sort key because:

- SORTKEY considerations:
    - It's often necessary to filter data by date and time, so sort by start_time will help to accelarate the queries that filter by date and time because redshift will be able to skip blocks that fall outside the time range
    - I choose COMPOUND because the data will be sorted in the same order that the sortkey columns and the table filter will be probably done by sortkey columns and the type INTERLEAVED isn't a good choose for columns like datetime and autoincrements id's
- DISTKEY consideration:
    - By start_time is not a good because. Performance and parallelism may decrease depending on the data period fetched. For example, data for an entire month can be in a single slice (cpu)
    - We can have just one distkey per table, and the best practices tells to us to choose based on how frequently it is joined and the size of the joining rows.
    - If i filter by date, the highest cardinality on the result set will be users. The ids of songs will be restricted. But we don't need to now the users name or something like that, we need to answer questions about songs and artists. So the most frequent join will be with songs.
    - Spotfy has about 70 million tracks (https://newsroom.spotify.com/company-info/), so we have a good cardinality to use this a distkey
    - Population is about 7.594 billion

# Dimension Tables

1. __users__ - users in the app
    - user_id, first_name, last_name, gender, level
2. __songs__ - songs in music database
    - song_id, title, artist_id, year, duration
3. __artists__ - artists in music database
    - artist_id, name, location, lattitude, longitude
4. __time__ - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday


# Project Datasets

You'll be working with two datasets that reside in S3. Here are the S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data
- Log data json path: s3://udacity-dend/log_json_path.json