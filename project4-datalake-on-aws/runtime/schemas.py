from pyspark.sql.types import (
    StructType,
    StringType,
    ShortType,
    IntegerType,
    LongType,
    DoubleType,
    DecimalType)

# http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html?highlight=read%20csv#pyspark.sql.DataFrameReader
# https://spark.apache.org/docs/latest/sql-ref-datatypes.html

# song source schema
song_src_schema = StructType()
song_src_schema.add("num_songs", IntegerType(), True)
song_src_schema.add("artist_id", StringType(), True)
song_src_schema.add("artist_latitude", DoubleType(), True)
song_src_schema.add("artist_longitude", DoubleType(), True)
song_src_schema.add("artist_location", StringType(), True)
song_src_schema.add("artist_name", StringType(), True)
song_src_schema.add("song_id", StringType(), True)
song_src_schema.add("title", StringType(), True)
song_src_schema.add("duration", DecimalType(), True)
song_src_schema.add("year", IntegerType(), True)


def song_src_schema_to_dw_schema(df):
    """
    Rename the song src columns to dimensional table format.
    dim_song columns: song_id, title, artist_id, year, duration.
    """
    return df


# log data source schema
log_src_schema = StructType()
log_src_schema.add("artist", StringType(), True)
log_src_schema.add("auth", StringType(), True)
log_src_schema.add("firstName", StringType(), True)
log_src_schema.add("gender", StringType(), True)
log_src_schema.add("itemInSession", IntegerType(), True)
log_src_schema.add("lastName", StringType(), True)
log_src_schema.add("length", DecimalType(), True)
log_src_schema.add("level", StringType(), True)
log_src_schema.add("location", StringType(), True)
log_src_schema.add("method", StringType(), True)
log_src_schema.add("page", StringType(), True)
log_src_schema.add("registration", LongType(), True)
log_src_schema.add("sessionId", IntegerType(), True)
log_src_schema.add("song", StringType(), True) # evaluate if rename to title or not
log_src_schema.add("status", ShortType(), True)
log_src_schema.add("ts", LongType(), True)
log_src_schema.add("userAgent", StringType(), True)
log_src_schema.add("userId", IntegerType(), True)


def event_src_schema_to_dw_schema(df):
    """
    Rename the event src columns to dimensional and fact tables format.
    fact_songplays columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    """
    df = df.withColumnRenamed("ts", "start_time")
    df = df.withColumnRenamed("userId", "user_id")
    df = df.withColumnRenamed("userAgent", "user_agent")
    df = df.withColumnRenamed("sessionId", "session_id")
    df = df.withColumnRenamed("lastName", "last_name")
    df = df.withColumnRenamed("firstName", "first_name")

    return df


# songplay source schema

fact_songplays_schema = StructType()
fact_songplays_schema.add("start_time", LongType(), True)
fact_songplays_schema.add("user_id", IntegerType(), True)  # todo refactory
fact_songplays_schema.add("level", StringType(), True)
fact_songplays_schema.add("song_id", StringType(), True)
fact_songplays_schema.add("artist_id", StringType(), True)
fact_songplays_schema.add("session_id", IntegerType(), True)
fact_songplays_schema.add("location", StringType(), True)
fact_songplays_schema.add("user_agent", StringType(), True)
fact_songplays_schema.add("month", ShortType(), False)
fact_songplays_schema.add("year", ShortType(), False)


# Dim song schema

dim_song_schema = StructType()
dim_song_schema.add("song_id", StringType(), False)
dim_song_schema.add("title", StringType(), False)
dim_song_schema.add("artist_id", StringType(), True)
dim_song_schema.add("year", ShortType(), True)
dim_song_schema.add("duration", DecimalType(), True)

# Dim artist schema
dim_artist_schema = StructType()
dim_artist_schema.add("artist_id", StringType(), False)
dim_artist_schema.add("name", StringType(), False)
dim_artist_schema.add("location", StringType(), True)
dim_artist_schema.add("latitude", DoubleType(), True)
dim_artist_schema.add("longitude", DoubleType(), True)


def artist_src_schema_to_dw_schema(df):
    """
    Rename the event src columns to dimensional and fact tables format.
    dim_artist columns: artist_id, name, location, latitude, longitude
    """
    df = df.withColumnRenamed("artist_name", "name")
    df = df.withColumnRenamed("artist_location", "location")
    df = df.withColumnRenamed("artist_latitude", "latitude")
    df = df.withColumnRenamed("artist_longitude", "longitude")

    return df

# Dim time schema
dim_time_schema = StructType()
dim_time_schema.add("start_time", LongType(), False)
dim_time_schema.add("hour", ShortType(), False)
dim_time_schema.add("day", ShortType(), False)
dim_time_schema.add("week", ShortType(), False)
dim_time_schema.add("month", ShortType(), False)
dim_time_schema.add("year", ShortType(), False)
dim_time_schema.add("weekday", ShortType(), False)

# Dim user schema
dim_user_schema = StructType()
dim_user_schema.add("user_id", IntegerType(), False)
dim_user_schema.add("first_name", StringType(), True)
dim_user_schema.add("last_name", StringType(), True)
dim_user_schema.add("gender", StringType(), True)
dim_user_schema.add("level", StringType(), True)
