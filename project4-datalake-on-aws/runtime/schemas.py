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
log_src_schema.add("song", StringType(), True)
log_src_schema.add("status", ShortType(), True)
log_src_schema.add("ts", LongType(), True)
log_src_schema.add("userAgent", StringType(), True)
log_src_schema.add("userId", IntegerType(), True)

songplay_src_schema = StructType()
songplay_src_schema.add("start_time", LongType(), True)
songplay_src_schema.add("user_id", IntegerType(), True) # todo refactory
songplay_src_schema.add("level", StringType(), True)
songplay_src_schema.add("song_id", IntegerType(), True)
songplay_src_schema.add("artist_id", IntegerType(), True)
songplay_src_schema.add("sessionId", IntegerType(), True)
songplay_src_schema.add("location", StringType(), True)
songplay_src_schema.add("userAgent", StringType(), True)
songplay_src_schema.add("month", ShortType(), False)
songplay_src_schema.add("year", ShortType(), False)


dim_song_schema = StructType()
dim_song_schema.add("song_id", IntegerType(), False)
dim_song_schema.add("title", StringType(), False)
dim_song_schema.add("artist_id", IntegerType(), True)
dim_song_schema.add("year", IntegerType(), True)
dim_song_schema.add("duration", DecimalType(), True)

dim_artist_schema = StructType()
dim_artist_schema.add("artist_id", IntegerType(), False)
dim_artist_schema.add("artist_name", StringType(), False)
dim_artist_schema.add("artist_location", StringType(), True)
dim_artist_schema.add("artist_latitude", DoubleType(), True)
dim_artist_schema.add("artist_longitude", DoubleType(), True)

dim_time_schema = StructType()
dim_time_schema.add("start_time", LongType(), False)
dim_time_schema.add("hour", ShortType(), False)
dim_time_schema.add("day", ShortType(), False)
dim_time_schema.add("week", ShortType(), False)
dim_time_schema.add("month", ShortType(), False)
dim_time_schema.add("year", ShortType(), False)
dim_time_schema.add("weekday", ShortType(), False)

dim_user_schema = StructType()
dim_user_schema.add("user_id", IntegerType(), False)
dim_user_schema.add("first_name", StringType(), True)
dim_user_schema.add("last_name", StringType(), True)
dim_user_schema.add("gender", StringType(), True)
dim_user_schema.add("level", StringType(), True)
