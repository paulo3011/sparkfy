from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
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


# dim tables
dim_song_schema = StructType()
dim_song_schema.add("song_id", StringType(), True)
dim_song_schema.add("title", StringType(), True)
dim_song_schema.add("artist_id", StringType(), True)
dim_song_schema.add("year", IntegerType(), True)
dim_song_schema.add("duration", DecimalType(), True)
