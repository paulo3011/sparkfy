import configparser
# from datetime import datetime
import os
from settings import (
    S3_DESTINATION_REGION,
    S3_DESTINATION_AWS_ACCESS_KEY_ID,
    S3_DESTINATION_AWS_SECRET_ACCESS_KEY,
    SONG_DATA,
    LOG_DATA,
    LAKE_DIM_SONG,
    LAKE_DIM_ARTIST
    )
from schemas import (
    song_src_schema,
    dim_song_schema,
    dim_artist_schema,
    log_src_schema
    )
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (
    year,
    month,
    dayofmonth,
    hour,
    weekofyear,
    date_format
)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_REGION'] = S3_DESTINATION_REGION
os.environ['AWS_ACCESS_KEY_ID'] = S3_DESTINATION_AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY'] = S3_DESTINATION_AWS_SECRET_ACCESS_KEY

# Setup the Spark Process

conf = SparkConf() \
       .setAppName("UDACITY_ETL") \
       .set("fs.s3a.multipart.size", "104M") \
       .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.1") \
       .setMaster("local[*]")


def create_spark_session():
    """
    Create the entry point to programming Spark with the Dataset and DataFrame API.
    The entry point into all functionality in Spark is the SparkSession class.
    Instead of having a spark context, hive context, SQL context, now all of it is encapsulated in a Spark session.
    Seealso: http://spark.apache.org/docs/3.1.1/api/python/reference/pyspark.sql.html#spark-session-apis
    """
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    return spark


def _to_lowercase(input):
    input["title"] = input["title"].lower()
    return input


def process_song_data(sparkSession, input_data, output_data):

    song_source = input_data + SONG_DATA

    # read song data file using dataframe api
    # All built-in file sources (including Text/CSV/JSON/ORC/Parquet) are able
    # to discover and infer partitioning information automatically, but the
    # paritions needs to be in the format key=value, which is not that case.
    song_df = sparkSession.read.json(
        path=song_source,
        schema=song_src_schema,
        recursiveFileLookup=True)

    song_df.printSchema()

    # does not make efect in song_df
    # song_df = song_df.dropDuplicates()

    # total rows: 14896
    print(song_df.take(1))

    # nPartitions = song_df.rdd.getNumPartitions()
    song_df = song_df.repartition(8)
    # nPartitions = song_df.rdd.getNumPartitions()

    # extract columns to create songs table
    songs_table = song_df.select(dim_song_schema.names)

    # write songs table to parquet files partitioned by year and artist
    # http://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.parquet.html#pyspark.sql.DataFrameWriter.parquet
    compression = "snappy"
    song_path = output_data + LAKE_DIM_SONG
    songs_table.write.parquet(
        path=song_path,
        partitionBy=["year", "artist_id"],
        compression=compression,
        mode="overwrite")

    # extract columns to create artists table
    artists_table = song_df.select(dim_artist_schema.names)

    print(artists_table.take(1))

    # remove duplicates resulting in 10025 rows
    artists_table = artists_table.dropDuplicates()

    # write artists table to parquet files
    artists_path = output_data + LAKE_DIM_ARTIST
    artists_table.write.parquet(
        path=artists_path,
        compression=compression,
        # this will clear previous files before write the new ones
        mode="overwrite"
    )

    return (broadcast(songs_table), broadcast(artists_table))


def process_log_data(sparkSession, input_data, output_data, songs_table, artists_table):
    # get filepath to log data file
    log_data = input_data + LOG_DATA

    # read log data file
    log_df = sparkSession.read.json(
        path=log_data,
        schema=log_src_schema,
        recursiveFileLookup=True)

    log_df.createOrReplaceTempView("stage_events")
    songs_table.createOrReplaceTempView("dim_song")
    artists_table.createOrReplaceTempView("dim_artist")
    df = sparkSession.sql("""
        SELECT * FROM dim_song ON (dim_song)
        LEFT JOIN dim_artist ON (dim_artist.artist_id = dim_song.artist_id)
        RIGHT JOIN stage_events ON (
            dim_song.title = BTRIM(stage_events.song)
            AND dim_artist.name = BTRIM(stage_events.artist)
        )
    """)

    print(df.take(1))

    print(log_df.take(1))

    # filter by actions for song plays
    # records in log data associated with song plays (page NextSong)
    song_plays_df = log_df.filter("page='NextSong'")

    print(song_plays_df.take(1))

    # todo: add song_id and artist_id to songplay events
    # join tables to log_df, dt são imutáveis
    # fazer um test de sql com joing pra ver se funciona
    # https://review.udacity.com/#!/rubrics/2502/view

    # extract columns for users table
    users_table = "todo"

    # write users table to parquet files
    users_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = "todo"

    # create datetime column from original timestamp column
    get_datetime = udf()
    df = "todo"

    # extract columns to create time table
    time_table = "todo"

    # write time table to parquet files partitioned by year and month
    time_table = "todo"

    # read in song data to use for songplays table
    song_df = "todo"

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = "todo"

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    # input_data = "s3a://moreira-ud/udacity-dend/"
    input_data = "/home/paulo/projects/paulo3011/sparkfy/data/"
    # s3a://moreira-ud/lake/
    output_data = "/tmp/spark/sparkfy/lake/"

    songs_table, artists_table = process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data, songs_table, artists_table)


if __name__ == "__main__":
    main()
