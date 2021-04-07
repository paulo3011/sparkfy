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
    LAKE_DIM_ARTIST,
    LAKE_DIM_USER,
    LAKE_DIM_TIME,
    LAKE_DIM_SONGPLAY
    )
from udf.datetimeudf import _add_date_time_columns_to_df
from schemas import (
    song_src_schema,
    dim_song_schema,
    dim_artist_schema,
    log_src_schema,
    dim_time_schema,
    dim_user_schema,
    songplay_src_schema
    )
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, row_number

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
    print(song_df.show(1))

    # nPartitions = song_df.rdd.getNumPartitions()
    song_df = song_df.repartition(7)
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

    print(artists_table.show(1))

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

    log_df = _add_date_time_columns_to_df(log_df)
    log_df.createOrReplaceTempView("stage_events")
    songs_table.createOrReplaceTempView("dim_song")
    artists_table.createOrReplaceTempView("dim_artist")

    df = sparkSession.sql("""
        SELECT
            dim_song.song_id,
            dim_song.title as song_title,
            dim_song.year as song_year,
            dim_song.duration as song_duration,
            dim_artist.*,
            stage_events.ts as start_time,
            stage_events.userId as user_id,
            stage_events.firstName as first_name,
            stage_events.lastName as last_name,
            stage_events.gender,
            stage_events.level,
            stage_events.song,
            stage_events.artist,
            stage_events.event_date,
            stage_events.hour,
            stage_events.day,
            stage_events.week,
            stage_events.month,
            stage_events.year,
            stage_events.weekday,
            stage_events.page,
            stage_events.sessionId,
            stage_events.location,
            stage_events.userAgent
        FROM dim_song
        LEFT JOIN dim_artist ON (
            dim_artist.artist_id = dim_song.artist_id
            )
        RIGHT JOIN stage_events ON (
            dim_song.title = stage_events.song
            AND dim_artist.artist_name = stage_events.artist
        )
        WHERE stage_events.page='NextSong'
        ;""")

    df.cache()

    # stage_events total: 6820
    print("stage_events total: " + str(df.count()))
    print(df.show(1))

    # extract columns for users table
    users_table = df.select(dim_user_schema.names).dropDuplicates()
    print("users_table total: " + str(users_table.count()))
    print(users_table.show(1))

    compression = "snappy"

    # write users table to parquet files
    user_path = output_data + LAKE_DIM_USER
    users_table.write.parquet(
        path=user_path,
        compression=compression,
        mode="overwrite")

    # create timestamp column from original timestamp column
    dim_time_df = df.select(dim_time_schema.names).dropDuplicates()

    # dim time table: 6813
    print("dim time table: " + str(dim_time_df.count()))
    print(dim_time_df.show(1))

    # write time table to parquet files partitioned by year and month
    time_path = output_data + LAKE_DIM_TIME
    dim_time_df.write.parquet(
        path=time_path,
        partitionBy=["year", "month"],
        compression=compression,
        mode="overwrite")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.select(songplay_src_schema.names)
    print("songplays_table total: " + str(songplays_table.count()))
    print(songplays_table.show(1))

    # write songplays table to parquet files partitioned by year and month
    song_plays_path = output_data + LAKE_DIM_SONGPLAY
    songplays_table.write.parquet(
        path=song_plays_path,
        partitionBy=["year", "month"],
        compression=compression,
        mode="overwrite")


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
