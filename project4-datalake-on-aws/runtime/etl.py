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
    fact_songplays_schema,
    event_src_schema_to_dw_schema,
    artist_src_schema_to_dw_schema
    )
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.types import (IntegerType)

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
    # partitions needs to be in the format key=value, which is not that case.
    song_df = sparkSession.read.json(
        path=song_source,
        schema=song_src_schema,
        recursiveFileLookup=True)

    song_df.printSchema()

    # total rows: 14896
    print(song_df.show(1))

    # nPartitions = song_df.rdd.getNumPartitions()
    song_df = song_df.repartition(7)
    # nPartitions = song_df.rdd.getNumPartitions()

    # extract columns to create dim songs table
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
    song_df = artist_src_schema_to_dw_schema(song_df)
    artists_table = song_df.select(dim_artist_schema.names)

    # artists_table total before dropDuplicates: 14896
    print("artists_table total before dropDuplicates: " + str(artists_table.count()))
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


def process_log_data(sparkSession, input_data, output_data, dim_song, dim_artist):
    # get filepath to log data file
    log_data = input_data + LOG_DATA

    # read log data file
    event_df = sparkSession.read.json(
        path=log_data,
        schema=log_src_schema,
        recursiveFileLookup=True)

    # add datetime columns and rename columns to dw format
    event_df = _add_date_time_columns_to_df(event_df)
    event_df = event_src_schema_to_dw_schema(event_df)

    # create temp views
    event_df.createOrReplaceTempView("stage_events")
    dim_song.createOrReplaceTempView("dim_song")
    dim_artist.createOrReplaceTempView("dim_artist")

    # Create a dataframe with song plays data
    event_df = sparkSession.sql("""
        SELECT
            dim_song.song_id,
            dim_artist.artist_id,
            stage_events.*
        FROM dim_song
        LEFT JOIN dim_artist ON (
            dim_artist.artist_id = dim_song.artist_id
            )
        RIGHT JOIN stage_events ON (
            dim_song.title = stage_events.song
            AND dim_artist.name = stage_events.artist
        )
        WHERE stage_events.page='NextSong'
        ;""")

    event_df = event_df.withColumn("user_id", event_df["user_id"].cast(IntegerType()))
    event_df.cache()

    # extract columns for users table
    dim_user = event_df.select(dim_user_schema.names).dropDuplicates()

    compression = "snappy"

    # write users table to parquet files
    user_path = output_data + LAKE_DIM_USER
    dim_user.write.parquet(
        path=user_path,
        compression=compression,
        mode="overwrite")

    # create timestamp column from original timestamp column
    dim_time = event_df.select(dim_time_schema.names).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_path = output_data + LAKE_DIM_TIME
    dim_time.write.parquet(
        path=time_path,
        partitionBy=["year", "month"],
        compression=compression,
        mode="overwrite")

    # extract columns from joined song and log datasets to create songplays table
    fact_songplays = event_df.select(fact_songplays_schema.names)

    # write songplays table to parquet files partitioned by year and month
    song_plays_path = output_data + LAKE_DIM_SONGPLAY
    fact_songplays.write.parquet(
        path=song_plays_path,
        partitionBy=["year", "month"],
        compression=compression,
        mode="overwrite")

    # dim_user total: 104
    # stage_events total: 6820
    # fact_songplays total: 6820
    # print("fact_songplays total: " + str(fact_songplays.count()))
    # print(fact_songplays.show(1))


def main():
    spark = create_spark_session()
    # Hadoop’s “S3A” client offers high-performance IO against Amazon S3 object store and compatible implementations
    input_data = "s3a://moreira-ud/udacity-dend/"
    output_data = "s3a://moreira-ud/lake/"
    # input_data = "/home/paulo/projects/paulo3011/sparkfy/data/"
    # output_data = "/tmp/spark/sparkfy/lake/"

    songs_table, artists_table = process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data, songs_table, artists_table)


if __name__ == "__main__":
    main()
