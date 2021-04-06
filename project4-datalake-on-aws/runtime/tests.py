import configparser
from datetime import datetime
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
from pyspark.sql.types import (TimestampType, IntegerType, ShortType)

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


@udf(returnType=TimestampType())
def timestamp_to_date(timestamp):
    return datetime.fromtimestamp(timestamp / 1000.0)


@udf(returnType=ShortType())
def timestamp_to_weekday(timestamp):
    """
    Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
    """
    return int(datetime.fromtimestamp(timestamp / 1000.0).strftime("%w"))


@udf(returnType=ShortType())
def timestamp_to_week_of_year(timestamp):
    """
    Week number of the year (Sunday as the first day of the week)
    as a zero padded decimal number.
    All days in a new year preceding the first Sunday are considered
    to be in week 0. (00, 01, …, 53)
    """
    return int(datetime.fromtimestamp(timestamp / 1000.0).strftime("%U"))


@udf(returnType=ShortType())
def timestamp_to_year(timestamp):
    """
    Year with century as a decimal number. (0001, 0002, …, 2013, 2014, …, 9998, 9999)
    """
    return int(datetime.fromtimestamp(timestamp / 1000.0).strftime("%Y"))


@udf(returnType=ShortType())
def timestamp_to_month(timestamp):
    """
    Month as a zero-padded decimal number (01, 02, …, 12)
    """
    return int(datetime.fromtimestamp(timestamp / 1000.0).strftime("%m"))


@udf(returnType=ShortType())
def timestamp_to_day(timestamp):
    """
    Day of the month as a zero-padded decimal number (1, 02, …, 31)
    """
    return int(datetime.fromtimestamp(timestamp / 1000.0).strftime("%d"))


@udf(returnType=ShortType())
def timestamp_to_hour(timestamp):
    """
    Hour (24-hour clock) as a zero-padded decimal number.
    """
    return int(datetime.fromtimestamp(timestamp / 1000.0).strftime("%H"))


def _add_date_time_columns_to_df(df, timestamp_column="ts"):
    df = df.withColumn("event_date", timestamp_to_date(timestamp_column))
    df = df.withColumn("hour", timestamp_to_hour(timestamp_column))
    df = df.withColumn("day", timestamp_to_day(timestamp_column))
    df = df.withColumn("week", timestamp_to_week_of_year(timestamp_column))
    df = df.withColumn("month", timestamp_to_month(timestamp_column))
    df = df.withColumn("year", timestamp_to_year(timestamp_column))
    df = df.withColumn("weekday", timestamp_to_weekday(timestamp_column))
    return df


def right_join(sparkSession):
    events = [(1, "hello", "paulo", 1513720872284), (1, "musica sem cadastro", "cantor nao cadastrado", 1541110994796)]
    events_columns = ["userid", "song", "artist", "ts"]
    song = [(8, "hello", 99), (10, "musica nova", 98)]
    song_columns = ["song_id", "title", "artist_id"]
    artists = [(99, "paulo"), (98, "madona")]
    artists_columns = ["artist_id", "name"]

    log_df = sparkSession.createDataFrame(events, events_columns)
    log_df = _add_date_time_columns_to_df(log_df)
    log_df.createOrReplaceTempView("stage_events")

    songs_table = sparkSession.createDataFrame(song, song_columns)
    songs_table.createOrReplaceTempView("dim_song")

    artists_table = sparkSession.createDataFrame(artists, artists_columns)
    artists_table.createOrReplaceTempView("dim_artist")

    df = sparkSession.sql("""
        SELECT
            'dim_song=>' as dim_song,
            dim_song.*,
            'dim_artist=>' as dim_artist,
            dim_artist.*,
            'stage_events=>' as stage_events,
            stage_events.*
        FROM dim_song
        LEFT JOIN dim_artist ON (dim_artist.artist_id = dim_song.artist_id)
        RIGHT JOIN stage_events ON (
            dim_song.title = stage_events.song
            AND dim_artist.name = stage_events.artist
        );""")

    print(artists_table.show())
    print(songs_table.show())
    print(df.show())
    # print(df.explain(mode="formatted"))


def main():
    date = datetime.fromtimestamp(1513720872284 / 1000.0)
    test = date.strftime("%w")
    tt = date.timetuple()
    spark = create_spark_session()
    right_join(spark)


if __name__ == "__main__":
    main()
