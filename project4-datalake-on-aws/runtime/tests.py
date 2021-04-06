# import os
import configparser
from datetime import datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from udf.datetimeudf import _add_date_time_columns_to_df
from schemas import (
    song_src_schema,
    dim_song_schema,
    dim_artist_schema,
    log_src_schema,
    dim_time_schema,
    dim_user_schema
    )
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

config = configparser.ConfigParser()
config.read('dl.cfg')

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


def right_join(sparkSession):
    events = [
        (1, "hello", "paulo", 1513720872284,"karen B.", "LN", "F", "free"),
        (1, "musica sem cadastro", "cantor nao cadastrado", 1614951030000, "karen B.", "LN", "F", "paid"),
        (2, "hello", "paulo", 1617734648000, "karter", "Smith", "M", "free"),
        (2, "hello", "paulo", 1617648248000, "karter", "Smith", "M", "free"),
        ]
    events_columns = ["user_id", "song", "artist", "ts", "first_name", "last_name", "gender", "level"]

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
            stage_events.ts as start_time,
            stage_events.user_id,
            stage_events.first_name,
            stage_events.last_name,
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
            stage_events.weekday
        FROM dim_song
        LEFT JOIN dim_artist ON (dim_artist.artist_id = dim_song.artist_id)
        RIGHT JOIN stage_events ON (
            dim_song.title = stage_events.song
            AND dim_artist.name = stage_events.artist
        );""")

    print("artists table:")
    print(artists_table.show())
    print("songs table:")
    print(songs_table.show())
    print(df.show())

    dim_time_df = df.select(dim_time_schema.names).dropDuplicates()
    print("dim time table:")
    print(dim_time_df.show())

    user_window = Window.partitionBy("user_id").orderBy(col("event_date").desc())
    user_stage_columns = dim_user_schema.names.copy()
    user_stage_columns.append("event_date")
    dim_user_df = df.select(user_stage_columns)

    dim_user_df = dim_user_df.withColumn('rank', row_number().over(user_window))

    print("users by rank:")
    print(dim_user_df.show())

    dim_user_df = dim_user_df.filter("rank=1") \
        .select(dim_user_schema.names) \

    print("users dim table:")
    print(dim_user_df.show())

    # print(df.explain(mode="formatted"))


def main():
    date = datetime.fromtimestamp(1513720872284 / 1000.0)
    test = date.strftime("%w")
    tt = date.timetuple()
    spark = create_spark_session()
    right_join(spark)


if __name__ == "__main__":
    main()
