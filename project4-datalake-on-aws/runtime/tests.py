# import os
import configparser
from datetime import datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from udf.datetimeudf import _add_date_time_columns_to_df

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
