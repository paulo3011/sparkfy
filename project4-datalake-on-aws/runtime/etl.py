import configparser
from datetime import datetime
import os
from settings import *
from schemas import *
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
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

# https://docs.qubole.com/en/latest/user-guide/engines/spark/spark-supportability.html
# http://hortonworks.com/wp-content/uploads/2016/03/asparagus-chart-hdp24.png

# was used org.apache.hadoop:hadoop-aws:3.1.1 for pyspark 3.1.1
# https://spark.apache.org/docs/latest/api/python/getting_started/install.html#dependencies
# https://spark.apache.org/downloads.html

# https://stackoverflow.com/questions/60172792/reading-data-from-s3-using-pyspark-throws-java-lang-numberformatexception-for-i
# https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/bk_cloud-data-access/content/s3a-fast-upload-config.html

conf = SparkConf() \
       .setAppName("UDACITY_ETL") \
       .set("fs.s3a.multipart.size", 104857600) \
       .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.1.1") \
       .setMaster("local[*]")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    # https://codelovingyogi.medium.com/pyspark-connect-to-aws-s3a-filesystem-82bee54e0812
    # spark._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", S3_DESTINATION_AWS_ACCESS_KEY_ID)
    # spark._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", S3_DESTINATION_AWS_SECRET_ACCESS_KEY)
    # spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
    # spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    # spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "us-east-1.amazonaws.com")

    return spark


def process_song_data(spark, input_data, output_data):

    test_file = SONG_DATA + "/A/Y/L/TRAYLWV128F92FBA1D.json"

    # read song data file
    # http://spark.apache.org/docs/3.1.1/api/python/reference/index.html
    df = spark.read.json(path=test_file, schema=song_src_schema)

    print(df.take(1))
    df.printSchema()

    # extract columns to create songs table
    dim_song_columns = dim_song_schema
    songs_table = df.select(dim_song_schema.names)
    print(songs_table.take(1))

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(path=LAKE_DIM_SONG,partitionBy=["year","artist_id"],compression="snappy")

    # extract columns to create artists table
    # todo: check sql from artist_table_insert
    artists_table = "todo"

    # write artists table to parquet files
    artists_table


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = LOG_DATA

    # read log data file
    df = "todo"

    # filter by actions for song plays
    df = "todo"

    # extract columns for users table
    artists_table = "todo"

    # write users table to parquet files
    artists_table

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
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
