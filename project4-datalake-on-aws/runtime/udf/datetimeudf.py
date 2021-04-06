from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import (TimestampType, ShortType)


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
