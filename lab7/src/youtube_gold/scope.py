from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TRENDING_DATE_FORMAT = "yy.dd.MM"

def parse_trending_date(df: DataFrame) -> DataFrame:
    return df.withColumn("trending_dt", F.to_date("trending_date", TRENDING_DATE_FORMAT))

def filter_to_period(df: DataFrame, start: str, end: str) -> DataFrame:
    return df.filter(F.col("trending_dt").between(start, end))

def scope_silver(df: DataFrame, start: str, end: str) -> DataFrame:
    return filter_to_period(parse_trending_date(df), start, end)