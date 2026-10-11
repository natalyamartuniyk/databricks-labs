from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

def dedup_latest_per_day(df: DataFrame) -> DataFrame:
    latest_per_day = Window.partitionBy("video_id", "trending_dt").orderBy(
     F.col("_silver_processed_at").desc()
    ) 

    return (
        df.withColumn("rn", F.row_number().over(latest_per_day))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

def lookup_channel_key(df: DataFrame, dim_channel: DataFrame) -> DataFrame:
    return df.join(
            dim_channel.select("channel_key", "channel_title"),
            on="channel_title",
            how="left",
    ) 

def add_views_delta(df: DataFrame) -> DataFrame:
    video_history = Window.partitionBy("video_id").orderBy("trending_dt")
    return df.withColumn("views_delta", F.col("views") - F.lag("views").over(video_history))

def add_trending_day_num(df: DataFrame) -> DataFrame:
    video_history = Window.partitionBy("video_id").orderBy("trending_dt")
    return df.withColumn("trending_day_num", F.row_number().over(video_history))

def to_fact_schema(df: DataFrame) -> DataFrame:
    return df.select(
        F.date_format("trending_dt", "yyyyMMdd").cast("int").alias("date_key"),
        "video_id",
        "channel_key",
        F.col("category_id").cast("int").alias("category_id"),
        "views",
        "likes",
        "dislikes",
        "comment_count",
        "views_delta",
        "trending_day_num",
    )

def build_fact_source(scoped_silver: DataFrame, dim_channel: DataFrame) -> DataFrame:
        df = dedup_latest_per_day(scoped_silver)
        df = lookup_channel_key(df, dim_channel)
        df = add_views_delta(df)
        df = add_trending_day_num(df)
        return to_fact_schema(df)

def find_missing_channel_keys(fact_df: DataFrame) -> DataFrame:
    return fact_df.filter(F.col("channel_key").isNull())


def find_orphans(fact_df: DataFrame, dim_df: DataFrame, key: str) -> DataFrame:
    return fact_df.join(dim_df.select(key), on=key, how="left_anti")


