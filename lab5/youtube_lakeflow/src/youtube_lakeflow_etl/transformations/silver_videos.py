from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, coalesce, lit, current_timestamp

SILVER_SCHEMA = spark.conf.get("youtube.silver_schema")

VIDEO_QUALITY_RULES = {
    "valid_views": "views >= 0",
    "valid_likes": "likes >= 0",
    "valid_dislikes": "dislikes >= 0",
    "valid_comment_count": "comment_count >= 0",
}

ALL_RULES_SQL = " AND ".join(VIDEO_QUALITY_RULES.values())


def _typed_videos_stream():
    return (
        spark.readStream.table("youtube_videos_files_bronze")
        .withColumn("views", col("views").cast("long"))
        .withColumn("likes", col("likes").cast("long"))
        .withColumn("dislikes", col("dislikes").cast("long"))
        .withColumn("comment_count", col("comment_count").cast("long"))
        .withColumn("title", trim(col("title")))
        .withColumn("channel_title", trim(col("channel_title")))
        .withColumn("description", coalesce(trim(col("description")), lit("No description")))
    )


@dp.table(
    name=f"{SILVER_SCHEMA}.youtube_videos_ldp_silver",
    comment="Cleaned YouTube video metadata that passed all quality expectations",
    table_properties={"pipelines.reset.allowed": "false"},
)
@dp.expect_all_or_drop(VIDEO_QUALITY_RULES)
def youtube_videos_silver():
    return _typed_videos_stream().withColumn("_silver_processed_at", current_timestamp())


@dp.table(
    name=f"{SILVER_SCHEMA}.youtube_videos_ldp_quarantine",
    comment="Video rows that failed quality expectations, kept here for investigation instead of being silently dropped",
    table_properties={"pipelines.reset.allowed": "false"},
)
def youtube_videos_quarantine():
    return (
        _typed_videos_stream()
        .filter(f"NOT coalesce({ALL_RULES_SQL}, false)")
        .withColumn("_quarantined_at", current_timestamp())
    )
