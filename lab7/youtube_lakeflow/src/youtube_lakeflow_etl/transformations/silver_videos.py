from pyspark import pipelines as dp
from pyspark.sql.functions import (
    array,
    array_compact,
    coalesce,
    col,
    current_timestamp,
    expr,
    lit,
    trim,
    when,
)

SILVER_SCHEMA = spark.conf.get("youtube.silver_schema")

KNOWN_CATEGORY_IDS = [
    "1", "2", "10", "15", "17", "19", "20", "22",
    "23", "24", "25", "26", "27", "28", "29", "43",
]
KNOWN_CATEGORY_IDS_SQL = ", ".join(f"'{category_id}'" for category_id in KNOWN_CATEGORY_IDS)

DROP_RULES = {
    "video_id_not_null": "video_id IS NOT NULL",
    "trending_date_not_null": "trending_date IS NOT NULL",
    "valid_views": "views >= 0",
    "valid_likes": "likes >= 0",
    "valid_dislikes": "dislikes >= 0",
    "valid_comment_count": "comment_count >= 0",
}

# Systemic problem (source date format changed) -> stop the pipeline.
FAIL_RULES = {
    "trending_date_format": "trending_date IS NULL OR try_to_date(trending_date, 'yy.dd.MM') IS NOT NULL",
}

WARN_RULES = {
    "video_id_length_11": "length(video_id) = 11",
    "known_category_id": f"category_id IN ({KNOWN_CATEGORY_IDS_SQL})",
    "published_before_trending": (
        "try_cast(substring(publish_time, 1, 10) AS DATE) "
        "<= try_to_date(trending_date, 'yy.dd.MM')"
    ),
}

DROP_RULES_SQL = " AND ".join(DROP_RULES.values())


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

def _failed_drop_rules():
    return array_compact(
        array(*[
            when(~coalesce(expr(condition), lit(False)), lit(name))
            for name, condition in DROP_RULES.items()
        ])
    )


@dp.table(
    name=f"{SILVER_SCHEMA}.youtube_videos_ldp_silver",
    comment="Cleaned YouTube video metadata that passed all quality expectations",
    table_properties={"pipelines.reset.allowed": "false"},
)
@dp.expect_all_or_fail(FAIL_RULES)
@dp.expect_all_or_drop(DROP_RULES)
@dp.expect_all(WARN_RULES)
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
        .filter(f"NOT coalesce({DROP_RULES_SQL}, false)")
        .withColumn("_dq_failed_rules", _failed_drop_rules())   
        .withColumn("_quarantined_at", current_timestamp())
    )
