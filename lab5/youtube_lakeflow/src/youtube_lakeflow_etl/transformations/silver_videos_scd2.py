from pyspark import pipelines as dp
from pyspark.sql.functions import col, struct, to_date

CATALOG = spark.conf.get("youtube.catalog")
SILVER_SCHEMA = spark.conf.get("youtube.silver_schema")
SCD2_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.youtube_videos_ldp_scd2"

dp.create_streaming_table(
    name=SCD2_TABLE,
    comment="Full change history of video stats (views, likes, dislikes, comment_count) per video_id, built with Auto CDC SCD Type 2",
    table_properties={"pipelines.reset.allowed": "false"},
)

dp.create_auto_cdc_flow(
    target=SCD2_TABLE,
    source=f"{CATALOG}.{SILVER_SCHEMA}.youtube_videos_ldp_silver",
    keys=["video_id"],
    sequence_by=struct(to_date(col("trending_date"), "yy.dd.MM"), col("_silver_processed_at")),
    stored_as_scd_type=2,
    track_history_column_list=["views", "likes", "dislikes", "comment_count"],
)