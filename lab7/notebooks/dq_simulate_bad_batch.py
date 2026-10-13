# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate a batch with bad rows (Lab 7, Part B)
# MAGIC Writes a small CSV with deliberately invalid rows into the raw volume.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("scenario", "bad_rows", ["bad_rows", "bad_date_format"], "Scenario")
scenario = dbutils.widgets.get("scenario")

# COMMAND ----------

import time
from pyspark.sql import functions as F

BRONZE_TABLE = f"{catalog}.{config['bronze_schema']}.{config['source']['videos_bronze']}"
RAW_VOLUME_PATH = f"/Volumes/{catalog}/{config['source']['raw_volume']}"

base = (
    spark.table(BRONZE_TABLE)
    .drop("_source_file", "_ingested_at", "_rescued_data")
    .limit(1)
    .withColumn("trending_date", F.date_format(F.current_date(), "yy.dd.MM")) 
    .withColumn("description", F.lit("DQ test row"))
)

def case(title, **overrides):
    df = base.withColumn("title", F.lit(title))
    for column, value in overrides.items():
        df = df.withColumn(column, F.lit(value).cast("string"))
    return df


if scenario == "bad_rows":
    cases = [
        case("DQ test: missing video_id", video_id=None),
        case("DQ test: negative views", video_id="DQTEST00002", views="-5"),
        case("DQ test: missing video_id and negative likes", video_id=None, likes="-1"),
        case("DQ test: short video_id", video_id="DQTEST4"),
        case("DQ test: unknown category", video_id="DQTEST00005", category_id="99"),
        case("DQ test: published after trending", video_id="DQTEST00006",
             publish_time="2030-01-01T00:00:00.000Z"),
    ]
else:
    cases = [
        case("DQ test: wrong trending_date format", video_id="DQTEST00007",
             trending_date="2026-10-13"),
    ]

batch = cases[0]
for df in cases[1:]:
    batch = batch.unionByName(df)

# COMMAND ----------

batch_path = f"{RAW_VOLUME_PATH}/dq_test_batch_{int(time.time())}"

batch.coalesce(1).write.format("csv").option("header", "true").save(batch_path)

display(batch.select("title", "video_id", "views", "likes", "category_id", "publish_time", "trending_date"))
