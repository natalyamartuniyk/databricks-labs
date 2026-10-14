#Databricks notebook source
# MAGIC %md
# MAGIC # Data quality suite (Lab 7, Part B)
# MAGIC Runs data quality checks across bronze, silver and gold, writes results to dq_results
# MAGIC and fails the run if any error-level check fails.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import yaml

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

dq_engine = DQEngine(WorkspaceClient())

with open("dq_checks.yml") as f:
    gold_checks = yaml.safe_load(f)

ref_dfs = {
    name: spark.table(gold[name])
    for name in ["dim_channel", "dim_category", "dim_date", "dim_video"]
}

# COMMAND ----------

def run_dqx_checks(table_name, checks):
    checked = dq_engine.apply_checks_by_metadata(
        spark.table(gold[table_name]), checks, ref_dfs=ref_dfs
    )

    issues = (
        checked.select(F.explode("_errors").alias("issue"))
        .unionByName(checked.select(F.explode("_warnings").alias("issue")))
        .groupBy(F.col("issue.name").alias("check_name"))
        .count()
    )
    failed_by_check = {row["check_name"]: row["count"] for row in issues.collect()}
    total_rows = checked.count()

    return [
        {
            "layer": "gold",
            "table_name": table_name,
            "check_name": check["name"],
            "criticality": check["criticality"],
            "failed_rows": failed_by_check.get(check["name"], 0),
            "total_rows": total_rows,
        }
        for check in checks
    ]

# COMMAND ----------

dqx_results = []
for table_name, checks in gold_checks.items():
    dqx_results += run_dqx_checks(table_name, checks)

display(spark.createDataFrame(dqx_results))

# COMMAND ----------

from datetime import datetime, timezone

BRONZE_TABLE = f"{catalog}.{config['bronze_schema']}.{config['source']['videos_bronze']}"
QUARANTINE_TABLE = f"{catalog}.{config['silver_schema']}.{config['source']['videos_quarantine']}"
DQ_RESULTS_TABLE = f"{catalog}.{config['gold_schema']}.dq_results"
FACT_TABLE = gold["fact_video_daily_stats"]
AGG_CATEGORY_TABLE = gold["agg_category_daily"]
PERIOD_START = config["trending_period"]["start"]
PERIOD_END = config["trending_period"]["end"]


SQL_CHECKS = [
    {
        "layer": "bronze",
        "table_name": config["source"]["videos_bronze"],
        "check_name": "bronze_video_id_not_null",
        "criticality": "warn",
        "query": f"""
            SELECT count_if(video_id IS NULL) AS failed_rows, COUNT(*) AS total_rows
            FROM {BRONZE_TABLE}
        """,
    },
    {
        "layer": "bronze",
        "table_name": config["source"]["videos_bronze"],
        "check_name": "bronze_views_numeric",
        "criticality": "warn",
        "query": f"""
            SELECT count_if(views IS NOT NULL AND try_cast(views AS BIGINT) IS NULL) AS failed_rows,
                   COUNT(*) AS total_rows
            FROM {BRONZE_TABLE}
        """,
    },
    {
        "layer": "silver",
        "table_name": config["source"]["videos_quarantine"],
        "check_name": "trending_date_format_share_24h",
        "criticality": "error",
        "max_failed_ratio": 0.01,
        "query": f"""
            SELECT
                (SELECT COUNT(*) FROM {QUARANTINE_TABLE}
                 WHERE array_contains(_dq_failed_rules, 'trending_date_format')
                   AND _quarantined_at >= current_timestamp() - INTERVAL 24 HOURS) AS failed_rows,
                (SELECT COUNT(*) FROM {BRONZE_TABLE}
                 WHERE _ingested_at >= current_timestamp() - INTERVAL 24 HOURS) AS total_rows
        """,
    },
    {
        "layer": "silver",
        "table_name": config["source"]["videos_silver"],
        "check_name": "silver_processed_last_48h",
        "criticality": "warn",
        "query": f"""
            SELECT CASE WHEN MAX(_silver_processed_at) >= current_timestamp() - INTERVAL 48 HOURS
                        THEN 0 ELSE 1 END AS failed_rows,
                   1 AS total_rows
            FROM {videos_silver}
        """,
    },
    {
        "layer": "reconciliation",
        "table_name": "bronze -> silver + quarantine",
        "check_name": "bronze_equals_silver_plus_quarantine",
        "criticality": "error",
        "query": f"""
            SELECT
                ABS((SELECT COUNT(*) FROM {BRONZE_TABLE})
                    - (SELECT COUNT(*) FROM {videos_silver})
                    - (SELECT COUNT(*) FROM {QUARANTINE_TABLE})) AS failed_rows,
                (SELECT COUNT(*) FROM {BRONZE_TABLE}) AS total_rows
        """,
    },
    {
        "layer": "reconciliation",
        "table_name": "silver -> fact_video_daily_stats",
        "check_name": "silver_video_days_equal_fact_rows",
        "criticality": "error",
        "query": f"""
            SELECT
                ABS((SELECT COUNT(DISTINCT video_id, try_to_date(trending_date, 'yy.dd.MM'))
                     FROM {videos_silver}
                     WHERE try_to_date(trending_date, 'yy.dd.MM') BETWEEN '{PERIOD_START}' AND '{PERIOD_END}')
                    - (SELECT COUNT(*) FROM {FACT_TABLE})) AS failed_rows,
                (SELECT COUNT(*) FROM {FACT_TABLE}) AS total_rows
        """,
    },
    {
        "layer": "reconciliation",
        "table_name": "fact_video_daily_stats -> agg_category_daily",
        "check_name": "fact_views_delta_equals_agg_views",
        "criticality": "error",
        "query": f"""
            SELECT
                ABS((SELECT COALESCE(SUM(views_delta), 0) FROM {FACT_TABLE})
                    - (SELECT COALESCE(SUM(views_gained), 0) FROM {AGG_CATEGORY_TABLE})) AS failed_rows,
                (SELECT COALESCE(SUM(views_delta), 0) FROM {FACT_TABLE}) AS total_rows
        """,
    },
]

sql_results = []
for check in SQL_CHECKS:
    row = spark.sql(check["query"]).collect()[0]
    sql_results.append({
        "layer": check["layer"],
        "table_name": check["table_name"],
        "check_name": check["check_name"],
        "criticality": check["criticality"],
        "failed_rows": row["failed_rows"],
        "total_rows": row["total_rows"],
        "max_failed_ratio": check.get("max_failed_ratio", 0.0),
    })

for result in dqx_results:
    result["max_failed_ratio"] = 0.0

# COMMAND ----------

run_at = datetime.now(timezone.utc)

results_df = (
    spark.createDataFrame(dqx_results + sql_results)
    .withColumn("run_at", F.lit(run_at))
    .withColumn("failed_ratio", F.when(F.col("total_rows") > 0, F.col("failed_rows") / F.col("total_rows")).otherwise(F.lit(0.0)),)
    .withColumn("passed", F.col("failed_ratio") <= F.col("max_failed_ratio"))
    .select(
        "run_at", "layer", "table_name", "check_name", "criticality",
        "failed_rows", "total_rows", "failed_ratio", "max_failed_ratio", "passed",
    )
)

results_df.write.mode("append").saveAsTable(DQ_RESULTS_TABLE)

display(results_df)

# COMMAND ----------

failed_errors = (
    results_df.filter("criticality = 'error' AND NOT passed")
    .select("table_name", "check_name", "failed_rows")
    .collect()
)

if failed_errors:
    problems = ", ".join(f"{r['table_name']}.{r['check_name']} ({r['failed_rows']} rows)" for r in failed_errors)
    raise ValueError(f"Data quality gate failed: {problems}")

print("Data quality gate passed")