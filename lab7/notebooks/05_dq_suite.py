 # Databricks notebook source
# MAGIC %md
# MAGIC # Data quality suite (Lab 7, Part B)
# MAGIC Runs data quality checks across bronze, silver and gold, writes results to dq_results
# MAGIC and fails the run if any error-level check fails.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from importlib.metadata import version

print("DQX version:", version("databricks-labs-dqx"))

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
    ]

# COMMAND ----------

dqx_results = []
for table_name, checks in gold_checks.items():
    dqx_results += run_dqx_checks(table_name, checks)

display(spark.createDataFrame(dqx_results))