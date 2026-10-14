 # Databricks notebook source
# MAGIC %md
# MAGIC # Data quality suite (Lab 7, Part B)
# MAGIC Runs data quality checks across bronze, silver and gold, writes results to dq_results
# MAGIC and fails the run if any error-level check fails.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

from importlib.metadata import version

print("DQX version:", version("databricks-labs-dqx"))