# Databricks notebook source
# MAGIC %md
# MAGIC # Copy raw files
# MAGIC Copies the source CSV files (top level of the source volume) into the raw volume
# MAGIC of the current target. A file is copied only if it is not in the target yet,
# MAGIC so the notebook is safe to run on every job run.
# MAGIC When source and target are the same volume (dev), nothing is copied.

# COMMAND ----------

dbutils.widgets.text("raw_source_path", "")
dbutils.widgets.text("raw_volume_path", "")

source_path = dbutils.widgets.get("raw_source_path").rstrip("/")
target_path = dbutils.widgets.get("raw_volume_path").rstrip("/")

if not source_path or not target_path:
    raise ValueError("Parameters 'raw_source_path' and 'raw_volume_path' must be set")

# COMMAND ----------

if source_path == target_path:
    dbutils.notebook.exit("Source and target are the same volume: nothing to copy")

# COMMAND ----------

source_files = [f for f in dbutils.fs.ls(source_path) if f.name.endswith(".csv")]
if not source_files:
    raise ValueError(f"No CSV files found in {source_path}")

existing_files = {f.name for f in dbutils.fs.ls(target_path)}
new_files = [f for f in source_files if f.name not in existing_files]

for f in new_files:
    dbutils.fs.cp(f.path, f"{target_path}/{f.name}")

print(f"Copied {len(new_files)} of {len(source_files)} files: {[f.name for f in new_files]}")