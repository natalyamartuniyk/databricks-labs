# Databricks notebook source
# MAGIC %md
# MAGIC # Gold CHECK constraints (Lab 7, Part B)
# MAGIC Adds CHECK constraints to existing gold tables and shows that a bad write is rejected.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

CONSTRAINTS = {
    "dim_date": {
        "date_key_matches_full_date": "date_key = CAST(date_format(full_date, 'yyyyMMdd') AS INT)",
        "is_weekend_matches_day_of_week": "is_weekend = (day_of_week IN (1, 7))",
        "quarter_matches_month": "quarter = CEIL(month / 3)",
    }
}

for table, rules in CONSTRAINTS.items():
    for name, condition in rules.items():
        spark.sql(f"ALTER TABLE {gold[table]} DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE {gold[table]} ADD CONSTRAINT {name} CHECK ({condition})")
        print(f"{gold[table]}: {name} added")

# COMMAND ----------

display (
    spark.sql(f"SHOW TBLPROPERTIES {gold['dim_date']}")
    .filter("key LIKE 'delta.constraints.%'")
)



