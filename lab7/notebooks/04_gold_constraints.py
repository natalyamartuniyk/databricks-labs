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

# COMMAND ----------

dim_date = gold["dim_date"]

try:
    spark.sql(f"""
        INSERT INTO {dim_date}
            (date_key, full_date, year, quarter, month, month_name,
             day_of_month, day_of_week, day_name, is_weekend, _updated_at)
        VALUES
            (20991231, DATE'2099-12-31', 2099, 4, 12, 'December', 31, 5, 'Thursday', false, current_timestamp()),
            (20991230, DATE'2099-12-31', 2099, 4, 12, 'December', 31, 5, 'Thursday', false, current_timestamp())
    """)
    print("Insert succeeded: constraints are NOT working")
except Exception as error:
    print("Insert rejected:")
    print(str(error)[:500])

display(spark.sql(f"SELECT COUNT(*) AS test_rows FROM {dim_date} WHERE year = "2099")
                  
# COMMAND ----------

spark.sql(f"DELETE FROM {dim_date} WHERE year = 2099")