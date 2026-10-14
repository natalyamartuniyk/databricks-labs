# Databricks notebook source
# MAGIC %md
# MAGIC # Gold CHECK constraints (Lab 7, Part B)
# MAGIC Adds CHECK constraints to existing gold tables and shows that a bad write is rejected.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

CONSTRAINTS = {
    "fact_video_daily_stats": {
        "counters_non_negative": "views >= 0 AND likes >= 0 AND dislikes >= 0 AND comment_count >= 0",
        "trending_day_num_positive": "trending_day_num >= 1",
        "views_delta_only_after_first_day": "(trending_day_num = 1) = (views_delta IS NULL)",
    },
    "dim_date": {
        "date_key_matches_full_date": "date_key = CAST(date_format(full_date, 'yyyyMMdd') AS INT)",
    }
}

for table, rules in CONSTRAINTS.items():
    for name, condition in rules.items():
        spark.sql(f"ALTER TABLE {gold[table]} DROP CONSTRAINT IF EXISTS {name}")
        spark.sql(f"ALTER TABLE {gold[table]} ADD CONSTRAINT {name} CHECK ({condition})")
        print(f"{gold[table]}: {name} added")

# COMMAND ----------

display (
    spark.sql(f"SHOW TBLPROPERTIES {gold['fact_video_daily_stats']}")
    .filter("key LIKE 'delta.constraints.%'")
)

# COMMAND ----------

fact = gold["fact_video_daily_stats"]

try:
    spark.sql(f"""
        INSERT INTO {fact}
            (date_key, video_id, channel_key, category_id, views, likes, dislikes,
             comment_count, views_delta, trending_day_num, _updated_at)
        VALUES
            (20180614, 'DQTEST_OK01', 1, 10, 100, 10, 1, 5, NULL, 1, current_timestamp()),
            (20180614, 'DQTEST_BAD1', 1, 10, -5, 10, 1, 5, NULL, 1, current_timestamp())
    """)
    print("Insert succeeded: constraints are NOT working")
except Exception as error:
    print("Insert rejected:")
    print(str(error)[:500])

display(spark.sql(f"SELECT COUNT(*) AS test_rows FROM {fact} WHERE video_id LIKE 'DQTEST%'"))

# COMMAND ----------

spark.sql(f"DELETE FROM {fact} WHERE video_id LIKE 'DQTEST%'")