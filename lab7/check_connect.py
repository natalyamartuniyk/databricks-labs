from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("academy").getOrCreate()

print("Spark version:", spark.version)
spark.range(5).show()