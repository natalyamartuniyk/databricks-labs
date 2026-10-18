import os
import pytest

@pytest.fixture(scope="session")
def spark():
    if "DATABRICKS_RUNTIME_VERSION"  in os.environ:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.profile("academy").getOrCreate()