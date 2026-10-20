# Databricks notebook source
# MAGIC %md
# MAGIC # Unit tests
# MAGIC Runs the pytest suite from `tests/` against the code in `src/`.
# MAGIC Fails the task if any test fails.

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

import os
import sys

import pytest

sys.dont_write_bytecode = True

project_dir = os.path.abspath("..")
os.chdir(project_dir)

exit_code = pytest.main(["-v", "-p", "no:cacheprovider"])

if exit_code != 0:
    raise Exception(f"Unit tests failed (pytest exit code {exit_code})")