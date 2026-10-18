# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import os
import sys

import pytest

sys.dont_write_bytecode = True

lab7_dir = os.path.abspath("..")
os.chdir(lab7_dir)

exit_code = pytest.main(["-v", "-p", "no:cacheprovider"])

if exit_code != 0:
    raise Exception(f"Unit tests failed (pytest exit code {exit_code})")