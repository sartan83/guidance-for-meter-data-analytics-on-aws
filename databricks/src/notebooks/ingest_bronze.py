# Databricks notebook source
# MAGIC %md
# MAGIC # MDA pipeline - `ingest_bronze`
# MAGIC Thin wrapper: job parameters arrive as widgets, the logic lives in `src/mda/tasks/ingest_bronze.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from mda.config import params_from_widgets
from mda.tasks import ingest_bronze

params = params_from_widgets(dbutils)
result = ingest_bronze.run(spark, params)
dbutils.notebook.exit(str(result))
