# Databricks notebook source
# MAGIC %md
# MAGIC # MDA pipeline - `silver_transform`
# MAGIC Thin wrapper: job parameters arrive as widgets, the logic lives in `src/mda/tasks/silver_transform.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from mda.config import params_from_widgets
from mda.tasks import silver_transform

params = params_from_widgets(dbutils)
result = silver_transform.run(spark, params)
dbutils.notebook.exit(str(result))
