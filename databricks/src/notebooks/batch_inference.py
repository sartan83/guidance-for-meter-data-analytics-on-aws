# Databricks notebook source
# MAGIC %md
# MAGIC # MDA pipeline - `batch_inference`
# MAGIC Thin wrapper: job parameters arrive as widgets, the logic lives in `src/mda/tasks/batch_inference.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from mda.config import params_from_widgets
from mda.tasks import batch_inference

params = params_from_widgets(dbutils)
result = batch_inference.run(spark, params)
dbutils.notebook.exit(str(result))
