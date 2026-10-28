# Databricks notebook source
# MAGIC %md
# MAGIC # MDA pipeline - `anomaly_detection`
# MAGIC Thin wrapper: job parameters arrive as widgets, the logic lives in `src/mda/tasks/anomaly_detection.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from mda.config import params_from_widgets
from mda.tasks import anomaly_detection

params = params_from_widgets(dbutils)
result = anomaly_detection.run(spark, params)
dbutils.notebook.exit(str(result))
