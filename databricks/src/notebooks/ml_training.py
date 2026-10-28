# Databricks notebook source
# MAGIC %md
# MAGIC # MDA pipeline - `ml_training`
# MAGIC Thin wrapper: job parameters arrive as widgets, the logic lives in `src/mda/tasks/ml_training.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from mda.config import params_from_widgets
from mda.tasks import ml_training

params = params_from_widgets(dbutils)
result = ml_training.run(spark, params)
dbutils.notebook.exit(str(result))
