# Databricks notebook source
# MAGIC %md
# MAGIC # MDA pipeline - `gold_aggregates`
# MAGIC Thin wrapper: job parameters arrive as widgets, the logic lives in `src/mda/tasks/gold_aggregates.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from mda.config import params_from_widgets
from mda.tasks import gold_aggregates

params = params_from_widgets(dbutils)
result = gold_aggregates.run(spark, params)
dbutils.notebook.exit(str(result))
