# Databricks notebook source
# MAGIC %md
# MAGIC # MDA pipeline - `ingest_topology`
# MAGIC Thin wrapper: job parameters arrive as widgets, the logic lives in `src/mda/tasks/ingest_topology.py`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))

from mda.config import params_from_widgets
from mda.tasks import ingest_topology

params = params_from_widgets(dbutils)
result = ingest_topology.run(spark, params)
dbutils.notebook.exit(str(result))
