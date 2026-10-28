"""silver_transform: bronze readings -> typed, deduplicated silver Delta table.

Port of meterreadings-integration-job.py (ApplyMapping staging -> integrated parquet) and
meterreadings-late-reads-cleanup-job.py: late-arriving or corrected readings are upserted
with MERGE on (meter_id, reading_type, reading_date_time) instead of rewriting partitions
signalled through the late-arriving SQS queue. Rows failing expectations go to quarantine
(the SQS DLQ equivalent).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from mda.config import TaskParams, set_task_value
from mda.io import delta_table, rows_written
from mda.quality import READINGS_EXPECTATIONS, split_by_expectations
from mda.transforms import READING_KEYS, latest_per_key, normalize_readings, unpivot_historic

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS {name} (
  meter_id STRING NOT NULL,
  reading_type STRING NOT NULL,
  reading_value DOUBLE,
  reading_date_time TIMESTAMP NOT NULL,
  unit STRING,
  obis_code STRING,
  phase STRING,
  reading_source STRING,
  year INT,
  month INT,
  day INT,
  hour INT,
  reading_date DATE,
  _source_file STRING,
  _ingested_at TIMESTAMP
) USING DELTA
CLUSTER BY (reading_date, reading_type)
COMMENT 'Integrated meter readings (former Glue integrated DB meter_readings_integrated_parquet)'
"""

STAGING_COLUMNS = ["meter_id", "reading_type", "reading_value", "reading_date_time", "unit", "obis_code", "phase", "reading_source", "_source_file", "_ingested_at"]


def upsert_batch(batch: DataFrame, target: str, quarantine_table: str) -> None:
    spark = batch.sparkSession
    typed = normalize_readings(batch, keep_raw=True)
    valid, quarantine = split_by_expectations(typed, READINGS_EXPECTATIONS)
    valid = latest_per_key(valid.drop("_raw_reading_value", "_raw_reading_date_time"))
    cond = " AND ".join(f"t.{k} = s.{k}" for k in READING_KEYS)
    (
        delta_table(spark, target).alias("t")
        .merge(valid.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    quarantine.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table)


def run(spark: SparkSession, params: TaskParams) -> dict[str, int]:
    layout = params.layout
    target = layout.silver("meter_readings")
    quarantine_table = layout.silver("meter_readings_quarantine")
    spark.sql(SILVER_DDL.format(name=target))

    readings = spark.readStream.table(layout.bronze("meter_readings_raw")).select(*STAGING_COLUMNS)
    historic = unpivot_historic(spark.readStream.table(layout.bronze("meter_readings_historic_raw"))).select(*STAGING_COLUMNS)

    query = (
        readings.unionByName(historic)
        .writeStream.foreachBatch(lambda df, _id: upsert_batch(df, target, quarantine_table))
        .option("checkpointLocation", layout.checkpoint("silver_meter_readings"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    counts = {
        "processed": rows_written(query),
        "silver_rows": spark.table(target).count(),
        "quarantined": spark.table(quarantine_table).count() if spark.catalog.tableExists(quarantine_table) else 0,
    }
    for k, v in counts.items():
        set_task_value(k, v)
    print(f"silver_transform: {counts}")
    return counts
