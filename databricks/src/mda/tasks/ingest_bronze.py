"""ingest_bronze: Auto Loader from the UC landing volume into bronze Delta tables.

Replaces Kinesis Firehose -> S3 staging (readings/json), the inbound SQS file-range queue,
QueueStatusStateMachine (polling SQS before starting StagingGlueWorkflow) and
meterreadings-inbound-historic-job.py (historic CSV). Auto Loader tracks processed files in
its checkpoint, so no queue polling is needed; the job's file-arrival trigger starts the run.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from mda.config import TaskParams, set_task_value
from mda.io import ensure_dir, read_files_stream, rows_written
from mda.transforms import HISTORIC_READING_SCHEMA, STAGING_READING_SCHEMA

AUDIT_COLUMNS = "_rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP"


def _ddl(schema) -> str:
    return ", ".join(f"`{f.name}` STRING" for f in schema.fields)


def sources(params: TaskParams):
    layout = params.layout
    return [
        ("meter_readings_raw", "json", f"{layout.landing}/meter_readings", STAGING_READING_SCHEMA, {}),
        ("meter_readings_historic_raw", "csv", f"{layout.landing}/historic", HISTORIC_READING_SCHEMA, {"header": "true"}),
    ]


def run(spark: SparkSession, params: TaskParams) -> dict[str, int]:
    layout = params.layout
    use_autoloader = params.flag("use_autoloader")
    queries = {}
    for table, fmt, path, schema, options in sources(params):
        ensure_dir(path)
        target = layout.bronze(table)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({_ddl(schema)}, {AUDIT_COLUMNS}) USING DELTA")
        stream = read_files_stream(
            spark, fmt, path, schema, layout.checkpoint(f"{table}/schema"), use_autoloader, options
        )
        # Both streams run concurrently on the same cluster; availableNow = incremental batch.
        queries[table] = (
            stream.writeStream.format("delta")
            .option("checkpointLocation", layout.checkpoint(table))
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(target)
        )
    counts = {}
    for table, query in queries.items():
        query.awaitTermination()
        counts[table] = rows_written(query)
        set_task_value(f"{table}_rows", counts[table])
    print(f"ingest_bronze: {counts}")
    return counts
