"""ingest_topology: grid topology JSON -> bronze + silver reference tables.

Replaces topology.dataflow (S3 event -> topology_transformer Lambda -> integrated parquet).
Entities are processed in parallel; invalid rows go to silver.topology_quarantine.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mda.config import TaskParams
from mda.io import ensure_dir
from mda.parallel import run_parallel
from mda.quality import TOPOLOGY_EXPECTATIONS, split_by_expectations
from mda.transforms import normalize_topology

ENTITIES = [
    "power_grid",
    "substation",
    "distribution_transformer",
    "service_transformer",
    "smart_meter",
    "location",
    "voltage_control_zone",
]


def _process(spark: SparkSession, params: TaskParams, entity: str) -> int:
    layout = params.layout
    path = f"{layout.landing}/topology/{entity}.json"
    if not os.path.exists(path):
        print(f"ingest_topology: {path} not found, skipping {entity}")
        return 0
    raw = (
        spark.read.option("multiLine", "true").option("primitivesAsString", "true").json(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(layout.bronze(f"topology_{entity}"))

    bronze = spark.table(layout.bronze(f"topology_{entity}"))
    valid, quarantine = split_by_expectations(normalize_topology(bronze, entity), TOPOLOGY_EXPECTATIONS)
    valid.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(layout.silver(f"topology_{entity}"))
    (
        quarantine.select(
            F.lit(entity).alias("entity"),
            F.to_json(F.struct(*[c for c in quarantine.columns if not c.startswith("_")])).alias("payload"),
            "_failed_expectations",
            "_source_file",
            "_quarantined_at",
        )
        .writeTo(layout.silver("topology_quarantine"))
        .append()
    )
    return spark.table(layout.silver(f"topology_{entity}")).count()


def run(spark: SparkSession, params: TaskParams) -> dict[str, int]:
    # Created up front: entities append concurrently and must not race on table creation.
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {params.layout.silver('topology_quarantine')} (entity STRING, payload STRING, "
        "_failed_expectations ARRAY<STRING>, _source_file STRING, _quarantined_at TIMESTAMP) USING DELTA"
    )
    ensure_dir(f"{params.layout.landing}/topology")
    counts = run_parallel({e: (lambda e=e: _process(spark, params, e)) for e in ENTITIES})
    print(f"ingest_topology: {counts}")
    return counts
