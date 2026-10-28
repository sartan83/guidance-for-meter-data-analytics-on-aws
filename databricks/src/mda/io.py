from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def ensure_dir(path: str) -> None:
    """Works for local paths and for UC volumes (/Volumes is FUSE-mounted on Databricks)."""
    os.makedirs(path, exist_ok=True)


def read_files_stream(
    spark: SparkSession,
    fmt: str,
    path: str,
    schema: StructType,
    schema_location: str,
    use_autoloader: bool,
    options: dict[str, str] | None = None,
) -> DataFrame:
    """Auto Loader (cloudFiles) on Databricks; plain file streaming source for local runs."""
    options = options or {}
    if use_autoloader:
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("rescuedDataColumn", "_rescued_data")
        )
    else:
        reader = spark.readStream.format(fmt)
    df = reader.options(**options).schema(schema).load(path)
    if "_rescued_data" not in df.columns:
        df = df.withColumn("_rescued_data", F.lit(None).cast("string"))
    return df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_ingested_at", F.current_timestamp())


def delta_table(spark: SparkSession, name: str):
    """DeltaTable.forName; OSS Spark rejects the 3-part `spark_catalog.` name used by local runs."""
    from delta.tables import DeltaTable

    name = name.removeprefix("spark_catalog.")
    return DeltaTable.forName(spark, name)


def table_exists(spark: SparkSession, name: str) -> bool:
    return spark.catalog.tableExists(name)


def rows_written(query) -> int:
    return int(sum(p.get("numInputRows", 0) for p in (query.recentProgress or [])))
