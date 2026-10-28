"""Pure DataFrame transformations ported from the AWS Glue jobs."""
from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

READING_TYPES = ["load", "crrnt", "pf", "kva", "kw", "vltg"]
READING_KEYS = ["meter_id", "reading_type", "reading_date_time"]

# Staging record produced by the inbound adapters (adapter_csv_inbound_file_range_worker,
# HES adapter) and written by Firehose to the staging bucket (readings/json).
STAGING_READING_SCHEMA = StructType(
    [
        StructField(name, StringType(), True)
        for name in [
            "meter_id",
            "reading_type",
            "reading_value",
            "reading_date_time",
            "unit",
            "obis_code",
            "phase",
            "reading_source",
        ]
    ]
)

# Historic CSV dropped in the inbound bucket (data/historic) - meterreadings-inbound-historic-job.py
HISTORIC_READING_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in ["device_id", "time", *READING_TYPES]]
)

SILVER_READINGS_COLUMNS = [
    "meter_id",
    "reading_type",
    "reading_value",
    "reading_date_time",
    "unit",
    "obis_code",
    "phase",
    "reading_source",
    "year",
    "month",
    "day",
    "hour",
    "reading_date",
    "_source_file",
    "_ingested_at",
]


def unpivot_historic(df: DataFrame) -> DataFrame:
    """Wide historic CSV (one column per reading type) -> long staging records."""
    stack = ", ".join(f"'{t}', `{t}`" for t in READING_TYPES)
    return df.select(
        F.col("device_id").alias("meter_id"),
        F.col("time").alias("reading_date_time"),
        F.expr(f"stack({len(READING_TYPES)}, {stack}) AS (reading_type, reading_value)"),
        F.lit(None).cast("string").alias("unit"),
        F.lit(None).cast("string").alias("obis_code"),
        F.lit(None).cast("string").alias("phase"),
        F.lit("B").alias("reading_source"),
        "_source_file",
        "_ingested_at",
    )


RAW_COLUMNS = ["_raw_reading_value", "_raw_reading_date_time"]


def normalize_readings(df: DataFrame, keep_raw: bool = False) -> DataFrame:
    """Typed silver readings: the ApplyMapping of meterreadings-integration-job.py.

    try_cast / try_to_timestamp keep the job ANSI-safe: unparsable values become NULL
    and are then routed to quarantine by the expectations.
    """
    ts = F.expr("coalesce(try_to_timestamp(reading_date_time), try_to_timestamp(reading_date_time, 'yyyy-MM-dd HH:mm:ss.SSS'))")
    out = (
        df.withColumn("_raw_reading_value", F.col("reading_value").cast("string"))
        .withColumn("_raw_reading_date_time", F.col("reading_date_time").cast("string"))
        .withColumn("meter_id", F.trim("meter_id"))
        .withColumn("reading_type", F.lower(F.trim("reading_type")))
        .withColumn("reading_value", F.expr("try_cast(reading_value AS DOUBLE)"))
        .withColumn("reading_date_time", ts)
        .withColumn("year", F.year("reading_date_time"))
        .withColumn("month", F.month("reading_date_time"))
        .withColumn("day", F.dayofmonth("reading_date_time"))
        .withColumn("hour", F.hour("reading_date_time"))
        .withColumn("reading_date", F.to_date("reading_date_time"))
    )
    return out.select(*SILVER_READINGS_COLUMNS, *(RAW_COLUMNS if keep_raw else []))


def latest_per_key(df: DataFrame, keys=READING_KEYS, order_col: str = "_ingested_at") -> DataFrame:
    """Keep the last-ingested record per key so MERGE sees one source row per target row."""
    from pyspark.sql import Window

    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last(), F.col("_source_file").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def snake_case(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def normalize_topology(df: DataFrame, entity: str) -> DataFrame:
    """topology_transformer Lambda: flatten JSON, snake_case columns, numeric coordinates."""
    out = df.select([F.col(f"`{c}`").alias(snake_case(c)) for c in df.columns if not c.startswith("_")] + [c for c in df.columns if c.startswith("_")])
    if entity == "location":
        for c in ("latitude", "longitude"):
            if c in out.columns:
                out = out.withColumn(c, F.expr(f"try_cast({c} AS DOUBLE)"))
    for c, t in out.dtypes:
        if t == "string" and c not in ("latitude", "longitude"):
            out = out.withColumn(c, F.nullif(F.trim(F.col(c)), F.lit("")))
    return out


def hourly_readings(silver: DataFrame) -> DataFrame:
    return silver.groupBy("meter_id", "reading_type", F.date_trunc("hour", "reading_date_time").alias("date_hour")).agg(
        F.sum("reading_value").alias("hourly_value"),
        F.avg("reading_value").alias("avg_value"),
        F.min("reading_value").alias("min_value"),
        F.max("reading_value").alias("max_value"),
        F.count(F.lit(1)).alias("n_readings"),
    )


def daily_consumption(silver: DataFrame) -> DataFrame:
    """Anomaly-detection input (daily kw sum per meter) also published as a gold table."""
    return (
        silver.filter(F.col("reading_type") == "kw")
        .groupBy("meter_id", F.to_date("reading_date_time").alias("date"))
        .agg(F.sum("reading_value").alias("aggregated_value"), F.count(F.lit(1)).alias("n_readings"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]).cast("int"))
    )


def transformer_hourly(hourly: DataFrame, smart_meter: DataFrame, service_transformer: DataFrame) -> DataFrame:
    """Hourly load and voltage per distribution transformer (grid view used by Grafana/Volt-VAR)."""
    m = smart_meter.select(F.col("id").alias("meter_id"), "service_transformer_id")
    st = service_transformer.select(F.col("id").alias("service_transformer_id"), "distribution_transformer_id")
    joined = hourly.join(m, "meter_id").join(st, "service_transformer_id")
    return joined.groupBy("distribution_transformer_id", "date_hour").agg(
        F.sum(F.when(F.col("reading_type") == "kw", F.col("hourly_value"))).alias("kw_sum"),
        F.avg(F.when(F.col("reading_type") == "vltg", F.col("avg_value"))).alias("vltg_avg"),
        F.countDistinct("meter_id").alias("n_meters"),
    )
