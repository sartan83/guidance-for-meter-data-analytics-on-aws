"""DLT-style expectations with a quarantine split.

The rule dictionaries use the same ``{name: SQL constraint}`` shape accepted by
``@dlt.expect_all_or_drop``; rows violating any rule are routed to a quarantine
table instead of being dropped silently (the Databricks equivalent of an SQS DLQ).
"""
from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

READINGS_EXPECTATIONS: dict[str, str] = {
    "valid_meter_id": "meter_id IS NOT NULL AND trim(meter_id) <> ''",
    "valid_reading_type": "reading_type IS NOT NULL AND reading_type <> ''",
    "valid_reading_value": "reading_value IS NOT NULL AND NOT isnan(reading_value)",
    "valid_reading_date_time": "reading_date_time IS NOT NULL",
}

TOPOLOGY_EXPECTATIONS: dict[str, str] = {
    "valid_id": "id IS NOT NULL AND trim(id) <> ''",
}

FAILED_COL = "_failed_expectations"


def failed_expectations(rules: Mapping[str, str]) -> Column:
    checks = [F.when(~F.coalesce(F.expr(sql), F.lit(False)), F.lit(name)) for name, sql in rules.items()]
    return F.filter(F.array(*checks), lambda c: c.isNotNull())


def split_by_expectations(df: DataFrame, rules: Mapping[str, str]) -> tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``; quarantine rows carry the names of the failed rules."""
    tagged = df.withColumn(FAILED_COL, failed_expectations(rules))
    valid = tagged.filter(F.size(FAILED_COL) == 0).drop(FAILED_COL)
    quarantine = tagged.filter(F.size(FAILED_COL) > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, quarantine
