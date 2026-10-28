"""gold_aggregates: hourly/daily consumption and transformer-level aggregates.

Gold equivalents of the Athena queries behind the API Lambdas (feature_get_consumption,
feature_get_voltage) and the Grafana dashboards; also the training input for ml_training.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from mda.config import TaskParams
from mda.parallel import run_parallel
from mda.transforms import daily_consumption, hourly_readings, transformer_hourly


def _overwrite(df: DataFrame, name: str) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)
    return df.sparkSession.table(name).count()


def run(spark: SparkSession, params: TaskParams) -> dict[str, int]:
    layout = params.layout
    silver = spark.table(layout.silver("meter_readings"))
    hourly_name = layout.gold("meter_readings_hourly")
    counts = run_parallel(
        {
            "meter_readings_hourly": lambda: _overwrite(hourly_readings(silver), hourly_name),
            "meter_consumption_daily": lambda: _overwrite(daily_consumption(silver), layout.gold("meter_consumption_daily")),
        }
    )
    counts["transformer_hourly"] = _overwrite(
        transformer_hourly(
            spark.table(hourly_name),
            spark.table(layout.silver("topology_smart_meter")),
            spark.table(layout.silver("topology_service_transformer")),
        ),
        layout.gold("transformer_hourly"),
    )
    print(f"gold_aggregates: {counts}")
    return counts
