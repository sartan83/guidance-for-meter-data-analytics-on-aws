"""batch_inference: PrepareInference + SageMaker Batch Transform + StoreResults.

Loads `models:/<catalog>.gold.<model_name>@champion` from Unity Catalog, scores the next
forecast_period*24 hours for each meter with a Spark pandas UDF and upserts
gold.forecast_results (former Glue table meter_forecast_results).
"""
from __future__ import annotations

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from mda.config import TaskParams, set_task_value
from mda.forecast import FEATURES, future_grid, kw_hourly, with_features
from mda.io import delta_table

RESULT_TYPE = "mean double, p10 double, p50 double, p90 double"


def run(spark: SparkSession, params: TaskParams) -> dict:
    layout = params.layout
    horizon = params.int("forecast_period") * 24
    mlflow.set_registry_uri(params.get("registry_uri"))
    model_name = params.registered_model
    alias = params.get("model_alias")
    version = mlflow.MlflowClient().get_model_version_by_alias(model_name, alias).version
    model_uri = f"models:/{model_name}@{alias}"

    hourly = kw_hourly(spark.table(layout.gold("meter_readings_hourly")))
    max_hour = hourly.agg(F.max("date_hour")).first()[0]
    history = hourly.filter(F.col("date_hour") >= F.lit(max_hour) - F.expr(f"INTERVAL {params.int('training_time_frame')} DAYS"))
    features = with_features(future_grid(history, horizon), history, horizon)

    predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type=RESULT_TYPE, env_manager="local")
    scored = (
        features.withColumn("pred", predict(F.struct(*[F.col(c).cast("double").alias(c) for c in FEATURES])))
        .select(
            "meter_id",
            "forecast_start",
            F.col("date_hour").alias("forecast_hour"),
            F.col("pred.mean").alias("forecast_mean"),
            F.col("pred.p10").alias("forecast_p10"),
            F.col("pred.p50").alias("forecast_p50"),
            F.col("pred.p90").alias("forecast_p90"),
            F.lit(model_name).alias("model_name"),
            F.lit(str(version)).alias("model_version"),
            F.lit(params.get("run_id")).alias("run_id"),
            F.current_timestamp().alias("created_at"),
        )
    )

    target = layout.gold("forecast_results")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} (meter_id STRING, forecast_start TIMESTAMP, forecast_hour TIMESTAMP, "
        "forecast_mean DOUBLE, forecast_p10 DOUBLE, forecast_p50 DOUBLE, forecast_p90 DOUBLE, model_name STRING, "
        "model_version STRING, run_id STRING, created_at TIMESTAMP) USING DELTA CLUSTER BY (model_name, forecast_start)"
    )
    (
        delta_table(spark, target).alias("t")
        .merge(scored.alias("s"), "t.model_name = s.model_name AND t.meter_id = s.meter_id AND t.forecast_hour = s.forecast_hour")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    )
    rows = spark.table(target).filter(F.col("model_version") == str(version)).count()
    set_task_value("forecast_rows", rows)
    out = {"model_uri": model_uri, "version": str(version), "forecast_rows": rows}
    print(f"batch_inference: {out}")
    return out
