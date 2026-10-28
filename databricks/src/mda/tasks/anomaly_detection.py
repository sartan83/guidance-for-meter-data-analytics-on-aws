"""anomaly_detection: port of assets/glue/scripts/mlpipeline-anomaly-detection-job.py.

Same Prophet model per meter (applyInPandas), same incremental logic. Changes:
- SSM `/mlpipeline/<stack>/Anomaly/LastDatetime` + parquet date tracker on S3
  -> Delta table gold.anomaly_date_tracker (meter_id, max_ds).
- Glue catalog table meter_anomaly_results -> gold.anomaly_results, upserted on (meter_id, ds).
- SSM `/mlpipeline/<stack>/Steps/Anomaly` -> job parameter run_anomaly (condition task).
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, StructField, StructType

from mda.config import TaskParams, set_task_value
from mda.io import delta_table

RESULT_SCHEMA = StructType(
    [
        StructField("ds", DateType()),
        StructField("meter_id", StringType()),
        StructField("consumption", FloatType()),
        StructField("yhat", FloatType()),
        StructField("yhat_upper", FloatType()),
        StructField("yhat_lower", FloatType()),
        StructField("anomaly", IntegerType()),
        StructField("importance", FloatType()),
    ]
)
RESULT_COLUMNS = [f.name for f in RESULT_SCHEMA.fields]
MIN_POINTS = 10


def prepare_input(silver: DataFrame) -> DataFrame:
    """Daily kw sum per meter, meters with <= 10 points removed, weekend regressor."""
    df = (
        silver.filter(F.col("reading_type") == "kw")
        .groupBy("meter_id", F.to_date("reading_date_time").alias("ds"))
        .agg(F.sum("reading_value").alias("y"))
    )
    enough = df.groupBy("meter_id").count().filter(F.col("count") > MIN_POINTS).select("meter_id")
    return df.join(enough, "meter_id").withColumn("weekend", F.dayofweek("ds").isin([1, 7]).cast("int"))


def previous_dates(spark: SparkSession, tracker: str) -> DataFrame:
    if spark.catalog.tableExists(tracker):
        return spark.table(tracker).select("meter_id", "max_ds")
    return spark.createDataFrame([], "meter_id STRING, max_ds DATE")


def make_forecaster(holidays_country: str):
    def forecast_meter_id(history_pd: pd.DataFrame) -> pd.DataFrame:
        from prophet import Prophet

        def calculate(history: pd.DataFrame, prev_date) -> pd.DataFrame:
            history = history.dropna()
            m = Prophet(
                daily_seasonality=False,
                yearly_seasonality=True,
                weekly_seasonality=True,
                seasonality_mode="multiplicative",
                interval_width=0.98,
                changepoint_range=0.8,
            )
            if holidays_country:
                m.add_country_holidays(country_name=holidays_country)
            m.add_regressor("weekend")
            m.fit(history)

            predict_pd = history if prev_date is None else history[history.ds > prev_date]
            forecast = m.predict(predict_pd)
            predict_pd = predict_pd[["ds", "y"]].copy()
            predict_pd["ds"] = pd.to_datetime(predict_pd["ds"])
            forecast = forecast.merge(predict_pd, on="ds").rename(columns={"y": "consumption"})
            forecast["meter_id"] = history["meter_id"].iloc[0]
            forecast["anomaly"] = 0
            forecast.loc[forecast["consumption"] > forecast["yhat_upper"], "anomaly"] = 1
            forecast.loc[forecast["consumption"] < forecast["yhat_lower"], "anomaly"] = -1
            forecast["importance"] = 0.0
            up, low = forecast["anomaly"] == 1, forecast["anomaly"] == -1
            forecast.loc[up, "importance"] = (forecast["consumption"] - forecast["yhat_upper"]) / forecast["consumption"]
            forecast.loc[low, "importance"] = (forecast["yhat_lower"] - forecast["consumption"]) / forecast["consumption"]
            return forecast[RESULT_COLUMNS]

        history_pd = history_pd.copy()
        history_pd["ds"] = pd.to_datetime(history_pd["ds"])
        prev = history_pd["prev_max_ds"].iloc[0]
        empty = pd.DataFrame({c: pd.Series(dtype="object") for c in RESULT_COLUMNS})
        if pd.isna(prev):
            out = calculate(history_pd[["meter_id", "ds", "y", "weekend"]], None)
        elif pd.Timestamp(history_pd["max_ds"].iloc[0]) > pd.Timestamp(prev):
            out = calculate(history_pd[["meter_id", "ds", "y", "weekend"]], pd.Timestamp(prev))
        else:
            return empty
        out["ds"] = pd.to_datetime(out["ds"]).dt.date
        for c in ("consumption", "yhat", "yhat_upper", "yhat_lower", "importance"):
            out[c] = out[c].astype("float32")
        out["anomaly"] = out["anomaly"].astype("int32")
        return out

    return forecast_meter_id


def run(spark: SparkSession, params: TaskParams) -> dict[str, int]:
    layout = params.layout
    results_table = layout.gold("anomaly_results")
    tracker_table = layout.gold("anomaly_date_tracker")

    df = prepare_input(spark.table(layout.silver("meter_readings")))
    prev = previous_dates(spark, tracker_table)
    max_ds = df.groupBy("meter_id").agg(F.max("ds").alias("max_ds"))
    comparison = max_ds.join(prev.withColumnRenamed("max_ds", "prev_max_ds"), "meter_id", "left")
    joined = df.join(comparison, "meter_id", "left")

    results = (
        joined.groupBy("meter_id")
        .applyInPandas(make_forecaster(params.get("holidays_country")), schema=RESULT_SCHEMA)
        .withColumn("run_id", F.lit(params.get("run_id")))
        .withColumn("detected_at", F.current_timestamp())
    ).cache()

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {results_table} (ds DATE, meter_id STRING, consumption FLOAT, yhat FLOAT, "
        "yhat_upper FLOAT, yhat_lower FLOAT, anomaly INT, importance FLOAT, run_id STRING, detected_at TIMESTAMP) "
        "USING DELTA CLUSTER BY (meter_id, ds)"
    )
    (
        delta_table(spark, results_table).alias("t")
        .merge(results.alias("s"), "t.meter_id = s.meter_id AND t.ds = s.ds")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    )

    new_max = results.groupBy("meter_id").agg(F.max("ds").alias("new_max_ds"))
    tracker = (
        prev.join(new_max, "meter_id", "outer")
        .select("meter_id", F.greatest("max_ds", "new_max_ds").alias("max_ds"))
    )
    tracker.write.format("delta").mode("overwrite").saveAsTable(tracker_table + "_tmp")
    spark.table(tracker_table + "_tmp").write.format("delta").mode("overwrite").saveAsTable(tracker_table)
    spark.sql(f"DROP TABLE IF EXISTS {tracker_table}_tmp")

    counts = {"scored_days": results.count(), "anomalies": results.filter("anomaly <> 0").count()}
    results.unpersist()
    for k, v in counts.items():
        set_task_value(k, v)
    print(f"anomaly_detection: {counts}")
    return counts
