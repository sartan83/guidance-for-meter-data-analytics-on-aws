"""Hourly load forecast shared by ml_training and batch_inference.

Replaces SageMaker DeepAR (forecasting-deepar) with a global gradient-boosting model per
quantile. DeepAR batch transform returned mean + quantiles 0.1/0.5/0.9; the pyfunc model
returns the same outputs. Features only use values at least `horizon` hours old, so a
full forecast window can be scored in one pass without recursion.
"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FEATURES = ["hour", "dow", "weekend", "lag_h", "lag_2h", "meter_mean"]
OUTPUTS = ["mean", "p10", "p50", "p90"]
QUANTILES = {"p10": 0.1, "p50": 0.5, "p90": 0.9}


def kw_hourly(gold_hourly: DataFrame) -> DataFrame:
    return gold_hourly.filter(F.col("reading_type") == "kw").select("meter_id", "date_hour", "hourly_value")


def with_features(grid: DataFrame, history: DataFrame, horizon_hours: int) -> DataFrame:
    def lag(hours: int, name: str) -> DataFrame:
        return history.select(
            "meter_id",
            (F.col("date_hour") + F.expr(f"INTERVAL {hours} HOURS")).alias("date_hour"),
            F.col("hourly_value").alias(name),
        )

    meter_mean = history.groupBy("meter_id").agg(F.avg("hourly_value").alias("meter_mean"))
    return (
        grid.join(lag(horizon_hours, "lag_h"), ["meter_id", "date_hour"], "left")
        .join(lag(2 * horizon_hours, "lag_2h"), ["meter_id", "date_hour"], "left")
        .join(meter_mean, "meter_id", "left")
        .withColumn("hour", F.hour("date_hour"))
        .withColumn("dow", F.dayofweek("date_hour"))
        .withColumn("weekend", F.col("dow").isin([1, 7]).cast("int"))
    )


def future_grid(history: DataFrame, horizon_hours: int) -> DataFrame:
    """PrepareInference: one row per meter and forecast hour after the last observed hour."""
    last = history.groupBy("meter_id").agg(F.max("date_hour").alias("last_hour"))
    return last.select(
        "meter_id",
        (F.col("last_hour") + F.expr("INTERVAL 1 HOURS")).alias("forecast_start"),
        F.explode(
            F.sequence(
                F.col("last_hour") + F.expr("INTERVAL 1 HOURS"),
                F.col("last_hour") + F.expr(f"INTERVAL {horizon_hours} HOURS"),
                F.expr("INTERVAL 1 HOURS"),
            )
        ).alias("date_hour"),
    )


try:
    import mlflow.pyfunc

    _Base = mlflow.pyfunc.PythonModel
except Exception:  # pragma: no cover - mlflow missing
    _Base = object


class QuantileForecastModel(_Base):
    def __init__(self, models: dict):
        self.models = models

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.DataFrame:
        X = model_input[FEATURES].astype("float64")
        return pd.DataFrame({name: self.models[name].predict(X) for name in OUTPUTS})


def fit_models(train: pd.DataFrame, max_iter: int = 200) -> dict:
    from sklearn.ensemble import HistGradientBoostingRegressor

    X, y = train[FEATURES].astype("float64"), train["hourly_value"].astype("float64")
    models = {"mean": HistGradientBoostingRegressor(max_iter=max_iter, random_state=42).fit(X, y)}
    for name, q in QUANTILES.items():
        models[name] = HistGradientBoostingRegressor(loss="quantile", quantile=q, max_iter=max_iter, random_state=42).fit(X, y)
    return models


def evaluate(model: QuantileForecastModel, test: pd.DataFrame) -> dict[str, float]:
    import numpy as np

    pred = model.predict(None, test)
    y = test["hourly_value"].to_numpy()
    err = pred["mean"].to_numpy() - y
    return {
        "mae": float(np.mean(np.abs(err))),
        "rmse": float(np.sqrt(np.mean(err**2))),
        "p10_p90_coverage": float(np.mean((y >= pred["p10"].to_numpy()) & (y <= pred["p90"].to_numpy()))),
        "test_rows": float(len(test)),
    }
