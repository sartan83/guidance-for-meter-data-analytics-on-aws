"""ml_training: replaces MLPipelineModelTrainingStateMachine.

PrepareTraining Glue job (hourly kw series, train = all but the last 2*forecast_period days)
+ SageMaker DeepAR training + CreateModel/EndpointConfig/Endpoint become: one MLflow run,
model registered in Unity Catalog (<catalog>.gold.<model_name>) and alias `champion`
moved to the new version (the alias replaces the SageMaker endpoint update).
"""
from __future__ import annotations

import os
from datetime import timedelta

import mlflow
from mlflow.models import infer_signature
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import mda
from mda.config import TaskParams, set_task_value
from mda.forecast import FEATURES, OUTPUTS, QuantileForecastModel, evaluate, fit_models, kw_hourly, with_features


def run(spark: SparkSession, params: TaskParams) -> dict:
    layout = params.layout
    horizon = params.int("forecast_period") * 24
    hourly = kw_hourly(spark.table(layout.gold("meter_readings_hourly")))
    max_hour = hourly.agg(F.max("date_hour")).first()[0]
    if max_hour is None:
        raise RuntimeError("ml_training: gold.meter_readings_hourly has no kw data")
    start = max_hour - timedelta(days=params.int("training_time_frame"))
    train_end = max_hour - timedelta(days=2 * params.int("forecast_period"))
    history = hourly.filter(F.col("date_hour") >= F.lit(start))

    data = with_features(history, history, horizon).filter(F.col("lag_h").isNotNull())
    pdf = data.select("meter_id", "date_hour", "hourly_value", *FEATURES).toPandas()
    train = pdf[pdf["date_hour"] <= train_end]
    test = pdf[pdf["date_hour"] > train_end]
    if train.empty or test.empty:
        raise RuntimeError(f"ml_training: not enough history (train={len(train)}, test={len(test)} rows)")

    mlflow.set_registry_uri(params.get("registry_uri"))
    mlflow.set_experiment(params.get("experiment_path"))
    model_name = params.registered_model
    with mlflow.start_run(run_name=f"mda_forecast_{params.get('run_id') or 'manual'}") as run:
        mlflow.log_params(
            {
                "forecast_period_days": params.get("forecast_period"),
                "prediction_length_hours": horizon,
                "training_time_frame_days": params.get("training_time_frame"),
                "train_end": str(train_end),
                "train_rows": len(train),
                "features": ",".join(FEATURES),
            }
        )
        model = QuantileForecastModel(fit_models(train))
        metrics = evaluate(model, test)
        mlflow.log_metrics(metrics)
        sample = train[FEATURES].head(20).astype("float64")
        info = mlflow.pyfunc.log_model(
            name="model",
            python_model=model,
            code_paths=[os.path.dirname(mda.__file__)],
            signature=infer_signature(sample, model.predict(None, sample)[OUTPUTS]),
            input_example=sample.head(5),
            pip_requirements=["scikit-learn", "pandas", "numpy"],
            registered_model_name=model_name,
        )
    version = str(info.registered_model_version)
    client = mlflow.MlflowClient()
    client.set_registered_model_alias(model_name, params.get("model_alias"), version)
    set_task_value("model_version", version)
    set_task_value("mlflow_run_id", run.info.run_id)
    out = {"model": model_name, "version": version, **metrics}
    print(f"ml_training: {out}")
    return out
