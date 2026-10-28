# Meter Data Analytics on Databricks (Unity Catalog, medallion)

Databricks Asset Bundle that replaces the AWS orchestration of this guidance (Step Functions,
Glue, SQS/DLQ, Athena, DynamoDB, SageMaker, SSM flags) with one Databricks Workflows job on
Unity Catalog. The Glue scripts in `assets/glue/scripts/` and the state machines in
`assets/statemachine/` were ported, not rewritten: the original logic lives in `src/mda/tasks/`.

```
databricks/
├── databricks.yml                    bundle, variables, dev/prod targets
├── resources/mda_pipeline.job.yml    Workflows DAG (replaces the state machines)
├── src/mda/                          shared package: config, quality (expectations), transforms, forecast
│   └── tasks/                        one module per DAG task
├── src/notebooks/                    thin notebook wrappers (+ voltvar_feature.sql for the SQL warehouse)
├── scripts/run_e2e.sh                UC setup (parallel) + sample data + bundle deploy + jobs run-now
├── scripts/generate_sample_data.py   landing-zone sample (JSON staging, historic CSV, grid topology)
├── scripts/local_run.py              runs the same DAG locally (PySpark + Delta + MLflow/sqlite)
└── tests/                            pytest: transforms, DAG semantics, bundle JSON schema, run_e2e.sh
```

## DAG

```
ingest_bronze ──> silver_transform ──┬─> check_run_anomaly ─(true)─> anomaly_detection
ingest_topology ─────────────────────┼─> voltvar_feature        (SQL warehouse)
                                     └─> gold_aggregates ─> check_run_training ─(true)─> ml_training
                                                        └───────────────┬───────────────────┘
                                                      check_run_inference (run_if NONE_FAILED) ─(true)─> batch_inference
```

- `ingest_bronze` and `ingest_topology` start together; after `silver_transform` the job fans out
  to `anomaly_detection`, `voltvar_feature` and `gold_aggregates`, which run concurrently on the
  shared `etl_cluster` / separate `ml_cluster` job clusters.
- `ml_training` depends only on `gold_aggregates` (its features), not on anomaly or Volt-VAR.
- `check_run_inference` uses `run_if: NONE_FAILED`, so inference also runs when training is
  skipped (`run_training=FALSE`) and scores with the current `@champion` model, like the AWS
  orchestrator running `PrepareInference` with an existing endpoint.
- Inside tasks, independent writes run in parallel too (two Auto Loader streams in
  `ingest_bronze`, seven topology entities in `ingest_topology`, hourly/daily aggregates in
  `gold_aggregates`).

## AWS → Databricks mapping

| AWS (this repo) | Databricks |
|---|---|
| S3 inbound/staging buckets, Firehose `readings/json` | UC volume `<catalog>.bronze.landing` (`meter_readings/`, `historic/`, `topology/`) |
| `QueueStatusStateMachine` (polls inbound SQS, then starts `StagingGlueWorkflow`) | removed: Auto Loader checkpoints track new files; job `file_arrival` trigger on the landing volume |
| `meterreadings-inbound-historic-job.py` (wide CSV → staging) | `ingest_bronze` (CSV stream) + `unpivot_historic` in `silver_transform` |
| Glue staging DB | `<catalog>.bronze.meter_readings_raw`, `meter_readings_historic_raw` (all strings + `_rescued_data`, `_source_file`, `_ingested_at`) |
| `meterreadings-integration-job.py` (ApplyMapping → integrated parquet) | `silver_transform` → `<catalog>.silver.meter_readings` (typed, liquid clustered) |
| `meterreadings-late-reads-cleanup-job.py` + late-arriving SQS queue | Delta `MERGE` on `(meter_id, reading_type, reading_date_time)` in `silver_transform` |
| topology Lambda → integrated parquet | `ingest_topology` → `bronze/silver.topology_<entity>` |
| Volt-VAR Lambdas + Athena/PyAthena + `VoltVarInputQueue`/`VoltVarCalculationQueue` + DynamoDB | `voltvar_feature.sql` on SQL warehouse → `<catalog>.gold.voltvar_results` (same CVR formulas) |
| `mlpipeline-anomaly-detection-job.py` (Prophet) | `anomaly_detection` (same Prophet model, `applyInPandas`) → `gold.anomaly_results` |
| SSM `/mlpipeline/<stack>/Anomaly/LastDatetime` + S3 date tracker | `gold.anomaly_date_tracker` (Delta) |
| `MLPipelineModelTrainingStateMachine`: PrepareTraining + SageMaker DeepAR + CreateModel/Endpoint | `ml_training`: MLflow run + UC model `<catalog>.gold.<model_name>`, alias `champion` |
| PrepareInference + SageMaker Batch Transform + StoreResults | `batch_inference`: `models:/…@champion` as Spark UDF → `gold.forecast_results` (MERGE) |
| Glue tables `meter_anomaly_results`, `meter_forecast_results`; Athena | Delta tables in `gold`, queried from SQL warehouse |
| CloudWatch / Step Functions execution history | Workflows run history, task values, MLflow experiment |

Gold tables written by `gold_aggregates`: `meter_readings_hourly`, `meter_consumption_daily`,
`transformer_hourly`.

### SSM parameters → job parameters

| SSM Parameter Store | Job parameter | Effect |
|---|---|---|
| `/mlpipeline/<stack>/Steps/Anomaly` | `run_anomaly` (`TRUE`/`FALSE`) | `check_run_anomaly` condition task gates `anomaly_detection` |
| `/mlpipeline/<stack>/Steps/Training` | `run_training` | `check_run_training` gates `ml_training` |
| `/mlpipeline/<stack>/Steps/Inference` | `run_inference` | `check_run_inference` gates `batch_inference` |
| `/mlpipeline/<stack>/Forecast/ForecastPeriod` | `forecast_period` (days) | forecast horizon = days × 24 h |
| `/mlpipeline/<stack>/Forecast/TrainingTimeFrame` | `training_time_frame` (days) | training window |
| `/mlpipeline/<stack>/Forecast/ModelName` | `model_name` | UC model `<catalog>.gold.<model_name>` |
| `/mlpipeline/<stack>/Forecast/EndpointName` | `model_alias` | UC alias replaces the endpoint |
| `/mlpipeline/<stack>/ML/UseWeather` | `use_weather` | passed through (unused, as in the Glue jobs) |
| stack name / Glue DB names | `catalog` | one catalog per environment (`mda_dev`, `mda_prod`) |

Parameters are set with defaults in the job and overridden per run
(`databricks jobs run-now --json '{"job_id":…, "job_parameters":{"run_anomaly":"TRUE"}}'`).

### SQS / DLQ → expectations / quarantine

The rules in `src/mda/quality.py` use the `{name: SQL constraint}` shape of
`@dlt.expect_all_or_drop`, but failing rows are written to a quarantine table with the list of
failed rules instead of being dropped, which is what an SQS DLQ used to hold.

| AWS | Expectations | Quarantine table |
|---|---|---|
| inbound/late-arriving SQS + DLQ for malformed readings | `valid_meter_id`, `valid_reading_type`, `valid_reading_value`, `valid_reading_date_time` | `silver.meter_readings_quarantine` (raw value/timestamp kept) |
| topology files | `valid_id` | `silver.topology_quarantine` |
| `VoltVarInputDLQ` / `VoltVarCalculationDLQ` (`maxReceiveCount: 2`) | `has_vltg_readings_in_window`, `positive_mean_voltage` | `gold.voltvar_quarantine` |

Retries that SQS gave through `maxReceiveCount` are `max_retries` on the task.

## Run

Prerequisites: Databricks CLI ≥ 0.218 authenticated (`databricks auth login` or
`DATABRICKS_HOST`/`DATABRICKS_TOKEN`), a UC metastore with `CREATE CATALOG` permission,
a SQL warehouse, `jq`, `python3`.

```bash
cd databricks
./scripts/run_e2e.sh -t dev -c mda_dev -w <warehouse_id>          # all steps on, sample data
./scripts/run_e2e.sh -c mda_dev --run-training FALSE --no-sample-data
DRY_RUN=TRUE ./scripts/run_e2e.sh                                 # print the commands only
```

`run_e2e.sh` steps:
1. `CREATE CATALOG` (optionally `CATALOG_MANAGED_LOCATION`), then the three schemas in parallel,
   then the volumes `bronze.landing`, `bronze.checkpoints`, `gold.artifacts` in parallel
   (SQL Statement Execution API).
2. Generates sample data and uploads the three landing folders in parallel.
3. `databricks bundle validate` + `bundle deploy` with `--var catalog=… --var warehouse_id=…`.
4. `databricks jobs run-now` with the job parameters; waits and prints the per-task result.

## Local verification

```bash
pip install pyspark==3.5.3 delta-spark==3.2.1 mlflow prophet scikit-learn pyyaml pytest jsonschema setuptools
python -m pytest databricks/tests -q
python databricks/scripts/generate_sample_data.py --out /tmp/landing
python databricks/scripts/local_run.py --workdir /tmp/mda --landing /tmp/landing --param run_anomaly=TRUE --param run_training=TRUE --param run_inference=TRUE
```

`local_run.py` executes the DAG from `mda_pipeline.job.yml` with Workflows semantics
(dependencies, condition outcomes, `run_if`) in a thread pool. It differs from Databricks in:
plain file streaming instead of Auto Loader, `spark_catalog` instead of a UC catalog, MLflow on
sqlite instead of the UC registry.

## Notes and limits

- The forecaster replaces SageMaker DeepAR with a scikit-learn gradient-boosting model (mean +
  p10/p50/p90, the quantiles DeepAR returned), logged as an MLflow pyfunc. DeepAR itself is not
  available on Databricks; swapping in another model only touches `src/mda/forecast.py`.
- Volt-VAR constants (set point, CVR factor, prices) were hard-coded in the calculator Lambda;
  they are now `base_parameters` of the `voltvar_feature` task.
- Weather integration (`weatherdata-integration-job.py`) and the Timestream export are not part
  of this DAG.
