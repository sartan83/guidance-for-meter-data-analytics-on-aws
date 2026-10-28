-- Databricks notebook source
-- MAGIC %md
-- MAGIC # MDA pipeline - `voltvar_feature` (SQL warehouse)
-- MAGIC Replaces the Volt-VAR feature stack: `feature_volt_var_input` (Athena: list distribution
-- MAGIC transformers -> VoltVarInputQueue), `feature_volt_var_avg_voltage` (Athena: 1h average
-- MAGIC voltage per transformer -> VoltVarCalculationQueue) and `feature_volt_var_calculator`
-- MAGIC (CVR savings -> DynamoDB). SQS fan-out becomes set-based SQL; transformers that would
-- MAGIC have produced no message or a DLQ entry land in `gold.voltvar_quarantine`.
-- MAGIC
-- MAGIC Parameters (named markers `:name`) come from the job: `catalog`, `reference_time`, `run_id`
-- MAGIC and the CVR constants in the task's `base_parameters`. For interactive runs add text widgets
-- MAGIC with the same names.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.gold.voltvar_results') (
  id STRING,
  device_id STRING,
  reference_time TIMESTAMP,
  mean_voltage DOUBLE,
  n_readings BIGINT,
  pct_voltage_reduction DOUBLE,
  pct_energy_savings DOUBLE,
  actual_energy_savings DOUBLE,
  cost_avoided DOUBLE,
  cust_dollar_savings DOUBLE,
  carbon_reduction_lbs DOUBLE,
  run_id STRING,
  computed_at TIMESTAMP
) USING DELTA
COMMENT 'Volt-VAR CVR savings per distribution transformer (former DynamoDB table)'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.gold.voltvar_quarantine') (
  device_id STRING,
  reference_time TIMESTAMP,
  mean_voltage DOUBLE,
  n_readings BIGINT,
  failed_expectations ARRAY<STRING>,
  run_id STRING,
  quarantined_at TIMESTAMP
) USING DELTA
COMMENT 'Volt-VAR inputs rejected by expectations (former VoltVarInputDLQ / VoltVarCalculationDLQ)'

-- COMMAND ----------

INSERT INTO IDENTIFIER(:catalog || '.gold.voltvar_results')
WITH ref AS (
  SELECT coalesce(
    try_to_timestamp(nullif(:reference_time, '')),
    (SELECT max(reading_date_time) FROM IDENTIFIER(:catalog || '.silver.meter_readings') WHERE reading_type = 'vltg')
  ) AS reference_time
),
mean_voltage AS (
  SELECT
    dt.id AS device_id,
    ref.reference_time,
    avg(r.reading_value) * 1000 AS mean_voltage,
    count(r.reading_value) AS n_readings
  FROM IDENTIFIER(:catalog || '.silver.topology_distribution_transformer') dt
  CROSS JOIN ref
  LEFT JOIN IDENTIFIER(:catalog || '.silver.topology_service_transformer') st
    ON st.distribution_transformer_id = dt.id
  LEFT JOIN IDENTIFIER(:catalog || '.silver.topology_smart_meter') m
    ON m.service_transformer_id = st.id
  LEFT JOIN IDENTIFIER(:catalog || '.silver.meter_readings') r
    ON r.meter_id = m.id
   AND r.reading_type = 'vltg'
   AND r.reading_date_time BETWEEN ref.reference_time - INTERVAL 1 HOUR AND ref.reference_time
  GROUP BY dt.id, ref.reference_time
),
calc AS (
  SELECT
    *,
    round((CAST(:default_set_point AS DOUBLE) - mean_voltage) * 100 / (CAST(:default_set_point AS DOUBLE) * 12), 9) AS pct_voltage_reduction
  FROM mean_voltage
  WHERE mean_voltage IS NOT NULL AND mean_voltage > 0
),
savings AS (
  SELECT
    *,
    round(pct_voltage_reduction * CAST(:cvr AS DOUBLE), 9) AS pct_energy_savings,
    round(round(pct_voltage_reduction * CAST(:cvr AS DOUBLE), 9) * CAST(:mw_level AS DOUBLE) / 1200, 9) AS actual_energy_savings
  FROM calc
)
SELECT
  concat(device_id, '_', CAST(unix_millis(current_timestamp()) AS STRING)) AS id,
  device_id,
  reference_time,
  mean_voltage,
  n_readings,
  pct_voltage_reduction,
  pct_energy_savings,
  actual_energy_savings,
  round(CAST(:energy_purchase_price AS DOUBLE) * actual_energy_savings * 1000 / 12, 9) AS cost_avoided,
  round(CAST(:electricity_rate AS DOUBLE) * actual_energy_savings * 1000 / (12 * CAST(:num_cust AS DOUBLE)), 9) AS cust_dollar_savings,
  round(CAST(:carbon_savings_per_mwh AS DOUBLE) * actual_energy_savings / 12, 9) AS carbon_reduction_lbs,
  :run_id AS run_id,
  current_timestamp() AS computed_at
FROM savings

-- COMMAND ----------

INSERT INTO IDENTIFIER(:catalog || '.gold.voltvar_quarantine')
WITH ref AS (
  SELECT coalesce(
    try_to_timestamp(nullif(:reference_time, '')),
    (SELECT max(reading_date_time) FROM IDENTIFIER(:catalog || '.silver.meter_readings') WHERE reading_type = 'vltg')
  ) AS reference_time
),
mean_voltage AS (
  SELECT
    dt.id AS device_id,
    ref.reference_time,
    avg(r.reading_value) * 1000 AS mean_voltage,
    count(r.reading_value) AS n_readings
  FROM IDENTIFIER(:catalog || '.silver.topology_distribution_transformer') dt
  CROSS JOIN ref
  LEFT JOIN IDENTIFIER(:catalog || '.silver.topology_service_transformer') st
    ON st.distribution_transformer_id = dt.id
  LEFT JOIN IDENTIFIER(:catalog || '.silver.topology_smart_meter') m
    ON m.service_transformer_id = st.id
  LEFT JOIN IDENTIFIER(:catalog || '.silver.meter_readings') r
    ON r.meter_id = m.id
   AND r.reading_type = 'vltg'
   AND r.reading_date_time BETWEEN ref.reference_time - INTERVAL 1 HOUR AND ref.reference_time
  GROUP BY dt.id, ref.reference_time
)
SELECT
  device_id,
  reference_time,
  mean_voltage,
  n_readings,
  filter(array(
    CASE WHEN n_readings = 0 THEN 'has_vltg_readings_in_window' END,
    CASE WHEN mean_voltage IS NULL OR mean_voltage <= 0 THEN 'positive_mean_voltage' END
  ), x -> x IS NOT NULL) AS failed_expectations,
  :run_id AS run_id,
  current_timestamp() AS quarantined_at
FROM mean_voltage
WHERE mean_voltage IS NULL OR mean_voltage <= 0
