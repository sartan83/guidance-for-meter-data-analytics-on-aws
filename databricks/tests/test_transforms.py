from pyspark.sql import functions as F

from mda.config import TaskParams, as_bool
from mda.quality import READINGS_EXPECTATIONS, split_by_expectations
from mda.transforms import HISTORIC_READING_SCHEMA, STAGING_READING_SCHEMA, latest_per_key, normalize_readings, unpivot_historic


def test_flags_replace_ssm_values():
    assert as_bool("TRUE") and as_bool("true") and as_bool("1")
    assert not as_bool("FALSE") and not as_bool("") and not as_bool(None)
    p = TaskParams({"catalog": "c1", "run_anomaly": "FALSE", "forecast_period": ""})
    assert p.flag("run_anomaly") is False
    assert p.int("forecast_period") == 7  # empty -> default, like a missing SSM parameter
    assert p.layout.silver("meter_readings") == "c1.silver.meter_readings"
    assert p.layout.landing == "/Volumes/c1/bronze/landing"
    assert p.registered_model == "c1.gold.mda_forecast"


def _staging(spark, rows):
    cols = [f.name for f in STAGING_READING_SCHEMA.fields]
    full = [tuple(r.get(c) for c in cols) for r in rows]
    return spark.createDataFrame(full, STAGING_READING_SCHEMA).withColumn("_source_file", F.lit("f")).withColumn("_ingested_at", F.current_timestamp())


def test_normalize_and_quarantine(spark):
    df = _staging(
        spark,
        [
            {"meter_id": "m1", "reading_type": "kw", "reading_value": "1.5", "reading_date_time": "2026-10-01 10:00:00.000"},
            {"meter_id": "", "reading_type": "kw", "reading_value": "1.0", "reading_date_time": "2026-10-01 10:00:00.000"},
            {"meter_id": "m1", "reading_type": "kw", "reading_value": "n/a", "reading_date_time": "2026-10-01 11:00:00.000"},
            {"meter_id": "m1", "reading_type": "kw", "reading_value": "2.0", "reading_date_time": "not-a-date"},
        ],
    )
    valid, quarantine = split_by_expectations(normalize_readings(df, keep_raw=True), READINGS_EXPECTATIONS)
    v = valid.collect()
    assert len(v) == 1 and v[0].reading_value == 1.5 and (v[0].year, v[0].month, v[0].day, v[0].hour) == (2026, 10, 1, 10)
    failed = sorted(r._failed_expectations[0] for r in quarantine.collect())
    assert failed == ["valid_meter_id", "valid_reading_date_time", "valid_reading_value"]
    assert {r._raw_reading_value for r in quarantine.collect()} >= {"n/a"}


def test_unpivot_historic_matches_glue_job(spark):
    cols = [f.name for f in HISTORIC_READING_SCHEMA.fields]
    row = {"device_id": "m9", "time": "2026-01-01 00:00:00.000", "load": "1", "crrnt": "2", "pf": "0.9", "kva": "3", "kw": "4", "vltg": "0.22"}
    df = spark.createDataFrame([tuple(row.get(c) for c in cols)], HISTORIC_READING_SCHEMA).withColumn("_source_file", F.lit("f")).withColumn("_ingested_at", F.current_timestamp())
    out = {r.reading_type: r.reading_value for r in unpivot_historic(df).collect()}
    assert out == {"load": "1", "crrnt": "2", "pf": "0.9", "kva": "3", "kw": "4", "vltg": "0.22"}


def test_latest_per_key_keeps_last_ingested(spark):
    df = spark.createDataFrame(
        [("m1", "kw", "2026-01-01 00:00:00", 1.0, 1, "a"), ("m1", "kw", "2026-01-01 00:00:00", 2.0, 2, "b")],
        "meter_id string, reading_type string, reading_date_time string, reading_value double, _ingested_at int, _source_file string",
    )
    assert [r.reading_value for r in latest_per_key(df).collect()] == [2.0]
