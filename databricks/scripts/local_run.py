#!/usr/bin/env python3
"""Run the bundle's job DAG locally (PySpark + Delta + MLflow on sqlite) for verification.

Reads resources/mda_pipeline.job.yml and applies Databricks Workflows semantics:
depends_on, condition_task outcomes, run_if (ALL_SUCCESS / NONE_FAILED), parameter
substitution ({{job.parameters.x}}, {{job.run_id}}). Independent tasks run concurrently
in a thread pool sharing one SparkSession, like tasks sharing a job cluster.
Auto Loader is replaced by the plain file streaming source (use_autoloader=FALSE).
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

import yaml

BUNDLE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BUNDLE_DIR / "src"))

JOB_FILE = BUNDLE_DIR / "resources" / "mda_pipeline.job.yml"


def load_job(catalog: str) -> dict:
    raw = JOB_FILE.read_text().replace("${var.catalog}", catalog)
    return yaml.safe_load(raw)["resources"]["jobs"]["mda_pipeline"]


def substitute(value: str, job_params: dict, run_id: str) -> str:
    value = value.replace("{{job.run_id}}", run_id)
    return re.sub(r"\{\{job\.parameters\.(\w+)\}\}", lambda m: str(job_params[m.group(1)]), value)


def spark_session(warehouse: Path):
    from pyspark.sql import SparkSession

    # Arrow in Spark 3.5 (applyInPandas / pandas UDFs) fails on Java 21; DBR uses JDK 17 too.
    release = Path(os.environ.get("JAVA_HOME", "/nonexistent")) / "release"
    java_version = re.search(r'JAVA_VERSION="(\d+)', release.read_text()) if release.exists() else None
    if java_version is None or int(java_version.group(1)) >= 21:
        jdk17 = sorted(Path("/usr/lib/jvm").glob("java-17-openjdk*")) if Path("/usr/lib/jvm").exists() else []
        if jdk17:
            os.environ["JAVA_HOME"] = str(jdk17[0])
    return (
        SparkSession.builder.master("local[4]")
        .appName("mda-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse / "warehouse"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={warehouse / 'metastore_db'};create=true")
        .enableHiveSupport()
        .getOrCreate()
    )


def run_sql_notebook(spark, path: Path, args: dict) -> dict:
    cells = path.read_text().split("-- COMMAND ----------")
    executed = 0
    for cell in cells:
        stmt = "\n".join(l for l in cell.splitlines() if not l.lstrip().startswith("-- ")).strip().rstrip(";")
        if stmt:
            used = {k: v for k, v in args.items() if re.search(rf":{k}\b", stmt)}
            spark.sql(stmt, args=used).collect()
            executed += 1
    return {"statements": executed}


def make_runner(spark, job_params: dict, run_id: str):
    from mda.config import TaskParams

    def run_task(task: dict):
        nb = task["notebook_task"]
        base = {k: substitute(str(v), job_params, run_id) for k, v in (nb.get("base_parameters") or {}).items()}
        params = {**job_params, **base}
        path = (JOB_FILE.parent / nb["notebook_path"]).resolve()
        if path.suffix == ".sql":
            return run_sql_notebook(spark, path, params)
        module = __import__(f"mda.tasks.{path.stem}", fromlist=["run"])
        return module.run(spark, TaskParams(params))

    return run_task


def schedule(tasks: list[dict], run_task, job_params: dict, run_id: str, max_workers: int = 8) -> dict:
    """Workflow semantics; returns {task_key: {state, outcome?, result?, seconds}}."""
    by_key = {t["task_key"]: t for t in tasks}
    status: dict[str, dict] = {}
    running = {}
    t0 = time.time()

    def ready_state(task: dict) -> str | None:
        """None = wait, 'RUN', or a terminal skip state."""
        deps = task.get("depends_on") or []
        if any(d["task_key"] not in status for d in deps):
            return None
        states = []
        for d in deps:
            s = status[d["task_key"]]
            if s["state"] == "SUCCESS" and "outcome" in d and s.get("outcome") != d["outcome"]:
                states.append("EXCLUDED")
            else:
                states.append(s["state"])
        run_if = task.get("run_if", "ALL_SUCCESS")
        if run_if == "ALL_SUCCESS":
            if all(s == "SUCCESS" for s in states):
                return "RUN"
            return "UPSTREAM_FAILED" if any(s in ("FAILED", "UPSTREAM_FAILED") for s in states) else "EXCLUDED"
        if run_if == "NONE_FAILED":
            if any(s in ("FAILED", "UPSTREAM_FAILED") for s in states):
                return "UPSTREAM_FAILED"
            return "RUN" if any(s == "SUCCESS" for s in states) or not states else "EXCLUDED"
        if run_if == "ALL_DONE":
            return "RUN"
        raise ValueError(f"unsupported run_if {run_if}")

    def log(key: str, msg: str):
        print(f"[{time.time() - t0:7.1f}s] {key:<20} {msg}", flush=True)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        while len(status) < len(tasks):
            for key, task in by_key.items():
                if key in status or key in running.values():
                    continue
                decision = ready_state(task)
                if decision is None:
                    continue
                if decision != "RUN":
                    status[key] = {"state": decision, "seconds": 0.0}
                    log(key, decision)
                    continue
                if "condition_task" in task:
                    c = task["condition_task"]
                    left, right = substitute(c["left"], job_params, run_id), substitute(c["right"], job_params, run_id)
                    assert c["op"] == "EQUAL_TO", c["op"]
                    outcome = "true" if left == right else "false"
                    status[key] = {"state": "SUCCESS", "outcome": outcome, "seconds": 0.0}
                    log(key, f"condition {left!r} == {right!r} -> {outcome}")
                    continue
                log(key, "START")
                running[pool.submit(lambda t=task: (time.time(), run_task(t)))] = key
            if not running:
                continue
            done, _ = wait(list(running), return_when=FIRST_COMPLETED)
            for fut in done:
                key = running.pop(fut)
                try:
                    started, result = fut.result()
                    status[key] = {"state": "SUCCESS", "result": result, "seconds": round(time.time() - started, 1)}
                    log(key, f"SUCCESS {result}")
                except Exception as exc:  # noqa: BLE001
                    status[key] = {"state": "FAILED", "error": repr(exc), "seconds": 0.0}
                    log(key, f"FAILED {exc!r}")
    return status


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", required=True, help="local root for Volumes, warehouse and mlflow")
    ap.add_argument("--landing", help="sample data dir (generate_sample_data.py --out); copied into the landing volume")
    ap.add_argument("--param", action="append", default=[], help="job parameter override name=value")
    ap.add_argument("--run-id", default=str(int(time.time())))
    ap.add_argument("--report", help="write JSON status report here")
    args = ap.parse_args()

    import shutil

    work = Path(args.workdir).resolve()
    catalog = "spark_catalog"
    job = load_job(catalog)
    job_params = {p["name"]: str(p["default"]) for p in job["parameters"]}
    job_params.update(
        {
            "use_autoloader": "FALSE",
            "volumes_root": str(work / "Volumes"),
            "registry_uri": f"sqlite:///{work / 'mlflow.db'}",
            "experiment_path": "mda_forecast_local",
        }
    )
    for kv in args.param:
        k, v = kv.split("=", 1)
        job_params[k] = v
    os.environ.setdefault("MLFLOW_TRACKING_URI", job_params["registry_uri"])

    landing = work / "Volumes" / catalog / "bronze" / "landing"
    if args.landing:
        shutil.copytree(args.landing, landing, dirs_exist_ok=True)
    spark = spark_session(work)
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{schema}")

    print(f"job parameters: {json.dumps(job_params)}")
    status = schedule(job["tasks"], make_runner(spark, job_params, args.run_id), job_params, args.run_id)
    if args.report:
        Path(args.report).write_text(json.dumps(status, indent=2, default=str))
    failed = [k for k, s in status.items() if s["state"] in ("FAILED", "UPSTREAM_FAILED")]
    print("\nSUMMARY")
    for k, s in status.items():
        print(f"  {k:<20} {s['state']:<16} {s.get('outcome', '')}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
