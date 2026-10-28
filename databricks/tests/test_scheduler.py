"""Workflow semantics of the job DAG (condition outcomes, run_if) with stub tasks."""
import threading
import time

import local_run

JOB = local_run.load_job("mda_dev")


def simulate(**flags):
    params = {p["name"]: str(p["default"]) for p in JOB["parameters"]} | flags
    started, lock, active, peak = [], threading.Lock(), [0], [0]

    def run_task(task):
        with lock:
            started.append(task["task_key"])
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.05)
        with lock:
            active[0] -= 1
        return "ok"

    status = local_run.schedule(JOB["tasks"], run_task, params, "1")
    return status, started, peak[0]


def test_all_flags_true_runs_everything_with_parallel_fan_out():
    status, started, peak = simulate(run_anomaly="TRUE", run_training="TRUE", run_inference="TRUE")
    assert all(s["state"] == "SUCCESS" for s in status.values())
    assert peak >= 2
    assert started.index("ml_training") < started.index("batch_inference")


def test_flags_false_skip_like_ssm_steps():
    status, started, _ = simulate(run_anomaly="FALSE", run_training="FALSE", run_inference="FALSE")
    for key in ("anomaly_detection", "ml_training", "batch_inference"):
        assert status[key]["state"] == "EXCLUDED"
    assert {"ingest_bronze", "silver_transform", "voltvar_feature", "gold_aggregates"} <= set(started)


def test_inference_without_training_uses_existing_champion():
    status, _, _ = simulate(run_anomaly="FALSE", run_training="FALSE", run_inference="TRUE")
    assert status["ml_training"]["state"] == "EXCLUDED"
    assert status["batch_inference"]["state"] == "SUCCESS"
