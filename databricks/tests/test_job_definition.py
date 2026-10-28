"""Static checks of the bundle: DAG shape, conditions replacing SSM flags, notebook paths, JSON schema."""
import json
import shutil
import subprocess
from pathlib import Path

import local_run
import pytest
import yaml

BUNDLE = Path(__file__).resolve().parents[1]
JOB = local_run.load_job("mda_dev")
TASKS = {t["task_key"]: t for t in JOB["tasks"]}


def deps(key):
    return {d["task_key"]: d.get("outcome") for d in TASKS[key].get("depends_on", [])}


def test_dag_shape():
    assert deps("ingest_bronze") == {} and deps("ingest_topology") == {}
    assert deps("silver_transform") == {"ingest_bronze": None}
    for fan_out in ("check_run_anomaly", "voltvar_feature", "gold_aggregates"):
        assert "silver_transform" in deps(fan_out)
    assert deps("anomaly_detection") == {"check_run_anomaly": "true"}
    assert deps("ml_training") == {"check_run_training": "true"}
    assert deps("batch_inference") == {"check_run_inference": "true"}
    assert TASKS["check_run_inference"]["run_if"] == "NONE_FAILED"


@pytest.mark.parametrize("flag", ["run_anomaly", "run_training", "run_inference"])
def test_ssm_flags_are_job_parameters_with_condition(flag):
    assert flag in {p["name"] for p in JOB["parameters"]}
    cond = TASKS[f"check_{flag}"]["condition_task"]
    assert cond == {"op": "EQUAL_TO", "left": f"{{{{job.parameters.{flag}}}}}", "right": "TRUE"}


def test_notebooks_exist_and_tasks_importable():
    for t in JOB["tasks"]:
        if "notebook_task" in t:
            path = (BUNDLE / "resources" / t["notebook_task"]["notebook_path"]).resolve()
            assert path.exists(), path
            if path.suffix == ".py":
                __import__(f"mda.tasks.{path.stem}")


def _strip_patterns(node):
    """The CLI schema uses ECMA regex (\\p{..}) that Python's re cannot compile. Drop the patterns and
    relax oneOf -> anyOf, since the `${var.x}` string alternative is only exclusive through its pattern."""
    if isinstance(node, dict):
        return {("anyOf" if k == "oneOf" else k): _strip_patterns(v) for k, v in node.items() if k != "pattern"}
    if isinstance(node, list):
        return [_strip_patterns(v) for v in node]
    return node


@pytest.mark.skipif(shutil.which("databricks") is None, reason="databricks CLI not installed")
def test_bundle_matches_cli_json_schema(tmp_path):
    jsonschema = pytest.importorskip("jsonschema")
    schema = _strip_patterns(json.loads(subprocess.run(["databricks", "bundle", "schema"], check=True, capture_output=True, text=True).stdout))
    for f in [BUNDLE / "databricks.yml", BUNDLE / "resources" / "mda_pipeline.job.yml"]:
        doc = yaml.safe_load(f.read_text())
        errors = sorted(jsonschema.Draft202012Validator(schema).iter_errors(doc), key=str)
        assert not errors, f"{f.name}: " + "; ".join(e.message[:200] for e in errors[:5])
