"""run_e2e.sh against a stub databricks CLI: order of operations, parallel UC creation, run-now parameters."""
import json
import os
import stat
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_e2e.sh"

STUB = r"""#!/usr/bin/env bash
echo "$(date +%s.%N) $*" >> "$STUB_LOG"
case "$1 $2" in
  "warehouses list") echo '[{"id":"wh1","state":"RUNNING"}]' ;;
  "api post") sleep 0.5; echo '{"statement_id":"s1","status":{"state":"SUCCEEDED"}}' ;;
  "bundle summary") echo '{"resources":{"jobs":{"mda_pipeline":{"id":"123"}}}}' ;;
  "jobs run-now") echo '{"run_id":9,"run_page_url":"https://x/run/9","state":{"life_cycle_state":"TERMINATED","result_state":"SUCCESS"},"tasks":[{"task_key":"ingest_bronze","start_time":1,"state":{"result_state":"SUCCESS"}}]}' ;;
esac
"""


def test_run_e2e_with_stub_cli(tmp_path):
    stub = tmp_path / "databricks"
    stub.write_text(STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    log = tmp_path / "calls.log"
    env = os.environ | {"DATABRICKS_CLI": str(stub), "STUB_LOG": str(log), "SAMPLE_DIR": str(tmp_path / "sample")}
    res = subprocess.run(
        [str(SCRIPT), "-c", "mda_test", "--run-training", "FALSE"], check=False, env=env, capture_output=True, text=True, timeout=120
    )
    assert res.returncode == 0, res.stdout + res.stderr
    calls = [line.split(" ", 1) for line in log.read_text().splitlines()]
    cmds = [c[1] for c in calls]

    sql = [(float(t), json.loads(c.split("--json ", 1)[1])["statement"]) for t, c in calls if c.startswith("api post")]
    assert sql[0][1].startswith("CREATE CATALOG IF NOT EXISTS `mda_test`")
    schemas = [t for t, s in sql if s.startswith("CREATE SCHEMA")]
    volumes = [t for t, s in sql if s.startswith("CREATE VOLUME")]
    assert len(schemas) == 3 and len(volumes) == 3
    assert max(schemas) - min(schemas) < 0.4, "schemas must be created in parallel"
    assert max(volumes) - min(volumes) < 0.4, "volumes must be created in parallel"
    assert min(volumes) > max(schemas)

    assert sum(c.startswith("fs cp -r --overwrite") for c in cmds) == 3
    deploy = next(i for i, c in enumerate(cmds) if c.startswith("bundle deploy"))
    run_now = next(i for i, c in enumerate(cmds) if c.startswith("jobs run-now"))
    assert deploy < run_now and "--var catalog=mda_test" in cmds[deploy] and "--var warehouse_id=wh1" in cmds[deploy]
    body = json.loads(cmds[run_now].split("--json ", 1)[1])
    assert body["job_id"] == 123
    assert body["job_parameters"].items() >= {"catalog": "mda_test", "run_anomaly": "TRUE", "run_training": "FALSE", "run_inference": "TRUE"}.items()
    assert "run 9: TERMINATED / SUCCESS" in res.stdout
