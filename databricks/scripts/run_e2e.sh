#!/usr/bin/env bash
# End-to-end deploy + run of the MDA medallion pipeline on Databricks (Unity Catalog).
#
#   1. create catalog, then schemas bronze/silver/gold in parallel, then volumes in parallel
#   2. (optional) generate sample data and upload landing folders to the volume in parallel
#   3. databricks bundle validate + deploy
#   4. databricks jobs run-now with job parameters (replace SSM /mlpipeline/<stack>/Steps/*)
#
# Requires: databricks CLI >= 0.218 (authenticated via profile or DATABRICKS_HOST/TOKEN), jq, python3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUNDLE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TARGET="${TARGET:-dev}"
CATALOG="${CATALOG:-mda_dev}"
WAREHOUSE_ID="${WAREHOUSE_ID:-}"
CATALOG_MANAGED_LOCATION="${CATALOG_MANAGED_LOCATION:-}"
RUN_ANOMALY="${RUN_ANOMALY:-TRUE}"
RUN_TRAINING="${RUN_TRAINING:-TRUE}"
RUN_INFERENCE="${RUN_INFERENCE:-TRUE}"
FORECAST_PERIOD="${FORECAST_PERIOD:-7}"
TRAINING_TIME_FRAME="${TRAINING_TIME_FRAME:-395}"
MODEL_NAME="${MODEL_NAME:-mda_forecast}"
SAMPLE_DATA="${SAMPLE_DATA:-TRUE}"
SAMPLE_DIR="${SAMPLE_DIR:-$BUNDLE_DIR/.sample_data}"
RUN_TIMEOUT="${RUN_TIMEOUT:-2h0m0s}"
NO_WAIT="${NO_WAIT:-FALSE}"
DRY_RUN="${DRY_RUN:-FALSE}"
DATABRICKS_CLI="${DATABRICKS_CLI:-databricks}"

usage() {
  cat <<USAGE
Usage: $0 [-t target] [-c catalog] [-w warehouse_id] [-p profile] [--no-sample-data] [--no-wait] [--dry-run]
          [--run-anomaly TRUE|FALSE] [--run-training TRUE|FALSE] [--run-inference TRUE|FALSE]
Environment variables with the same names (TARGET, CATALOG, WAREHOUSE_ID, RUN_ANOMALY, ...) are also honoured.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--target) TARGET="$2"; shift 2 ;;
    -c|--catalog) CATALOG="$2"; shift 2 ;;
    -w|--warehouse-id) WAREHOUSE_ID="$2"; shift 2 ;;
    -p|--profile) export DATABRICKS_CONFIG_PROFILE="$2"; shift 2 ;;
    --run-anomaly) RUN_ANOMALY="$2"; shift 2 ;;
    --run-training) RUN_TRAINING="$2"; shift 2 ;;
    --run-inference) RUN_INFERENCE="$2"; shift 2 ;;
    --no-sample-data) SAMPLE_DATA=FALSE; shift ;;
    --no-wait) NO_WAIT=TRUE; shift ;;
    --dry-run) DRY_RUN=TRUE; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

LOG_DIR="$(mktemp -d)"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

dbx() {
  if [[ "$DRY_RUN" == "TRUE" ]]; then
    printf 'DRY-RUN: databricks %s\n' "$*" >&2
    return 0
  fi
  "$DATABRICKS_CLI" "$@"
}

for bin in jq python3; do command -v "$bin" >/dev/null || { echo "missing dependency: $bin" >&2; exit 1; }; done
[[ "$DRY_RUN" == "TRUE" ]] || command -v "$DATABRICKS_CLI" >/dev/null || { echo "missing databricks CLI" >&2; exit 1; }

# Run each "name::command" in the background, wait for all, fail if any failed.
parallel() {
  local pids=() names=() spec name rc=0
  for spec in "$@"; do
    name="${spec%%::*}"
    ( eval "${spec#*::}" ) >"$LOG_DIR/$name.log" 2>&1 &
    pids+=("$!"); names+=("$name")
  done
  for i in "${!pids[@]}"; do
    if wait "${pids[$i]}"; then
      log "  ok   ${names[$i]}"
    else
      log "  FAIL ${names[$i]}"; sed 's/^/       /' "$LOG_DIR/${names[$i]}.log" >&2; rc=1
    fi
  done
  return $rc
}

resolve_warehouse() {
  if [[ -z "$WAREHOUSE_ID" ]]; then
    if [[ "$DRY_RUN" == "TRUE" ]]; then WAREHOUSE_ID="dry-run-warehouse"; return; fi
    WAREHOUSE_ID="$(dbx warehouses list -o json | jq -r '[.[] | select(.state == "RUNNING")][0].id // .[0].id // empty')"
    [[ -n "$WAREHOUSE_ID" ]] || { echo "no SQL warehouse found: pass -w <warehouse_id>" >&2; exit 1; }
  fi
  log "SQL warehouse: $WAREHOUSE_ID"
}

# Execute one SQL statement on the warehouse and fail unless it SUCCEEDED.
sql() {
  local statement="$1" body resp state
  body="$(jq -nc --arg s "$statement" --arg w "$WAREHOUSE_ID" '{statement: $s, warehouse_id: $w, wait_timeout: "50s", on_wait_timeout: "CONTINUE"}')"
  if [[ "$DRY_RUN" == "TRUE" ]]; then printf 'DRY-RUN SQL: %s\n' "$statement" >&2; return 0; fi
  resp="$(dbx api post /api/2.0/sql/statements --json "$body")"
  state="$(jq -r '.status.state' <<<"$resp")"
  local id; id="$(jq -r '.statement_id' <<<"$resp")"
  while [[ "$state" == "PENDING" || "$state" == "RUNNING" ]]; do
    sleep 2
    resp="$(dbx api get "/api/2.0/sql/statements/$id")"
    state="$(jq -r '.status.state' <<<"$resp")"
  done
  if [[ "$state" != "SUCCEEDED" ]]; then
    echo "SQL failed ($state): $statement" >&2
    jq -r '.status.error.message // empty' <<<"$resp" >&2
    return 1
  fi
  echo "$statement -> $state"
}
export -f sql dbx
export WAREHOUSE_ID DRY_RUN DATABRICKS_CLI

step_unity_catalog() {
  log "1/4 Unity Catalog objects in catalog '$CATALOG'"
  local location=""
  [[ -n "$CATALOG_MANAGED_LOCATION" ]] && location=" MANAGED LOCATION '$CATALOG_MANAGED_LOCATION'"
  sql "CREATE CATALOG IF NOT EXISTS \`$CATALOG\`$location COMMENT 'Meter Data Analytics (migrated from AWS)'" >/dev/null
  log "  ok   catalog"
  parallel \
    "schema_bronze::sql \"CREATE SCHEMA IF NOT EXISTS \\\`$CATALOG\\\`.bronze COMMENT 'Raw ingested data (Auto Loader)'\"" \
    "schema_silver::sql \"CREATE SCHEMA IF NOT EXISTS \\\`$CATALOG\\\`.silver COMMENT 'Typed, deduplicated readings + quarantine'\"" \
    "schema_gold::sql \"CREATE SCHEMA IF NOT EXISTS \\\`$CATALOG\\\`.gold COMMENT 'Aggregates, features, ML results, UC models'\""
  parallel \
    "volume_landing::sql \"CREATE VOLUME IF NOT EXISTS \\\`$CATALOG\\\`.bronze.landing COMMENT 'Landing zone (former S3 inbound bucket)'\"" \
    "volume_checkpoints::sql \"CREATE VOLUME IF NOT EXISTS \\\`$CATALOG\\\`.bronze.checkpoints COMMENT 'Auto Loader / streaming checkpoints'\"" \
    "volume_artifacts::sql \"CREATE VOLUME IF NOT EXISTS \\\`$CATALOG\\\`.gold.artifacts COMMENT 'ML artifacts and exports'\""
}

step_sample_data() {
  [[ "$SAMPLE_DATA" == "TRUE" ]] || { log "2/4 sample data skipped"; return; }
  log "2/4 sample data -> /Volumes/$CATALOG/bronze/landing"
  python3 "$SCRIPT_DIR/generate_sample_data.py" --out "$SAMPLE_DIR" | tee "$LOG_DIR/sample.json"
  local specs=() sub
  for sub in meter_readings historic topology; do
    specs+=("upload_$sub::dbx fs cp -r --overwrite '$SAMPLE_DIR/$sub' 'dbfs:/Volumes/$CATALOG/bronze/landing/$sub'")
  done
  parallel "${specs[@]}"
}

step_deploy() {
  log "3/4 bundle validate + deploy (target=$TARGET)"
  local vars=(--var "catalog=$CATALOG" --var "warehouse_id=$WAREHOUSE_ID")
  (cd "$BUNDLE_DIR" && dbx bundle validate -t "$TARGET" "${vars[@]}" >/dev/null && dbx bundle deploy -t "$TARGET" "${vars[@]}")
  if [[ "$DRY_RUN" == "TRUE" ]]; then JOB_ID="0"; else
    JOB_ID="$(cd "$BUNDLE_DIR" && dbx bundle summary -t "$TARGET" "${vars[@]}" -o json | jq -r '.resources.jobs.mda_pipeline.id')"
  fi
  [[ -n "$JOB_ID" && "$JOB_ID" != "null" ]] || { echo "could not resolve job id" >&2; exit 1; }
  log "  job id: $JOB_ID"
}

step_run() {
  log "4/4 jobs run-now (run_anomaly=$RUN_ANOMALY run_training=$RUN_TRAINING run_inference=$RUN_INFERENCE)"
  local body
  body="$(jq -nc --argjson job_id "$JOB_ID" \
    --arg catalog "$CATALOG" --arg ra "$RUN_ANOMALY" --arg rt "$RUN_TRAINING" --arg ri "$RUN_INFERENCE" \
    --arg fp "$FORECAST_PERIOD" --arg ttf "$TRAINING_TIME_FRAME" --arg mn "$MODEL_NAME" \
    '{job_id: $job_id, job_parameters: {catalog: $catalog, run_anomaly: $ra, run_training: $rt, run_inference: $ri,
      forecast_period: $fp, training_time_frame: $ttf, model_name: $mn}}')"
  if [[ "$NO_WAIT" == "TRUE" ]]; then
    dbx jobs run-now --no-wait --json "$body"
    return
  fi
  local out="$LOG_DIR/run.json"
  if dbx jobs run-now --timeout "$RUN_TIMEOUT" --json "$body" >"$out"; then rc=0; else rc=$?; fi
  [[ "$DRY_RUN" == "TRUE" ]] && return 0
  jq -r '"run \(.run_id): \(.state.life_cycle_state) / \(.state.result_state // "-")  \(.run_page_url)"' "$out"
  jq -r '.tasks | sort_by(.start_time) | .[] | "  \(.task_key | . + " " * (22 - length)) \(.state.result_state // .state.life_cycle_state)"' "$out"
  return "$rc"
}

resolve_warehouse
step_unity_catalog
step_sample_data
step_deploy
step_run
log "done (logs: $LOG_DIR)"
