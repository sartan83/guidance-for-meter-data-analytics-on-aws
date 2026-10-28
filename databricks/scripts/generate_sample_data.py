#!/usr/bin/env python3
"""Generate a deterministic landing-zone sample for the MDA pipeline.

Layout written under --out (mirrors /Volumes/<catalog>/bronze/landing):
  meter_readings/readings-<date>.jsonl  staging records (HES/adapter -> Firehose format)
  historic/historic-readings.csv        wide historic CSV (meterreadings-inbound-historic-job input)
  topology/<entity>.json                copied from showcase/data/grid_topology
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import shutil
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPOLOGY_DIR = REPO_ROOT / "showcase" / "data" / "grid_topology"
READING_TYPES = ["load", "crrnt", "pf", "kva", "kw", "vltg"]


def pick_meters(n_meters: int, skip_transformers: int) -> list[str]:
    meters = json.loads((TOPOLOGY_DIR / "smart_meter.json").read_text())
    st_to_dt = {st["id"]: st["distributionTransformerId"] for st in json.loads((TOPOLOGY_DIR / "service_transformer.json").read_text())}
    transformers = [dt["id"] for dt in json.loads((TOPOLOGY_DIR / "distribution_transformer.json").read_text())]
    covered = transformers[: max(1, len(transformers) - skip_transformers)]
    by_dt: dict[str, list[str]] = defaultdict(list)
    for m in meters:
        dt = st_to_dt.get(m["serviceTransformerId"])
        if dt in covered:
            by_dt[dt].append(m["id"])
    picked: list[str] = []
    while len(picked) < n_meters and any(by_dt.values()):
        for dt in covered:
            if by_dt[dt] and len(picked) < n_meters:
                picked.append(by_dt[dt].pop(0))
    return picked


def reading(rng: random.Random, meter_idx: int, ts: datetime, anomaly: bool) -> dict[str, float]:
    base = 1.0 + 0.15 * meter_idx
    daily = 0.6 + 0.4 * math.sin((ts.hour - 6) / 24 * 2 * math.pi)
    weekly = 1.15 if ts.weekday() >= 5 else 1.0
    kw = max(0.05, base * daily * weekly + rng.gauss(0, 0.05))
    if anomaly:
        kw *= 4.0
    vltg = 0.2205 + rng.gauss(0, 0.0015)
    return {
        "load": round(kw * 0.9, 4),
        "crrnt": round(kw / vltg, 4),
        "pf": round(0.9 + rng.random() * 0.08, 4),
        "kva": round(kw / 0.95, 4),
        "kw": round(kw, 4),
        "vltg": round(vltg, 5),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--meters", type=int, default=12)
    ap.add_argument("--days", type=int, default=45)
    ap.add_argument("--historic-days", type=int, default=15, help="oldest days written as historic CSV")
    ap.add_argument("--end", default="", help="last hour (ISO); default: now truncated to the hour")
    ap.add_argument("--interval-minutes", type=int, default=60)
    ap.add_argument("--skip-transformers", type=int, default=1, help="transformers without meters (Volt-VAR quarantine demo)")
    ap.add_argument("--bad-records", type=int, default=5, help="malformed staging records (silver quarantine demo)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    out = Path(args.out)
    for sub in ("meter_readings", "historic", "topology"):
        (out / sub).mkdir(parents=True, exist_ok=True)
    for f in TOPOLOGY_DIR.glob("*.json"):
        shutil.copy(f, out / "topology" / f.name)

    end = datetime.fromisoformat(args.end) if args.end else datetime.now().replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=args.days) + timedelta(minutes=args.interval_minutes)
    historic_end = start + timedelta(days=args.historic_days)
    meters = pick_meters(args.meters, args.skip_transformers)
    anomaly_days = {(start + timedelta(days=rng.randrange(args.days))).date() for _ in range(3)}

    files: dict[str, list[str]] = defaultdict(list)
    historic_rows = []
    ts, n_json = start, 0
    while ts <= end:
        for i, meter in enumerate(meters):
            values = reading(rng, i, ts, anomaly=(ts.date() in anomaly_days and i == 0))
            time_str = ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            if ts < historic_end:
                historic_rows.append({"device_id": meter, "time": time_str, **values})
                continue
            day = ts.strftime("%Y%m%d")
            for rtype, value in values.items():
                files[day].append(json.dumps({"meter_id": meter, "reading_type": rtype, "reading_value": value, "reading_date_time": time_str, "reading_source": "A"}))
                n_json += 1
        ts += timedelta(minutes=args.interval_minutes)

    last_day = max(files)
    for k in range(args.bad_records):
        bad = [
            {"meter_id": "", "reading_type": "kw", "reading_value": 1.0, "reading_date_time": end.isoformat(sep=" ")},
            {"meter_id": meters[0], "reading_type": "kw", "reading_value": "n/a", "reading_date_time": end.isoformat(sep=" ")},
            {"meter_id": meters[0], "reading_type": "kw", "reading_value": 1.0, "reading_date_time": "not-a-date"},
        ][k % 3]
        files[last_day].append(json.dumps(bad))
    for day, lines in files.items():
        (out / "meter_readings" / f"readings-{day}.jsonl").write_text("\n".join(lines) + "\n")

    with open(out / "historic" / "historic-readings.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["device_id", "time", *READING_TYPES])
        w.writeheader()
        w.writerows(historic_rows)

    print(json.dumps({
        "out": str(out), "meters": len(meters), "start": str(start), "end": str(end),
        "json_records": n_json, "historic_rows": len(historic_rows), "bad_records": args.bad_records,
        "anomaly_days": sorted(str(d) for d in anomaly_days),
    }))


if __name__ == "__main__":
    main()
