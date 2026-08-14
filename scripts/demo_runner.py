"""Demo-runner for the Grid Reliability Accelerator POC (Phase 6, Definition of
Done: "A demo-runner script/notebook exists that: resets the environment, starts
the stream, triggers the anomaly on command, and confirms detection appears on
the dashboard.")

**Status: drafted, not yet run against a live workspace.** Shells out to the
Databricks CLI (`databricks`) - assumes it's installed and `databricks configure`
has already been run with the workspace host + PAT/OAuth token (Hard Constraint
#3). The exact job/pipeline names referenced below (RESET_SQL, PIPELINE_NAME,
GENERATOR_JOB_NAME) are placeholders to fill in once those are actually created
in the workspace - this script documents the *sequence*, which is the part that
doesn't need a live workspace to get right; the specific resource names do.

Usage:
    python scripts/demo_runner.py reset
    python scripts/demo_runner.py start
    python scripts/demo_runner.py trigger --device-id dev_0005 --fault-type transient
    python scripts/demo_runner.py confirm --device-id dev_0005
    python scripts/demo_runner.py full-demo --device-id dev_0005 --fault-type transient
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time

# [CONFIRM once created in the workspace] - fill these in during Phase 6 setup.
CATALOG = "grid_poc"
DLT_PIPELINE_NAME = "grid_poc_bronze_silver_gold"  # covers 10/20/40 in pipelines/
FAULT_DETECTION_JOB_NAME = "grid_poc_fault_detection_stream"  # runs pipelines/30_fault_detection_stream.py
GENERATOR_JOB_NAME = "grid_poc_telemetry_generator"  # runs src/telemetry_generator against a UC Volume
CONTROL_VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/landing/control/trigger.json"


def run_databricks_cli(args: list[str]) -> str:
    result = subprocess.run(["databricks", *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"databricks {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def cmd_reset(_args: argparse.Namespace) -> None:
    print("[demo_runner] resetting gold tables and checkpoints...")
    run_databricks_cli([
        "sql", "query",
        "--warehouse-id", "<SQL_WAREHOUSE_ID>",  # [ASK USER / CONFIRM]
        "-e", f"TRUNCATE TABLE {CATALOG}.gold.fault_classifications",
    ])
    # feeder_health/sensor_readings are DLT-managed streaming tables - a full
    # reset means a fresh pipeline run with reset, not a manual truncate:
    run_databricks_cli(["pipelines", "start-update", "--full-refresh", "--pipeline-name", DLT_PIPELINE_NAME])
    print("[demo_runner] reset complete")


def cmd_start(_args: argparse.Namespace) -> None:
    print("[demo_runner] starting bronze/silver/gold DLT pipeline...")
    run_databricks_cli(["pipelines", "start-update", "--pipeline-name", DLT_PIPELINE_NAME])
    print("[demo_runner] starting fault detection stream...")
    run_databricks_cli(["jobs", "run-now", "--job-name", FAULT_DETECTION_JOB_NAME])
    print("[demo_runner] starting telemetry generator...")
    run_databricks_cli(["jobs", "run-now", "--job-name", GENERATOR_JOB_NAME])
    print("[demo_runner] all components started - give it ~30s to warm up before triggering a fault")


def cmd_trigger(args: argparse.Namespace) -> None:
    import json
    import tempfile
    import uuid

    payload = {
        "nonce": str(uuid.uuid4()),
        "device_id": args.device_id,
        "feeder_id": args.feeder_id,
        "fault_type": args.fault_type,
        "duration_s": args.duration_s,
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(payload, f)
        local_path = f.name

    print(f"[demo_runner] writing trigger request to {CONTROL_VOLUME_PATH}: {payload}")
    run_databricks_cli(["fs", "cp", "--overwrite", local_path, f"dbfs:{CONTROL_VOLUME_PATH}"])
    print("[demo_runner] trigger sent - the running generator job polls this path each tick")


def cmd_confirm(args: argparse.Namespace) -> None:
    print(f"[demo_runner] polling gold.fault_classifications for device {args.device_id}...")
    deadline = time.time() + args.timeout_s
    while time.time() < deadline:
        out = run_databricks_cli([
            "sql", "query",
            "--warehouse-id", "<SQL_WAREHOUSE_ID>",  # [ASK USER / CONFIRM]
            "-e",
            (
                f"SELECT * FROM {CATALOG}.gold.fault_classifications "
                f"WHERE device_id = '{args.device_id}' "
                f"ORDER BY timestamp DESC LIMIT 1"
            ),
        ])
        if args.device_id in out:
            print(f"[demo_runner] CONFIRMED - detection surfaced within {args.timeout_s - (deadline - time.time()):.1f}s")
            print(out)
            return
        time.sleep(2)
    print(f"[demo_runner] NOT CONFIRMED within {args.timeout_s}s - check the pipeline/job status")
    sys.exit(1)


def cmd_full_demo(args: argparse.Namespace) -> None:
    cmd_trigger(args)
    cmd_confirm(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="demo_runner")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("reset", help="reset gold tables and do a full DLT pipeline refresh").set_defaults(func=cmd_reset)
    sub.add_parser("start", help="start the DLT pipeline, detection job, and generator job").set_defaults(func=cmd_start)

    for name, fn in [("trigger", cmd_trigger), ("full-demo", cmd_full_demo)]:
        p = sub.add_parser(name)
        p.add_argument("--device-id", default=None)
        p.add_argument("--feeder-id", default=None)
        p.add_argument("--fault-type", choices=["transient", "sustained"], default=None)
        p.add_argument("--duration-s", type=float, default=None)
        if name == "full-demo":
            p.add_argument("--timeout-s", type=float, default=15.0)
        p.set_defaults(func=fn)

    p_confirm = sub.add_parser("confirm", help="poll gold.fault_classifications until the triggered fault appears")
    p_confirm.add_argument("--device-id", required=True)
    p_confirm.add_argument("--timeout-s", type=float, default=15.0)
    p_confirm.set_defaults(func=cmd_confirm)

    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
