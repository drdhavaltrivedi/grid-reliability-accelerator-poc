"""CLI for the synthetic grid telemetry generator.

Two subcommands:

  run      Start the generator loop, ticking at --rate Hz and writing to a
           sink. Polls --control for fault-trigger requests written by a
           separate `trigger` invocation, so a fault can be injected
           on demand into an already-running stream (the live demo moment)
           without restarting it.

  trigger  Write a fault-trigger request to --control for a running `run`
           process to pick up on its next tick.

Example (two terminals):
  terminal 1: python -m telemetry_generator run --num-devices 100 --sink jsonl --out data/bronze/sensor_readings_raw/stream.jsonl
  terminal 2: python -m telemetry_generator trigger --device-id dev_0005 --fault-type sustained
"""
from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path

from .generator import TelemetryGenerator
from .sinks import build_sink


def _read_control(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def cmd_run(args: argparse.Namespace) -> None:
    gen = TelemetryGenerator(num_devices=args.num_devices, num_feeders=args.num_feeders, seed=args.seed)
    sink = build_sink(args.sink, args.out)

    control_path = Path(args.control)
    last_nonce: str | None = None

    print(
        f"[telemetry_generator] {args.num_devices} devices across {gen.num_feeders} feeders, "
        f"rate={args.rate}Hz, sink={args.sink}:{args.out}, control={control_path}",
        flush=True,
    )

    start = time.time()
    tick_count = 0
    period = 1.0 / args.rate
    try:
        while args.duration is None or (time.time() - start) < args.duration:
            tick_start = time.time()

            request = _read_control(control_path)
            if request and request.get("nonce") != last_nonce:
                last_nonce = request.get("nonce")
                fault_id = gen.trigger_fault(
                    device_id=request.get("device_id"),
                    feeder_id=request.get("feeder_id"),
                    fault_type=request.get("fault_type"),
                    duration_s=request.get("duration_s"),
                )
                print(f"[telemetry_generator] triggered fault {fault_id} from control file: {request}", flush=True)

            rows = gen.tick()
            sink.write(rows)
            tick_count += 1

            elapsed = time.time() - tick_start
            time.sleep(max(0.0, period - elapsed))
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()
        print(f"[telemetry_generator] stopped after {tick_count} ticks", flush=True)


def cmd_trigger(args: argparse.Namespace) -> None:
    control_path = Path(args.control)
    control_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "nonce": str(uuid.uuid4()),
        "device_id": args.device_id,
        "feeder_id": args.feeder_id,
        "fault_type": args.fault_type,
        "duration_s": args.duration_s,
    }
    control_path.write_text(json.dumps(payload), encoding="utf-8")
    print(f"[telemetry_generator] wrote trigger request to {control_path}: {payload}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="telemetry_generator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="start the generator loop")
    p_run.add_argument("--num-devices", type=int, default=100)
    p_run.add_argument("--num-feeders", type=int, default=None)
    p_run.add_argument("--seed", type=int, default=42)
    p_run.add_argument("--rate", type=float, default=1.0, help="readings per device per second")
    p_run.add_argument("--duration", type=float, default=None, help="seconds to run; omit to run until Ctrl+C")
    p_run.add_argument("--sink", choices=["console", "jsonl", "csv", "parquet"], default="console")
    p_run.add_argument("--out", default="data/bronze/sensor_readings_raw", help="file path (jsonl/csv) or directory (parquet)")
    p_run.add_argument("--control", default="control/trigger.json")
    p_run.set_defaults(func=cmd_run)

    p_trigger = sub.add_parser("trigger", help="request a fault injection on a running generator")
    p_trigger.add_argument("--control", default="control/trigger.json")
    p_trigger.add_argument("--device-id", default=None)
    p_trigger.add_argument("--feeder-id", default=None)
    p_trigger.add_argument("--fault-type", choices=["transient", "sustained"], default=None)
    p_trigger.add_argument("--duration-s", type=float, default=None)
    p_trigger.set_defaults(func=cmd_trigger)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
