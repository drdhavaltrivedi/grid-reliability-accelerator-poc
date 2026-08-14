"""Standalone smoke test for the telemetry generator (Phase 2 - "test it
standalone before wiring it into the pipeline"). Plain assertions, no test
framework dependency - run with:  python tests/test_generator.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from telemetry_generator import TelemetryGenerator  # noqa: E402

REQUIRED_FIELDS = {"reading_id", "device_id", "feeder_id", "ts", "voltage", "current", "frequency", "event_flag", "source"}


def test_schema_and_baseline():
    gen = TelemetryGenerator(num_devices=20, num_feeders=4, seed=1)
    rows = gen.tick(t=1000.0)
    assert len(rows) == 20, "one row per device"
    assert len(gen.feeder_ids) == 4
    for row in rows:
        assert REQUIRED_FIELDS.issubset(row.keys())
        assert row["event_flag"] == 0
        assert row["source"] == "synthetic"
        assert 6800 < row["voltage"] < 7600, f"baseline voltage out of range: {row['voltage']}"
        assert 59.5 < row["frequency"] < 60.5
    print("test_schema_and_baseline: PASS")


def test_transient_fault_injection_and_clear():
    gen = TelemetryGenerator(num_devices=20, num_feeders=4, seed=2)
    t0 = 2000.0
    fault_id = gen.trigger_fault(device_id="dev_0005", fault_type="transient", duration_s=1.0, t=t0)
    assert fault_id

    rows_during = {r["device_id"]: r for r in gen.tick(t=t0 + 0.1)}
    faulted = rows_during["dev_0005"]
    assert faulted["event_flag"] == 1
    assert faulted["_fault_type_truth"] == "transient"
    assert faulted["_fault_id"] == fault_id
    assert faulted["voltage"] < 6800, "voltage should sag during a fault"
    baseline_current = next(d.baseline_current for d in gen.devices if d.device_id == "dev_0005")
    assert faulted["current"] > baseline_current * 1.5, "current should spike during a fault"

    other = rows_during["dev_0000"]
    assert other["event_flag"] == 0, "fault should be isolated to the targeted device"

    rows_after = {r["device_id"]: r for r in gen.tick(t=t0 + 2.0)}
    assert rows_after["dev_0005"]["event_flag"] == 0, "fault should self-clear after duration_s"
    assert rows_after["dev_0005"]["_fault_type_truth"] is None
    print("test_transient_fault_injection_and_clear: PASS")


def test_sustained_fault_outlasts_transient_range():
    gen = TelemetryGenerator(num_devices=10, num_feeders=2, seed=3)
    t0 = 3000.0
    gen.trigger_fault(device_id="dev_0002", fault_type="sustained", duration_s=10.0, t=t0)
    row = {r["device_id"]: r for r in gen.tick(t=t0 + 5.0)}["dev_0002"]
    assert row["event_flag"] == 1
    assert row["_fault_type_truth"] == "sustained"
    print("test_sustained_fault_outlasts_transient_range: PASS")


def test_random_device_selection_when_unspecified():
    gen = TelemetryGenerator(num_devices=50, num_feeders=5, seed=4)
    fault_id = gen.trigger_fault(feeder_id="feeder_002", t=5000.0)
    affected = [d for d in gen.devices if gen._active_faults.get(d.device_id) and gen._active_faults[d.device_id].fault_id == fault_id]
    assert len(affected) == 1
    assert affected[0].feeder_id == "feeder_002"
    print("test_random_device_selection_when_unspecified: PASS")


if __name__ == "__main__":
    test_schema_and_baseline()
    test_transient_fault_injection_and_clear()
    test_sustained_fault_outlasts_transient_range()
    test_random_device_selection_when_unspecified()
    print("\nAll tests passed.")
