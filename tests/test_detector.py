"""Standalone smoke test for the statistical fault detector (Phase 4),
run against the Phase 2 generator's actual output - same "test it standalone"
approach as tests/test_generator.py. Run with: python tests/test_detector.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from telemetry_generator import TelemetryGenerator  # noqa: E402
from fault_detector import StatisticalFaultDetector  # noqa: E402
from fault_detector.detector import TRANSIENT, SUSTAINED  # noqa: E402


def _warm_up(gen, det, t0, seconds=15):
    t = t0
    for _ in range(seconds):
        for row in gen.tick(t=t):
            det.process(row)
        t += 1.0
    return t


def test_transient_fault_detected_and_classified():
    gen = TelemetryGenerator(num_devices=10, num_feeders=2, seed=10)
    det = StatisticalFaultDetector(sustained_threshold_s=5.0)
    t = _warm_up(gen, det, t0=100000.0)

    gen.trigger_fault(device_id="dev_0003", fault_type="transient", duration_s=1.5, t=t)

    events = []
    for _ in range(5):  # 5 more seconds, well past the 1.5s fault
        for row in gen.tick(t=t):
            result = det.process(row)
            if result and result["device_id"] == "dev_0003":
                events.append(result)
        t += 1.0

    assert len(events) >= 2, f"expected at least a start + finalize event, got {events}"
    assert events[0]["fault_type"] is None, "first emission should be the unclassified event start"
    final = events[-1]
    assert final["fault_type"] == TRANSIENT, f"expected transient, got {final}"
    assert final["event_id"] == events[0]["event_id"]
    assert 0.0 < final["confidence_score"] <= 1.0
    print(f"test_transient_fault_detected_and_classified: PASS ({len(events)} emissions)")


def test_sustained_fault_upgraded_while_active():
    gen = TelemetryGenerator(num_devices=10, num_feeders=2, seed=11)
    det = StatisticalFaultDetector(sustained_threshold_s=5.0)
    t = _warm_up(gen, det, t0=200000.0)

    gen.trigger_fault(device_id="dev_0007", fault_type="sustained", duration_s=10.0, t=t)

    events = []
    for _ in range(7):  # tick to just past the 5s sustained threshold, fault still active (10s long)
        for row in gen.tick(t=t):
            result = det.process(row)
            if result and result["device_id"] == "dev_0007":
                events.append(result)
        t += 1.0

    assert any(e["fault_type"] == SUSTAINED for e in events), (
        f"expected an in-flight upgrade to sustained before the fault even clears, got {events}"
    )
    print(f"test_sustained_fault_upgraded_while_active: PASS ({len(events)} emissions)")


def test_no_false_positives_during_normal_operation():
    gen = TelemetryGenerator(num_devices=20, num_feeders=4, seed=12)
    det = StatisticalFaultDetector()
    t = 300000.0
    false_positives = 0
    for _ in range(60):
        for row in gen.tick(t=t):
            if det.process(row) is not None:
                false_positives += 1
        t += 1.0
    assert false_positives == 0, f"expected zero anomalies with no injected fault, got {false_positives}"
    print("test_no_false_positives_during_normal_operation: PASS")


if __name__ == "__main__":
    test_transient_fault_detected_and_classified()
    test_sustained_fault_upgraded_while_active()
    test_no_false_positives_during_normal_operation()
    print("\nAll tests passed.")
