"""Statistical (z-score) fault detector - the Hard Constraint #2 fallback,
used here as the *primary* live detector rather than a fallback (see
README.md's "CNN vs statistical detector" decision: comtrade-accelerator's
CNN expects high-resolution waveform captures, a different data modality
from this continuous 1 reading/sec stream).

Per-device rolling baseline (mean/std over a trailing window of *non-anomalous*
readings, so the baseline doesn't drift during a fault) drives a z-score test
on voltage and current. An active anomaly is tracked as an event; its duration
determines fault_type once it either clears (< sustained threshold -> transient)
or outlasts the threshold while still active (-> sustained, upgraded in place
so the demo can show a near-real-time classification, not just after the fact).

Designed to be portable to Spark Structured Streaming's applyInPandasWithState
(grouped by device_id) for Phase 3's silver.sensor_readings stream - the
per-device DeviceState here maps directly to that API's per-group state.
"""
from __future__ import annotations

import statistics
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

TRANSIENT = "transient"
SUSTAINED = "sustained"

DEFAULT_WINDOW_SIZE = 30
DEFAULT_MIN_BASELINE_READINGS = 10
DEFAULT_VOLTAGE_Z_THRESHOLD = 6.0
DEFAULT_CURRENT_Z_THRESHOLD = 6.0
DEFAULT_SUSTAINED_THRESHOLD_S = 5.0


@dataclass
class _RollingBaseline:
    window_size: int
    voltage: deque = field(default_factory=deque)
    current: deque = field(default_factory=deque)

    def update(self, voltage: float, current: float) -> None:
        self.voltage.append(voltage)
        self.current.append(current)
        if len(self.voltage) > self.window_size:
            self.voltage.popleft()
            self.current.popleft()

    @property
    def ready(self) -> bool:
        return len(self.voltage) >= 2

    def z_scores(self, voltage: float, current: float) -> tuple[float, float]:
        v_mean, v_std = statistics.mean(self.voltage), statistics.pstdev(self.voltage) or 1e-6
        c_mean, c_std = statistics.mean(self.current), statistics.pstdev(self.current) or 1e-6
        return (voltage - v_mean) / v_std, (current - c_mean) / c_std


@dataclass
class _ActiveEvent:
    event_id: str
    device_id: str
    feeder_id: str
    start_ts: float
    peak_abs_z: float


@dataclass
class _DeviceState:
    baseline: _RollingBaseline
    active_event: Optional[_ActiveEvent] = None


class StatisticalFaultDetector:
    def __init__(
        self,
        window_size: int = DEFAULT_WINDOW_SIZE,
        min_baseline_readings: int = DEFAULT_MIN_BASELINE_READINGS,
        voltage_z_threshold: float = DEFAULT_VOLTAGE_Z_THRESHOLD,
        current_z_threshold: float = DEFAULT_CURRENT_Z_THRESHOLD,
        sustained_threshold_s: float = DEFAULT_SUSTAINED_THRESHOLD_S,
    ) -> None:
        self.window_size = window_size
        self.min_baseline_readings = min_baseline_readings
        self.voltage_z_threshold = voltage_z_threshold
        self.current_z_threshold = current_z_threshold
        self.sustained_threshold_s = sustained_threshold_s
        self._devices: dict[str, _DeviceState] = {}

    def _state_for(self, device_id: str) -> _DeviceState:
        if device_id not in self._devices:
            self._devices[device_id] = _DeviceState(baseline=_RollingBaseline(self.window_size))
        return self._devices[device_id]

    def process(self, reading: dict) -> Optional[dict]:
        """Ingest one silver.sensor_readings-shaped row
        (device_id, feeder_id, ts, voltage, current, frequency).
        Returns a gold.fault_classifications-shaped dict when an event
        starts, is upgraded to sustained, or is finalized as transient -
        None otherwise (the common case, one no-op per normal reading).
        """
        device_id = reading["device_id"]
        feeder_id = reading["feeder_id"]
        voltage = reading["voltage"]
        current = reading["current"]
        ts = reading["ts"] if isinstance(reading["ts"], (int, float)) else _parse_ts(reading["ts"])

        state = self._state_for(device_id)
        baseline = state.baseline

        is_anomalous = False
        peak_abs_z = 0.0
        if baseline.ready and len(baseline.voltage) >= self.min_baseline_readings:
            v_z, c_z = baseline.z_scores(voltage, current)
            is_anomalous = abs(v_z) >= self.voltage_z_threshold or abs(c_z) >= self.current_z_threshold
            peak_abs_z = max(abs(v_z), abs(c_z))

        result = None

        if is_anomalous:
            if state.active_event is None:
                state.active_event = _ActiveEvent(
                    event_id=str(uuid.uuid4()),
                    device_id=device_id,
                    feeder_id=feeder_id,
                    start_ts=ts,
                    peak_abs_z=peak_abs_z,
                )
                result = self._event_row(state.active_event, ts, fault_type=None)
            else:
                event = state.active_event
                event.peak_abs_z = max(event.peak_abs_z, peak_abs_z)
                duration = ts - event.start_ts
                if duration >= self.sustained_threshold_s:
                    result = self._event_row(event, ts, fault_type=SUSTAINED)
            # do NOT update the baseline while anomalous - keeps it from drifting toward the fault
        else:
            if state.active_event is not None:
                event = state.active_event
                duration = ts - event.start_ts
                fault_type = SUSTAINED if duration >= self.sustained_threshold_s else TRANSIENT
                result = self._event_row(event, ts, fault_type=fault_type)
                state.active_event = None
            baseline.update(voltage, current)

        return result

    @staticmethod
    def _event_row(event: _ActiveEvent, ts: float, fault_type: Optional[str]) -> dict:
        confidence = min(1.0, event.peak_abs_z / 20.0)  # heuristic: z=20 -> full confidence
        return {
            "event_id": event.event_id,
            "device_id": event.device_id,
            "feeder_id": event.feeder_id,
            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "fault_type": fault_type,  # None while still ramping up / not yet classified
            "confidence_score": round(confidence, 3),
        }


def _parse_ts(ts_str: str) -> float:
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
