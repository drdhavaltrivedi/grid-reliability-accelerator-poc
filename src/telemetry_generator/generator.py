"""Parameterized synthetic grid-edge telemetry generator.

Pattern is modeled on databricks-industry-solutions/digital-twin's
line_data_generator (per-entity seeded generation, configurable noise,
on-demand anomaly injection) - see CLAUDE_CODE_BUILD_PROMPT.md Data Sources
#1. It is a fresh implementation rather than a literal adaptation: that
repo's generator is manufacturing-domain (temperature/pressure/vibration
sensors on production lines) and depends on a non-PyPI package (mandrova),
neither of which fit grid telemetry.

Produces one row per simulated device per tick with fields:
    reading_id, device_id, feeder_id, ts, voltage, current, frequency,
    event_flag, source
plus ground-truth columns (_fault_id, _fault_type_truth) used only for demo
validation / scoring - a real feed would not carry these, since knowing the
fault type in advance is what detection is for.
"""
from __future__ import annotations

import math
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .faults import FaultSignature, make_fault

DEFAULT_NOMINAL_VOLTAGE = 7200.0  # volts, typical US rural distribution line-to-neutral
DEFAULT_NOMINAL_FREQUENCY = 60.0  # Hz, North America
DEFAULT_CURRENT_RANGE = (15.0, 80.0)  # amps, per-device baseline load range


@dataclass
class DeviceProfile:
    device_id: str
    feeder_id: str
    baseline_current: float
    phase_offset: float  # radians, staggers each device's daily-load sinusoid


class TelemetryGenerator:
    def __init__(
        self,
        num_devices: int = 100,
        num_feeders: Optional[int] = None,
        seed: int = 42,
        nominal_voltage: float = DEFAULT_NOMINAL_VOLTAGE,
        nominal_frequency: float = DEFAULT_NOMINAL_FREQUENCY,
        current_range: tuple[float, float] = DEFAULT_CURRENT_RANGE,
        voltage_noise_sigma: float = 25.0,
        current_noise_sigma_frac: float = 0.03,
        frequency_noise_sigma: float = 0.015,
    ) -> None:
        if num_devices < 1:
            raise ValueError("num_devices must be >= 1")

        self.num_devices = num_devices
        self.num_feeders = num_feeders or max(1, num_devices // 20)
        self.nominal_voltage = nominal_voltage
        self.nominal_frequency = nominal_frequency
        self.voltage_noise_sigma = voltage_noise_sigma
        self.current_noise_sigma_frac = current_noise_sigma_frac
        self.frequency_noise_sigma = frequency_noise_sigma

        self._rng = random.Random(seed)
        self.devices: list[DeviceProfile] = self._build_devices(current_range)
        self._devices_by_id = {d.device_id: d for d in self.devices}
        self._active_faults: dict[str, FaultSignature] = {}

    def _build_devices(self, current_range: tuple[float, float]) -> list[DeviceProfile]:
        devices = []
        for i in range(self.num_devices):
            device_id = f"dev_{i:04d}"
            feeder_id = f"feeder_{i % self.num_feeders:03d}"
            # Per-device seeded RNG so each device's characteristics are
            # deterministic/reproducible across runs, mirroring the
            # line_data_generator pattern of seeding on the entity id.
            device_rng = random.Random(f"{device_id}:{feeder_id}")
            baseline_current = device_rng.uniform(*current_range)
            phase_offset = device_rng.uniform(0, 2 * math.pi)
            devices.append(DeviceProfile(device_id, feeder_id, baseline_current, phase_offset))
        return devices

    @property
    def feeder_ids(self) -> list[str]:
        return sorted({d.feeder_id for d in self.devices})

    def trigger_fault(
        self,
        device_id: Optional[str] = None,
        feeder_id: Optional[str] = None,
        fault_type: Optional[str] = None,
        duration_s: Optional[float] = None,
        t: Optional[float] = None,
    ) -> str:
        """Arm a fault signature that will start affecting readings from the
        next tick(). This is the on-demand injection hook required for the
        live demo moment - callable directly (function call) or via the CLI
        `trigger` subcommand (which writes a control file another process's
        run loop polls).
        """
        t = time.time() if t is None else t

        if device_id is None:
            candidates = (
                [d for d in self.devices if d.feeder_id == feeder_id] if feeder_id else self.devices
            )
            if not candidates:
                raise ValueError(f"no devices found for feeder_id={feeder_id!r}")
            device_id = self._rng.choice(candidates).device_id

        device = self._devices_by_id.get(device_id)
        if device is None:
            raise ValueError(f"unknown device_id={device_id!r}")

        fault = make_fault(
            device_id=device.device_id,
            feeder_id=device.feeder_id,
            start_time=t,
            fault_type=fault_type,
            duration_s=duration_s,
            rng=self._rng,
        )
        self._active_faults[device.device_id] = fault
        return fault.fault_id

    def tick(self, t: Optional[float] = None) -> list[dict]:
        """Generate one reading per device for time t (unix seconds,
        defaults to now). Clears any faults that have run their course.
        """
        t = time.time() if t is None else t
        ts_iso = datetime.fromtimestamp(t, tz=timezone.utc).isoformat()

        # expire completed faults
        for device_id in [d for d, f in self._active_faults.items() if f.has_ended(t)]:
            del self._active_faults[device_id]

        rows = []
        for device in self.devices:
            voltage = self.nominal_voltage + self._rng.gauss(0, self.voltage_noise_sigma)
            current = device.baseline_current * (
                1 + 0.15 * math.sin(t / 3600.0 * 2 * math.pi + device.phase_offset)
            )
            current += self._rng.gauss(0, current * self.current_noise_sigma_frac)
            frequency = self.nominal_frequency + self._rng.gauss(0, self.frequency_noise_sigma)

            fault = self._active_faults.get(device.device_id)
            fault_type_truth = None
            fault_id = None
            event_flag = 0
            if fault is not None and fault.is_active(t):
                voltage *= fault.voltage_factor
                current *= fault.current_factor
                frequency += fault.freq_offset_hz
                event_flag = 1
                fault_type_truth = fault.fault_type
                fault_id = fault.fault_id

            rows.append(
                {
                    "reading_id": str(uuid.uuid4()),
                    "device_id": device.device_id,
                    "feeder_id": device.feeder_id,
                    "ts": ts_iso,
                    "voltage": round(voltage, 2),
                    "current": round(max(current, 0.0), 3),
                    "frequency": round(frequency, 4),
                    "event_flag": event_flag,
                    "source": "synthetic",
                    "_fault_id": fault_id,
                    "_fault_type_truth": fault_type_truth,
                }
            )
        return rows
