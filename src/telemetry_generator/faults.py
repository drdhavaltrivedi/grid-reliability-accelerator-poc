"""Fault signature definitions for the synthetic grid telemetry generator.

Two fault types are modeled, matching the transient/sustained distinction the
POC needs to demonstrate (roughly 70% of distribution faults are transient
per the business context in CLAUDE_CODE_BUILD_PROMPT.md):

- transient: a short voltage sag + current spike that self-clears in under a
  few seconds (e.g. a tree branch briefly touching a line, animal contact).
- sustained: the same signature but held for longer, standing in for a fault
  that would actually require a truck roll.

These are deliberately simple, not a physical fault model - the point of the
POC is to show detection/classification/dashboarding working end-to-end, not
to simulate distribution-system electromagnetics.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field


TRANSIENT = "transient"
SUSTAINED = "sustained"

# Duration ranges (seconds). Sustained faults are set well above the
# transient ceiling so a simple threshold rule can tell them apart from
# duration alone once the fault has run its course.
TRANSIENT_DURATION_RANGE_S = (0.5, 2.5)
SUSTAINED_DURATION_RANGE_S = (8.0, 30.0)

# Perturbation ranges applied on top of nominal readings while a fault is
# active. Values are multiplicative factors (voltage/current) or an additive
# offset in Hz (frequency).
VOLTAGE_SAG_FACTOR_RANGE = (0.35, 0.70)   # fraction of nominal remaining
CURRENT_SPIKE_FACTOR_RANGE = (2.0, 5.0)   # multiple of nominal
FREQUENCY_OFFSET_RANGE_HZ = (-0.35, 0.35)


@dataclass
class FaultSignature:
    fault_id: str
    device_id: str
    feeder_id: str
    fault_type: str  # "transient" | "sustained"
    start_time: float  # unix seconds
    duration_s: float
    voltage_factor: float
    current_factor: float
    freq_offset_hz: float

    def is_active(self, t: float) -> bool:
        return self.start_time <= t < (self.start_time + self.duration_s)

    def has_ended(self, t: float) -> bool:
        return t >= (self.start_time + self.duration_s)


def make_fault(
    device_id: str,
    feeder_id: str,
    start_time: float,
    fault_type: str | None = None,
    duration_s: float | None = None,
    rng: random.Random | None = None,
) -> FaultSignature:
    """Build a new fault signature. fault_type/duration_s can be forced
    (used by the on-demand trigger for the live demo moment); otherwise
    they're drawn at random, weighted ~70% transient / 30% sustained to
    match the business context.
    """
    rng = rng or random.Random()

    if fault_type is None:
        fault_type = TRANSIENT if rng.random() < 0.70 else SUSTAINED

    if duration_s is None:
        lo, hi = (
            TRANSIENT_DURATION_RANGE_S if fault_type == TRANSIENT else SUSTAINED_DURATION_RANGE_S
        )
        duration_s = rng.uniform(lo, hi)

    return FaultSignature(
        fault_id=str(uuid.uuid4()),
        device_id=device_id,
        feeder_id=feeder_id,
        fault_type=fault_type,
        start_time=start_time,
        duration_s=duration_s,
        voltage_factor=rng.uniform(*VOLTAGE_SAG_FACTOR_RANGE),
        current_factor=rng.uniform(*CURRENT_SPIKE_FACTOR_RANGE),
        freq_offset_hz=rng.uniform(*FREQUENCY_OFFSET_RANGE_HZ),
    )
