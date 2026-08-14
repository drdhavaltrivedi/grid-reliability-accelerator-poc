# Databricks notebook source
# MAGIC %md
# MAGIC # Fault Detection Stream -> gold.fault_classifications
# MAGIC
# MAGIC Runs `src/fault_detector.StatisticalFaultDetector`'s per-device logic (already
# MAGIC unit-tested locally against the Phase 2 generator - see `tests/test_detector.py`)
# MAGIC as a Spark Structured Streaming arbitrary-stateful transformation
# MAGIC (`applyInPandasWithState`, grouped by `device_id`) over `silver.sensor_readings`,
# MAGIC upserting into `gold.fault_classifications` via `foreachBatch` + `MERGE`.
# MAGIC
# MAGIC Written as a **plain Structured Streaming job, not a DLT table** - DLT's
# MAGIC Python API doesn't cleanly support arbitrary stateful transformations the way
# MAGIC this per-device event state machine needs, so this runs as its own streaming
# MAGIC query/job rather than as a `pipelines/10_bronze_ingestion.py`-style `@dlt.table`.
# MAGIC
# MAGIC **Not yet run against a live workspace - unverified.** `applyInPandasWithState`
# MAGIC is the right primitive for this but its exact typed-state API has version-specific
# MAGIC edges; treat this as a strong first draft to debug against a real cluster, not
# MAGIC as validated code, the way pipelines/10 and /20 also are.
# MAGIC
# MAGIC Per the CNN-vs-statistical decision in README.md, this *is* the primary live
# MAGIC detector (not a fallback) for the synthetic stream - the comtrade-accelerator's
# MAGIC CNN is validated separately against native Comtrade waveform data.

# COMMAND ----------

import sys

sys.path.insert(0, "../src")  # repo layout: pipelines/ and src/ are siblings

from typing import Iterator
import pandas as pd
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from fault_detector.detector import (
    _RollingBaseline,
    _ActiveEvent,
    TRANSIENT,
    SUSTAINED,
    DEFAULT_WINDOW_SIZE,
    DEFAULT_MIN_BASELINE_READINGS,
    DEFAULT_VOLTAGE_Z_THRESHOLD,
    DEFAULT_CURRENT_Z_THRESHOLD,
    DEFAULT_SUSTAINED_THRESHOLD_S,
)

# COMMAND ----------

# MAGIC %run "./00_config"

# COMMAND ----------

OUTPUT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("device_id", StringType()),
    StructField("feeder_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("fault_type", StringType()),  # null while an event is open but not yet classified
    StructField("confidence_score", DoubleType()),
])

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.fault_classifications (
        event_id STRING,
        device_id STRING,
        feeder_id STRING,
        timestamp TIMESTAMP,
        fault_type STRING,
        confidence_score DOUBLE
    ) USING DELTA
""")

# COMMAND ----------

def detect_faults_per_device(
    device_id: str,
    readings: Iterator[pd.DataFrame],
    state: GroupState,
) -> Iterator[pd.DataFrame]:
    """applyInPandasWithState entry point. State is a plain tuple
    (baseline, active_event) reusing the exact dataclasses from
    src/fault_detector/detector.py so the detection logic is identical to
    what's already tested in tests/test_detector.py - only the state
    plumbing differs (Spark GroupState instead of an in-process dict)."""
    import pickle

    if state.exists:
        baseline_bytes, active_event_bytes = state.get
        baseline = pickle.loads(baseline_bytes)
        active_event = pickle.loads(active_event_bytes) if active_event_bytes is not None else None
    else:
        baseline, active_event = _RollingBaseline(DEFAULT_WINDOW_SIZE), None

    out_rows = []

    for pdf in readings:
        pdf = pdf.sort_values("ts")
        for row in pdf.itertuples(index=False):
            ts = row.ts.timestamp()
            is_anomalous = False
            peak_abs_z = 0.0
            if baseline.ready and len(baseline.voltage) >= DEFAULT_MIN_BASELINE_READINGS:
                v_z, c_z = baseline.z_scores(row.voltage, row.current)
                is_anomalous = (
                    abs(v_z) >= DEFAULT_VOLTAGE_Z_THRESHOLD or abs(c_z) >= DEFAULT_CURRENT_Z_THRESHOLD
                )
                peak_abs_z = max(abs(v_z), abs(c_z))

            if is_anomalous:
                if active_event is None:
                    import uuid

                    active_event = _ActiveEvent(str(uuid.uuid4()), device_id, row.feeder_id, ts, peak_abs_z)
                    out_rows.append((active_event.event_id, device_id, row.feeder_id, row.ts, None, 0.0))
                else:
                    active_event.peak_abs_z = max(active_event.peak_abs_z, peak_abs_z)
                    duration = ts - active_event.start_ts
                    if duration >= DEFAULT_SUSTAINED_THRESHOLD_S:
                        conf = min(1.0, active_event.peak_abs_z / 20.0)
                        out_rows.append((active_event.event_id, device_id, row.feeder_id, row.ts, SUSTAINED, conf))
            else:
                if active_event is not None:
                    duration = ts - active_event.start_ts
                    fault_type = SUSTAINED if duration >= DEFAULT_SUSTAINED_THRESHOLD_S else TRANSIENT
                    conf = min(1.0, active_event.peak_abs_z / 20.0)
                    out_rows.append((active_event.event_id, device_id, row.feeder_id, row.ts, fault_type, conf))
                    active_event = None
                baseline.update(row.voltage, row.current)

    state.update((pickle.dumps(baseline), pickle.dumps(active_event) if active_event is not None else None))
    state.setTimeoutDuration("1 hour")  # evict devices that go quiet, matches an idle sensor scenario
    return iter([pd.DataFrame(out_rows, columns=[f.name for f in OUTPUT_SCHEMA.fields])]) if out_rows else iter([])

# COMMAND ----------

def upsert_fault_classifications(batch_df, batch_id: int) -> None:
    batch_df.createOrReplaceTempView("fault_classifications_batch")
    spark.sql(f"""
        MERGE INTO {CATALOG}.{GOLD_SCHEMA}.fault_classifications AS target
        USING fault_classifications_batch AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

sensor_readings_stream = spark.readStream.table(f"{CATALOG}.{SILVER_SCHEMA}.sensor_readings")

fault_events_stream = (
    sensor_readings_stream
    .groupBy("device_id")
    .applyInPandasWithState(
        detect_faults_per_device,
        outputStructType=OUTPUT_SCHEMA,
        stateStructType="baseline BINARY, active_event BINARY",  # serialized via pickle in practice
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
)

query = (
    fault_events_stream.writeStream
    .foreachBatch(upsert_fault_classifications)
    .option("checkpointLocation", f"/Volumes/{CATALOG}/{GOLD_SCHEMA}/checkpoints/fault_detection_stream")
    .trigger(processingTime="5 seconds")  # Phase 6 wants detection to surface "within a few seconds"
    .start()
)
