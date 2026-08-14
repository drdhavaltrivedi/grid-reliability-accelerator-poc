# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transform
# MAGIC Builds `grid_poc.silver.sensor_readings` (unified schema across synthetic + LCL)
# MAGIC and `grid_poc.silver.fault_events` (parsed Comtrade events) per
# MAGIC CLAUDE_CODE_BUILD_PROMPT.md's Data Architecture section.
# MAGIC
# MAGIC **Not yet run against a live workspace** - see README.md.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# COMMAND ----------

# Config inlined rather than `%run "./00_config"` - see 10_bronze_ingestion.py
# for why. Keep in sync with 00_config.py if these change.
CATALOG = "grid_poc"
BRONZE_SCHEMA = "bronze"
LCL_ASSUMED_NOMINAL_VOLTAGE = 230.0  # UK domestic supply voltage

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.sensor_readings — synthetic half
# MAGIC Straight pass-through of the bronze synthetic readings (already in the target
# MAGIC shape) with type casting and the ground-truth-only `_fault_*` columns dropped -
# MAGIC those exist for local demo validation (see `src/telemetry_generator`), not for
# MAGIC the unified operational schema.

# COMMAND ----------

@dlt.table(name="_sensor_readings_synthetic_silver", comment="Cleaned synthetic readings (internal, feeds sensor_readings)")
@dlt.expect_or_drop("valid_voltage", "voltage IS NOT NULL AND voltage > 0")
@dlt.expect_or_drop("valid_reading_id", "reading_id IS NOT NULL")
def _sensor_readings_synthetic_silver():
    # cross-schema read: sensor_readings_raw lives in the bronze pipeline/schema,
    # this notebook runs as its own (silver) pipeline - see README.md "3 separate
    # pipelines" deployment note. Fully-qualified spark.readStream.table (not
    # dlt.read_stream) since it's outside this pipeline's own DAG.
    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.sensor_readings_raw")
        .select(
            F.col("reading_id"),
            F.col("device_id"),
            F.col("feeder_id"),
            F.to_timestamp("ts").alias("ts"),
            F.col("voltage").cast("double"),
            F.col("current").cast("double"),
            F.col("frequency").cast("double"),
            F.lit("synthetic").alias("source"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.sensor_readings — LCL half
# MAGIC LCL is household energy **consumption** (kWh per half hour), not an electrical
# MAGIC sensor feed - it has no voltage/current/frequency fields natively. Per the
# MAGIC spec ("use it only for pattern/noise realism, not as a literal rural-utility
# MAGIC load profile"), we derive a current-like value from the consumption shape
# MAGIC (P = E / 0.5h, I = P / V at an assumed UK domestic 230V) purely so it fits the
# MAGIC unified schema for pattern-realism use (e.g. noise/shape reference in a
# MAGIC dashboard) - it is **not** a real electrical reading and should not be treated
# MAGIC as one. `feeder_id` uses a distinct `feeder_lcl_reference` namespace so it can't
# MAGIC silently blend into real feeder rollups in gold. Frequency is left at the UK
# MAGIC grid's true 50Hz (vs. 60Hz for the synthetic North American context) rather than
# MAGIC faked to 60Hz, to avoid fabricating a reading that was never measured.
# MAGIC
# MAGIC Column names confirmed against the actual zip contents (2026-08-14, see
# MAGIC `scripts/fetch_lcl_sample.py`): `LCLid`, `DateTime`, and `KWH/hh (per half hour) `
# MAGIC - note the trailing space in that last header, preserved as-is since
# MAGIC `fetch_lcl_sample.py` writes the CSV through unchanged.

# COMMAND ----------

@dlt.table(name="_sensor_readings_lcl_silver", comment="Derived pattern-reference readings from LCL consumption data (internal, feeds sensor_readings)")
def _sensor_readings_lcl_silver():
    # bronze renamed the source's "KWH/hh (per half hour) " header to this
    # Delta-safe name (Delta rejects spaces/parens in column names).
    kwh_per_hh = F.col("kwh_per_half_hour").cast("double")
    power_w = kwh_per_hh * 2.0 * 1000.0  # kWh/hh -> average W over the half hour
    current_a = power_w / F.lit(LCL_ASSUMED_NOMINAL_VOLTAGE)

    return (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.lcl_smart_meter_raw")
        .filter(kwh_per_hh.isNotNull())
        .select(
            F.concat(F.lit("lcl_"), F.col("LCLid"), F.lit("_"), F.col("DateTime")).alias("reading_id"),
            F.col("LCLid").alias("device_id"),
            F.lit("feeder_lcl_reference").alias("feeder_id"),
            F.to_timestamp("DateTime").alias("ts"),
            F.lit(LCL_ASSUMED_NOMINAL_VOLTAGE).alias("voltage"),
            current_a.alias("current"),
            F.lit(50.0).alias("frequency"),  # true UK grid frequency, not fabricated to match 60Hz
            F.lit("lcl").alias("source"),
        )
    )

# COMMAND ----------

@dlt.table(
    name="sensor_readings",
    comment="Unified sensor readings across synthetic + LCL sources. Provenance kept via `source` - gold-layer aggregations for feeder health / fault detection should filter to source='synthetic' (and later comtrade-derived); LCL rows are pattern/noise reference only, not real grid telemetry.",
    table_properties={"quality": "silver"},
)
def sensor_readings():
    synthetic = dlt.read_stream("_sensor_readings_synthetic_silver")
    lcl = dlt.read_stream("_sensor_readings_lcl_silver")
    return synthetic.unionByName(lcl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver.fault_events
# MAGIC Parsed Comtrade events, adapted from comtrade-accelerator's `02_Read_COMTRADE_Files.py`
# MAGIC readings-restructuring logic (arrays_zip + explode + pivot pattern) - reproduced
# MAGIC here as a streaming-compatible `applyInPandas` pivot the way
# MAGIC `04_Fault_Detection_DLT.py` does it, since DLT doesn't support `.pivot()`
# MAGIC directly. Feeds Phase 4's fault detection model input (IA/IB/IC per timestep).

# COMMAND ----------

pivoted_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("ts", TimestampType(), False),
    StructField("station_name", StringType(), True),
    StructField("IA", DoubleType(), True),
    StructField("IB", DoubleType(), True),
    StructField("IC", DoubleType(), True),
])

def _pivot_comtrade_channels(pdf):
    import pandas as pd

    pivoted = pdf.pivot_table(
        index=["event_id", "ts", "station_name"],
        columns="analog_channel_name",
        values="analog_value",
        aggfunc="first",
    ).reset_index()
    for ch in ["IA", "IB", "IC"]:
        if ch not in pivoted.columns:
            pivoted[ch] = None
    return pivoted[["event_id", "ts", "station_name", "IA", "IB", "IC"]]

# COMMAND ----------

@dlt.table(
    name="fault_events",
    comment="Comtrade fault events flattened to one row per (event, timestep) with IA/IB/IC current channels pivoted to columns.",
    table_properties={"quality": "silver"},
)
def fault_events():
    exploded = (
        spark.readStream.table(f"{CATALOG}.{BRONZE_SCHEMA}.comtrade_events_raw")
        .withColumn(
            "reading",
            F.explode(F.arrays_zip(F.col("microseconds"), F.col("analog"))),
        )
        .withColumn("ts", F.to_timestamp(F.col("reading.microseconds") / 1_000_000))
        .withColumn(
            "channel",
            F.explode(F.arrays_zip(F.col("analog_channel_names"), F.col("reading.analog"))),
        )
        .select(
            F.col("base_name").alias("event_id"),
            F.col("ts"),
            F.col("station_name"),
            F.col("channel.analog_channel_names").alias("analog_channel_name"),
            F.col("channel.analog").alias("analog_value"),
        )
    )
    return exploded.groupBy("event_id").applyInPandas(_pivot_comtrade_channels, pivoted_schema)
