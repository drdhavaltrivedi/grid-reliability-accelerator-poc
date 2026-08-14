# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC Lands all four Data Sources (per CLAUDE_CODE_BUILD_PROMPT.md) into
# MAGIC `grid_poc.bronze`. This is a Lakeflow Declarative Pipelines (DLT) definition -
# MAGIC **do not run interactively**, it only executes as part of a pipeline (same
# MAGIC constraint the comtrade-accelerator's own `04_Fault_Detection_DLT.py` notes).
# MAGIC
# MAGIC **Not yet run against a live workspace.** Written and reasoned about locally;
# MAGIC needs Databricks credentials to actually execute (see README.md).

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
# comtrade/numpy/json imports removed - only needed by the disabled Comtrade
# ingestion (see the comment block below); dropped so this notebook has no
# extra cluster library dependency now that path is dead.

# COMMAND ----------

# Config inlined rather than `%run "./00_config"` - %run-included variables
# didn't reliably propagate into DLT's execution model (NameError on first
# deploy attempt, 2026-08-14). See 00_config.py for the documented/canonical
# source of these values; keep both in sync if they change.
CATALOG = "grid_poc"
BRONZE_SCHEMA = "bronze"
SENSOR_READINGS_LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/sensor_readings_raw"
COMTRADE_SOURCE_PATH = "s3://db-gtm-industry-solutions/data/rcg/comtrade/source"
LCL_LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/lcl_smart_meter_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Synthetic sensor telemetry
# MAGIC Auto Loader over the parquet files written by `src/telemetry_generator`'s
# MAGIC `ParquetSink`. Point the generator's `--out` at `SENSOR_READINGS_LANDING_PATH`
# MAGIC (a Unity Catalog Volume) when running it from a Databricks cluster/notebook -
# MAGIC the generator itself needs no code change, only the output path.

# COMMAND ----------

# Declared explicitly rather than letting Auto Loader infer it: inference fails
# outright on an empty landing directory (CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE),
# which is exactly the state right after `demo_runner.py reset`. An explicit
# schema lets the pipeline start cleanly before any telemetry has landed.
# Matches src/telemetry_generator's output (see docs/data_dictionary.md).
SENSOR_READINGS_RAW_SCHEMA = StructType([
    StructField("reading_id", StringType()),
    StructField("device_id", StringType()),
    StructField("feeder_id", StringType()),
    StructField("ts", StringType()),  # ISO 8601, cast to timestamp in silver
    StructField("voltage", DoubleType()),
    StructField("current", DoubleType()),
    StructField("frequency", DoubleType()),
    StructField("event_flag", LongType()),
    StructField("source", StringType()),
    StructField("_fault_id", StringType()),
    StructField("_fault_type_truth", StringType()),
])

@dlt.table(
    name="sensor_readings_raw",
    comment="Synthetic grid-edge telemetry landed by src/telemetry_generator via Auto Loader.",
    table_properties={"quality": "bronze"},
)
def sensor_readings_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{SENSOR_READINGS_LANDING_PATH}/_schema")
        .schema(SENSOR_READINGS_RAW_SCHEMA)
        .load(SENSOR_READINGS_LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Comtrade fault event files — DISABLED, confirmed inaccessible
# MAGIC **Tested directly from this workspace on 2026-08-14** (a one-off serverless
# MAGIC job run against `COMTRADE_SOURCE_PATH`): `403 Forbidden`, `UNAUTHORIZED_ACCESS`,
# MAGIC using `AnonymousAWSCredentials` - this bucket does not allow anonymous S3
# MAGIC reads and no AWS instance profile is configured for it in this workspace. This
# MAGIC is not a "might work from a real cluster" risk anymore - it's confirmed dead.
# MAGIC Per Hard Constraint #2, this means the CNN path is out entirely; the
# MAGIC statistical detector (`src/fault_detector`, wired in `30_fault_detection_stream.py`)
# MAGIC is the sole detection method.
# MAGIC
# MAGIC The real implementation (Auto Loader binaryFile + join-on-filename + comtrade
# MAGIC library decode, adapted from comtrade-accelerator's `04_Fault_Detection_DLT.py`)
# MAGIC is preserved below as a **plain comment**, not live code, in case a working
# MAGIC data source is found later (e.g. an AWS account with legitimate access, or a
# MAGIC different sample dataset). The live table below is a stub matching its
# MAGIC intended schema, same pattern as `eia_grid_data_raw`.
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table(name="_comtrade_cfg_files_bronze", comment="Raw .cfg files (internal, feeds comtrade_events_raw)")
# MAGIC def _comtrade_cfg_files_bronze():
# MAGIC     return (
# MAGIC         spark.readStream.format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "binaryFile")
# MAGIC         .option("pathGlobFilter", "*.cfg")
# MAGIC         .load(COMTRADE_SOURCE_PATH)
# MAGIC         .withColumn("base_name", F.element_at(F.split(F.input_file_name(), "[.]"), 1))
# MAGIC         .withColumnRenamed("content", "content_cfg")
# MAGIC         .withColumnRenamed("modificationTime", "cfg_mod_time")
# MAGIC     )
# MAGIC
# MAGIC @dlt.table(name="_comtrade_dat_files_bronze", comment="Raw .dat files (internal, feeds comtrade_events_raw)")
# MAGIC def _comtrade_dat_files_bronze():
# MAGIC     return (
# MAGIC         spark.readStream.format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "binaryFile")
# MAGIC         .option("pathGlobFilter", "*.dat")
# MAGIC         .load(COMTRADE_SOURCE_PATH)
# MAGIC         .withColumn("base_name", F.element_at(F.split(F.input_file_name(), "[.]"), 1))
# MAGIC         .withColumnRenamed("content", "content_dat")
# MAGIC         .withColumnRenamed("modificationTime", "dat_mod_time")
# MAGIC     )
# MAGIC
# MAGIC # (comtrade_events_raw would join the two above on base_name, then decode
# MAGIC # via the comtrade library's Comtrade() reader - see git history for the
# MAGIC # full version, or accelerators/comtrade-accelerator/04_Fault_Detection_DLT.py)
# MAGIC ```

# COMMAND ----------

COMTRADE_RAW_SCHEMA = StructType([
    StructField("base_name", StringType()),
    StructField("frequency", DoubleType()),
    StructField("rec_dev_id", StringType()),
    StructField("station_name", StringType()),
    StructField("microseconds", ArrayType(LongType())),
    StructField("analog", ArrayType(ArrayType(DoubleType()))),
    StructField("analog_units", ArrayType(StringType())),
    StructField("analog_channel_names", ArrayType(StringType())),
    StructField("_ingested_at", StringType()),
])

@dlt.table(
    name="comtrade_events_raw",
    comment="DISABLED - source bucket confirmed inaccessible (403 Forbidden, tested 2026-08-14). Empty stub, same pattern as eia_grid_data_raw.",
    table_properties={"quality": "bronze"},
)
def comtrade_events_raw():
    return spark.createDataFrame([], COMTRADE_RAW_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Low Carbon London smart meter subset
# MAGIC Auto Loader over the CSV subset produced by `scripts/fetch_lcl_sample.py`
# MAGIC (a few hundred households, a few months - **not** the full ~167M row dataset,
# MAGIC per Hard Constraint #2) and uploaded to `LCL_LANDING_PATH`.

# COMMAND ----------

# Explicit for the same reason as SENSOR_READINGS_RAW_SCHEMA above. Column names
# (including the trailing space in the kWh header) confirmed against the real
# source files - see scripts/fetch_lcl_sample.py.
LCL_RAW_SCHEMA = StructType([
    StructField("LCLid", StringType()),
    StructField("stdorToU", StringType()),
    StructField("DateTime", StringType()),
    StructField("KWH/hh (per half hour) ", StringType()),
])

@dlt.table(
    name="lcl_smart_meter_raw",
    comment="Low Carbon London half-hourly household smart meter subset. Pattern/noise reference only - UK/urban data, not a literal rural-utility load profile.",
    table_properties={"quality": "bronze"},
)
def lcl_smart_meter_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{LCL_LANDING_PATH}/_schema")
        .option("header", "true")
        .schema(LCL_RAW_SCHEMA)
        .load(LCL_LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: EIA regional grid data — STUBBED
# MAGIC No API key registered yet (README.md: "skip EIA for now"). Table is declared
# MAGIC so the data dictionary and downstream `gold.grid_context` shape are documented,
# MAGIC but it is **not populated** - this cell is intentionally left as a static empty
# MAGIC table rather than a live API call. Un-stub by replacing the body with a call to
# MAGIC the EIA API (`EIA_API_BASE`, using a Databricks secret for `EIA_API_KEY`) once a
# MAGIC key exists.

# COMMAND ----------

EIA_RAW_SCHEMA = StructType([
    StructField("region", StringType()),
    StructField("period", StringType()),  # ISO timestamp, hourly cadence
    StructField("demand_mwh", DoubleType()),
    StructField("generation_mwh", DoubleType()),
    StructField("_ingested_at", StringType()),
])

@dlt.table(
    name="eia_grid_data_raw",
    comment="STUBBED - EIA Open Data API not yet integrated (no API key). Empty table, schema-only placeholder.",
    table_properties={"quality": "bronze"},
)
def eia_grid_data_raw():
    return spark.createDataFrame([], EIA_RAW_SCHEMA)
