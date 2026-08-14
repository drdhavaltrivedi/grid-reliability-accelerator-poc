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
import numpy as np
import json
from comtrade import Comtrade  # pip install comtrade==0.0.10 - same dependency comtrade-accelerator uses

# COMMAND ----------

# MAGIC %run "./00_config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Synthetic sensor telemetry
# MAGIC Auto Loader over the parquet files written by `src/telemetry_generator`'s
# MAGIC `ParquetSink`. Point the generator's `--out` at `SENSOR_READINGS_LANDING_PATH`
# MAGIC (a Unity Catalog Volume) when running it from a Databricks cluster/notebook -
# MAGIC the generator itself needs no code change, only the output path.

# COMMAND ----------

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
        .load(SENSOR_READINGS_LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Comtrade fault event files
# MAGIC Adapted from comtrade-accelerator's `04_Fault_Detection_DLT.py` bronze tables
# MAGIC (`config_files_bronze`, `dat_files_bronze`, `joined_files_bronze`) - same
# MAGIC binaryFile + join-on-filename pattern, renamed to fit our naming and folded
# MAGIC into a single `comtrade_events_raw` table per the spec's bronze table list.

# COMMAND ----------

@dlt.table(name="_comtrade_cfg_files_bronze", comment="Raw .cfg files (internal, feeds comtrade_events_raw)")
def _comtrade_cfg_files_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.cfg")
        .load(COMTRADE_SOURCE_PATH)
        .withColumn("base_name", F.element_at(F.split(F.input_file_name(), "[.]"), 1))
        .withColumnRenamed("content", "content_cfg")
        .withColumnRenamed("modificationTime", "cfg_mod_time")
    )

# COMMAND ----------

@dlt.table(name="_comtrade_dat_files_bronze", comment="Raw .dat files (internal, feeds comtrade_events_raw)")
def _comtrade_dat_files_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.dat")
        .load(COMTRADE_SOURCE_PATH)
        .withColumn("base_name", F.element_at(F.split(F.input_file_name(), "[.]"), 1))
        .withColumnRenamed("content", "content_dat")
        .withColumnRenamed("modificationTime", "dat_mod_time")
    )

# COMMAND ----------

@dlt.table(
    name="comtrade_events_raw",
    comment="Comtrade .cfg/.dat pairs joined on filename, decoded to analog readings via the comtrade library.",
    table_properties={"quality": "bronze"},
)
def comtrade_events_raw():
    cfg = dlt.read_stream("_comtrade_cfg_files_bronze")
    dat = dlt.read_stream("_comtrade_dat_files_bronze")

    joined = cfg.alias("cfg").join(
        dat.alias("dat"),
        on="base_name",
        how="inner",
    ).select(
        F.col("base_name"),
        F.col("cfg.content_cfg"),
        F.col("dat.content_dat"),
    )

    json_schema = StructType([
        StructField("frequency", DoubleType()),
        StructField("rec_dev_id", StringType()),
        StructField("station_name", StringType()),
        StructField("microseconds", ArrayType(LongType())),
        StructField("analog", ArrayType(ArrayType(DoubleType()))),
        StructField("analog_units", ArrayType(StringType())),
        StructField("analog_channel_names", ArrayType(StringType())),
    ])

    @F.udf("string")
    def get_comtrade_as_json(cfg_content: bytes, dat_content: bytes) -> str:
        # Same decode logic as comtrade-accelerator's 02_Read_COMTRADE_Files.py /
        # 04_Fault_Detection_DLT.py - kept identical since it's just wrapping the
        # comtrade library's parsing, not something specific to our design.
        ct = Comtrade()
        ct._cfg.read(cfg_content.decode())
        ct._cfg_extract_channels_ids(ct._cfg)
        ct._cfg_extract_phases(ct._cfg)
        dat_reader = ct._get_dat_reader()
        dat_reader.read(dat_content.decode() if ct.ft == "ASCII" else dat_content, ct._cfg)
        ct._dat_extract_data(dat_reader)

        ct_dict = {
            "frequency": ct.frequency,
            "rec_dev_id": ct.rec_dev_id,
            "station_name": ct.station_name,
            "microseconds": [int(ct.start_timestamp.timestamp()) + int(s * 1e6) for s in ct.time],
        }
        if ct.analog_count > 0:
            ct_dict["analog"] = np.vstack(ct.analog).transpose().tolist()
            ct_dict["analog_units"] = [c.uu for c in ct._cfg.analog_channels]
            ct_dict["analog_channel_names"] = ct.analog_channel_ids
        return json.dumps(ct_dict)

    return (
        joined
        .withColumn("ctrade_json", get_comtrade_as_json("content_cfg", "content_dat"))
        .withColumn("ctrade", F.from_json("ctrade_json", json_schema))
        .select("base_name", "ctrade.*")
        .withColumn("_ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: Low Carbon London smart meter subset
# MAGIC Auto Loader over the CSV subset produced by `scripts/fetch_lcl_sample.py`
# MAGIC (a few hundred households, a few months - **not** the full ~167M row dataset,
# MAGIC per Hard Constraint #2) and uploaded to `LCL_LANDING_PATH`.

# COMMAND ----------

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
