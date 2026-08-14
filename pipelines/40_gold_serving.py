# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: Serving Layer
# MAGIC Builds `gold.feeder_health` and `gold.grid_context` - the two remaining gold
# MAGIC tables from CLAUDE_CODE_BUILD_PROMPT.md's Data Architecture section
# MAGIC (`gold.fault_classifications` is built by `30_fault_detection_stream.py`).
# MAGIC Phase 5's AI/BI dashboard reads from these three gold tables.
# MAGIC
# MAGIC **Not yet run against a live workspace** - see README.md.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# Config inlined rather than `%run "./00_config"` - see 10_bronze_ingestion.py
# for why. Keep in sync with 00_config.py if these change.
CATALOG = "grid_poc"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.feeder_health
# MAGIC 1-minute tumbling-window aggregation per feeder over `silver.sensor_readings`
# MAGIC (synthetic source only - LCL reference rows are explicitly excluded, per the
# MAGIC `feeder_lcl_reference` namespace decision in `20_silver_transform.py`), stream-
# MAGIC static joined against `gold.fault_classifications` for a same-window fault
# MAGIC count. This is deliberately the *same* underlying data (`silver.sensor_readings`,
# MAGIC `gold.fault_classifications`) the detector and Genie also read - the "one data
# MAGIC model, not three disconnected tools" point from the business context.

# COMMAND ----------

@dlt.table(
    name="feeder_health",
    comment="Per-feeder health status aggregated over 1-minute windows: reading stats plus fault count in that window.",
    table_properties={"quality": "gold"},
)
def feeder_health():
    # cross-schema read: this notebook runs as its own (gold) pipeline, separate
    # from 20_silver_transform.py's (silver) pipeline - see README.md "3 separate
    # pipelines" deployment note.
    readings = spark.readStream.table(f"{CATALOG}.{SILVER_SCHEMA}.sensor_readings").filter(F.col("source") == "synthetic")

    windowed = (
        readings
        .withWatermark("ts", "30 seconds")
        .groupBy(F.window("ts", "1 minute"), F.col("feeder_id"))
        .agg(
            F.avg("voltage").alias("avg_voltage"),
            F.avg("current").alias("avg_current"),
            F.avg("frequency").alias("avg_frequency"),
            # approx_count_distinct, not countDistinct: exact distinct aggregations
            # aren't supported in streaming aggregations. At demo scale (tens to
            # low hundreds of devices per feeder) HyperLogLog is exact in practice.
            F.approx_count_distinct("device_id").alias("device_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "feeder_id",
            "avg_voltage",
            "avg_current",
            "avg_frequency",
            "device_count",
        )
    )

    # Stream-static join: fault_classifications is written by its own streaming
    # job (30_fault_detection_stream.py); re-read as a table each trigger so this
    # pipeline doesn't need a stream-stream join against another pipeline's output.
    faults = spark.read.table(f"{CATALOG}.{GOLD_SCHEMA}.fault_classifications")
    fault_counts = (
        faults.groupBy("feeder_id", F.window("timestamp", "1 minute"))
        .agg(F.countDistinct("event_id").alias("fault_count"))
        .select(F.col("window.start").alias("window_start"), "feeder_id", "fault_count")
    )

    joined = windowed.join(fault_counts, on=["window_start", "feeder_id"], how="left").fillna({"fault_count": 0})

    return joined.withColumn(
        "health_status",
        F.when(F.col("fault_count") > 0, "fault").otherwise("healthy"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold.grid_context
# MAGIC STUBBED - passes through `eia_grid_data_raw`, which is itself an empty stub
# MAGIC (no EIA API key). Declared so the schema/shape exists for a dashboard tile or
# MAGIC Genie question to reference without erroring, but returns no rows until EIA
# MAGIC ingestion is un-stubbed in `10_bronze_ingestion.py`.

# COMMAND ----------

@dlt.table(
    name="grid_context",
    comment="STUBBED - regional demand/generation context from EIA. Empty until an API key is provided (see README.md).",
    table_properties={"quality": "gold"},
)
def grid_context():
    return (
        spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.eia_grid_data_raw")
        .select("region", "period", "demand_mwh", "generation_mwh")
    )
