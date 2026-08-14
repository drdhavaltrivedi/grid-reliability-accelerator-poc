# Databricks notebook source
# MAGIC %md
# MAGIC # Config
# MAGIC Shared configuration for the Grid Reliability Accelerator POC pipelines.
# MAGIC
# MAGIC **Status: not yet run against a live workspace** (see README.md — no Databricks
# MAGIC credentials provided as of this writing). Catalog/schema names below follow the
# MAGIC spec's suggested naming (`CLAUDE_CODE_BUILD_PROMPT.md`, Data Architecture section)
# MAGIC as-is, since there's no existing workspace convention to check against yet.

# COMMAND ----------

CATALOG = "grid_poc"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Landing zone for the synthetic generator's parquet sink (src/telemetry_generator).
# Point TelemetryGenerator's ParquetSink --out at this same Volume path when running
# the generator from a Databricks cluster/notebook - no code change needed there,
# only the output path.
SENSOR_READINGS_LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/sensor_readings_raw"

# Comtrade sample data shipped by the accelerator (Data Sources #4).
# Source: https://github.com/databricks-industry-solutions/comtrade-accelerator
COMTRADE_SOURCE_PATH = "s3://db-gtm-industry-solutions/data/rcg/comtrade/source"

# Low Carbon London smart meter subset (Data Sources #3). Populated by
# scripts/fetch_lcl_sample.py, then uploaded to this Volume path. CC-BY licensed.
# Source: https://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households
LCL_LANDING_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing/lcl_smart_meter_raw"

# EIA Open Data API (Data Sources #2) - STUBBED. No API key provided/registered yet
# (decision recorded in README.md: "skip for now"). eia_grid_data_raw / grid_context
# are defined but not populated until a key is supplied and 30_eia_ingestion.py is
# un-stubbed.
EIA_API_KEY = None  # set via a Databricks secret (dbutils.secrets), never hardcode
EIA_API_BASE = "https://api.eia.gov/v2"

# Assumed nominal line-to-neutral voltage used only to derive a current-like value
# from LCL household kWh readings for the unified silver.sensor_readings schema
# (LCL has no voltage/current fields natively - see 20_silver_transform.py).
LCL_ASSUMED_NOMINAL_VOLTAGE = 230.0  # UK domestic supply voltage
