# Databricks notebook source
# MAGIC %md
# MAGIC # Fault Detection → `gold.fault_classifications`
# MAGIC
# MAGIC Detects grid faults on `silver.sensor_readings` and classifies each as
# MAGIC **transient** or **sustained**, writing to `gold.fault_classifications`.
# MAGIC
# MAGIC ## Why this is SQL window functions, not `applyInPandasWithState`
# MAGIC
# MAGIC The original draft ported `src/fault_detector/detector.py`'s per-device state
# MAGIC machine into `applyInPandasWithState`. That works in principle but has two
# MAGIC practical problems: its typed-state API is version-sensitive, and it required
# MAGIC pickling detector objects into Spark `BINARY` state columns - the least-proven
# MAGIC code in the repo, and a bad thing to have under a live demo.
# MAGIC
# MAGIC The detection logic is expressible directly in Spark SQL window functions,
# MAGIC which is dramatically more robust:
# MAGIC
# MAGIC | Detector concept | SQL equivalent |
# MAGIC |---|---|
# MAGIC | per-device rolling baseline | `AVG/STDDEV OVER (PARTITION BY device_id ORDER BY ts ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING)` |
# MAGIC | z-score test | `ABS(voltage - basline_mean) / baseline_std >= 6` |
# MAGIC | group consecutive anomalies into one event | gaps-and-islands: running `SUM` of "is this the start of a new run" |
# MAGIC | transient vs sustained | event duration vs the 5s threshold |
# MAGIC
# MAGIC `src/fault_detector/` remains the reference implementation and is what the unit
# MAGIC tests (`tests/test_detector.py`) exercise; the thresholds below are kept
# MAGIC deliberately identical to it. **Keep the two in sync if thresholds change.**
# MAGIC
# MAGIC One honest caveat: the baseline window here is "the 30 readings before this
# MAGIC one", where the Python detector excludes readings taken *during* an active
# MAGIC fault from the baseline. With a 6-sigma threshold and faults this pronounced,
# MAGIC that difference doesn't change the classification, but it is a difference.

# COMMAND ----------

# Config inlined rather than `%run "./00_config"` - %run-included variables don't
# reliably propagate here. Keep in sync with 00_config.py.
CATALOG = "grid_poc"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Kept identical to src/fault_detector/detector.py's defaults.
BASELINE_WINDOW = 30          # DEFAULT_WINDOW_SIZE
MIN_BASELINE_READINGS = 10    # DEFAULT_MIN_BASELINE_READINGS
Z_THRESHOLD = 6.0             # DEFAULT_VOLTAGE_Z_THRESHOLD / DEFAULT_CURRENT_Z_THRESHOLD
SUSTAINED_THRESHOLD_S = 5.0   # DEFAULT_SUSTAINED_THRESHOLD_S
CONFIDENCE_Z_FULL_SCALE = 20.0  # z=20 -> confidence 1.0

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

# MAGIC %md
# MAGIC ## The detection query
# MAGIC Four stacked CTEs: baseline → anomaly flag → group into events → classify.

# COMMAND ----------

DETECTION_SQL = f"""
WITH scored AS (
    SELECT
        device_id,
        feeder_id,
        ts,
        voltage,
        current,
        AVG(voltage) OVER w  AS v_mean,
        STDDEV(voltage) OVER w AS v_std,
        AVG(current) OVER w  AS c_mean,
        STDDEV(current) OVER w AS c_std,
        COUNT(*) OVER w      AS baseline_n
    FROM {CATALOG}.{SILVER_SCHEMA}.sensor_readings
    WHERE source = 'synthetic'
    WINDOW w AS (
        PARTITION BY device_id ORDER BY ts
        ROWS BETWEEN {BASELINE_WINDOW} PRECEDING AND 1 PRECEDING
    )
),
flagged AS (
    SELECT
        *,
        GREATEST(
            ABS(voltage - v_mean) / NULLIF(v_std, 0),
            ABS(current - c_mean) / NULLIF(c_std, 0)
        ) AS abs_z
    FROM scored
    WHERE baseline_n >= {MIN_BASELINE_READINGS}
      AND v_std > 0 AND c_std > 0
),
marked AS (
    SELECT
        *,
        CASE WHEN abs_z >= {Z_THRESHOLD} THEN 1 ELSE 0 END AS is_anomalous
    FROM flagged
),
-- gaps-and-islands: a new event starts wherever an anomalous reading follows
-- a non-anomalous one. The running sum of those starts becomes the event key.
islands AS (
    SELECT
        *,
        SUM(is_event_start) OVER (PARTITION BY device_id ORDER BY ts
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS event_seq
    FROM (
        SELECT
            *,
            CASE
                WHEN is_anomalous = 1
                 AND COALESCE(LAG(is_anomalous) OVER (PARTITION BY device_id ORDER BY ts), 0) = 0
                THEN 1 ELSE 0
            END AS is_event_start
        FROM marked
    )
)
SELECT
    md5(CONCAT(device_id, '|', CAST(MIN(ts) AS STRING))) AS event_id,
    device_id,
    MIN(feeder_id)  AS feeder_id,
    MIN(ts)         AS timestamp,
    CASE
        WHEN UNIX_TIMESTAMP(MAX(ts)) - UNIX_TIMESTAMP(MIN(ts)) >= {SUSTAINED_THRESHOLD_S}
        THEN 'sustained' ELSE 'transient'
    END AS fault_type,
    LEAST(1.0, MAX(abs_z) / {CONFIDENCE_Z_FULL_SCALE}) AS confidence_score
FROM islands
WHERE is_anomalous = 1
GROUP BY device_id, event_seq
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert into gold
# MAGIC `event_id` is derived deterministically from (device, event start time), so
# MAGIC re-running is idempotent and an in-flight event that later grows long enough
# MAGIC to be sustained gets **upgraded in place** rather than duplicated - which is
# MAGIC what makes the live demo show a transient→sustained transition.

# COMMAND ----------

def run_detection():
    detected = spark.sql(DETECTION_SQL)
    detected.createOrReplaceTempView("detected_faults")
    spark.sql(f"""
        MERGE INTO {CATALOG}.{GOLD_SCHEMA}.fault_classifications AS target
        USING detected_faults AS source
          ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    return detected.count()

n = run_detection()
print(f"[fault_detection] {n} fault event(s) detected")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.fault_classifications
        ORDER BY timestamp DESC LIMIT 20
    """)
)
