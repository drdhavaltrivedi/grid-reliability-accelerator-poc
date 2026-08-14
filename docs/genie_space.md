# Genie Space Setup (Phase 5)

**Status: drafted, not yet created in a workspace.** Genie spaces are created/configured
through the Databricks UI (Data Rooms / Genie Spaces) rather than notebook code, so
this is a setup guide + the required NL question set, not runnable code. Follow this
once workspace access exists; test each question and record the actual answer/SQL
Genie produces before calling Phase 5 done.

## Tables to include — gold layer only, per the spec

Per `CLAUDE_CODE_BUILD_PROMPT.md` Phase 5: "Genie space over gold-layer tables only
(not bronze/silver)."

- `grid_poc.gold.fault_classifications` (`event_id, device_id, feeder_id, timestamp, fault_type, confidence_score`)
- `grid_poc.gold.feeder_health` (`window_start, window_end, feeder_id, avg_voltage, avg_current, avg_frequency, device_count, fault_count, health_status`)
- `grid_poc.gold.grid_context` (`region, period, demand_mwh, generation_mwh`) — will return **no rows** until EIA is un-stubbed; include the table anyway so a context question doesn't error, but don't expect it to answer anything yet.

Do **not** add `silver.sensor_readings` or `silver.fault_events`, or Genie will be
able to answer from raw readings instead of demonstrating the governed gold layer.

## Table/column descriptions to set in Genie

The DLT table `comment=` strings in `pipelines/10-40_*.py` already describe each
table's purpose — copy those into Genie's table descriptions when adding them,
plus these column-level notes:

- `fault_classifications.fault_type`: "transient = self-cleared quickly (under ~5s), sustained = still active or ran long (~5s+); NULL means the event is still open and hasn't been classified yet."
- `feeder_health.health_status`: "'fault' if any fault event was detected on this feeder in this 1-minute window, otherwise 'healthy'."
- `feeder_health.window_start` / `window_end`: "1-minute aggregation window boundaries."

## Required NL questions (spec: "write and test 4-5")

Each includes the SQL I'd expect a correctly-grounded Genie space to produce —
use these to verify Genie's actual answer during Phase 6's dry run, not as a
substitute for testing it live.

1. **"Which feeder had the most recent fault?"**
   ```sql
   SELECT feeder_id, MAX(timestamp) AS last_fault
   FROM grid_poc.gold.fault_classifications
   GROUP BY feeder_id ORDER BY last_fault DESC LIMIT 1
   ```

2. **"How does this week's fault count compare to last week?"**
   ```sql
   SELECT
     SUM(CASE WHEN timestamp >= date_trunc('week', current_date()) THEN 1 ELSE 0 END) AS this_week,
     SUM(CASE WHEN timestamp >= date_trunc('week', current_date()) - INTERVAL 7 DAYS
              AND timestamp < date_trunc('week', current_date()) THEN 1 ELSE 0 END) AS last_week
   FROM grid_poc.gold.fault_classifications
   ```

3. **"What percentage of faults today were transient vs sustained?"**
   ```sql
   SELECT fault_type, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
   FROM grid_poc.gold.fault_classifications
   WHERE date(timestamp) = current_date() AND fault_type IS NOT NULL
   GROUP BY fault_type
   ```

4. **"Which feeders are currently unhealthy?"**
   ```sql
   SELECT feeder_id FROM grid_poc.gold.feeder_health
   WHERE health_status = 'fault' AND window_end = (SELECT MAX(window_end) FROM grid_poc.gold.feeder_health)
   ```

5. **"What's the average voltage on \<feeder_id\> in the last hour?"**
   ```sql
   SELECT AVG(avg_voltage) FROM grid_poc.gold.feeder_health
   WHERE feeder_id = '<feeder_id>' AND window_start >= current_timestamp() - INTERVAL 1 HOUR
   ```

## Phase 6 dry-run checklist for this space

- [ ] All 5 questions return a correct, non-empty answer against live demo data
- [ ] Confirm Genie doesn't accidentally fall back to bronze/silver tables (it shouldn't have access, but verify the space's table list is exactly the 3 gold tables above)
- [ ] Re-run after triggering a live fault (`python -m src.telemetry_generator trigger ...` from a Databricks job/cluster) and confirm question 1 and 4 reflect it within the demo's "a few seconds" bar
