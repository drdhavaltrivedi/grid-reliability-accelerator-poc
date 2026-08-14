# Build Prompt: Grid Reliability Accelerator POC (Databricks)

Use this as the instruction file for Claude Code. Either paste this whole document into the chat, or save it as `BUILD_SPEC.md` in the project root and tell Claude Code: "Read BUILD_SPEC.md and start with Phase 1." The second approach works better for a multi-day build since Claude Code can re-read it across sessions.

---

## Role and Objective

You are building a sales-enablement proof-of-concept for a Databricks consulting partner. The POC demonstrates real-time grid-edge fault detection for electric utilities, built on Databricks (Delta Lake, Unity Catalog, Structured Streaming, AI/BI dashboards, Genie). This is our own IP, not a reskin of Databricks' own sample accelerator — use the referenced accelerators as technical starting points, not as the finished product.

**Do not silently assume anything marked [CONFIRM] or [ASK USER] below — surface it and ask before proceeding, or make the decision explicit in your output so it can be corrected.**

## Business Context (condensed — for narrative decisions only, don't over-engineer around this)

- **ICP:** North American rural electric cooperatives and mid-market/municipal utilities — not large investor-owned utilities. This segment has real reliability pain but is priced out of enterprise point solutions (GE, Siemens, ABB, Sense), which is the actual differentiation angle.
- **Problem being demonstrated:** Up to 80% of distribution faults occur on unmonitored/under-protected laterals at the grid edge, and roughly 70% of those are transient rather than sustained. A system that distinguishes "temporary blip" from "sustained fault" in real time reduces unnecessary truck rolls — which matters more for co-ops with limited crew capacity across large rural service territories than it does for a large IOU.
- **What "winning" looks like for this demo:** a prospect stakeholder sees a live-triggered anomaly detected and classified in near-real-time, asks a natural-language question about it via Genie, and the governance layer (Unity Catalog) is visibly the same data model powering all of it — not three disconnected tools.

## Hard Constraints

1. No real customer/prospect data. Synthetic and public data only, at this stage.
2. Do not build the fault-classification model from scratch. Clone and run `databricks-industry-solutions/grid-edge-analytics` first, confirm it works, then adapt it. If it's not accessible or won't run in the available environment, fall back to a simpler statistical anomaly detector (z-score or isolation forest) rather than attempting to reinvent the CNN approach.
3. This targets a live Databricks workspace. You will need Databricks CLI or SDK access (workspace URL + PAT/OAuth token) to actually deploy anything beyond local code generation. **[ASK USER]** for workspace host and auth before attempting any deployment step — do not assume credentials are already configured.
4. Keep compute cost-conscious — this is a demo environment, not a load test. Default to the smallest cluster/serverless option that keeps the streaming demo responsive.
5. Cite real sources in code comments/docs (accelerator repo names, dataset URLs) — do not fabricate data sources, license terms, or API endpoints not listed below.

## Data Sources — build exactly these four, in this order of priority

### 1. Synthetic streaming telemetry generator (primary — build first)
- Parameterized Python module, pattern modeled on `databricks-industry-solutions/digital-twin`'s `line_data_generator`.
- Must support triggering a specific fault signature on demand (function call or CLI flag), not just random noise — this is required for the live demo moment.
- Suggested fields **[CONFIRM against accelerator schema before finalizing]**: `device_id`, `feeder_id`, `timestamp`, `voltage`, `current`, `frequency`, `event_flag`.
- Suggested starting scale **[CONFIRM, adjust for demo responsiveness]**: 50–200 simulated sensors, 1 reading/second baseline.
- Output: stream directly into a bronze Delta table via Structured Streaming or Auto Loader.

### 2. EIA Open Data API (real, public, secondary context)
- `eia.gov/opendata` — free, requires API key registration. **[ASK USER]** to register and provide the key, or register it yourself if you have web access and can complete the signup.
- Hourly cadence. Use for regional demand/generation context only — not load-bearing for fault detection.

### 3. Low Carbon London smart meter dataset (real, public, pattern reference only)
- `data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households` (also on Kaggle).
- CC-BY licensed, half-hourly readings, 5,567 households. **Do not ingest the full ~167M row dataset** — pull a subset (a few hundred households, a few months) sufficient for consumption-shape realism. This is UK/urban data; use it only for pattern/noise realism, not as a literal rural-utility load profile.

### 4. Grid-Edge Analytics accelerator sample data (real, public, model reference)
- `databricks.com/solutions/accelerators/grid-edge-analytics` — ships with Comtrade (`.cfg`/`.dat`) fault event files.
- Clone the accelerator repo, run its sample pipeline as-is first to confirm it works, before adapting anything.

## Data Architecture (medallion, Unity Catalog)

Suggested catalog/schema naming **[CONFIRM against any existing workspace convention]**: `grid_poc.bronze`, `grid_poc.silver`, `grid_poc.gold`

**Bronze:** `sensor_readings_raw`, `eia_grid_data_raw`, `lcl_smart_meter_raw`, `comtrade_events_raw`

**Silver:** `sensor_readings` (unified schema across synthetic + real sources), `fault_events` (parsed Comtrade events)

**Gold:** `feeder_health` (aggregated status per feeder per window), `fault_classifications` (event_id, feeder_id, timestamp, fault_type [transient|sustained], confidence_score), `grid_context` (EIA regional data for dashboard context)

Suggested `silver.sensor_readings` schema **[CONFIRM fields against what the generator and model actually produce]**:

| Field | Type | Notes |
|---|---|---|
| reading_id | string | PK |
| device_id | string | |
| feeder_id | string | |
| ts | timestamp | |
| voltage | double | |
| current | double | |
| frequency | double | |
| source | string | 'synthetic' / 'eia' / 'lcl' — keep provenance visible |

## Build Phases

**Phase 1 — Validate the accelerators (do this before writing new code)**
Clone `grid-edge-analytics` and `digital-twin` from `github.com/databricks-industry-solutions`. Run both end-to-end on their own sample data in the target workspace. Confirm both work before customizing anything. Report back what worked, what didn't, and what had to change.

**Phase 2 — Synthetic generator**
Build the parameterized telemetry generator with on-demand anomaly injection (see Data Sources #1). Test it standalone before wiring it into the pipeline.

**Phase 3 — Ingestion pipeline**
Structured Streaming/DLT pipeline landing all four sources into bronze, transforming into silver with the unified schema above.

**Phase 4 — Detection**
Adapt the Grid-Edge Analytics CNN approach on the silver data, outputting transient/sustained classification into `gold.fault_classifications`. If this proves too time-consuming, fall back to the statistical anomaly detector (Hard Constraint #2) — flag this decision explicitly rather than silently downgrading.

**Phase 5 — Serving**
AI/BI dashboard on the gold layer (live-refreshing feeder health + flagged events). Genie space over gold-layer tables only (not bronze/silver) — write and test 4-5 natural-language questions that reliably return correct answers, e.g. "which feeder had the most recent fault" or "how does this week's fault count compare to last week."

**Phase 6 — Dry run**
Trigger the injected anomaly live, confirm detection surfaces within a few seconds, confirm the dashboard and Genie both reflect it correctly. Run this twice to confirm repeatability before calling it demo-ready.

## Definition of Done

- Bronze → silver → gold pipeline runs end-to-end without manual intervention
- A demo-runner script/notebook exists that: resets the environment, starts the stream, triggers the anomaly on command, and confirms detection appears on the dashboard
- Data dictionary matches what was actually built (update the schema tables above if fields changed during the build)
- A short runbook exists for re-running the demo
- Every [CONFIRM] and [ASK USER] item above has either been resolved and documented, or is called out explicitly as still open

## What to report back after each phase

State what was built, what deviated from this spec and why, and what's still a placeholder/assumption — don't mark something done if a [CONFIRM] item under it hasn't actually been resolved.
