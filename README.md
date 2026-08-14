# Grid Reliability Accelerator POC — Build Status

Sales-enablement POC for a Databricks consulting partner: real-time grid-edge fault
detection for electric utilities (Delta Lake, Unity Catalog, Structured Streaming,
AI/BI dashboards, Genie). Built per `CLAUDE_CODE_BUILD_PROMPT.md` in this directory —
read that first for full business context and requirements.

Last updated: 2026-08-14.

## Standing decisions (resolved [ASK USER]/[CONFIRM] items)

| Item | Decision | Why |
|---|---|---|
| Databricks workspace access | **Connected 2026-08-14.** Free Edition workspace `https://dbc-d79ec249-6e44.cloud.databricks.com`, CLI profile `grid_poc`, OAuth browser login (not a PAT). Created catalog `grid_poc` with `bronze`/`silver`/`gold` schemas, alongside the existing `wind_dev` catalog (left untouched — that's your other project). Cloned the GitHub repo into the workspace at `/Repos/dhaval.m@brilworks.com/grid-reliability-accelerator-poc`. | You provided the workspace URL and confirmed connecting. |
| EIA Open Data API key | **Skipped for now.** `gold.grid_context` / EIA ingestion is stubbed, not implemented. | You chose to skip EIA. |
| Project location | `C:\Users\LENOVO\Downloads\energy` | Confirmed. |
| Dev tooling | Git 2.55 and Python 3.12 installed via winget (machine had neither). | Needed to clone repos / run code. |
| Accelerator repo name | Spec says `databricks-industry-solutions/grid-edge-analytics` — **that repo doesn't exist (404)**. Using `databricks-industry-solutions/comtrade-accelerator` instead: matches the spec's description exactly (Comtrade fault detection, DLT, electric utilities). | You confirmed this substitution after I found it via GitHub search. |
| `digital-twin` scope | Repo has evolved into a manufacturing-specific Zerobus/RDF/Lakebase/Databricks-Apps system, not the simple line-data generator the spec describes. Only actually used its `line_data_generator` submodule as an architectural **pattern reference** (seeded per-entity generation, configurable noise/anomaly injection) — not run or adapted directly, since it's manufacturing-domain and depends on a non-PyPI package (`mandrova`). | Spec only cited that submodule, not the whole repo. |
| CNN vs statistical detector for the live stream | comtrade-accelerator's CNN consumes 726-sample, high-resolution 3-phase **waveform captures** (relay-triggered, sub-second). Our synthetic stream is continuous 1 reading/sec telemetry — a different data modality. Plan: use the statistical fallback (Hard Constraint #2) as the **primary live detector** in Phase 4; the CNN stays validated separately against native Comtrade data, not force-fit onto the synthetic stream. | Physical/architectural mismatch, not a cost or access problem. Still open to reconsider in Phase 4. |
| Catalog/schema naming | Using the spec's suggestion as-is: `grid_poc.bronze` / `grid_poc.silver` / `grid_poc.gold`. | No existing workspace to check a convention against — nothing to conflict with yet. |

## ⚠️ Open risk: Comtrade sample bucket may not be publicly readable

`s3://db-gtm-industry-solutions/data/rcg/comtrade/source/` (the Comtrade sample data
Hard Constraint #2 requires cloning/running against) returned **`AccessDenied` on
anonymous HTTPS GET and prefix listing** when checked directly (2026-08-14). This
could mean: (a) it only allows reads from an authenticated AWS/Databricks execution
context and will work fine once we're actually running on a Databricks cluster, or
(b) the bucket has been locked down since the accelerator's 2023 publication and
won't work there either. **Not yet verified either way — needs a live workspace to
test.** If (b), Hard Constraint #2's fallback applies immediately: skip the CNN
entirely (not just for the synthetic stream, as already decided above, but for
*any* real training) and go straight to the statistical detector. Re-test this as
the first thing once Databricks credentials are available, before spending any more
time on CNN-adaptation code.

## Phase status

- [x] **Phase 1 — Validate accelerators.** Cloned `comtrade-accelerator` and `digital-twin` into `accelerators/`. Read through all 5 comtrade-accelerator notebooks and RUNME.py. Could not run end-to-end (no workspace) — validated by code inspection instead. Key finding: its gold table (`fault_detection_gold`) outputs a continuous `fault_score`, not a transient/sustained label — Phase 4 will need to add a duration-based rule on top of it (or the statistical fallback) to produce `fault_type`.
- [x] **Phase 2 — Synthetic generator.** `src/telemetry_generator/` — done and tested standalone (4/4 smoke tests pass, CLI verified live: triggered a transient fault, watched voltage sag/current spike/self-clear). See "What's built" below.
- [x] **Phase 3 — Ingestion pipeline (code written, not yet run).** `pipelines/00_config.py`, `10_bronze_ingestion.py`, `20_silver_transform.py` — DLT definitions for all 4 bronze sources + unified `silver.sensor_readings` + `silver.fault_events`. EIA is a stubbed empty table (no key, per your "skip for now"). LCL: downloaded the real 759MB zip, inspected its actual structure (168 blocks, `LCLid`/`DateTime`/`KWH/hh (per half hour) ` columns), pulled 300 households × 3 months each (1.3M rows, 53.5MB) via `scripts/fetch_lcl_sample.py`, sanity-checked the derived current values against physical expectations (mean ~2.2A, peak ~58A at 230V — plausible household loads). Comtrade S3 source returned `AccessDenied` on anonymous access — flagged as an open risk above, needs testing from an actual Databricks cluster. None of this DLT code has been run against a live workspace yet.
- [x] **Phase 4 — Detection (statistical detector done + tested; Spark wiring written, not run).** `src/fault_detector/` — per-device rolling z-score detector with a duration-based transient/sustained rule, tested standalone against the Phase 2 generator (3/3 tests pass: correct transient classification, in-flight upgrade to sustained, zero false positives over 1200 normal readings). `pipelines/30_fault_detection_stream.py` ports the same logic into Spark's `applyInPandasWithState` + `MERGE INTO gold.fault_classifications` — written as a plain Structured Streaming job rather than a DLT table (arbitrary stateful processing doesn't fit DLT's Python table API cleanly). Unverified against a real cluster, as with the rest of the pipeline code.
- [x] **Phase 5 — Serving (drafted, not built/tested — needs a live workspace).** Added `gold.feeder_health` + `gold.grid_context` (`pipelines/40_gold_serving.py`), completing the medallion architecture. `docs/genie_space.md` — 5 tested-in-design NL questions with expected SQL, table/column descriptions for grounding, gold-only table list. `docs/dashboard_plan.md` — 5-tile content plan; `docs/dashboard_draft.lvdash.json` — best-effort 2-tile JSON skeleton, explicitly low-confidence (Lakeview's JSON schema is intricate enough that I don't trust a memory-written version without test-importing it).
- [x] **Phase 6 — Drafted, not run (needs a live workspace).** `scripts/demo_runner.py` (reset/start/trigger/confirm/full-demo via Databricks CLI — resource names are `[CONFIRM]` placeholders to fill in once created), `docs/runbook.md` (setup steps + demo sequence + troubleshooting), `docs/data_dictionary.md` (actual-as-built schema for every table across all 4 layers, with every deviation from the spec's suggested schema called out). The spec's "run twice for repeatability" requirement is written into the runbook but not yet actually done.

## What's built so far

### `src/telemetry_generator/` (Phase 2 — done)

Parameterized synthetic grid telemetry generator, runnable standalone (no Databricks needed).

- `generator.py` — `TelemetryGenerator` class. 50–200+ simulated devices across feeders, 1 reading/device/tick baseline, seeded/deterministic per device.
- `faults.py` — fault physics: voltage sag (35–70% of nominal) + current spike (2–5x) + small frequency offset. Transient faults last 0.5–2.5s, sustained faults 8–30s, drawn 70/30 to match the business context's "~70% of faults are transient" framing.
- `sinks.py` — pluggable output: `console`, `jsonl`, `csv`, `parquet`. Parquet sink writes micro-batch files in the directory shape Auto Loader's `cloudFiles` will later ingest from — swapping to real bronze ingestion is a path change, not a rewrite.
- `cli.py` — `python -m src.telemetry_generator run ...` and `... trigger ...`. `trigger` writes a small control file; a running `run` process polls it each tick, so a fault can be injected on demand into an already-live stream (the requirement for the live demo moment) without restarting anything.
- Schema produced: `reading_id, device_id, feeder_id, ts, voltage, current, frequency, event_flag, source` (matches the spec's suggested fields exactly) plus `_fault_id`/`_fault_type_truth` — ground-truth columns for demo validation only, not something a real feed would have.
- `tests/test_generator.py` — standalone smoke tests, run with `python tests/test_generator.py`.

### `accelerators/` (Phase 1 reference material, not our code)

- `comtrade-accelerator/` — cloned, read, not modified yet. Source of the fault-detection model approach for Phase 4.
- `digital-twin/` — cloned, read for pattern reference only (see decisions table above).

### `pipelines/` (Phase 3 — code written, not yet run)

- `00_config.py` — catalog/schema names, source paths, the LCL voltage assumption.
- `10_bronze_ingestion.py` — DLT tables: `sensor_readings_raw` (Auto Loader over the generator's parquet landing zone), `comtrade_events_raw` (binaryFile + join-on-filename + comtrade-library decode, adapted from comtrade-accelerator's own DLT notebook), `lcl_smart_meter_raw` (Auto Loader over the CSV subset), `eia_grid_data_raw` (stub, empty).
- `20_silver_transform.py` — `sensor_readings` (unions cleaned synthetic readings with LCL-derived readings — LCL kWh converted to a current-like value via P=E/0.5h, I=P/230V, tagged `source='lcl'` and a distinct `feeder_lcl_reference` feeder namespace so it can't silently blend into real feeder rollups downstream), `fault_events` (Comtrade events flattened/pivoted to IA/IB/IC per timestep, feeds Phase 4's model input shape).

### `scripts/fetch_lcl_sample.py` (Phase 3 — done, run against the real dataset)

Downloads/reads the LCL zip's block CSVs (without extracting all 168), collects 300 households' first 3 months each, writes `data/raw/lcl_sample/lcl_sample.csv` (1.3M rows, 53.5MB — 0.8% of the full ~167M-row dataset). Verified the derived current values are physically plausible (mean ~2.2A, peak ~58A at 230V for household loads).

### `src/fault_detector/` (Phase 4 — statistical detector done + tested)

- `detector.py` — `StatisticalFaultDetector`. Per-device rolling baseline (trailing 30 readings, only updated while *not* anomalous so a fault doesn't drag its own baseline toward it) drives a z-score test on voltage/current (threshold 6σ — with the generator's noise levels, a real fault produces z-scores in the hundreds, so this has essentially zero false-positive rate while catching every injected fault). An active anomaly becomes a tracked event; duration ≥5s while still active upgrades it to `sustained` in place (so the demo shows near-real-time classification, not just a post-hoc one); clearing before 5s finalizes it as `transient`.
- Emits `gold.fault_classifications`-shaped rows (`event_id, device_id, feeder_id, timestamp, fault_type, confidence_score` — `device_id` added beyond the spec's 4 listed columns since it's needed for the demo narrative/dashboard drill-down).
- `tests/test_detector.py` — 3/3 pass: transient correctly classified after clearing, sustained correctly upgraded mid-event (before the fault even ends), zero false positives across 20 devices × 60s of normal operation.
- `pipelines/30_fault_detection_stream.py` ports the identical per-device logic (literally imports the same dataclasses from `src/fault_detector/detector.py`) into Spark's `applyInPandasWithState`, grouped by `device_id`, upserting into `gold.fault_classifications` via `foreachBatch` + `MERGE INTO`. Written as a **plain Structured Streaming job, not a DLT table** — arbitrary stateful processing doesn't fit DLT's Python table API cleanly, so this runs as its own query rather than alongside `pipelines/10`/`20`. **Unverified against a real cluster** — the state-serialization plumbing (pickling the baseline/event objects into the `BINARY` columns `applyInPandasWithState` expects) is written carefully but is exactly the kind of thing that needs a real Spark session to confirm.

### `pipelines/40_gold_serving.py` + `docs/` (Phases 5-6 — drafted, not built/tested)

- `40_gold_serving.py` — `gold.feeder_health` (1-minute windowed aggregation over `silver.sensor_readings`, stream-static joined to `gold.fault_classifications` for a per-window fault count) and `gold.grid_context` (stub pass-through of the still-empty EIA bronze table).
- `docs/genie_space.md` — which 3 gold tables to add, column descriptions for grounding, the 5 required NL questions with their expected SQL (to verify Genie's actual answers against during Phase 6).
- `docs/dashboard_plan.md` — 5-tile content plan (feeder status grid, voltage/current time series, recent fault events, week-over-week fault count, EIA context stub). `docs/dashboard_draft.lvdash.json` — a deliberately minimal 2-tile (table-only) JSON skeleton; explicitly flagged low-confidence since I can't test-import Lakeview's JSON schema without a workspace.
- `scripts/demo_runner.py` — CLI orchestration (`reset` / `start` / `trigger` / `confirm` / `full-demo`) shelling out to the `databricks` CLI. Resource names (job/pipeline names, SQL warehouse ID) are `[CONFIRM]` placeholders — fill in once those resources actually exist.
- `docs/runbook.md` — one-time setup steps, the demo run sequence, troubleshooting notes, and a pointer back to the known-gaps list below.
- `docs/data_dictionary.md` — actual-as-built schema for every bronze/silver/gold table, with every deviation from the spec's suggested schema explicitly called out (e.g. `fault_classifications.device_id` added, LCL's derived/non-measured voltage-current-frequency values, the `feeder_lcl_reference` namespace).

## What's still a placeholder / open

- **No Databricks workspace connected.** Nothing in `accelerators/` or `pipelines/` has actually been run end-to-end. Everything is validated by local execution (generator, LCL fetch) or code reading (accelerators, DLT pipeline code). This blocks real completion/verification of Phases 3–6.
- **EIA regional context data** — not implemented, no API key. `gold.grid_context` doesn't exist yet.
- **Comtrade bucket access** — returned `AccessDenied` anonymously; unverified whether it works from within Databricks. See risk callout above.
- **`applyInPandasWithState` state serialization** (`pipelines/30_fault_detection_stream.py`) — written to spec but genuinely unverified; this is the piece most likely to need a real-cluster debugging pass.
- **Cost-conscious cluster sizing** — comtrade-accelerator's own `RUNME.py` provisions a 5-worker cluster by default; will need to be right-sized (or moved to serverless) before any real deployment, per Hard Constraint #4.
- **LCL data quality quirk** — a handful of rows (6 out of 1.3M in the sample) have the literal string `"Null"` in the kWh column instead of being empty; the silver transform's `.cast("double")` + `.isNotNull()` filter already handles this correctly, noting it here so it isn't mistaken for a bug later.
- **Dashboard JSON** — only a 2-tile skeleton exists; the other 3 tiles in `docs/dashboard_plan.md` (line chart, status-grid, counter) need building in the Lakeview UI directly, since I judged their widget-spec schema too uncertain to hand-write reliably.
- **`scripts/demo_runner.py` placeholders** — job/pipeline names and the SQL warehouse ID are placeholders; fill in once those resources are created (see `docs/runbook.md` step 10).
- **Two clean dry runs** — not done (needs a workspace). Per Phase 6, don't trust the demo until this happens.

## Repo layout

```
energy/
  CLAUDE_CODE_BUILD_PROMPT.md   the build spec — source of truth for requirements
  README.md                     this file
  requirements.txt              local Python deps (pandas, pyarrow)
  src/telemetry_generator/      Phase 2 generator (done)
  src/fault_detector/           Phase 4 statistical detector (done + tested)
  pipelines/                    Phase 3-5 DLT/streaming code (written, not run)
  scripts/fetch_lcl_sample.py   Phase 3 LCL subset puller (done)
  scripts/demo_runner.py        Phase 6 demo orchestration (drafted, not run)
  docs/                         Phase 5-6 Genie/dashboard/runbook/data-dictionary (drafted)
  tests/                        smoke tests
  accelerators/                 cloned reference repos (Phase 1, read-only reference)
  data/raw/lcl_sample/          LCL subset (300 households, 3 months each)
  data/bronze/                  local landing zone the generator writes to
  control/                      trigger.json control file for the CLI's `trigger` command
```

## Next step

Workspace access is now connected and the repo is live in `/Repos` (see above).
Remaining before demo-ready: run `docs/runbook.md`'s setup steps for real (Volumes,
DLT pipeline, jobs, SQL warehouse ID already known: `a7d6c2cb218c12fa`), fix whatever
breaks (the `applyInPandasWithState` state serialization and the Comtrade bucket
access risk are the two most likely spots — worth testing the Comtrade bucket from
an actual cluster/serverless context first, since that's now possible and decides
whether Phase 4's CNN path is viable at all), build the 3 dashboard tiles that
weren't safe to hand-write as JSON, and complete the two required dry runs.
