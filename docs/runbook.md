# Demo Runbook

**Status: drafted from the built pipeline code, not yet executed against a live
workspace** (see README.md — every step below needs to be walked through and
corrected against what actually happens on a real cluster before this is
"demo-ready." Per Phase 6's Definition of Done, do two clean dry runs before
trusting this in front of a prospect.)

## One-time setup (per workspace)

1. `databricks configure` with the workspace host + PAT/OAuth token.
2. Create the catalog/schemas: `grid_poc.bronze`, `grid_poc.silver`, `grid_poc.gold` (Unity Catalog).
3. Create Volumes for landing zones: `/Volumes/grid_poc/bronze/landing/sensor_readings_raw`, `/lcl_smart_meter_raw`, `/control`.
4. Upload `data/raw/lcl_sample/lcl_sample.csv` to the `lcl_smart_meter_raw` volume path.
5. Create a DLT pipeline (`grid_poc_bronze_silver_gold` per `scripts/demo_runner.py`) covering `pipelines/10_bronze_ingestion.py`, `20_silver_transform.py`, `40_gold_serving.py`. **Right-size the cluster** — comtrade-accelerator's own default is a 5-worker cluster, oversized for a demo (Hard Constraint #4); start with the smallest/serverless option that keeps the stream responsive and scale up only if the dashboard lags during rehearsal.
6. Create a job running `pipelines/30_fault_detection_stream.py` (`grid_poc_fault_detection_stream`).
7. Create a job running `src/telemetry_generator` (`grid_poc_telemetry_generator`) with `--sink parquet --out /Volumes/grid_poc/bronze/landing/sensor_readings_raw --control /Volumes/grid_poc/bronze/landing/control/trigger.json`.
8. Build the AI/BI dashboard per `docs/dashboard_plan.md` (use `docs/dashboard_draft.lvdash.json` as an unverified starting point).
9. Create the Genie space per `docs/genie_space.md`; test all 5 NL questions before the first dry run.
10. Fill in the `[CONFIRM]` placeholders in `scripts/demo_runner.py` (`<SQL_WAREHOUSE_ID>`, job/pipeline names) once the above resources exist.

## Running the demo

```
python scripts/demo_runner.py reset                                        # clean slate
python scripts/demo_runner.py start                                        # pipeline + jobs up
# wait ~30s for the stream to warm up, watch the dashboard
python scripts/demo_runner.py full-demo --device-id dev_0005 --fault-type transient
```

`full-demo` triggers the fault and polls `gold.fault_classifications` until it
shows up (or times out at 15s — adjust `--timeout-s` if real latency runs
higher). Then, in front of the prospect:

1. Point at the dashboard's feeder status tile — should show the targeted feeder go red.
2. Ask Genie one of the 5 tested questions (e.g. "which feeder had the most recent fault") and confirm it names the right feeder.
3. Point out the same `gold` tables answer both — the "one governed data model" point from the business context.

Per Phase 6: **run this twice** before trusting it in a real demo. If timing or
results differ between the two runs, that's a repeatability problem to fix, not
something to paper over.

## Troubleshooting

- **No fault appears in `gold.fault_classifications`**: check the generator job is actually running and writing to the landing volume (`databricks fs ls`); check the DLT pipeline's bronze table is picking up new files (Auto Loader schema location can get stuck — check pipeline event log); check the detection job (`30_fault_detection_stream.py`) is alive, not just the DLT pipeline.
- **Genie gives a wrong/stale answer**: gold tables may not have refreshed yet — re-run `full-demo` and wait for `confirm` to succeed before asking Genie.
- **Comtrade bronze table is empty**: expected — the sample S3 bucket returned `AccessDenied` on anonymous access as of 2026-08-14 (README.md risk callout). If this repros from the workspace too, `comtrade_events_raw`/`fault_events`/the CNN path are all blocked; the demo still works off the statistical detector alone, which doesn't depend on Comtrade data.
- **`grid_context` tile is empty**: expected, EIA is stubbed (no API key).

## Known gaps going into the first real dry run

See README.md's "What's still a placeholder / open" section for the full list.
The two most likely to bite during setup: the `applyInPandasWithState`
serialization in `30_fault_detection_stream.py` (written carefully but
genuinely unverified), and the Comtrade bucket access risk above.
