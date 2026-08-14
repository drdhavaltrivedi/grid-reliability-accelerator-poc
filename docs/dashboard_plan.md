# AI/BI Dashboard Plan (Phase 5)

**Status: content plan drafted; JSON attempted but low-confidence.** The Lakeview
dashboard JSON format (`.lvdash.json`) is complex and version-specific enough that I
don't trust myself to hand-write a fully correct one from memory without a workspace
to test-import it against - a subtly wrong widget spec can fail to import or render
wrong in ways that are hard to debug remotely. The content plan below is the reliable
part; build the actual dashboard in the Lakeview UI against this plan (fastest path),
or use `dashboard_draft.lvdash.json` as a rough starting skeleton and expect to fix it
by hand.

## Data sources (gold layer only, live-refreshing)

- `grid_poc.gold.feeder_health`
- `grid_poc.gold.fault_classifications`
- `grid_poc.gold.grid_context` (empty until EIA is un-stubbed — leave the tile in place, it'll populate later)

## Tiles

1. **Feeder status grid** (table or colored cards, one per feeder) — latest `window_end` row per `feeder_id` from `feeder_health`: `feeder_id`, `health_status` (color: green=healthy/red=fault), `avg_voltage`, `avg_current`, `device_count`. This is the tile a prospect stakeholder looks at during the live demo moment - the fault should turn a card red within a few seconds of triggering it.

2. **Voltage/current time series** — line chart, `avg_voltage` and `avg_current` from `feeder_health` over `window_start`, filterable by `feeder_id` (dashboard-level filter widget). Shows the sag/spike shape visually when a fault is triggered.

3. **Recent fault events table** — `fault_classifications` sorted by `timestamp` descending, columns `timestamp, feeder_id, device_id, fault_type, confidence_score`. This is what proves the transient/sustained classification is real, not just a flag.

4. **Fault count this week vs last week** — counter/comparison tile, same logic as Genie question 2 (`docs/genie_space.md`) — gives the dashboard and Genie a visibly identical answer to the same question, reinforcing the "one governed data model" narrative point.

5. **(stub) Regional grid context** — small tile reading `grid_context`, e.g. latest `demand_mwh`/`generation_mwh` for context. Will show "no data" until EIA is wired up; keep it in the layout as a visible placeholder rather than omitting it, so the gap is obvious rather than silently missing.

## Refresh / live behavior

Set the dashboard's schedule/refresh to the shortest practical interval (dashboards
poll on a schedule, they don't push) — a few seconds to ~30s if the SQL warehouse
supports it responsively, given Hard Constraint #4's cost-consciousness. Phase 6's
dry run should confirm the actual observed latency between triggering a fault and
seeing tile 1 go red.

## Filters

- Feeder picker (drives tiles 1-2)
- Date range picker (drives tiles 3-4, default: last 24 hours for the demo)
