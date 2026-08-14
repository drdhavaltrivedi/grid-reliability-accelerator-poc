# Data Dictionary — as actually built

Per Definition of Done: "Data dictionary matches what was actually built." This
supersedes the suggested schemas in `CLAUDE_CODE_BUILD_PROMPT.md`'s Data
Architecture section wherever they differ — differences are called out inline.
Schemas below are as written into `pipelines/*.py` and `src/*/`; **not yet
confirmed against an actual running pipeline** (see README.md).

## Bronze (`grid_poc.bronze`)

### `sensor_readings_raw`
| Field | Type | Notes |
|---|---|---|
| reading_id | string | |
| device_id | string | |
| feeder_id | string | |
| ts | string (ISO 8601) | cast to timestamp in silver |
| voltage | double | |
| current | double | |
| frequency | double | |
| event_flag | int | 0/1, generator-produced — **dropped in silver**, see below |
| source | string | always `'synthetic'` at bronze |
| _fault_id | string, nullable | ground truth, demo validation only — **dropped in silver** |
| _fault_type_truth | string, nullable | ground truth, demo validation only — **dropped in silver** |
| _ingested_at | timestamp | added by Auto Loader ingestion |

### `comtrade_events_raw`
| Field | Type | Notes |
|---|---|---|
| base_name | string | filename (minus extension), shared key across .cfg/.dat |
| frequency, rec_dev_id, station_name | various | from CFG header |
| microseconds | array\<long\> | per-timestep |
| analog | array\<array\<double\>\> | per-timestep, per-channel |
| analog_units, analog_channel_names | array\<string\> | |
| _ingested_at | timestamp | |

### `lcl_smart_meter_raw`
| Field | Type | Notes |
|---|---|---|
| LCLid | string | household id |
| stdorToU | string | tariff type, unused downstream |
| DateTime | string | cast to timestamp in silver |
| `KWH/hh (per half hour) ` | string/double | **trailing space in the real header — preserved as-is** |
| _ingested_at | timestamp | |

### `eia_grid_data_raw` — STUB, empty
| Field | Type |
|---|---|
| region | string |
| period | string |
| demand_mwh | double |
| generation_mwh | double |

## Silver (`grid_poc.silver`)

### `sensor_readings` — unified schema, matches spec table exactly
| Field | Type | Notes |
|---|---|---|
| reading_id | string | |
| device_id | string | for `source='lcl'`, this is the household `LCLid` |
| feeder_id | string | for `source='lcl'`, always the reference namespace `'feeder_lcl_reference'` — **not a real feeder**, see README.md |
| ts | timestamp | |
| voltage | double | for `source='lcl'`, a constant (`230.0`, assumed UK domestic) — not measured |
| current | double | for `source='lcl'`, **derived** from consumption (`I = kWh/hh * 2000 / 230`), not measured |
| frequency | double | for `source='lcl'`, `50.0` (true UK grid frequency, deliberately not faked to 60Hz) |
| source | string | `'synthetic'` \| `'lcl'` — **`'eia'` never appears here**, EIA feeds `gold.grid_context` directly, not this table (matches the spec's own Silver section, which doesn't list an EIA silver table) |

### `fault_events` — parsed Comtrade events
| Field | Type | Notes |
|---|---|---|
| event_id | string | = `comtrade_events_raw.base_name` |
| ts | timestamp | per-timestep |
| station_name | string | |
| IA, IB, IC | double, nullable | 3-phase current channels, pivoted to columns |

Deviation from spec: the spec didn't give `fault_events` an explicit schema
("parsed Comtrade events") — this is the schema needed to feed the CNN's input
shape, matching comtrade-accelerator's own `readings` table.

## Gold (`grid_poc.gold`)

### `fault_classifications`
| Field | Type | Notes |
|---|---|---|
| event_id | string | |
| device_id | string | **added beyond the spec's 4 listed columns** — needed for demo narrative/dashboard drill-down |
| feeder_id | string | |
| timestamp | timestamp | |
| fault_type | string, nullable | `'transient'` \| `'sustained'` \| `NULL` (event still open, not yet classified — see `docs/genie_space.md` for how to explain this to Genie) |
| confidence_score | double | `min(1.0, peak_z_score / 20.0)` — a detector-confidence heuristic, not a calibrated probability |

### `feeder_health`
| Field | Type | Notes |
|---|---|---|
| window_start, window_end | timestamp | 1-minute tumbling window |
| feeder_id | string | |
| avg_voltage, avg_current, avg_frequency | double | |
| device_count | long | distinct devices reporting in the window |
| fault_count | long | distinct fault events in the window (stream-static join to `fault_classifications`) |
| health_status | string | `'fault'` if `fault_count > 0` else `'healthy'` |

Deviation from spec: spec described this table only as "aggregated status per
feeder per window" without an explicit schema — the above is my design to fill
that in.

### `grid_context` — STUB, empty
| Field | Type |
|---|---|
| region | string |
| period | string |
| demand_mwh | double |
| generation_mwh | double |
