# ADR-040 — Five reporting execution modes and source-layer policy

- Status: **ACCEPTED** (Session 22)
- Related: ADR-033, ADR-041, ADR-042, ADR-043

## Context

The framework must support EOD, AUTO_CORRECT, FULFILL, STREAM_BATCH and STREAMING_RT.
The references map onto these unevenly, and two of the names collide with existing
concepts in this repository (`docs/REPORTING_REFERENCE_EVIDENCE.md` §12 F10):

- repo 1's `job_type='stream'` is an **Airflow micro-batch**, not streaming;
- repo 2's `_fulfilled` is a **late EOD close**, not a backfill;
- this repo's `spark/full_fill/` is **dimension-key repair**
  (`flow_runner.py:107 unresolved_rows` selects rows on `UNKNOWN_SK = -1`), not a
  business-date backfill;
- no reference implements backfill gap detection at all (finding F5).

## Options

| Option | Verdict |
|---|---|
| **Five modes, each = source layer + window + tier; one model per mart** | **CHOSEN** |
| One model per (mart, mode) — four near-identical SQL files | Rejected |
| Fewer modes: fold FULFILL into EOD with a date parameter | Rejected |
| Inherit the reference vocabulary as-is | Rejected |

Per-mode SQL makes a provisional-vs-certified variance ambiguous between "late data" and
"divergent logic", which is the argument `docs/FOUR_FLOWS.md` §1 already makes. Folding
FULFILL into EOD loses gap detection and the dry-run gate, which is where the expensive
mistake lives. Inheriting the vocabulary carries three name collisions (F10) into new code.

## Decision

**Five modes, each defined by exactly three things** — source layer, window, and the tier
it stamps. Everything else is shared, extending the property `spark/common/flows.py`
already enforces for the four Spark flows.

| Mode | `source_layer_policy` | Window | Stamps | Cadence |
|---|---|---|---|---|
| `EOD` | `EOD` | the closed COB date, pinned to its tag | `CERTIFIED` | daily, after the close gate |
| `AUTO_CORRECT` | `EOD_PLUS_REALTIME` | `[cob − lookback_days, now)` | `PROVISIONAL_CORRECTED` | every 30 min in-window |
| `FULFILL` | `FULL_CDC_OR_EOD` | explicit dates from a gap plan | tier of the source layer | manual |
| `STREAM_BATCH` | `REALTIME` | `[watermark − overlap, bounded_now)` | `PROVISIONAL_NRT` | 5–15 min in-window |
| `STREAMING_RT` | `FULL_CDC_APPEND` \| `KAFKA_DIRECT` | continuous | `PROVISIONAL_NRT` | continuous (ADR-041) |

`FULL_CDC_RAW` (L1) is not a permitted value (ADR-033).

**Policy is config, not convention.** A job that does not declare a
`source_layer_policy` for a mode **cannot run that mode**; compile fails rather than
runtime defaulting. This is the §11 requirement that random jobs must not choose layers.

**`FULFILL` picks its layer per date**, not per job:

```
target_date <= last EOD close   → EOD      (deterministic; read at the date's tag)
otherwise                        → FULL_CDC (rebuild state at an explicit cutoff)
```

**`FULFILL` detects its own gaps**, which no reference does:

```
expected business dates (calendar)
  MINUS dates with a SUCCEEDED execution for (job_id, FULFILL|EOD)
  MINUS dates present and complete in the target mart
  = {missing, failed, incomplete}
```

`--dry-run` prints the plan and exits, and **defaults to true**. Dates already SUCCEEDED
are skipped unless `--force`.

**Naming collision resolved.** The existing `spark/full_fill/` flow is renamed in
documentation and config vocabulary to **`DIMENSION_REPAIR`**. It remains a Spark flow
outside the five reporting modes; its behaviour is unchanged, including its refusal to
stamp a status (`flows.py:99-108`), which is what stops it downgrading a certified row.

**One model per mart, not five.** The mode differences live entirely in
`resolve_source_layer()` and `incremental_filter()`. Four near-identical SQL files would
make a provisional-vs-certified variance ambiguous between "late data" and "divergent
logic" — the exact argument `docs/FOUR_FLOWS.md` §1 already makes, enforced there by
`test_all_four_flows_produce_identical_business_columns`. The reporting framework
inherits that test shape.

## Consequences

- `docs/FOUR_FLOWS.md` gains a mapping to the five reporting modes.
- A mart may enable a subset of modes; `job_flow_config.is_enabled` is per mode, which
  repo 1's single `is_skipped` could not express.
- AUTO_CORRECT gains impact analysis (repo 3's five scan sources,
  `main_autocorrect.py:472-585`) with a documented fallback to the full lookback window
  when impact cannot be derived; which path ran is recorded on the execution row.

## Cost

`STREAM_BATCH` frequency dominates cost, not size — recorded already in
`flow_runner.py:12-18`. `job_flow_config.cost_guard` carries `max_bytes_scanned_gb` and
`max_execution_minutes` per mode, and the intraday modes are paused outside the metered
window.

## Security

None.

## Rollback

Modes are per-job config. Disabling one is a config change; no data is affected.

## Validation

- Compile fails when a mode is enabled without a `source_layer_policy`.
- Compile fails on `FULL_CDC_RAW` as a source policy.
- All five modes produce identical business columns from identical input; only flow
  metadata differs.
- FULFILL gap detection finds a deliberately failed date and omits a successful one.
- `--dry-run` performs no write (asserted by snapshot count before/after).
