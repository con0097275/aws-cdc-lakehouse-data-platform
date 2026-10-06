# REALTIME Window Runbook

**Contract**: ADR-064. **Code**: `cdc/realtime.py`, `spark/jobs/realtime/realtime_engine.py`.

REALTIME is a **bounded rolling window over FULL_CDC**. It is a sibling of EOD, not a step
before it: EOD reads FULL_CDC directly, so nothing certified depends on this job's schedule
or on rows this window has already aged out.

---

## Shape first: what is IN the table

Before the window, the **shape** — and it is the only field a downstream reader needs
(ADR-082):

| `shape` | what the table holds | readable as current state? |
|---|---|---|
| `event_window` | the events themselves, bounded. A row updated three times appears three times | **no** — the reader must rank and collapse itself |
| `latest_state` | one row per business key, newest by source order | **yes**, with the tombstone rule — see `REALTIME_CURRENT_STATE_CONTRACT.md` |

`write_strategy` is the mechanism (`overwrite_window` / `append` / `guarded_merge`) and
changes cost and failure behaviour, never meaning. Not every pair is legal; the compiler
refuses the rest.

Today: `account`, `customer`, `loan`, `app_user` are `latest_state`;
`transaction`, `digital_event`, `payment_method` are `event_window`;
`branch`, `channel`, `merchant` have REALTIME off.

## Retention means different things per shape

For `event_window` the retention bound is a **prune**. For `latest_state` it is a **recovery
horizon and nothing is deleted by it** — a valid account may not change for months, and
dropping its row because its last event is old would empty the table of exactly the entities
that are most stable. `cdc-table-plan` prints this as `prunes_by_age`.

## How a run finds its rows

`processing.source_progress`:

* `window_scan` — re-filter the whole window every run. Cannot be incomplete, and re-reads
  96 hours 144 times a day.
* `iceberg_snapshot` — read only the FULL_CDC snapshots appended since the cursor in
  `ops.realtime_info`. Cheap, and correct only while the cursor is. Five conditions make a
  cursor untrustworthy and **all five fall back to the full scan with a recorded reason**
  (ADR-083, `REALTIME_FAILURE_RECOVERY.md` §2).

The window filter is applied either way: the window is the contract, the cursor is only how
candidates are found.

---

## What the window is

Three numbers, and they are not interchangeable:

| key | means | default |
|---|---|---|
| `lookback_days` / `window_hours` | how far back the window **serves** | 72h |
| `late_arrival_grace_days` / `late_grace_hours` | how far back it still **accepts** writes | 24h |
| `physical_retention_days` / `retention_hours` | how long rows physically **stay** | 168h |

**Retention must be at least lookback + grace, and the compiler enforces it.** Retention below
that deletes rows the window is still supposed to serve, and the result is not an error — it
is a window that quietly returns less than it claims.

## Two boundary kinds

```yaml
realtime: {window_hours: 72}                 # BOUNDARY_ROLLING_HOURS  -- 72h back from now
realtime: {lookback_days: 5}                 # BOUNDARY_CALENDAR_DAY   -- 5 whole days
```

`rolling_hours` moves continuously; `calendar_day` snaps to midnight in the table's business
timezone. They answer different questions and a table that switches gives different numbers
for the same request, so `cdc-table-plan` marks the change as business-semantic.

Eight of the nine shipped tables are `rolling_hours`; `oracle.coredb.corebank.loan` is the
`calendar_day` case, asserted by `test_no_deployed_table_changed_its_window_boundary`.

## Two refresh modes

* `incremental_merge` (default) — MERGE on `dv_event_id`. Idempotent, re-runnable.
* `full_refresh` — rebuild the window. Costs a full window read; use when a definition changed.

## Disabling it

```yaml
realtime: {enabled: false}
```

Correct for near-static reference tables — `branch`, `channel`, `merchant` ship this way. A
72h window over a table that changes twice a year is noise and a standing cost. EOD is
unaffected: it reads FULL_CDC.

---

## Operating

```bash
python3 scripts/cdc-table-plan.py --table <id>          # what the window is set to
python3 scripts/cdc-maintenance.py observe --table <id> # freshness / lag
```

Runs are recorded in the run ledger with the resolved window bounds, the run id, and the
counts. Bounds are recorded **as resolved**, not as configured — that is what makes a
misconfigured timezone visible after the fact rather than inferred.

### The window looks short

Check the resolved bounds in the ledger, not the config. A `calendar_day` window resolved in
the wrong business timezone is off by hours while still reporting the configured number of
days.

### Rows are missing that should be in the window

In order: (1) is the row in FULL_CDC at all — if not this is a capture problem, not a window
problem; (2) is its `source_commit_ts` inside `lookback + grace`; (3) has retention already
aged it out. Only (3) is a REALTIME defect and the compiler prevents it.

### Late data arrived outside the grace

It is in FULL_CDC and it will be in EOD. The window declines to accept it, which is the
contract — widen `late_arrival_grace_days` (and retention with it) if the source genuinely
runs that late.

## Rollback

REALTIME is **derived and disposable**. Any window can be rebuilt from FULL_CDC with
`full_refresh` at the cost of one window read. Nothing certified is at risk: EOD does not
read this layer, proven by a test on the plan key the engine would have to name.

## Cadence and sizing (Phase C, ADR-075)

Both live in `cdc/registry/sources.yaml` and are resolved at compile into the plan:

```yaml
realtime:
  schedule: "*/5 * * * *"      # omitted -> */10 * * * * (platform default)
  resource_profile: medium     # small | medium | large; omitted -> small
```

**One DAG per cadence, never per table.** Tables sharing a cron share a DAG and are
dynamic-mapped inside it. The default cadence keeps the id `cdc_realtime`; another cadence
produces e.g. `cdc_realtime_every_5m`. Moving a table between cadences moves it between DAGs,
so unpause the new one.

| profile | driver | executors | pool | timeout |
|---|---|---|---|---|
| `small` (default) | 1 core / 2g | 1 × 1 core / 2g | `reporting_jobs` | 20 min |
| `medium` | 2 / 4g | 2 × 2 / 4g | `reporting_jobs` | 45 min |
| `large` | 2 / 8g | 4 × 4 / 8g | `spark_jobs` | 90 min |

An unreadable cron or an unknown profile **fails at compile** (`make cdc-check`), not in
Airflow, because a DAG that fails to import disappears from the UI.

```bash
make cdc-compile && make cdc-verify          # resolve + confirm the plan is not stale
python3 -c "import sys;sys.path.insert(0,'.');import json;from cdc.realtime import schedule_groups;\
print(schedule_groups(json.load(open('artifacts/cdc/table-plan.json'))))"
```


---

## Two shapes: event window, or latest state

`refresh_mode` decides what the layer IS. They are different products, and a table picks one.

| | `full_refresh` (default) | `latest_state` |
|---|---|---|
| contents | every event in `[grace_lower, upper)` | one row per `dv_pk_hash` |
| written by | overwrite, whole window, every run | guarded MERGE + day-boundary rebuild |
| deletes | present as events | **tombstones** (`is_deleted`) until the next rebuild |
| `rows == distinct dv_pk_hash`? | **no** — several versions per key | yes |
| consumer collapses? | yes, `transform.collapse_to_grain` | no |
| `delete_policy` | any | **must be `soft_flag`** |
| good for | append-heavy tables, and any consumer that ranks anyway | hot-path reads of current state |

### Why `latest_state` rebuilds instead of upserting forever

An upsert cannot express *"this row should no longer exist at all"*. Three things accumulate
without a rebuild:

1. **tombstones** — a delete must be RETAINED, not applied, or a late out-of-order event
   resurrects the key (the guard has nothing left to compare against)
2. **aged-out keys** — a key whose last event left the window never leaves the table
3. **drift** — any gap in any single run persists indefinitely

The rebuild re-derives the whole window from FULL_CDC in one pass and clears all three. It
fires when the previous **SUCCEEDED** run's upper bound falls on an earlier day:

```
REALTIME_LATEST_STATE oracle.coredb.corebank.account REBUILD (day boundary)
REALTIME_LATEST_STATE oracle.coredb.corebank.account upsert
```

A failed run does not count as "we already rebuilt today" — that would skip the rebuild
exactly when the state is least trustworthy.

### Before you enable it on a table

**Its readers must filter `is_deleted`.** Nothing does today: not `stream_batch_flow`, not
`flow_runner`, not the dbt models. Between rebuilds a tombstone is a real row, and an
unfiltered consumer would carry a deleted key into a mart.

`config_loader` refuses `latest_state` with `delete_policy: exclude_from_snapshot` for
exactly that reason — the two say opposite things about a deleted key, and `soft_flag` is the
pairing whose contract already means "deleted rows are present and flagged".

```yaml
realtime: {refresh_mode: latest_state}
eod:      {delete_policy: soft_flag}
```

`make check` fails loudly if you set one without the other.
