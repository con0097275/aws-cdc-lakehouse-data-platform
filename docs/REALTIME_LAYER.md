# The REALTIME layer — config-driven window materialisation

REALTIME is a **bounded window of FULL_CDC**, materialised on a schedule. It is not a second
consumer of Kafka and it is not a reporting flow. ADR-064; engine
`spark/jobs/realtime/realtime_engine.py`; config `cdc/registry/sources.yaml` under `realtime:`.

```
Kafka --(decode, quarantine, MERGE)--> FULL_CDC   <- CANONICAL
                                          |
                            +-------------+-------------+
                            v                           v
                   REALTIME [T-N, T)              EOD (as-of T-1)
                            |
                            v
                   STREAM_BATCH reporting flow   <- reads REALTIME, keeps its OWN watermark
```

## 1. The four bounds

A run resolves four instants and then freezes them. They are genuinely four different things
and conflating any two produces a table that looks right and is not.

| bound | meaning |
|---|---|
| `upper` | the **frozen** run instant. Never moves while the run executes |
| `logical_lower` | what the layer **promises** to serve. The SLA is about this window |
| `grace_lower` | what is actually **materialised** — wider, so a late-arriving event is present rather than silently absent |
| `retention_bound` | rows older than this are pruned. At or below `grace_lower`, enforced at compile time |

Materialised range is `[grace_lower, upper)` — half-open at both ends. An inclusive upper
would put an event committed exactly on the boundary into two consecutive runs, and a
consumer counting across runs would double it.

## 2. "3 days" is not 72 hours

This is the distinction the layer exists to get right, and both readings are defensible:

| | `rolling_hours` | `calendar_day` |
|---|---|---|
| lower bound | `upper - N hours` | midnight in `business_timezone`, `N-1` days back |
| contents | three **partial** days | three **whole** days |
| between runs | shifts with the clock — two runs an hour apart disagree | stable — every run inside a day agrees |
| config | `window_hours`, `late_grace_hours`, `retention_hours` | `lookback_days`, `late_arrival_grace_days`, `physical_retention_days` |

A run at 09:15 on 2026-08-22 with N=3:

```
calendar_day    [2026-08-20T00:00Z ... 2026-08-22T09:15Z)   the 20th, 21st, 22nd
rolling_hours   [2026-08-19T09:15Z ... 2026-08-22T09:15Z)   72 hours
```

The platform does **not** guess. A table that declares any `_days` field becomes
`calendar_day`; a table configured only in hours stays `rolling_hours`, which is why every
table already in the registry is untouched by this phase. `boundary:` states it explicitly
when the default is not what you want.

`lookback_days: 1` means **today** — the window includes the day the run falls in.

The calendar arithmetic is done on the **local date** and then converted, never by
subtracting a timedelta: across a DST transition those differ by an hour, and the hour lands
in a neighbouring day's partition where nothing reports it.

## 3. Section E — REALTIME window vs STREAM_BATCH watermark

**These are different mechanisms with different owners, and the most common way to break
both is to treat one as the other.**

| | REALTIME window | STREAM_BATCH watermark |
|---|---|---|
| What it is | a **physical layer**: a materialised, reusable slice of CDC history | a **reporting flow**: a finite run that reads REALTIME and produces mart rows |
| What it remembers | nothing between runs — the window is recomputed from `run_upper_bound` every time | its own **successful** position, in `job_watermark_state` (ADR-043) |
| Moves when | every run, because the clock moved | only when a run **succeeds** |
| Rerun of a failure | recomputes the same window; harmless | must not advance; the watermark stays where it was |
| Owner | the CDC platform | the reporting framework (ADR-034 … ADR-045) |
| Ledger | `ops.realtime_run` — one row per materialisation | `job_master_execution_hist` — one row per flow run |
| Rebuild | re-read FULL_CDC (`--rebuild`), no Kafka | replay the flow from its watermark |

The practical consequences:

* **A REALTIME run failing does not move a reporting watermark**, and a STREAM_BATCH run
  failing does not un-materialise a window. They fail independently and are retried
  independently.
* **REALTIME's window must EXCEED the largest `safety_overlap` of any STREAM_BATCH job that
  reads it.** A batch that reaches back further than the window was materialised for asks
  for rows that have aged out and silently reads fewer than it should. That is the one hard
  coupling between the two, and it is a *sizing* constraint, not a shared mechanism.
* **A REALTIME rebuild does not rewind reporting.** Rebuilding the window puts the same rows
  back; the flows that already consumed them keep their watermarks. Reprocessing a report is
  a separate, deliberate act (FULFILL).
* Neither is `STREAMING_RT`, which is a long-running application with a Spark checkpoint
  (ADR-041). Three mechanisms, three vocabularies, no shared names.

## 4. Refresh modes

| mode | write | ages rows out by |
|---|---|---|
| `full_refresh` (default) | `overwrite` — replaces the window's data in one atomic snapshot | replacing everything outside the new window |
| `incremental_merge` | `MERGE` on `dv_event_id` | the retention prune, which must therefore run |

`overwrite`, **not** `createOrReplace`. Both are atomic; only one leaves the table
*definition* alone. `createOrReplace` resets the partition spec, write properties and
governance metadata the provisioner set — silently, to whatever the DataFrame implies.

## 5. Retention (section F)

```
physical_retention_days  >=  lookback_days + late_arrival_grace_days
```

Enforced at **compile time**, in whichever unit the policy is written in. Checking only the
hours fields would leave a day-configured table unvalidated while appearing checked, because
the hours defaults are always present.

Pruning is a **row-level `DELETE`**, never a file delete. Removing objects under a table's
location out from under Iceberg leaves metadata pointing at files that are gone: every
reader fails, and the table cannot even be time-travelled back to a good state because the
snapshots reference the same missing files. A `DELETE` produces a normal snapshot, so the
removal is itself revertible until snapshots expire.

## 6. Running it

```bash
# one table, window frozen at now
spark-submit spark/jobs/realtime/realtime_engine.py --plan s3://<lake>/artifacts/cdc/table-plan.json \
    --table oracle.coredb.corebank.account --warehouse s3://<lake>/warehouse/

# every enabled table
... --table ALL

# rebuild a PAST window from FULL_CDC -- no Kafka, no replay (section G)
... --table oracle.coredb.corebank.account --as-of 2026-08-20T00:00:00Z --rebuild
```

`--as-of` is section D exposed: a rebuild of a past window must use **that window's** bound,
not today's. A naive instant is refused — `naive.astimezone(utc)` silently assumes the
machine's local zone, so the same string would resolve to a different window on a laptop in
UTC+7 than on an EMR worker in UTC.

The engine **never creates a table**. A missing target is a refusal naming
`python3 -m cdc.provision` (ADR-063 section F).

## 7. What each run records — `ops.realtime_run`

`run_id`, `table_id`, source and target tables, `boundary`, `business_timezone`,
`refresh_mode`, all four bounds, `source_snapshot_id`, `target_snapshot_id`, `row_count`,
`pruned_count`, `status`, `failure_reason`, `started_at`, `finished_at`, `config_version`.

`source_snapshot_id` is what makes a run **reproducible**: re-reading that FULL_CDC snapshot
with those bounds must give the same rows. It is captured *before* the read, so the ledger
names the snapshot the run actually saw rather than a later one a concurrent ingest wrote.

The ledger write is best-effort and says so when it fails: a ledger write that could fail the
run would report a successfully materialised table as a failure and re-materialise it,
trading a missing audit row for real duplicated work.
