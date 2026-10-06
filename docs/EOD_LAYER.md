# The EOD layer — config-driven business-date closing

EOD is a **deterministic function of FULL_CDC and a cutoff**. It is not built from REALTIME.
ADR-065; engine `spark/jobs/eod/eod_engine.py`; config `cdc/registry/sources.yaml` under `eod:`.

```
Kafka --(decode, quarantine, MERGE)--> FULL_CDC   <- CANONICAL
                                          |
                            +-------------+-------------+
                            v                           v
                   REALTIME [T-N, T)              EOD (as of COB close)
                   a bounded WINDOW               a function of the WHOLE history + a cutoff
```

**Why not from REALTIME.** REALTIME ages rows out. Building EOD on it would make a historical
rebuild depend on a window that no longer contains the days being rebuilt, and would make a
certified balance depend on the *schedule* of a different job. Rebuilding a missed
2026-08-14 must not require recreating that day's REALTIME window first — and it does not.

## 1. The cutoff

```
cutoff_local = start of (COB_DATE + 1) in business_timezone
cutoff_utc   = the same instant, in UTC
predicate    = source_commit_ts < cutoff_utc
```

Strictly `<`. CLAUDE.md §5.6 allows `event_ts < T00:00` **or** `source_commit_ts <= cutoff`;
those are the same set with different cutoff values. This platform uses the first, because
"the last instant of a day" has no exact representation and every approximation — `23:59:59`,
`.999`, `.999999` — silently drops events in the gap.

The arithmetic is done on the **local date and then converted**, never by adding 24 hours:
across a DST transition those differ by an hour, and an hour of events would be certified into
the wrong business date.

Both cutoffs are recorded. `cutoff_local` is what a business reader checks; `cutoff_utc` is
what the predicate used. When they differ by an unexpected amount, the timezone is wrong —
and only keeping both makes that visible.

| COB | zone | `cutoff_local` | `cutoff_utc` |
|---|---|---|---|
| 2026-08-22 | UTC | 2026-08-23T00:00+00:00 | 2026-08-23T00:00Z |
| 2026-08-22 | Asia/Ho_Chi_Minh | 2026-08-23T00:00+07:00 | **2026-08-22T17:00Z** |
| 2026-11-01 | America/New_York | 2026-11-02T00:00-05:00 | 2026-11-02T05:00Z |

A second predicate on `event_date` exists **only** for partition pruning, and is one day wider
on each side. `event_date` is a UTC date while the business day is not, so a prune written as
`event_date == cob_date` would look like an optimisation and would silently drop real events
for every non-UTC table.

`business_date_lag_days` (default 1) decides which day a scheduled run closes, applied to the
**local** date — 02:00 UTC is still the previous day in New York.

## 2. Source-native ordering

The winning event per key is the **maximum** of the contract's order-key tuple:

```
position_primary, position_secondary, source_commit_ts, kafka_partition, kafka_offset
```

`kafka_partition` **precedes** `kafka_offset`. That is the whole of CLAUDE.md §5.4: an offset
is monotonic only within its own partition, so ordering by partition first means offsets are
only ever compared between rows that share one. The partition is a *determinism* tie-break,
not a claim that a higher partition number happened later.

The two engines need **opposite** treatment, and one "just pad it" helper would corrupt one:

| engine | wire form | treatment | trap |
|---|---|---|---|
| Oracle | numeric SCN | `lpad(pos, 24, '0')` | `'9' > '10'` as strings, `9 < 10` as numbers |
| SQL Server | `aaaaaaaa:bbbbbbbb:cccc` hex | `lower(pos)`, lexicographic | valid **only** because it is already fixed-width — so the width is validated, not assumed |

A position that does not match its engine's pattern is **refused**. The deployed
`spark/jobs/eod/job.py` casts to `decimal(38,0)`, which is Oracle-only: a hex LSN yields NULL,
`desc_nulls_last` sends every row to the back equally, and the ranking silently collapses onto
`kafka_offset`. The result is a snapshot that picks an arbitrary event per key and looks
entirely normal.

## 3. Deletes

Three policies, and the default was chosen by **inspecting what the platform already does**
(CLAUDE.md §5.7 names the exclusion first; the deployed job filters `op != 'd'`):

| policy | alias accepted | effect |
|---|---|---|
| `exclude_from_snapshot` **(default)** | `exclude_latest_delete` | the key is omitted when its winning event is a delete |
| `soft_flag` | `soft_delete` | the row is kept with `is_deleted = true` |
| `physical_delete` | — | additionally removes any row a **previous** run left for that key |

`physical_delete` differs from `exclude` only where the target retains state across runs.
Under `rolling_history` each COB owns its partition and no earlier row is in scope, so they
are the same thing; under `latest_state` they are not.

A delete's identity comes from the **before-image**: its after-image is NULL by construction,
so reading only the after image would give every delete a NULL key and collapse them all onto
one snapshot row.

## 4. Snapshot mode

| mode | grain | retained |
|---|---|---|
| `rolling_history` **(default)** | one row per PK per `business_date` | every closed COB |
| `latest_state` | one row per PK | only the most recently certified COB |

The default is an inspection result, not a preference: the provisioned EOD table is already
partitioned by `business_date` with `retention_days: 365`, and a 365-day retention on a table
holding one day is meaningless. Defaulting to `latest_state` would make the first run of every
table delete up to 364 certified partitions.

## 5. Certification — `ops.eod_run`

`CERTIFIED` requires **both** gates to pass. The data is written either way; the **completion
marker is withheld**, which is the existing contract (`spark/eod/audit.py::may_publish`).

* **DQ** — no NULL primary key, no NOT-NULL violation, and a non-empty window. An empty
  window is legitimate on a quiet day and must be *deliberate*, not the silent result of a
  wrong cutoff.
* **Reconciliation** — a genuine identity, not a restatement of the build:
  `distinct_keys − deletes == rows` (or `distinct_keys == rows` under `soft_flag`). If a key
  vanished between reading and writing, the snapshot still looks like a perfectly ordinary
  table; this is the only thing that notices.

The ledger also carries **position evidence** — `max_position_primary` and
`max_source_commit_ts` — which answers "how far into the source did this close actually
reach?", and `source_snapshot_id`, which makes the close reproducible: re-reading that
FULL_CDC snapshot with this cutoff must converge to the same business state.

## 6. Running it

```bash
# preview the cutoff and the ordering, no AWS
make cdc-table-eod TABLE=oracle.coredb.corebank.account COB_DATE=2026-08-22

# close one date
spark-submit spark/jobs/eod/eod_engine.py --plan s3://<lake>/artifacts/cdc/table-plan.json \
    --table oracle.coredb.corebank.account --cob-date 2026-08-22 \
    --warehouse s3://<lake>/warehouse/

# every enabled table, business date from business_date_lag_days
... --table ALL

# FULFILL a historical date from FULL_CDC alone -- no Kafka, no REALTIME window
... --cob-date 2026-08-14 --fulfill
```

A rerun of the same COB **converges**: the partition is replaced, not appended, so the second
run produces the same state rather than doubling every key.

The engine **never creates a table**. A missing target is a refusal naming
`python3 -m cdc.provision` (ADR-063 §F).

The process exit code is non-zero when any close built but did not certify — a scheduler
checks the exit code, and "built" must not read as "certified".

## 7. Relationship to the other layers

| | REALTIME | EOD |
|---|---|---|
| what it is | a bounded window `[T-N, T)` | one row per PK as of a cutoff |
| depends on | FULL_CDC | FULL_CDC — **never REALTIME** |
| ages out | yes, by design | no; `rolling_history` accumulates |
| rebuild | re-read FULL_CDC (`--rebuild`) | re-read FULL_CDC (`--fulfill`) |
| ledger | `ops.realtime_run` | `ops.eod_run` |
| gate | none — it is a materialisation | **certification**, DQ + reconciliation |

Both are siblings of FULL_CDC, not a chain. That symmetry is deliberate: they use the same
ordering contract, so a variance between a provisional number and a certified one means *late
data*, not divergent logic — and only one of those is a bug worth chasing.
