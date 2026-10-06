# EOD Snapshot Runbook

**Contract**: ADR-065. **Code**: `cdc/eod.py`, `spark/jobs/eod/eod_engine.py`.

EOD is a **deterministic function of FULL_CDC and a cutoff**. Same inputs, same output,
always. It never reads REALTIME — a bounded window ages rows out, so building on it would make
a historical rebuild depend on a window that no longer contains the days being rebuilt, and a
certified balance depend on another job's schedule.

---

## The cutoff

```
cutoff_local = start of (COB_DATE + 1) in business_timezone
cutoff_utc   = that instant in UTC
predicate    = source_commit_ts < cutoff_utc          <- strictly less-than
```

**Strictly `<`.** "The last instant of a day" has no exact representation, and every
approximation (`23:59:59`, `.999`, `.999999`) silently drops events in the gap.

The arithmetic is done on the **local date and then converted**, never by adding 24 hours:
across a DST transition those differ by an hour, and an hour of events would be certified into
the wrong business date.

Both cutoffs are recorded. The local one is what a business reader checks; the UTC one is what
the predicate used. Keeping only one hides a wrong timezone.

`business_date_lag_days` (default 1) decides which day a scheduled run closes, applied to the
**local** date — 02:00 UTC is still the previous day in New York.

A second, deliberately **wider** `event_date` predicate exists only so Iceberg can prune
partitions. It is one day wider on each side because `event_date` is a UTC date while the
business day need not be: written as `event_date = cob_date` it would look like an
optimisation and silently drop real events for every non-UTC table.

## Ordering — the two engines need opposite treatment

| | wire form | treatment |
|---|---|---|
| Oracle SCN | numeric | `lpad(pos, 24, '0')` — `'9' > '10'` as strings but `9 < 10` as numbers |
| SQL Server LSN | `aaaaaaaa:bbbbbbbb:cccc` hex, already fixed width | `lower(pos)`, compared lexicographically — valid **only** because of that padding, so the width is **validated**, not assumed |

Both yield a sortable string, so one ORDER BY serves both and no comparison depends on a cast
that can return NULL. A position not matching its engine's pattern is **refused**, not ranked.

```
position_primary, position_secondary, source_commit_ts, kafka_partition, kafka_offset
```

`kafka_partition` **precedes** `kafka_offset`. That is the whole of CLAUDE.md §5.4: an offset
is monotonic only within its own partition, so ordering by partition first means offsets are
only ever compared between rows that share one. Partition is a determinism tie-break, not a
claim that a higher partition happened later.

## Delete policy

| value | the snapshot |
|---|---|
| `exclude_from_snapshot` **(default)** | omits keys whose latest event is a delete |
| `soft_flag` | keeps them with `_is_deleted = true` |
| `physical_delete` | deletes the row outright |

The brief's names (`exclude_latest_delete`, `soft_delete`) are accepted as **aliases** and
normalised on the way in. The default is what the platform already did: a changed delete
default alters certified balances without altering a line of business SQL.

## Snapshot mode

`rolling_history` (default) keeps one partition per business date. `latest_state` keeps only
the newest. The default is not a preference — the provisioned table is
`PARTITIONED BY (business_date)` with `retention_days: 365`, so defaulting to `latest_state`
would make the **first run of every table delete up to 364 certified partitions.**

---

## Certification is a gate, not a label

`ops.eod_run` records both cutoffs, both snapshot ids, the position evidence
(`max_position_primary`, `max_source_commit_ts` — how far into the source the close actually
reached), the counts, and the DQ and reconciliation outcomes. `CERTIFIED` requires **both**
gates to pass.

The reconciliation is a genuine identity, not a restatement of the build:

```
distinct_keys - deletes == rows        (or distinct_keys == rows under soft_flag)
```

If a key vanished between reading and writing, the snapshot still looks like a perfectly
ordinary table. This is the only thing that notices.

**Data is written even when validation fails; the completion marker is withheld.** A snapshot
an operator can inspect beats one thrown away.

## Operating

```bash
# close one date
python3 scripts/emr-submit.sh eod --table oracle.coredb.corebank.account --cob-date 2026-08-21

# verify INDEPENDENTLY -- in Athena, not by asking the job that wrote it
python3 scripts/cdc-gate-observe.py --table <id> --cob-date <date> --out gate.json
```

### A close is not CERTIFIED

Read `ops.eod_run` for that run. DQ failure means a declared rule failed on real data — look
at the data. Reconciliation failure means the identity above did not hold, which is a
platform problem, not a data problem: escalate rather than re-running.

### A late event arrived for a date already CERTIFIED

This is the normal case, not an incident. The event lands in FULL_CDC whenever it arrives;
the certified snapshot does **not** change on its own, because a certified number that moves
without anyone asking is worse than a stale one.

```bash
python3 scripts/emr-submit.sh eod --table <id> --cob-date <the date> --fulfill
```

`--fulfill` records the intent — same mechanics, different reason. The COB partition is
replaced, so the rebuild is convergent and repeatable, and the new `ops.eod_run` row carries
a higher `max_source_commit_ts` than the original: that is the evidence the close reached
further into the source than it did the first time.

**Proven live** (Phase 8): a `payment_method` row was updated after its COB was certified.
FULL_CDC went 5 → 6 events while the snapshot correctly kept the old value, and re-closing
the same date picked the change up with no manual cleanup.

### A number looks wrong for a past date

**Rebuild it.** EOD is derived; re-running a date replaces its partition rather than appending
to it, so a rebuild is convergent and safe to repeat:

```bash
python3 scripts/emr-submit.sh eod --table <id> --cob-date <the date>
```

Verified in Phase 8 §D: closing a date twice produces byte-identical row counts and an
identical reconciliation, and closing it after a config change produces the config's result
with no manual cleanup.

### The DQ rule set is empty

An empty contract is not a contract: a close that cannot fail a rule certifies whatever it
produced. `cdc-table-provision` refuses a registered-but-empty `dq` block. The minimum is
the key.

## Rollback

A COB partition is **replaced, not appended**, so re-running a date with a reverted config
restores the previous state from FULL_CDC. EOD can always be rebuilt.

`latest_state` is the one setting whose rollback is not free — it deletes prior partitions and
recovering them means re-closing each date. That is why it is not the default.

## The control plane (Phase D, ADR-076)

| table | grain | written when |
|---|---|---|
| `ops.eod_info` | one row per `(table_id, cob_date)`, MERGEd | **only** on CERTIFIED |
| `ops.eod_run_hist` | one row per attempt, appended | every attempt, including WAITING/LATE/FAILED |
| `ops.eod_run` | the original append ledger | unchanged |

```sql
-- what is the current certified state of a business date?
SELECT cob_date, status, certification_status, prev_watermark_ts, watermark_ts, row_count
FROM   kafka_dev_lab_dev_ops.eod_info
WHERE  table_id = 'oracle.coredb.corebank.account' ORDER BY cob_date DESC;

-- why did today's close not certify?
SELECT attempt, status, err_msg, start_time
FROM   kafka_dev_lab_dev_ops.eod_run_hist
WHERE  table_id = 'oracle.coredb.corebank.account' AND cob_date = DATE '2026-09-19'
ORDER  BY attempt;
```

### Readiness

A close does not certify because the clock says 01:00. It needs the table's own watermark
**or** the platform ingest watermark at/past the cutoff, plus a healthy ingest and capture.

* `WAITING_SOURCE` — early; the DAG retries until `eod.source_sla_minutes` (default 360).
* `LATE_SOURCE` — the SLA expired. Nothing was built and no watermark moved. Investigate the
  source or the ingest, not the close.

### Cadence

`eod.schedule` is a validated cron, default `0 1 * * *`. **One DAG per cadence**: `cdc_eod`
today holds nine tables and `cdc_eod_0230` holds `loan`, which declares `30 2 * * *`.

### Historical rebuild

```bash
bash scripts/emr-submit.sh eod-rebuild s3://$LAKE/artifacts/code/eod_engine.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse \
  --table oracle.coredb.corebank.account --cob-date 2026-08-22 --skip-readiness
```
`--skip-readiness` is for a date the source moved past long ago. Never for the current day.

### Legacy consumers

```bash
make cdc-eod-legacy-view    # prints the Athena DDL for pre_datelastmaint / datelastmaint
```
It is a VIEW over `eod_info`. Those names are ambiguous about direction, so the unambiguous
pair stays canonical.
