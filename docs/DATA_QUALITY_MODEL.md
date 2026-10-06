# DATA QUALITY MODEL

- Phase: **DRP1** — result contracts implemented; nothing executed yet (DRP5)
- Code: `cdc/quality.py` · DDL: `spark/ops/ddl/reliability_tables.sql`
- Related: `spark/ops/dq_engine.py`, `docs/DATA_CONTRACT_MODEL.md`, ADR-087

---

## 1. Two records, one status vocabulary

| Record | Answers |
|---|---|
| `DqResult` | did this rule hold, for this dataset, over this interval |
| `ReconResult` | do these two datasets agree on this metric, within tolerance |

Both are **append-only**. "Why was COB 28 blocked" is answerable only from the records that
were later superseded; overwriting keeps the current answer and destroys the explanation.

## 2. Six statuses, not five — and why

The DRP1 brief names five: `PASS`, `WARN`, `FAIL`, `ERROR`, `SKIPPED`. This model defines
**six**.

| Status | Meaning | Blocks a required check? |
|---|---|---|
| `PASS` | evaluated and satisfied | no |
| `WARN` | evaluated and violated, at WARN severity | no |
| `FAIL` | evaluated and violated, at ERROR severity | **yes** |
| `ERROR` | the check itself could not run — exception, permission, bad SQL | **yes** |
| `NOT_EVALUATED` | it ran, and there was nothing to evaluate | **yes** |
| `SKIPPED` | deliberately not run for this interval | no |

`SKIPPED` and `NOT_EVALUATED` are the pair that matters, and collapsing them would undo the
one thing `spark/ops/dq_engine.py` was built around. Its own docstring states the failure:

> A uniqueness query returning zero rows means "no duplicates" if the table has data, and
> means **nothing at all** if the table is empty, the partition is missing or the predicate
> matched nothing.

`SKIPPED` is a choice and never blocks. `NOT_EVALUATED` is an absence of evidence and always
blocks a required check. One extra status is cheaper than losing that distinction.

`ERROR` and `NOT_EVALUATED` are also kept apart, and the difference matters to whoever is
paged: `ERROR` is an engineering defect in the check, `NOT_EVALUATED` is usually an upstream
job that did not produce data. One is fixed by editing code, the other by rerunning a
pipeline.

`BRIEF_STATUSES` names the five so the extension is visible in code rather than silently
absorbed.

## 3. A result never carries raw PII

`sample_reference` is a **pointer**, never a value. Failing rows go to the access-controlled
quarantine; the result carries a reference to them.

Accepted: `s3://…`, `glue_catalog.db.table`, `quarantine://…`. Anything else is refused at
construction.

A DQ table is the most widely read object in a lakehouse — dashboards, alerts, the AI
assistant, anyone debugging a pipeline. Copying failing rows into it is how a restricted
column ends up in an unrestricted table, with a perfectly good reason at the time ("just the
first three, to see what broke"). Validating the field as a location means putting a value
there has to be a deliberate lie rather than a convenience.

## 4. Invariants enforced at construction

| Refused | Why |
|---|---|
| a violation with `rows_examined = 0` | arithmetically impossible; it means the engine forgot `rows_examined`, which would make a `NOT_EVALUATED` look like a real `FAIL` |
| `PASS` with `failed_count > 0` | |
| a `DataInterval` with no start, end or COB | a result with no interval cannot be superseded by a later one |
| a `ReconResult` whose source and target are the same dataset | a dataset always agrees with itself, so the check can only ever pass |

`ReconResult.difference` is **`None`** when either side was not measured — never `0.0`.
Rendering an unmeasured comparison as a clean zero is the single most dangerous thing a
reconciliation ledger can do, and `difference` is nullable in the DDL for the same reason.

## 5. An empty suite blocks

`suite_blocks_publish([])` returns `True`. "No check failed" is not "the data was checked" —
the same defect as an unevaluated check passing, one level up.

## 6. Interval and COB are both carried

They answer different questions. The interval is what was **read** (a window, possibly
spanning days); the COB is what the result is **about**. A late-arriving correction read at
09:00 on the 30th can be evidence about COB 28, and a model with only one of the two cannot
say that.

## 7. Relationship to the live `ops.dq_result`

The v1 table (Session 14) is **not** dropped and **not** migrated in place. It holds the only
DQ history the platform has, and rewriting it to improve a schema would destroy evidence.

`LEGACY_DQ_RESULT_MAPPING` records how the two relate — including
`verdict` + `severity` → `status`, where `FAIL`+`WARN` becomes `WARN`.
`LEGACY_DQ_RESULT_MISSING` records the nine fields v1 has no source for (`dq_run_id`,
`expected_rule`, `failed_count`, `sample_reference`, `engine`, `config_version`,
`started_at`, `interval_start`, `interval_end`) so a backfill cannot quietly invent them.

## 8. What DRP1 did not do

Nothing runs. `ops.dq_result_v2` and `ops.reconciliation_run` are declared in
`spark/ops/ddl/reliability_tables.sql` and not created. `spark/ops/dq_engine.py` still reads
`governance/dq/rules.yml`, which the DRP0 audit measured at 1 of 7 datasets live — moving its
rule source is DRP5, behind its own gate, because that module currently blocks publishes.
