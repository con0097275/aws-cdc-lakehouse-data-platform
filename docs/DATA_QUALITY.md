# Data quality

- Session: 14
- Date: 2026-08-15
- Status: **engine and rules RUN against real Spark; not run against deployed data**

---

## 1. The failure this engine is built around

A DQ check that finds nothing looks exactly like a DQ check that passes.

```sql
SELECT pk, count(*) FROM t GROUP BY pk HAVING count(*) > 1    -- 0 rows
```

Zero rows means *"no duplicates"* if the table has data — and means **nothing at all** if
the table is empty, the partition is missing, or the predicate matched no rows.

A suite that reports green in the second case is **worse than no suite**: it actively
asserts health for a pipeline that did not run. The morning after an EOD job silently
produced no output, the dashboard is green and the DQ report agrees.

So every check returns one of **three** verdicts, never two:

| Verdict | Meaning |
|---|---|
| `PASS` | evaluated, and satisfied |
| `FAIL` | evaluated, and violated |
| **`NOT_EVALUATED`** | there was nothing to evaluate — **not a pass** |

`rows_examined` is stored alongside, so `NOT_EVALUATED` is distinguishable from `PASS` in
`ops.dq_result` without re-reading the code.

**An `ERROR`-severity check blocks the publish on `FAIL` *and* on `NOT_EVALUATED`**, because
*"we could not tell"* is not *"it is fine"*.

A check that **raises** — missing table, bad predicate — is also `NOT_EVALUATED`, never a
pass. An exception must not read as health.

## 2. Severity

| Severity | Effect |
|---|---|
| `ERROR` | blocks the certified publish |
| `WARN` | recorded, never blocks |

**Neither ever deletes anything.** Acceptance criterion: *bad data blocks certified publish
but does not erase audit history.* Quarantine holds **copies**; the originals stay in L1/L2
because those layers are the audit record and the replay source (`CLAUDE.md` §5.3).

Two rules are deliberately `WARN`:

- `after IS NULL` on L1 — a **delete legitimately has a NULL `after`**. Blocking would fail
  every day containing a deletion.
- `amount_base IS NULL` on the fact — NULL while the FX rate is unresolved (S09-8), which
  full-fill later fixes. Surfacing it without blocking is the correct response.

22 of 26 checks are `ERROR`. A suite that is mostly `WARN` cannot block a bad publish, which
is its whole job — `test_blocking_checks_outnumber_advisory_ones` pins the ratio.

## 3. The six check types

| Type | What it catches |
|---|---|
| **freshness** | `MAX(ts)` older than the SLA. Uses max-timestamp, not row count: a stuck CDC connector keeps *delivering* while every record carries an old timestamp |
| **completeness** | `% NULL` above a threshold |
| **uniqueness** | the declared grain is not actually unique |
| **validity** | rows failing a boolean business rule. **NULL counts as invalid** |
| **referential integrity** | a fact FK with no dimension row. `-1` excluded — an unresolved dimension is a known state, not an orphan (S10-7) |
| **reconciliation** | row counts between two layers |

### Why NULL counts as invalid

`NOT (amount > 0)` evaluates to NULL when `amount` is NULL. Treating that as passing means
the rule **silently exempts exactly the rows most likely to be wrong**. The engine counts
anything that is not explicitly `TRUE` as a violation.

### Why layer reconciliation is exact

L1 → L2 runs at **tolerance 0**. Both layers keep every event, so any difference is a lost
or duplicated record and must be explained, never rounded away (`CLAUDE.md` §5.3).
`test_layer_boundary_reconciliations_are_exact` refuses any tolerance above zero on a layer
boundary.

## 4. No Deequ

Deequ would add a JVM dependency and a version matrix to pin (`CLAUDE.md` §3.9) for six
checks that are each a few lines of Spark SQL. The session asks for open-source/AWS-native
and low cost; `dq_engine.py` runs on the Spark that already exists and adds nothing to the
bill.

Glue Data Quality is a flag, **off** — managed DQ is billed per DPU for checks this already
covers.

## 5. Running it

```bash
scripts/dq-check.sh --date 2026-08-14              # all layers
scripts/dq-check.sh --date 2026-08-14 --layer L2   # gate one stage
```

Exit codes are the contract, and Airflow reads them:

| Code | Meaning |
|---|---|
| 0 | no `ERROR` check failed — publish may proceed |
| 1 | refused (bad arguments, missing config) |
| **2** | **DQ blocks the publish** |

Exit 2 is not a crash — it is the gate working. It is wired into `eod_certified_pipeline` as
`governance_dq_suite` with `retries=0`: a DQ failure is a *data* problem, re-running the
same checks over the same data fails the same way, and a green retry would mean the data
changed underneath.

## 6. Results are queryable

`ops.dq_result`, partitioned by `business_date`, **append-only**. "Why was day T blocked" is
answerable only from the runs that were later superseded.

```sql
SELECT dataset, check_name, verdict, severity, observed, threshold, rows_examined, detail
FROM   ops.dq_result
WHERE  business_date = DATE '2026-08-14' AND verdict <> 'PASS'
ORDER  BY severity, dataset;

-- Score per dataset. NOT_EVALUATED counted separately, never folded into "pass".
SELECT dataset,
       sum(CASE WHEN verdict = 'PASS' THEN 1 ELSE 0 END)          AS passed,
       sum(CASE WHEN verdict = 'FAIL' THEN 1 ELSE 0 END)          AS failed,
       sum(CASE WHEN verdict = 'NOT_EVALUATED' THEN 1 ELSE 0 END) AS not_evaluated
FROM   ops.dq_result WHERE business_date = DATE '2026-08-14'
GROUP  BY dataset;
```

## 7. Quarantine

`ops.dq_quarantine` — **distinct from Session 06's poison-record path**, deliberately.
Session 06 quarantines records that could not be *parsed*; this holds records that parsed
fine and failed a *business* rule. Different problems, different owners: a parse failure is
an engineering defect, a validity failure is a data defect.

Rows are **copies**, and `payload_ref` is a pointer rather than an inline copy of the PII.
Resolution is recorded (`REPROCESSED` / `ACCEPTED` / `REJECTED`) so a quarantined row is not
silently forgotten.

## 8. Evidence

```
spark/tests/test_dq_engine.py    24 passed   real Spark
spark/tests/test_governance.py   27 passed   registry consistency
```

Four guards mutation-checked:

| Reverted | Caught by |
|---|---|
| `NOT_EVALUATED` treated as a pass | 3 tests |
| empty table returns `PASS` | `test_uniqueness_on_an_empty_table_is_not_evaluated` |
| PII table re-opened to BI | `test_datasets_with_pii_are_denied_to_bi` |
| IAM deny list drifts from the registry | `test_the_iam_deny_list_matches_the_registry` |

**Not tested:** the suite against deployed data. Nothing is applied, so every check has run
against local fixtures only.
