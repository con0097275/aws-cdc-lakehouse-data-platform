# Per-table CDC cutover — the non-destructive runbook

ADR-067. **Pilot PASSED live on 2026-09-10: 2 of 8 tables are cut over.** This document is
the runbook; `artifacts/validation/session-43/CUTOVER_PILOT_RESULT.md` is what happened.

## 0. The gate on the whole phase

ADR-062 approved per-table storage as the target and gated the *cutover* on two triggers,
explicitly excluding data volume. **Trigger 2 is satisfied**: the registry now carries
differentiated PII classification (`customer`, `app_user` = `confidential`), differentiated
realtime enablement, and differentiated freshness SLAs. ADR-060 denies the AI plane
`warehouse/full_cdc/` *wholesale* because a per-table grant is not expressible against a
monolith — that is the concrete cost this phase removes.

## 1. The flag

| mode | writes | **reads** | when |
|---|---|---|---|
| `LEGACY` (default) | monolith | monolith | today, and after any rollback |
| `DUAL` | both | **monolith** | while gathering the evidence |
| `PER_TABLE` | per-table | per-table | after the gate passes |

`DUAL` reads legacy on purpose: a mode where writes go both ways but reads follow the new
path would put consumers on unvalidated data during the very window meant to validate it.

Per **table**, never platform-wide. Groups move several at once when you choose to.

```bash
python3 scripts/cdc-cutover.py status      # every table and its read path
python3 scripts/cdc-cutover.py plan        # remaining tables, ascending risk
python3 scripts/cdc-cutover.py set --table <id> --mode DUAL
```

## 2. Reading through the resolver

```python
from cdc.cutover import CutoverResolver, load_state
r = CutoverResolver(plan, load_state()).resolve(table_id, "FULL_CDC")
r.identifier          # kafka_dev_lab_dev_full_cdc.cdc_events   (LEGACY)
r.required_predicate  # source_system = 'oracle' AND source_table = 'ACCOUNT'
```

**The legacy read always carries its predicate.** The monolith holds every source table, so a
consumer that forgets it reads eight tables' events as one — a plausible number, not an
error. The predicate comes back *with* the table so forgetting it is not possible.

`reporting/layers.yaml` stays the only file that names a physical Glue database (ADR-033); a
test asserts none appears in `cdc/cutover.py`.

## 3. Backfill

```bash
# dry run (the default): count and compare, write nothing
spark-submit spark/jobs/full_cdc/backfill.py --plan s3://…/table-plan.json \
    --table oracle.coredb.corebank.account --warehouse s3://…/warehouse/ \
    --legacy glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events
# then, deliberately
... --execute --out artifacts/cdc/backfill-account.json
```

**Identity is copied, never recomputed.** `dv_event_id` is
`sha2(topic|partition|offset|kafka_timestamp)`; a recompute that rendered the timestamp one
microsecond differently would give the same events different identities, and the
reconciliation would then compare two sets that *cannot* match for a reason unrelated to the
data.

The comparison is a count **and** an identity-set hash over sorted `dv_event_id`s. The legacy
table is read only — the job contains no `DROP`, `DELETE`, `TRUNCATE` or `overwrite`, and a
test asserts it.

## 4. The gate — nine checks, and unmeasured is a failure

```bash
python3 scripts/cdc-cutover.py gate --table <id>                      # list the checks
python3 scripts/cdc-cutover.py gate --table <id> --observed obs.json --record
```

| check | what it asserts |
|---|---|
| `event_identity_completeness` | zero rows on both sides of a full outer join on `dv_event_id` |
| `full_cdc_equivalence` | row count **and** identity-set hash agree |
| `eod_equivalence` | the same COB closed from each source gives identical state |
| `realtime_window_equivalence` | the same frozen window holds the same events |
| `dq` | the per-table target passes its own DQ contract |
| `schema_comparison` | the per-table columns are a superset of what consumers read |
| `consumer_test` | every consumer produces identical output from both paths |
| `latency` | the per-table path is not slower end to end |
| `benchmark_measured` | **the benchmark was run** — not that it was favourable |

**A check absent from the observations is a `FAIL`, not a skip.** "We did not check" and "it
was fine" must not reach the same conclusion. Compute the equivalences **independently in
Athena**, not with the job that wrote the data.

## 5. The benchmark (section I)

Five scenarios × two paths × nine metrics:

```
scenarios  one_table_one_day · one_table_seven_days · eod_source_scan · auto_correct · fulfill
metrics    athena_bytes_scanned · spark_input_files · spark_input_bytes · planning_time_ms
           runtime_ms · file_count · avg_file_size_bytes · output_rows · partitions_scanned
```

`BenchmarkReport.claim()` **raises** on an incomplete scenario rather than hedging — a partial
benchmark reads exactly like a complete one. A finished claim names the metric and both
numbers, and reports a regression as readily as an improvement.

**Expect the file-count metrics to get worse before they get better.** The Phase 0 audit
measured a mean data file of 137 KiB; splitting one table into eight multiplies partition
count and makes small files worse at current volume. Compaction is the remedy and is not a
partitioning decision (ADR-062). Do not present per-table storage as a small-file fix.

## 6. Cutting over

```bash
python3 scripts/cdc-cutover.py set --table <id> --mode PER_TABLE
```

Refused unless gate evidence exists, **passed**, and was recorded against the **current**
`config_version` — three separate ways it can be wrong. Reverting to `LEGACY` and entering
`DUAL` need no gate: `DUAL` is where the evidence is gathered.

## 7. Rollback

Set the mode back to `LEGACY`. The monolith was never dropped and, under `DUAL`, is still
being written — the rollback is a flag flip with no restore. Per-table targets left behind are
additive and hold nothing the monolith does not.

## 8. Order of expansion

`cdc-cutover.py plan` orders ascending by risk (classification, realtime enablement,
freshness SLA). Risk is a **band, not a score**: a decimal computed from a classification and
two flags would look like a measurement.

Move one table, let it run a full cycle — an EOD close, an AUTO_CORRECT, a FULFILL — then the
next.

## 9. What the pilot measured

| | `oracle/ACCOUNT` | `sqlserver/digital_event` |
|---|---|---|
| EOD source scan | 89,193 → 11,231 bytes (**-87.4%**) | 113,261 → 105,163 (**-7.1%**) |
| one table, one day | 75 → 0 bytes | 348 → 0 bytes |
| data files | 14 → 2 (-85.7%) | 14 → 2 (-85.7%) |
| `auto_correct` runtime | 730 → 2,264 ms (**worse**) | 1,467 → 547 ms |

**The pruning benefit is inversely proportional to a table's share of the monolith.**
`digital_event` *is* 12,000 of the monolith's 20,390 rows, so isolating it saves little;
`ACCOUNT` is 961, so isolating it saves almost everything. ADR-062 predicted "≈87% with 8
evenly-weighted tables" and got -87.4% on the evenly-weighted case — the prediction was right
about the mechanism and silent about the weighting.

Report a regression as readily as an improvement: `auto_correct` on ACCOUNT got **3x
slower**. Two small files with a filter that matches almost nothing is not obviously cheaper
than fourteen with the same filter.

## 10. What is still not done

* **No `DUAL` window was observed.** The pilot went LEGACY → backfill → gate → PER_TABLE,
  because the backfill reproduces history the monolith already holds while `DUAL` validates
  *new* events. Before cutting over a table that is actively receiving CDC, run `DUAL` for a
  full cycle first.
* **6 tables remain `LEGACY`**, in the risk order `cdc-cutover.py plan` prints.
* **320 `ACCOUNT` rows and 6,000 `digital_event` rows are not in the per-table targets** —
  they carry no `dv_event_id`. `job.py --backfill-identity` against the source topics is the
  documented remedy; until then the monolith is the only place they exist, which is one more
  reason it is never dropped.

---

## 11. Operations after cutover (ADR-068)

```bash
make cdc-maintenance-measure ARGS="--out artifacts/cdc/metrics.json"
make cdc-maintenance-plan    ARGS="--metrics artifacts/cdc/metrics.json --verbose"
make cdc-governance
make cdc-observe        TABLE=<id>
make cdc-decommission   TABLE=<id>
```

**Measure before you plan.** The job *requires* `--metrics`: a maintenance run with no
measurement is a cron, and this platform already knows what its files look like.

Measured 2026-09-10 — the legacy monolith at **14 files, 140,546 B mean, 13 manifests, 53
snapshots**, matching the Phase 0 audit. The two backfilled pilot tables hold **2 files
each**, which the file-count floor correctly skips: compacting two files into one is an EMR
run to eliminate one file.

**Known gap.** The maintenance job operates on *registered* tables, and the one table that
demonstrably needs maintenance — the monolith — is not one. It holds eight source tables, so
"whose `maintenance` policy governs it?" has no good answer; it needs its own policy entry,
which is a decision about its retirement path rather than a line of code.
