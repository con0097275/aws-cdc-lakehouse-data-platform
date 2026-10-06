# The four processing flows — accuracy tiers over one shared transformation

- Session: 09
- Date: 2026-08-14
- Status: **flow semantics PROVEN locally; latency/freshness `NOT_TESTED`**
- Contracts: [`reference/RECONCILIATION_AND_ACCURACY.md`], [`reference/KIMBALL_SAMPLE_MODEL.md`],
  [`docs/DATA_CONTRACTS.md`](DATA_CONTRACTS.md) §4.1

---

## 1. One transformation, four flows

The four flows differ in exactly **three** things:

| Flow | Input source | Window | Stamps |
|---|---|---|---|
| **STREAM_BATCH** (NRT) | `REALTIME` | `[watermark - overlap, frozen_upper)` | `PROVISIONAL_NRT` |
| **AUTO_CORRECT** | `FULL_CDC` or `EOD` | the **whole** of day T | `PROVISIONAL_CORRECTED` |
| **EOD** | `EOD` (as-of T-1) | the certified cutoff | `CERTIFIED` |
| **FULFILL** | `EOD` for the missing dates | those dates only | `RECONCILED` |

> **CORRECTED 2026-08-21.** This table previously named **L1 STREAM** as the input for NRT
> and AUTO_CORRECT. That was not a naming slip: ADR-033 forbids reporting from the raw
> Kafka landing and `spark/reporting/source_resolver.py::resolve()` refuses `FULL_CDC_RAW`
> outright, so the documented pipeline would have been rejected at runtime. Reading the
> landing also means reading events *before* the ordering and dedup guarantees that make a
> correction reproducible.
>
> The layer model is a fan-out, not a chain: Kafka feeds **FULL_CDC** directly (canonical,
> append-only), and **REALTIME** and **EOD** are sibling derivations of it. See
> `docs/TARGET_ARCHITECTURE.md` §3. The source layer per mode is normative in
> `SourceLayerPolicy` (`spark/reporting/models.py`); where this document and that enum
> disagree, the enum wins.

Everything else — the grain, the joins, the measures — is **one function**,
`spark/common/transform.py::build_fact_transaction`. There is no `if flow == ...` inside
it.

That is not a tidiness preference. The whole point of the tiers is that provisional
numbers are compared against certified ones:

```text
variance_pct = abs(provisional - certified) / nullif(abs(certified), 0) * 100
```

If NRT and EOD each carried their own copy of the business logic, a variance would have
**two** possible causes — data that had not arrived yet, or logic that had drifted — and
nothing in the numbers could tell them apart. Sharing one transformation makes a variance
mean exactly one thing.

`test_all_four_flows_produce_identical_business_columns` pins it: same input, four flows,
and the eleven business columns must come out identical. Only the flow metadata differs.

```
spark/common/
├── flows.py         accuracy ladder, anti-downgrade rule, FlowContext
├── transform.py     THE canonical transformation + grain enforcement
├── mart_writer.py   the one guarded MERGE path
├── flow_runner.py   per-flow source selection; certification ledger
├── reconcile.py     variance + tolerance policy
└── run_flow.py      one entrypoint
spark/streaming/nrt_mart.py   spark/correct/auto_correct.py
spark/eod/eod_certified.py    spark/full_fill/full_fill.py
```

The four driver modules are deliberately three lines each. If a flow ever needs a special
case inside the transform, that is the signal the shared-logic property has been lost —
not a reason to add a branch.

## 2. The accuracy ladder

```text
CERTIFIED (4) > RECONCILED (3) > PROVISIONAL_CORRECTED (2) > PROVISIONAL_NRT (1)
```

Defined **once**, in `flows.py::STATUS_RANK`. The SQL `CASE` used inside the MERGE is
*generated* from that dict rather than written out, because a second hardcoded copy inside
a MERGE statement is how an anti-downgrade rule silently stops matching the ladder it
enforces. Unrecognised statuses rank `0`, below every real tier, so an unknown value can
never win a comparison.

### The overwrite rule

```python
if incoming_rank != existing_rank:
    return incoming_rank > existing_rank      # 1. higher tier always wins
return incoming_cutoff >= existing_cutoff     # 2. same tier: later cutoff wins
```

Rule 2 is the one that is easy to miss. A strict `>` would make **EOD unable to correct
itself**: a rebuild after late events is `CERTIFIED` replacing `CERTIFIED`, which `>`
rejects, so the correction would be silently dropped. But a plain `>=` is wrong in the
other direction — it lets a *stale* rerun of the same tier overwrite a newer one.
Comparing `input_cutoff` settles both.

| Scenario | Result |
|---|---|
| Late NRT batch vs a CERTIFIED row | **rejected** — certified survives |
| AUTO_CORRECT vs CERTIFIED | **rejected** |
| CERTIFIED vs PROVISIONAL_NRT | accepted — upgrade |
| CERTIFIED rebuild, later cutoff | accepted — EOD can correct itself |
| CERTIFIED rerun, **stale** cutoff | **rejected** |

## 3. Grain enforcement — the defect this session found

The mart's grain is one row per `transaction_id`, and originally **nothing enforced it**.

**L1 STREAM keeps every I/U/D event** (`CLAUDE.md` §5.2). So any window read from L1
contains several rows for a transaction that was updated during the day — for NRT and
auto-correct that is the normal case, not an anomaly. Reproduced before the fix:

- the first write INSERTed **both** rows — two facts for one transaction, double-counted
  in every downstream sum, in a mart that still looked internally consistent;
- once a row existed, the same duplicates made the MERGE fail with a cardinality
  violation instead.

`collapse_to_grain()` now keeps the latest event per `transaction_id` **by source
position** — the same `event_order` precedence L3 uses (Session 08, `DATA_CONTRACTS` §4.1),
with Kafka offset last and never as a global sort key. Using a different rule from L3 would
make an intraday flow and the certified snapshot pick **different winning events**, so
provisional and certified would disagree for a reason unrelated to latency.

L1 and L3 expose their positions differently, so `flow_runner` normalises them into
`_ord_*` columns at the source boundary — the axis on which the flows already legitimately
differ — keeping a single dedup path downstream. A source without those columns is
**rejected**, because ranking without a total order picks a nondeterministic winner.

## 4. Deletes, unknown keys and late reference data

Every dimension join is a **LEFT** join defaulting to `UNKNOWN_SK = -1`, never an inner
join. An inner join would *drop* a fact whose dimension has not arrived, and a missing fact
reads as a smaller number rather than as an error — the failure mode that makes late
reference data so hard to notice. The `-1` member is **inserted** into each dimension, not
implied, so joins back from the fact do not drop the unresolved rows either.

`amount_base` stays **NULL** when the FX rate is unresolved rather than falling back to
`amount_original`. A silent `1.0` rate turns a missing reference into a *wrong* number that
reconciles against nothing and looks entirely plausible.

### Full-fill

Finds rows still on `-1` (or with a NULL `amount_base`), re-resolves them against the
**current** dimensions, and patches them in place. Two things it deliberately does not do:

1. **It never INSERTs.** There is no `WHEN NOT MATCHED` clause, plus a row-count assertion.
   A full-fill that inserted would create a second fact for a transaction already in the
   mart and double-count it in every sum.
2. **It never writes `processing_status`.** Resolving a surrogate key makes a row more
   *complete*, not less *certain*, so a certified row stays certified. Stamping a status
   here would downgrade it. `last_corrected_at` records that the patch happened.

It re-resolves through the **same canonical transform** rather than hand-patching keys — a
second resolution path would be a second copy of the join logic, and it would drift.

## 5. Tolerance policy and the zero-baseline trap

Default tolerance **5%**, configurable per metric.

The formula's `nullif(abs(certified), 0)` yields NULL for a zero baseline, and in SQL
`NULL <= tolerance` is NULL — which is not TRUE, but a naive `if not breach` reads it as a
**pass**. A metric that moved from a certified `0` to a provisional `1,000,000` would
report as within tolerance.

So `reconcile.py` never returns a bare number for that case:

| Case | `variance_pct` | Status | Passes? |
|---|---|---|---|
| within tolerance | a number | `WITHIN_TOLERANCE` | yes |
| over tolerance | a number | `TOLERANCE_BREACH` | no |
| certified 0, provisional 0 | `None` | `WITHIN_TOLERANCE` | yes |
| certified 0, provisional ≠ 0 | `None` | `UNDEFINED_BASELINE` | **no** |

An unmeasurable variance is not a satisfied one. `ops.metric_variance.variance_pct` is
nullable for the same reason, and `status` carries the verdict so no reader has to compare
a possibly-NULL number against a threshold.

## 6. Which metrics may be approximate intraday, and which must be exact

Scope item 9. The distinction is **not** about importance — it is about whether the metric
is a *monotone accumulation* of events that have already committed, or depends on the day
being complete.

| Metric | Intraday | Why |
|---|---|---|
| `txn_count`, `amount_sum` running totals | **approximate** | Monotone: late events only ever add. A partial total is a *lower bound*, which is a useful and honest intraday number. |
| Per-channel / per-merchant splits | **approximate** | Same, plus unresolved dimensions land on `-1` and move to their real member later. |
| `unknown_account`, `unresolved_amount` | **approximate, and expected to fall** | These are progress indicators for full-fill, not business measures. |
| **Daily closing balance** | **must be exact** | Not monotone — a balance is a *state as of* a moment, so a missing update makes it wrong, not merely low. |
| **Any figure published as CERTIFIED** | **must be exact** | It is read from L3, which has already deduplicated per PK and applied deletes. |
| **Reconciliation counts vs source** | **must be exact** | A mismatch is a lost or duplicated event and must be explained, never rounded away. |
| **Deletes / reversals** | **must be exact** | A reversal that has not arrived makes the total too HIGH; unlike a late insert it cannot be read as a lower bound. |

Power BI consumes this distinction directly: `processing_status = 'CERTIFIED'` is the
closing number for T-1 and earlier; day T carries a provisional badge and
`last_updated_at`.

## 7. Cost

`CLAUDE.md` §4 and the session constraint "avoid always-on Spark".

Every flow is a **scheduled batch**. None is a long-running streaming query: Structured
Streaming would hold EMR Serverless capacity 24h/day to serve a demo that runs for an hour,
while a 5-minute batch meets a ≤10-minute freshness target and bills only for the seconds
it runs.

| Flow | Demo-window schedule | Scan | Runs/day in window |
|---|---|---|---|
| NRT | `*/5 * * * *` | smallest — since watermark | ~12/hour |
| AUTO_CORRECT | `0,30 * * * *` | one day partition | 2/hour |
| EOD | `30 1 * * *` | one L3 partition | 1 |
| FULL_FILL | `0 2 * * *` | only unresolved rows | 1 |

**The dominant cost driver is NRT's frequency, not its size** — 12 small scans an hour cost
more than one daily scan of the whole day. Outside the demo window every schedule is
**paused**; `scripts/run-flow.sh` is dry-run by default so nothing starts by accident.

## 8. Test results

```
spark/tests/test_flows.py             17 passed   ladder, anti-downgrade, guards
spark/tests/test_reconcile.py         14 passed   variance, tolerance, zero baseline
spark/tests/test_four_flows_spark.py  16 passed   real Iceberg, real MERGE
Full suite (Sessions 06–09)          161 passed   (was 114 after Session 08)
```

| Acceptance criterion | Status |
|---|---|
| Certified data cannot be downgraded | **PASS** — end to end through the real MERGE |
| Auto-correct reduces variance | **PASS** — late event picked up, variance falls to 0 |
| Full-filled resolves unknown SK without duplicate facts | **PASS** — incl. a direct no-insert test |
| Tolerance breach fails according to policy | **PASS** — incl. the zero-baseline case |
| One row per `transaction_id` (grain) | **PASS** — was a live defect |
| Duplicate replay is idempotent | **PASS** |
| Out-of-order update does not regress a row | **PASS** |
| Late dimension → unknown SK, not a dropped fact | **PASS** |
| **Freshness demo ≤10 min** | **`NOT_TESTED`** — needs the pipeline running; see below |

### Why freshness is NOT_TESTED, not PASS

Sessions 02–09 are unapplied: there is no MSK cluster, no EMR Serverless application and no
L1 data. Freshness is a property of the deployed pipeline, and the local tests measure
correctness, not latency. Recording it as anything other than `NOT_TESTED` would be
inventing a result. The design target and its justification are in §7; the blocker is the
state-backend gate.

## 8b. STREAM_BATCH is not STREAMING_RT (ADR-041)

The reporting framework adds two modes whose names are one word apart and whose operational
behaviour has nothing in common. The table exists because an operator who confuses them
kills the wrong thing — repo 1 called its Airflow-triggered micro-batch `stream`, and repo 3
called its long-running application the same.

| | `STREAM_BATCH` | `STREAMING_RT` |
|---|---|---|
| Trigger | Airflow schedule (`*/10`) | its own trigger loop (30 s) |
| Lifetime | starts and exits per run | runs until stopped |
| Position | `ops.job_watermark_state` | the Spark checkpoint |
| History | one row per run | one row per **deployment** |
| Failure | Airflow retry | app restart, checkpoint recovery |
| Airflow's role | runs the batch | start, monitor, stop only |
| Source | `REALTIME` (bounded recent) | `FULL_CDC_APPEND` (canonical) |
| Pool | `reporting_jobs` | `streaming_apps` |
| Prefix | `sb_` | `rt_` |
| Checkpoint | none | `checkpoints/reporting/<env>/<job>/STREAMING_RT/` |
| Cost driver | frequency × per-run seconds | **hours the app is up** |
| Default | enabled | **`enable_streaming_rt=false`** |

The cost row is why the defaults differ. STREAM_BATCH bills per run; STREAMING_RT bills for
as long as it is up, which makes it the single largest cost lever in the framework and the
reason it ships off and runs in a bounded daily window.

## 9. Running a flow

```bash
scripts/run-flow.sh --flow NRT          --date 2026-08-14              # dry-run
scripts/run-flow.sh --flow AUTO_CORRECT --date 2026-08-14 --execute
scripts/run-flow.sh --flow EOD          --date 2026-08-13 --execute    # T-1, closed day
scripts/run-flow.sh --flow FULL_FILL    --date 2026-08-14 --execute
```

`EOD` **refuses** a business day that has not closed in UTC — certifying an open day
publishes a figure that will still change, under a status that promises it will not.

Every run appends to `ops.data_certification`, which is **append-only**: "why did day T
change between the 14:00 provisional and the certified figure" is answerable only from the
runs that were later superseded.
