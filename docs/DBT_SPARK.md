# dbt on Spark — what dbt owns, what Spark keeps, and why

> **Session 23 update.** ADR-044 supersedes the `method: thrift` EMR Serverless target that
> this document's profile section described: EMR Serverless exposes no Thrift endpoint, and
> standing one up means always-on compute (CLAUDE.md §4.5). Execution is now `dbtRunner` +
> `method: session` inside the EMR Serverless driver — see
> `docs/REPORTING_FRAMEWORK.md` "Phase 4" and `spark/reporting/run_dbt_job.py`. The
> ownership boundary this document sets out (§2 — dbt does not own the canonical
> transformation, the guarded MERGE or SCD2) is unchanged.


- Session: 11
- Date: 2026-08-14
- Status: **dbt project RUNS and PASSES locally against real Iceberg; not deployed**
- Related: [`docs/KIMBALL_MODEL.md`](KIMBALL_MODEL.md), [`docs/FOUR_FLOWS.md`](FOUR_FLOWS.md)

---

## 1. Version matrix

| Component | Pinned | Why |
|---|---|---|
| `dbt-core` | **1.9.4** | `require-dbt-version: [">=1.9.0", "<1.10.0"]` in `dbt_project.yml` |
| `dbt-spark` | **1.9.2** | Adapter minor **must** match core minor — the most common dbt-spark breakage |
| `pyspark` | **3.5.0** | Matches the Spark runtime the flows already use |
| `iceberg-spark-runtime` | **3.5_2.12:1.5.2** | Same pin as every other Spark job in this repo |
| dbt packages | **none** | See §7 |

Verified installed, not assumed:

```
$ dbt --version
Core: 1.9.4      Plugins: spark 1.9.2
```

## 2. The split — decided from evidence

The question is not "can dbt do this" but "can dbt do this *correctly*". Two things it
cannot, and both were verified against the installed macros rather than assumed.

### 2.1 dbt cannot express the anti-downgrade rule

`spark__get_merge_sql` (dbt-spark 1.9.2) emits:

```sql
merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on {{ predicates | join(' and ') }}
    when matched then update set ...      -- UNCONDITIONAL
    when not matched then insert *
```

The `when matched` clause is unconditional, and dbt's only lever — `incremental_predicates`
— is appended to the **ON** clause. That is not the same thing, and the difference is
destructive:

> If the rank test sits in `ON` and fails, the row is **not matched**, so
> `when not matched then insert *` fires and dbt **INSERTS A DUPLICATE**.

Demonstrated, not argued (`artifacts/validation/session-11/dbt-merge-proof.txt`):

```
ROWCOUNT: 2
   Row(transaction_id='T1', amount=100.00, processing_status='CERTIFIED')
   Row(transaction_id='T1', amount=999.00, processing_status='PROVISIONAL_NRT')
VERDICT: DUPLICATE FACT CREATED
```

The certified row survived — and the mart now has two rows for one transaction, breaking
the grain and double-counting every measure. That is **worse** than the downgrade the rule
exists to prevent.

So the guarded MERGE (S09-2) stays in PySpark, where `WHEN MATCHED AND <cond>` is available.

### 2.2 dbt snapshots do not fit this SCD2

To be fair to dbt: **dbt 1.9 added `dbt_valid_to_current`**, so the NULL-end-date problem
(S10-6) *is* configurable — that alone would not disqualify snapshots.

The decisive reason is different. A dbt snapshot **accumulates** history by polling a source
across successive runs: it can only record changes it was running to observe. This project's
SCD2 is **derived** from L3 history in a single pass, so it can be rebuilt from scratch at
any time and reproduces the same versions and the same surrogate keys (S10-1).

That difference is the whole reproducibility property. A dbt snapshot cannot reconstruct
history it did not witness; ours can. Secondary: `dbt_scd_id` is an MD5 **string**, while
facts carry `BIGINT` surrogate keys.

### 2.3 The resulting boundary

| Concern | Owner | Why |
|---|---|---|
| Canonical transformation | **Spark** | One implementation, or variance stops being diagnosable (S09-1) |
| Guarded MERGE / anti-downgrade | **Spark** | dbt inserts a duplicate instead (§2.1) |
| Grain collapse by source position | **Spark** | Needs the `event_order` contract (S09-4/5) |
| SCD2 dimensions + surrogate keys | **Spark** | Reproducible, not accumulated (§2.2) |
| Point-in-time joins | **Spark** | Built into fact construction (S10-4) |
| L1 → L2 → L3 | **Spark** | Streaming, partition overwrite semantics |
| **Aggregate marts** | **dbt** | Pure SQL over facts — dbt's actual strength |
| **Tests** | **dbt** | Declarative, next to the model, one artifact |
| **Docs / lineage / exposures** | **dbt** | No Spark equivalent worth building |
| **Source contracts** | **dbt** | `sources.yml` is the contract, executable |

`spark/tests/test_dbt_contract.py` **enforces** this boundary: it scans every dbt model
(comments stripped) for `row_number(`, `merge into`, `when matched then` and
`effective_from`, and fails if a model starts restating Spark logic.

## 3. Migration, not duplication

The four aggregate marts **moved** to dbt. `spark/marts/marts.py` was **deleted** in this
session — keeping both would be exactly the second implementation of the business logic
that S09-1 exists to prevent, and the two would drift.

Session 10's `TestMarts` was removed with it. Its properties are now asserted where the
code lives:

| Property | Now tested by |
|---|---|
| grain / no fan-out | dbt `dbt_utils_unique_combination` per mart |
| certification metadata | dbt `not_null` on `run_id`, `built_at` |
| weakest-status rule | dbt `assert_mart_status_is_weakest_input.sql` |
| `certified_at` consistency | dbt `assert_certified_at_matches_status.sql` |
| 10-minute bucket alignment | dbt `assert_monitoring_buckets_are_aligned.sql` |
| balance not pre-summed | dbt grain uniqueness on `(account_sk, business_date)` |
| ladder has not drifted | `spark/tests/test_dbt_contract.py` |

### The ladder stays defined once

`flows.STATUS_RANK` remains the single source of truth. `macros/status_priority.sql`
mirrors it because SQL needs it — and `test_dbt_macro_matches_python_status_rank` **parses
the macro** and fails if the two disagree. A second hardcoded rank table is precisely how
an anti-downgrade rule stops matching the ladder it enforces.

## 4. Project layout

```
dbt/
├── dbt_project.yml            pinned versions, materialisations, vars
├── profiles.yml.example       env-var only; no secrets, no defaults that work by accident
├── models/
│   ├── sources.yml            7 sources + generic tests on them
│   ├── staging/               3 models — EPHEMERAL (see §6)
│   ├── intermediate/          3 models — EPHEMERAL; pre-aggregate to mart grain (S10-13)
│   └── marts/                 4 incremental models + schema.yml + 2 exposures
├── macros/
│   ├── status_priority.sql    status_rank, weakest_status, status_rank_if_present
│   ├── audit_columns.sql      run_id / certified_at / built_at, business_date, safe_divide
│   └── generic_tests.sql      local unique-combination test (§7)
└── tests/                     8 singular tests
```

Intermediate models exist to make S10-13 **structural**: joining three facts of different
grains raw would fan out — a customer with 3 transactions and 2 accounts yields 6 rows,
every measure inflated, each row still looking correct. Collapsing to the mart grain first
makes that impossible rather than merely discouraged. This is genuinely clearer as three
named dbt models than as CTEs inside one PySpark function.

## 5. Incremental strategy: `insert_overwrite`, not `merge`

Every mart pins:

```yaml
+incremental_strategy: insert_overwrite
+partition_by: ["business_date"]
```

Rationale beyond §2.1: the marts are **derived and idempotent** — a mart for a business date
is a pure function of the facts for that date. Overwriting the whole day partition is
correct, cheap at demo volumes, and needs no row-level reconciliation.

**Proven idempotent**, not asserted: `scripts/dbt-verify.sh` runs the mart models three
times and compares counts.

```
mart_customer_360_daily                rows=2
mart_channel_performance_daily         rows=2
mart_transaction_monitoring_10m        rows=2
mart_account_balance_daily             rows=2
```

`contract: {enforced: false}` on the marts: dbt model contracts require the adapter to
enforce column constraints, and Spark/Iceberg support for `NOT NULL` constraints via dbt is
partial. Claiming an enforced contract that the engine does not enforce would be worse than
declaring none — the `not_null` **tests** do the real work.

## 6. What runs, and one honest gap

`scripts/dbt-verify.sh` runs the whole project against a throwaway local Iceberg warehouse.
**No AWS, no credentials, no cost** — which is the point: the project is fully validated
before anything is deployed.

```
dbt parse           OK
dbt compile         OK      50 compiled artifacts
dbt build           OK      Done. PASS=47 WARN=0 ERROR=0 SKIP=0 TOTAL=47
idempotency         OK      3 runs, counts unchanged
dbt docs generate   OK      manifest 674 KB, 53 nodes, 7 sources, 2 exposures
```

### Staging is EPHEMERAL because Iceberg views are not portable

The first build failed with:

```
Replacing a view is not supported by catalog: local
```

Rather than work around it, staging became `ephemeral`. Iceberg VIEW support varies by
catalog (Glue only gained it recently), and staging here is pure projection with no
filtering benefit — inlining it as a CTE costs nothing, creates no catalog object for Glue
to hold, and removes a portability dependency the models never needed.

### `catalog.json` is empty locally — stated, not hidden

`dbt docs generate` **succeeds** and `manifest.json` (lineage, models, tests, exposures) is
complete. But `catalog.json` has **0 entries**: dbt-spark's catalog query does not work
against a non-session Iceberg catalog.

So the lineage graph renders; column-level types and stats do not. The acceptance criterion
("docs artifact generated or command documented") is met by the manifest, and this
limitation is expected to disappear against Glue — but that is **`NOT_TESTED`**, and it is
recorded as such rather than assumed.

## 7. No dbt packages

`dbt_utils` would have supplied `unique_combination_of_columns`. It is not used, and there
is no `packages.yml`:

- `dbt deps` fetches from the network at build time. `CLAUDE.md` §3.9 forbids unpinned
  dependencies and §3.10 requires third-party code to be pinned to a verified reference.
- The one macro actually needed is ~8 lines of SQL (`macros/generic_tests.sql`).

Twenty lines of local SQL is a smaller cost than a supply-chain exception, and the build
stays hermetic and offline.

## 8. Secrets and security

`profiles.yml` is **not committed** — only `profiles.yml.example`, where every value comes
from `env_var()`. There is no default that would work by accident.

The lab has no password at all: EMR Serverless and Glue are reached through the default AWS
credential chain via an IAM role (`CLAUDE.md` §3.2), so there is nothing to put in a profile
even if we wanted to.

`spark/tests/test_dbt_contract.py::TestNoSecretsInDbtProject` enforces this: no committed
`profiles.yml`, no literal credential under any credential key, no AWS access key id, no
account id and no ARN anywhere in `dbt/`.

## 9. Tests, and proof they catch what they claim

40 generic + singular tests at first build; **47** after the mart tests were added.

Acceptance requires that the tests catch duplicate grain, SCD overlap and bad FKs. Each was
**injected into real data** and the run confirmed to fail
(`artifacts/validation/session-11/test-injection.txt`):

| Injected defect | Caught by |
|---|---|
| duplicate `transaction_id` | `source_unique_curated_fact_transaction_transaction_id` + reconciliation |
| overlapping SCD2 version | `assert_scd2_no_overlapping_validity` + `assert_scd2_one_current_row_per_key` |
| fact FK with no dimension row | `source_relationships_..._customer_sk` + reconciliation |
| `effective_to IS NULL` | `assert_scd2_effective_to_never_null` + `source_not_null` |

### A real bug this session found in its own model

The first successful build produced:

```
cust=101 txn=25.00 bal=250.00 status=UNKNOWN     <- wrong; should be PROVISIONAL_NRT
```

`status_rank` returns `0` for an unrecognised status — the right safety property — but that
makes NULL and garbage indistinguishable. After an outer join NULL means "this input
contributed no rows", which is not evidence of anything, so `LEAST(...)` picked `0` and the
mart reported `UNKNOWN`. The model's own comment claimed to handle exactly this.

Fixed with `status_rank_if_present`. The test suite had not caught it because
`accepted_values` **included** `'UNKNOWN'` — that value has been removed from the list, so
the same bug now fails the build.

## 10. Cost

**$0.00 incremental.** No AWS resource is created by this session.

Session constraint: *"Use existing on-demand Spark runtime; do not keep a cluster alive for
dbt."*

- dbt runs as **one on-demand job** against the same EMR Serverless application the flows
  use, which auto-stops (`CLAUDE.md` §4.6). No warm endpoint, no interactive cluster.
- Local verification (`scripts/dbt-verify.sh`) uses a PySpark session and a temp directory —
  no AWS at all, so CI costs nothing.
- Marts stay in **S3 Iceberg**; no warehouse is introduced. Athena remains the query engine.
- The dbt models add no new storage: they write the same four mart tables Session 10 already
  specified.

Session 12 wires `dbt build` into Airflow as a task in the existing DAG, not as a service.

## 11. Commands

```bash
scripts/dbt-verify.sh          # everything below, against a throwaway local warehouse
scripts/dbt-verify.sh --keep   # keep the warehouse to inspect the tables

cd dbt
dbt parse
dbt compile      --vars '{business_date: 2026-08-14}'
dbt build        --vars '{business_date: 2026-08-14}'
dbt run          --select tag:mart --vars '{business_date: 2026-08-14}'
dbt test         --vars '{business_date: 2026-08-14}'
dbt docs generate --vars '{business_date: 2026-08-14}'
dbt docs serve   # renders manifest lineage; catalog is empty locally (§6)
```
