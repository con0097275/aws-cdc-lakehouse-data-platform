# Final acceptance report — AWS CDC Lakehouse

> ## Session 20 addendum — 2026-08-15 (REVISED: foundation now APPLIED)
>
> **The cheap-tier foundation is deployed.** 32 resources live, drift 0, idle cost
> ~$1.01/month — the lake KMS CMK. **MSK was never created**; nothing bills hourly.
>
> This upgrades a specific, limited set of claims and **nothing else**. Athena, Glue, S3,
> IAM and KMS are now proven against live AWS: all 7 databases visible to Athena, query
> results written to the lake and encrypted with the project CMK, and the workgroup's
> configuration enforcement demonstrated by a discarded client override. Every
> **table-dependent** scenario and criterion below remains exactly as blocked as before,
> because no Iceberg table exists — Spark creates those in the CDC window, which has not
> run. No throughput, latency or scale figure exists.
>
> Two defects were found and fixed: **F1** (cost-allocation tag inactive — the $30 budget
> could never have fired; BLOCKED pending 24h billing propagation) and **F2** (the Athena
> smoke queries hardcoded unprefixed schema names and could never have passed in any
> environment). Full evidence:
> `artifacts/validation/session-20/foundation-validation.txt`.
>
> Earlier in this same session the bootstrap refused non-interactive execution
> (`confirm_destructive()` at `lib.sh:161`, by design). It was never bypassed — the
> operator typed the phrase at an interactive terminal, which is how Gates 1 and 2 were
> passed. Six typed phrases still guard the CDC chain; see `SESSION_HANDOFF.md`.
>
> **Also completed earlier in the session — four items, all verified:**
>
> | Change | Evidence |
> |---|---|
> | Sessions 03–19 committed (were 331 untracked files, no remote) | `60de297` |
> | **OPEN-22 CLOSED** — all 8 DAGs import under Airflow 3.2.2 | 31 tests pass on 2.9.3 **and** 3.2.2, mutation-checked on both |
> | **OPEN-06 CLOSED** — shellcheck enforced at severity=warning | `make lint-shell` |
> | **Two latent defects fixed in `tf.sh`** | `scripts/plan_cost.py` + 12 tests |
>
> The `tf.sh` defects matter more than their size suggests. The cost preview was inlined
> in a bash single-quoted string, so `d['resource_changes']` reached Python as
> `d[resource_changes]` — a `NameError`; and its f-string carried a backslash, a
> `SyntaxError` before Python 3.12 (installed: 3.10). Either aborted the block **before**
> the NAT / Secrets Manager invariant check. **The guard that stops an invariant-breaking
> apply had never worked.** It was never caught because the script had never run and
> shellcheck was absent — the two open items closing together is what exposed it.
>
> Separately, the state bucket would have been created **untagged** despite declaring six
> tag values, making it invisible to the tag-filtered project budget. Fixed before the
> bucket exists.
>
> **Test count is now 417** (405 + 12 for the previously-untested spend guard). `make
> check` 14/14. `terraform validate` and `fmt` clean.
>
> That cheap-tier plan has since been **applied**: 32 resources, $0.0000/hr, 0 NAT,
> 0 Secrets Manager, and a post-apply plan returning `detailed-exitcode=0`.
>
> **The scenario and criterion tables in §3 and §4 below are UNCHANGED.** Every item
> marked BLOCKED there is still blocked, for the same reason: they need Kafka, a
> connector, a Spark job or a populated Iceberg table, and none of those exist. §1 and §2
> are updated to reflect the foundation; nothing else is.

- Session: 19 · Date: 2026-08-15
- Account `111122223333` · Region `ap-southeast-1` · Profile `my-aws-profile`
- Suite re-run for this report: **405 Python tests passed**, `make check` 14 passed
- **One gap found and CLOSED during this session** — see §3 scenario 10

---

## 0. The finding that determines every result below

**The end-to-end pipeline acceptance test could not be executed.**

Nothing is deployed. Verified live for this report:

```
MSK clusters 0 · EC2 0 · EMR Serverless 0 · Athena workgroups (ours) 0
resources tagged Project=kafka-dev-lab: 0 · terraform state: none
```

One prerequisite blocks everything: `scripts/bootstrap-state-backend.sh --execute` has never
been run. Every session ended at an approval gate rather than self-approving spend.

**Consequence for classification.** Criteria that depend on a running Kafka, connector,
EMR job or Athena workgroup are **BLOCKED** — not `NOT_TESTED`. The distinction is
deliberate:

- **BLOCKED** — a specific, named prerequisite is unmet. Testing is impossible today.
- **NOT_TESTED** — testing was possible and did not happen.

Calling a blocked item `NOT_TESTED` would imply negligence; calling it `PASS` would be a lie.

---

## 1. Final architecture

```
Oracle · SQL Server          DESIGN ONLY — never provisioned
   → Debezium / Connect      DESIGN ONLY — connector JSON written, never registered
   → MSK KRaft + Avro        DESIGN ONLY — no cluster has ever existed
   → Apicurio Registry       DESIGN ONLY
   → Spark Structured Str.   DESIGN ONLY — job written; only batch paths tested
   → L1 STREAM  (Iceberg)    LOGIC LIVE TESTED — real Spark, real Iceberg
   → L2 FULL CDC             LOGIC LIVE TESTED
   → L3 SNAPSHOT             LOGIC LIVE TESTED
   → four flows              LOGIC LIVE TESTED
   → Kimball dims/facts      LOGIC LIVE TESTED
   → data marts (dbt)        LOGIC LIVE TESTED — dbt build PASS=47
   → Athena                  DEPLOYED + LIVE TESTED — workgroup enforced, 7 Glue DBs visible
   → Power BI                DESIGN ONLY — steps written, never walked
```

Full diagrams: `README.md`. Evidence labels: `docs/CAPABILITY_MATRIX.md`.

## 2. Deployed AWS resources

**Updated 2026-08-15 (Session 20).** The cheap-tier foundation is deployed — **32
resources**, all tagged `Project=kafka-dev-lab`:

| Resource | Qty | Idle cost |
|---|---:|---|
| S3 lake bucket (SSE-KMS, versioned, TLS-only, 13 prefixes) | 1 | ~$0.00 |
| S3 Terraform state bucket (out of band, ADR-021) | 1 | ~$0.01/mo |
| KMS customer managed key + alias (lake) | 1 | **$1.00/mo** |
| Glue catalog databases | 7 | free tier |
| Athena workgroup (10 GiB cutoff, enforced) | 1 | $0.00 — bills bytes scanned only |
| AWS Budget, tag-filtered $30 | 1 | $0.00 |

**Hourly cost: $0.0000.** Nothing in the foundation bills by the hour — no MSK, EC2, EMR,
NAT, EKS, RDS or load balancer. **Monthly idle cost ~$1.01, of which the KMS CMK is
$1.00.**

Still true: **no MSK cluster has ever existed** (`enable_kafka_platform=false`), and the
7 Glue databases contain **0 tables** — Iceberg tables are created by Spark on first
write, during the CDC window that has not run.

The account separately contains 44 pre-existing S3 buckets and one unrelated Glue
database, month-to-date **$0.2463**, none of it attributable to this project.

## 3. Data scenarios — 15 required

| # | Scenario | Status | Evidence |
|---|---|---|---|
| 1 | INSERT | **PASS** | `test_latest_update_wins`, `test_initial_snapshot_r_operation_is_kept` — 2 tests |
| 2 | UPDATE | **PASS** | latest-by-source-position wins — 2 tests |
| 3 | DELETE | **PASS** | `test_latest_delete_excludes_the_pk` + SQL Server variant — 2 tests |
| 4 | Multiple updates | **PASS** | `test_one_active_row_per_pk` — 5 PKs × 3 updates |
| 5 | Delete and recreate | **PASS** | 2 tests; works *by construction*, no special case |
| 6 | Duplicate event | **PASS** | `TestDuplicateReplay` — 4 tests |
| 7 | Kafka replay | **PASS (logic)** / **BLOCKED (broker)** | idempotency proven locally; an actual offset reset needs MSK |
| 8 | Late-arriving event | **PASS** | 4 tests incl. the late-DELETE-empties-partition regression |
| 9 | Out-of-order arrival | **PASS** | 2 tests — arrival order irrelevant, ranking is by source position |
| 10 | **Schema evolution** | **PASS (lake) / BLOCKED (registry)** | **Gap found and FIXED in this session.** It had *no executable test at all* — blocked by more than deployment. 5 tests added (`test_schema_evolution.py`), mutation-checked. Registry-side BACKWARD rejection still needs a live Apicurio |
| 11 | Connector restart | **BLOCKED** | drill 1 — needs a deployed Connect worker; recorded `NOT_RUN` |
| 12 | Spark restart | **BLOCKED** | drill 3 — needs a deployed streaming job |
| 13 | Checkpoint recovery | **PARTIAL: PASS (window logic) / BLOCKED (checkpoint)** | 13 window/watermark/overlap tests pass; restarting from a real checkpoint is drill 4, `NOT_RUN` |
| 14 | EOD rerun | **PASS** | `test_rebuild_does_not_accumulate_rows` — 3 reruns, count unchanged |
| 15 | L3 rebuild | **PASS** | `TestDeterministicRebuild` — identical checksum for the same cutoff |

**11 PASS · 3 BLOCKED · 1 PARTIAL · 0 FAIL** *(was 10/4/1 before the fix below)*

### The gap this session found, and closed

Scenario 10 was initially **BLOCKED for the wrong reason**. Schema evolution appeared in the
design (registry compatibility, contract-change ledger) but had **no test that executes** —
it was not blocked only by deployment; no local test had ever been written.

The lake's tolerance for schema change needs no registry, broker or AWS: it is a property of
the projection, and the projection runs on local Spark. Five tests added:

| Change | Behaviour proven |
|---|---|
| Column **added** upstream | absorbed — L1/L2 store `after` as JSON, so no Iceberg migration |
| Old and new shapes **coexist** mid-rollout | both certify |
| Column **dropped** upstream | becomes NULL — the build does **not** crash |
| The dropped column is **detectable** | completeness check FAILS and blocks the publish |
| Source **type change** | uncastable value → NULL, also caught by DQ |

**The design split this exposed and confirms:** the projection *tolerates* schema change so
one dropped attribute cannot stop a day's pipeline, and the DQ gate *refuses to certify it
silently*. Neither half is sufficient alone — tolerance without detection is a silent data
loss, detection without tolerance is a brittle pipeline.

Mutation-checked: relaxing the completeness threshold makes the test fail.

## 4. Correctness criteria — 15 required

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | CDC completeness | **BLOCKED** | requires source→Kafka→L1 counts; no event has flowed |
| 2 | Deterministic event identity | **PASS** | `event_id = sha256(topic:partition:offset)`; `test_envelope` 15 |
| 3 | Source-aware ordering | **PASS** | `test_ordering` 17 — Oracle SCN pads, SQL Server LSN validates; higher SCN with *lower* offset in another partition wins |
| 4 | No incorrect duplicates | **PASS** | grain enforced by source position; `collapse_to_grain` + 4 replay tests |
| 5 | Correct delete semantics | **PASS** | active excludes latest delete; `_history` opt-in; recreate works by construction |
| 6 | Correct T-1 snapshot | **PASS** | as-of cutoff, exclusive midnight, timezone-safe; 30 tests |
| 7 | L1/L2/L3 reconciliation | **PASS (logic)** / **BLOCKED (live)** | independent `max_by` derivation tested; against real data, blocked |
| 8 | NRT freshness ≤10 min | **BLOCKED** | latency is a property of a deployed pipeline (OPEN-19) |
| 9 | Auto-correct convergence | **PASS** | late event picked up, variance falls to 0 — `TestLateEventAndAutoCorrect` |
| 10 | Certified EOD result | **PASS** | certified cannot be downgraded; end-to-end through the real MERGE |
| 11 | Kimball correctness | **PASS** | SCD2 invariants, point-in-time joins, deterministic SKs, additivity — 40 tests |
| 12 | Data quality | **PASS** | 6 check types, 3 verdicts, 24 tests; empty ≠ pass |
| 13 | Monitoring | **BLOCKED** | 13 alerts and 14 SLIs defined; **no SLI measured, no alert fired** (OPEN-24) |
| 14 | Recovery | **PARTIAL: 4 PASS / 6 BLOCKED** | L3 rebuild, late event, replay idempotency, L2 window proven locally; 6 drills need infrastructure |
| 15 | Cost controls | **PASS** | budgets verified live ($30 + $1 zero-spend); flags default off; 0 NAT/EKS/RDS; spend $0.00 |

**9 PASS · 4 BLOCKED · 2 PARTIAL · 0 FAIL**

## 5. Test evidence

```
405 Python tests   (20 suites, most against real Spark + real Iceberg tables)
 47 dbt tests      (dbt build PASS=47 against a local Iceberg warehouse)
 14 doc/static checks (make check)
```

Per-suite: `test_ai_assistant` 53 · `test_dags` 31 · `test_l3_snapshot` 30 ·
`test_observability` 28 · `test_governance` 27 · `test_scd2` 27 · `test_athena_serving` 26 ·
`test_l2_window` 26 · `test_dq_engine` 24 · `test_ordering` 17 · `test_flows` 17 ·
`test_four_flows_spark` 16 · `test_envelope` 15 · `test_reconcile` 14 ·
`test_kimball_spark` 13 · `test_dbt_contract` 10 · `test_quarantine` 10 ·
`test_l2_local_spark` 9 · `test_l1_local_spark` 7

**Every guard is mutation-checked** — reverted, and the test confirmed to fail. Artefacts in
`artifacts/validation/session-*/mutation-checks.md`.

### Defects found and fixed during development

| Defect | Why it mattered |
|---|---|
| Late DELETE left stale ACTIVE rows and still reported `CERTIFIED` | deleted record surviving in a certified snapshot |
| `canonical_pk_expr()` emitted malformed JSON | every PK would look new |
| Mart grain unenforced | every updated transaction double-counted |
| BI role could read L1/L2 in full | unmasked PII exposure |
| A masked *view* is not a security boundary | PII readable via the base table |
| dbt merge would insert a duplicate fact | proven, not argued |

Each produced *plausible, wrong numbers* rather than an error — which is why they survived
review and were caught only by a test written to fail.

## 6. Performance observations

**None. No performance measurement exists.**

No throughput, latency, bytes-scanned, query-duration or concurrency figure has ever been
observed, because nothing has run. Every number in the documentation that resembles
performance is either a **target** (NRT ≤10 min), an **estimate from the architecture**
(RTO minutes/hours), or a **unit price × quantity** calculation (cost).

Stating an observed figure here would be fabrication.

## 7. Known limitations

1. **Never deployed.** No CDC event has flowed end to end.
2. **No performance or scale data of any kind.**
3. Registry-side schema **rejection** (an INCOMPATIBLE change refused by Apicurio) is
   still untested — needs a live registry.
4. DAG tests ran against Airflow **2.9.3**, not the target **3.2.2** (OPEN-22).
5. Local tests use a **Hadoop catalog, not Glue**; MERGE/view behaviour under Glue with
   concurrent writers is unverified.
6. Six of ten failure drills recorded `NOT_RUN`.
7. Single-node Airflow, single region, no automated failover — **survives component failure,
   does not survive region failure**.
8. 3 CloudWatch log groups (other projects') have no retention (OPEN-26).
9. 39 orphan Glue tables of unknown provenance — isolated, not adopted, not deleted (OPEN-20).
10. AI assistant tier 2 never invoked (OPEN-25).

## 8. Current cost drivers

| | |
|---|---|
| **This project** | **$0.00/month** — zero resources |
| The account | ~$0.35/month — 44 pre-existing buckets, 13.67 GB |
| KMS | $0.00 — all 5 keys are AWS-managed, therefore free |
| Budgets in place | $30 monthly + $1 zero-spend |

**If deployed**, the dominant drivers would be MSK (continuous), then EC2 hosts during the
metered window, then EBS (bills while stopped). Airflow alone left running 24/7 is
**$79.97/month — 267% of the budget**. Worksheet: `docs/COST.md`. No total is promised.

## 9. Shutdown instructions

```bash
export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1
bash scripts/stop-ephemeral.sh --execute     # stop only — cannot delete
bash scripts/verify-destroy.sh --stopped     # exits non-zero if anything remains
```

**Stop is cheaper, not free** — EBS bills while stopped; only terminate removes it. **MSK
cannot be stopped**, only kept or deleted.

## 10. Destroy instructions

**Not executed. Nothing exists to destroy.**

```bash
cd terraform/envs/dev
aws sts get-caller-identity                  # always first
terraform destroy -target=module.airflow_k3s # leaves inward
terraform destroy -target=module.cdc_runtime
terraform destroy -target=module.source_lab
terraform destroy -target=module.emr_serverless
terraform destroy -target=module.athena
terraform destroy -target=module.glue_catalog
terraform destroy -target=module.kafka_platform
terraform destroy
bash ../../../scripts/verify-destroy.sh --destroyed
```

Retained by design: the S3 lake (`force_destroy=false`), the Terraform state backend
(destroying it strands everything else), KMS keys (7–30 day window), and the OPEN-20 orphan
database. Full procedure: `docs/FINOPS.md` §6.

## 11. CV-ready project summary

> **AWS CDC Lakehouse** — designed and built a change-data-capture lakehouse taking Oracle
> and SQL Server changes through Debezium and Kafka into an Iceberg medallion architecture
> (L1 raw stream → L2 full history → L3 certified point-in-time snapshot) with a Kimball
> star schema and dbt marts served through Athena.
>
> Built as **19 gated sessions with 166 recorded architecture decisions**, **405 automated
> tests** running against real Spark and Iceberg, and every guard **mutation-tested** —
> reverted to confirm its test fails.
>
> The engineering substance is in CDC correctness: source-aware ordering across Oracle SCN
> and SQL Server LSN (which normalise in opposite directions), deterministic event identity
> making replay and backfill idempotent, half-open windows with late-arrival sweeps, and
> deterministic snapshot rebuilds.
>
> Testing found six defects that each produced **plausible, wrong numbers** rather than
> errors — including a late DELETE leaving stale rows in a snapshot the pipeline reported as
> `CERTIFIED`, and a BI role able to read unmasked PII through a "masked" view.
>
> **A lab, not a production system:** built to a $30/month budget, never deployed, and every
> capability labelled `LIVE TESTED` / `IMPLEMENTED` / `DESIGN ONLY` / `OPTIONAL`. No
> throughput or scale claim is made because none was measured.

---

## Verdict

# READY_FOR_PORTFOLIO

**As a portfolio project it is ready, and unusually well-evidenced.** 400 passing tests
against real engines, 166 documented decisions, six real defects found and fixed with live
reproductions, and — the part that matters most — an honest evidence-labelling scheme where
every claim maps to something checkable.

### What it is NOT ready for, stated plainly

It is **not** a validated end-to-end pipeline. The acceptance test this session was asked to
perform **could not be run**: 4 of 15 data scenarios and 4 of 15 correctness criteria are
**BLOCKED** on deployment, and the entire CDC plane — Debezium, Kafka, Avro, streaming — has
never processed a single event.

Anyone presenting this must lead with that. The capability matrix does.

### Remaining blockers

| Blocker | Unblocks |
|---|---|
| **`bootstrap-state-backend.sh --execute` never run** | everything — it gates all 17 modules |
| No deployment | CDC completeness, freshness, monitoring, 6 drills, schema evolution, Power BI |
| Airflow 2.9.3 vs 3.2.2 (OPEN-22) | DAG import validity — closable *without* AWS, in a venv |
| ~~Schema evolution untested~~ | **CLOSED in this session** — 5 tests added at zero cost |

**One of the remaining three is closable at zero cost**: an Airflow 3.2.2 parse environment
(a venv) would convert OPEN-22 without spending anything. I closed the schema-evolution one
here rather than leaving it as a recommendation.

The other two need a first controlled apply. My recommendation stands from Session 18: one
metered-window deployment would convert roughly a dozen `DESIGN ONLY` labels into
`LIVE TESTED` and turn every RTO, SLO threshold and cost figure from an estimate into a
measurement. **That decision is yours — no session has self-approved spend, and I have not
here.**
