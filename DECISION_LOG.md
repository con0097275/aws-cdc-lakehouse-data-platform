# DECISION LOG

Chronological record of decisions, including those still open. Full ADR bodies are in
`docs/adr/`; `DECISIONS.md` is the index. This file adds the **open** items and the
things that are decisions but not architecture.

---

## Session 30 — 2026-08-20 — Phase 11, STREAMING_RT

### D30-1 — the approved source is FULL_CDC_APPEND, and it was reconfirmed, not reopened

**Decision: UNCHANGED.** ADR-041 chose `FULL_CDC_APPEND` — an Iceberg incremental read of
the canonical layer — as the default, with `KAFKA_DIRECT` permitted under two obligations.
The pilot ships `FULL_CDC_APPEND`. Nothing about that was revisited on implementation
evidence, and the alternative was not silently adopted for convenience.

What changed is enforcement. The first obligation (`source_justification`) was already
checked at compile. The second — a mandatory reconciliation job against FULL_CDC — was
written in the ADR and enforced nowhere, so `resolve_stream_source()` now raises without
one. A divergence nobody checks for is one nobody finds, and the reference implementation
chose Kafka-direct and paid exactly that price: its correction pass exists to reconcile the
stream against the durable layer.

### D30-2 — a failed batch RAISES; it does not return quietly with an audit marker

**Decision: RAISE, diverging from the reference implementation deliberately.**

With `foreachBatch`, a handler that returns after catching an exception tells Spark the
batch succeeded, so Spark commits the offsets and the rows are gone. The reference repo
records the incident in its own source (`flow3_pl_stream_main/src/main.py:540-556`): a batch
died on a broadcast timeout, returned quietly, and ~100K offsets were committed and lost —
unrecoverable, because those rows had never been written anywhere a reconciler could find
them. Its mitigation is to write the batch's keys into an audit table before dropping the
batch.

That mitigation recovers a mistake. Raising prevents it: Spark does not commit, retries the
batch, and the data is still in the source. ADR-041 already specified this; the divergence
from the reference is intentional and is the single most important line in the module.

### D30-3 — `restart_count` counts restarts, and a failed batch is not one

**Decision: a new verb.** A raised batch is RETRIED by Spark inside the same query — the
application did not restart. Recording it through `record_streaming_restart` would inflate
`restart_count`, which is the one number that separates "one bad batch" from "the process
keeps dying". `record_streaming_error()` was added to the repository contract, both
adapters and `OpsClient` so the two failures have two counters.

### D30-4 — deletes are SOFT, and the reason is ordering, not only contract

**Decision: FIXED, found by a test I wrote to prove the opposite.** The first sink dropped
the row on a delete. A later-arriving but *older* update then found no row to lose to and
resurrected the key — a resurrection that no error reports and that only shows up as a row
that should not exist.

Tombstones keep the key's `event_order`, so the ordering comparison still works across a
delete. The job config already said `delete_strategy: SOFT`; the implementation did not
match it, and the real `MERGE INTO` in the Spark entrypoint had the identical flaw and the
identical fix.

### D30-5 — nothing on the start path can delete a checkpoint

**Decision: STRUCTURAL, not procedural.** `CheckpointInspector` has no delete method and a
test asserts it never grows one. A checkpoint that cannot be READ raises rather than being
treated as empty — assuming empty silently skips every event between the real offset and
now.

Deletion is `scripts/streaming-reset.sh`: dry-run by default, refuses non-interactively,
typed confirmation phrase, MOVES rather than deletes, audited, and with the teardown order
fixed (stop → move checkpoint → then tables). The reference repo encodes the same order
after hitting the failure it prevents.

`scripts/validate-docs.py` check 14 was extended to cover the new script, and to accept an
explicit `--execute -> DRY_RUN=0` alongside `parse_execution_flags`: the invariant is
"defaults to dry-run", not "calls one particular helper", and a script that takes its own
arguments cannot use a parser that dies on anything it does not recognise.

### D30-6 — STREAMING_RT ships registered and disabled

**Decision: TWO flags, both off.** `is_enabled: false` in the job config and
`ENABLE_STREAMING_RT=false` in the environment. ADR-041's position is that the framework
should compile, validate and test the contract while the application — the one hourly cost
here — is not deployed. `test_shipped_config_loads_and_validates` now asserts both halves:
the mode is registered AND it is off. A mode that vanished from the config and a mode that
shipped switched on look identical to a test that only counts modes.

---

## Session 29 — 2026-08-20 — Phase 10, STREAM_BATCH

### D29-1 — the STREAM_BATCH predicate compared a DATE column to a timestamp window

**Decision: FIXED, and the fix is pinned by evaluation rather than by inspection.**
`incremental_filter()`'s no-event-column fallback emitted
`business_date >= CAST('… 06:00:00' AS TIMESTAMP)`. `business_date` is a `DATE`; Spark
promotes it to midnight, so the predicate was false for every row on the window's own date.
The model selected nothing on every run, succeeded, and advanced the watermark past data it
had never written.

Nothing could have raised. The flow's quiet-window path treats zero rows as a legitimate
outcome — it is one — so an always-empty window is indistinguishable from a genuinely quiet
one from inside the framework.

The branch now projects the window onto the business dates it overlaps
(`DATE(low) … DATE(high)`) and recomputes them in full: a superset of the window, safe only
because the write is an idempotent guarded MERGE on the business key (ADR-042). A model with
an event column passes it and takes the precise half-open branch instead.

**What this says about the test that missed it.** `test_dbt_contract.py` reads macro text,
which proves the SQL was written a certain way and cannot prove it selects anything.
`spark/tests/test_reporting_stream_batch_sql.py` now evaluates both predicates — including
the defective one — in real Spark.

### D29-2 — `source_layer_substituted` was documented, read, and never written

**Decision: CLOSED.** `reporting/layers.yaml` states that REALTIME is `SUBSTITUTED` onto the
full_cdc database and that every execution reading it records the flag.
`ExecutionRecord.source_layer_substituted` existed and the DynamoDB adapter deserialised it;
no code path ever set it. A promise enforced nowhere.

`record_source_layer()` is added to the repository contract, both adapters and `OpsClient`,
and STREAM_BATCH writes it **before** the read, so a run that dies mid-read still says which
layer it was pointed at. A `SUBSTITUTED` binding without a `bounded_predicate` now fails the
run outright — the guard fired on first use, against a test fixture that had omitted the
predicate, which is the guard working.

**Not extended to AUTO_CORRECT**, which also touches REALTIME through `EOD_PLUS_REALTIME`.
That is Phase 8 code and is recorded as an open item rather than folded into this session
silently.

### D29-3 — the production reader reads nothing in Airflow

**Decision: `EngineSideBoundedReader`.** The in-process readers pull `ChangeRow`s so the
flow can tell a quiet window from a busy one before submitting — right for a test, wrong for
an Airflow task, because the change feed is data-plane traffic and the scheduler node also
runs the scheduler (`airflow/tests/test_dags.py` forbids exactly this).

So the bounded read happens inside Spark, against the same frozen window, with the model's
`incremental_filter()` applying the identical predicate. The cost is that the flow always
submits, including against an empty window: a few seconds of EMR Serverless, versus guessing
"probably empty" and advancing the watermark past real data.

### D29-4 — the Airflow adapter reads `safety_overlap_minutes` from the plan, never a default

**Decision: no default.** A mapped task whose job has no compiled flow config means the plan
cache is stale; `_flow_config` raises rather than substituting a value. Defaulting an overlap
silently widens or narrows every window the job reads.

### D29-5 — STREAM_BATCH is wired to Airflow; AUTO_CORRECT and FULFILL are not

**Decision: wired, and the asymmetry is deliberate.** Phase 10's scope is STREAM_BATCH. The
other two flows are implemented and unit-tested; only their adapters are outstanding, and
they remain raising `NotImplementedError` rather than being wired without a phase to test
them under.

---

## Session 22 — 2026-08-19 — Phase 3, reporting metadata foundation

### D22-1 — ADR-042's partitioning rule corrected under implementation evidence

**Decision: CORRECTED.** ADR-042 stated that `partition_spec` may not contain a
primary-key column. The compiler rejected the pilot job on its first execution, and the
rejection was correct under the rule as written — but the rule itself was wrong:
`mart_account_balance_daily` has grain `(account_sk, business_date)` and is
`PARTITIONED BY (business_date)` in the repository's own deployed DDL
(`spark/common/ddl/kimball.sql:182`), as does every other daily mart here.

The rule was expressed over the wrong attribute. `CLAUDE.md` §6 protects against a
partition whose cardinality approaches the row count; membership in a composite PK is not
that, being an identifier is. The corrected rule allows `business_date_column` even inside
the PK, rejects any other PK column, and rejects any column marked high-cardinality.

ADR-042 retains the original wording beside the correction rather than silently replacing
it — a rule stated over the wrong attribute is instructive.

This is the only approved design decision reopened in Phase 3, and it was reopened by
implementation evidence rather than by preference.

### D22-2 — `airflow_dag_id` added to the fields job_master may not carry

**Decision: EXTENDED.** Repo 1 keeps `airflow_dag_id` on `job_resource_config`, i.e. on
the config plane (`data_lake_init.sql:106`). It identifies the DAG that RAN a job, which
is a property of the run: the same job can be executed by a different DAG after a
refactor, and history must record which one actually did it. Found by
`test_forbidden_runtime_keys_cover_the_moved_fields`, which cross-checks the legacy
mapping against the compile-time guard.

### D22-3 — `application` and `updated_at` are dual-plane, not moved

**Decision: RECORDED.** Both exist on the config plane and the runtime plane with
genuinely different values: `JobMaster.application` is the authored entrypoint template
and `ExecutionRecord.application` is what the run actually submitted; `JobMaster.updated_at`
is when the config row was compiled and `ExecutionRecord.updated_at` is when the execution
row last changed. The legacy mapping records them as KEEP with two targets rather than
MOVE, because neither value is derivable from the other.

### D22-4 — no new Python dependency was introduced

**Decision: CONFIRMED.** `networkx` (the graph resolver), `PyYAML`, `jsonschema` and
`boto3` were all already present; `networkx` is a direct dependency of `dbt-core`. `moto`
is not installed and is not required — the DynamoDB adapter is tested against a
hand-written fake. Phase 3 therefore needed no install step and no network access.
See `docs/VERSIONS.md`.

### OPEN — carried into Phase 4

| # | Item | Why it blocks |
|---|---|---|
| O22-1 | ADR-036 operator sign-off (DynamoDB adds a service class) | blocks `terraform apply`, not code |
| O22-2 | ~~`enable_kafka_platform` discrepancy: tfvars `true`, applied tier ran `false`~~ **CLOSED 2026-08-20 (Session 31), the observation was stale.** The flag was honoured: the full platform including MSK was applied 2026-08-16T09:21Z and destroyed 2026-08-16T10:58Z (CloudTrail; the state object fell from 587 KB to 16 KB). This row described the 2026-08-15 cheap tier. The *real* issue it half-saw survives as **OPEN-32**: EMR, `lake_iam` and `airflow_k3s` are `count`-gated on `enable_kafka_platform` and `lake_iam` takes `msk_cluster_arn` as a required input, so no reporting job can run without an MSK cluster it never reads |
| O22-3 | `mart.dim_date` has `is_weekend` but no `is_working_day` | EOD and FULFILL date selection needs a working-day calendar. **Not solved in Phase 3, as instructed** |
| O22-4 | REALTIME layer does not exist | `layers.yaml` substitutes `full_cdc` with a bounded predicate and flags every read as `SUBSTITUTED` |

---

## Session 00 — 2026-08-07 (reconstructed)

Session 00 produced three documents but never created this log, so its decisions are
reconstructed here from them.

| ID | Decision | Outcome | Evidence |
|---|---|---|---|
| S00-1 | Which repository is the authoritative Kafka platform? | **Repo A** (`kafka-kraft-aws/kafka-aws-production-lab`) — authoritative by code and deployment history, but **not currently deployed** | `docs/SOURCE_REPOSITORY_DECISION.md` §1 |
| S00-2 | Is Repo B usable? | **Rejected.** Unrelated tutorial code, never applied to this account, violates four security invariants (`0.0.0.0/0` on nine ports, PLAINTEXT Kafka, SSH keypair, no version pinning) | ibid. §5 |
| S00-3 | What is the relationship between the repos? | Unrelated codebases solving the same problem differently — not duplicates, not parent/child, not environments | ibid. §2 |
| S00-4 | Are repo A's own status documents trustworthy? | **No.** `IMPLEMENTATION_REPORT.md` and `VALIDATION_REPORT.md` claim no apply ever ran; state files and CloudTrail prove an apply followed by a destroy. State and AWS APIs are ground truth | ibid. §4 |

---

## Session 01 — 2026-08-12

### Closed — with a full ADR

| ID | Question | Decision | ADR |
|---|---|---|---|
| S01-1 | Repository topology for consuming the Kafka platform | Absorb repo A as `terraform/modules/kafka_platform/` | [ADR-001](docs/adr/ADR-001-kafka-platform-absorption.md) |
| S01-2 | Terraform state backend (Gap 1) | S3 + CMK + `use_lockfile`, bucket managed out of band | [ADR-021](docs/adr/ADR-021-terraform-state-backend.md) |
| S01-3 | Private-subnet egress (Gap 8) | Per-workload: IGW for public workloads, S3 gateway + Glue interface for EMR Serverless. **No NAT** | [ADR-022](docs/adr/ADR-022-private-egress.md) |
| S01-4 | Project naming (D14) | `Project = kafka-dev-lab`, `Environment = dev` | [ADR-023](docs/adr/ADR-023-project-naming-and-tags.md) |
| S01-5 | Cutoff timezone (D8) | **UTC** everywhere | [ADR-024](docs/adr/ADR-024-cutoff-timezone-utc.md) |
| S01-6 | Implementation code location (D13) | New sibling repo `aws-cdc-lakehouse/`; guide package is read-only input | [ADR-025](docs/adr/ADR-025-implementation-repository-location.md) |
| S01-7 | Domain definition (D9) | Explicit registry `governance/catalog/domains.yml` | [ADR-028](docs/adr/ADR-028-domain-taxonomy.md) |
| S01-8 | Engine mutual exclusion (D17) | Plan-time Terraform `precondition` | [ADR-026](docs/adr/ADR-026-query-engine-flags.md) |
| S01-9 | Lifecycle model | Metered windows; **destroy, not stop** | [ADR-027](docs/adr/ADR-027-ephemeral-lifecycle.md) |
| S01-10 | Spark CPU architecture | **ARM64** — 20 % cheaper on vCPU and memory | [ADR-008](docs/adr/ADR-008-spark-runtime.md) |

### Closed — smaller decisions, recorded here rather than as ADRs

| ID | Decision | Rationale |
|---|---|---|
| S01-11 | **Separate CMKs for platform and lake** | Key policies, rotation and deletion lifecycles differ; a shared key makes least-privilege key policies impossible. $1/month for separable blast radius (`docs/TARGET_ARCHITECTURE.md` §5.1) |
| S01-12 | Rename `bootstrap_brokers_sasl_iam` → `msk_bootstrap_brokers_sasl_iam` | Every other contract key carries a subsystem prefix. Free now, breaking later |
| S01-13 | `broker_ebs_gib = 20`, autoscaling **off** | 100 GiB provisions for data deleted 24 h later. MSK storage is a one-way ratchet — $28.80/month saved, unrecoverable if missed (`docs/COST.md` §1.1, risk R12) |
| S01-14 | EMR Serverless logs to **S3, not CloudWatch** | Removes the `logs` interface endpoint from the required set, saving $0.0130/hr and one failure mode |
| S01-15 | `auto_destroy_after` carries a real timestamp | Repo A's `"manual"` satisfies the tag requirement while encoding no deadline, making it useless to any cleanup sweep |
| S01-16 | `before`/`after` stored as JSON `string`, not `struct` | A struct makes every source DDL change an Iceberg migration on the streaming table. JSON keeps L1 schema-stable (`docs/DATA_CONTRACTS.md` §5) |
| S01-17 | `event_order` is a **struct**, not a scalar | Ordering needs five components; flattening them into one sortable string is how subtle ordering bugs are introduced (`docs/DATA_CONTRACTS.md` §4) |
| S01-18 | Half-open cutoff `[start, end)`; use `<`, not `<=` | `CLAUDE.md` §5.6's `<=` variant would place a boundary event in two windows. Recorded as a defect in the guide's prose, not a decision to revisit |
| S01-19 | `remove_orphan_files` retention floor **72 h** | job timeout (120 min) × retries (3) + margin. Shorter deletes files an in-flight commit is about to reference (risk R14) |
| S01-20 | Connect in **distributed** mode with one worker | Standalone keeps offsets in a local file that dies with the ephemeral instance; distributed keeps them in Kafka, which is what makes the ephemeral model safe |

### Closed — Session 01 addendum (same date)

Prompted by a re-read of `prompts/00_PROMPT.md`, which requires three deliverables that
neither Session 00 nor Session 01 had produced.

| ID | Decision | Rationale |
|---|---|---|
| S01-21 | **Session 02 also builds the Kafka platform**, so `MASTER_PLAN.md`'s dependency rows for Sessions 03, 04, 13 and 17 are corrected | Absorption (ADR-001) makes Session 02 the sole gateway to every later session. Under the uncorrected reading, Session 03 has no VPC to place an instance in and Session 04 has no cluster to connect to (`docs/SESSION_DEPENDENCY_GRAPH.md` §2) |
| S01-22 | **Budget guardrails move from Session 17 to Session 02** | Session 02 is the first session that can spend. A FinOps session arriving 15 sessions after the first spend is a post-mortem, not a control |
| S01-23 | **Sessions are batched into 7 metered windows, not 16** | One window per session totals ~$85.03 and exceeds the budget. Six sessions (08, 10, 11, 13, 14, 18) need no Kafka at all once L1/L2 data is in S3 — a direct consequence of the single write path. Batching brings the core release to ~$48.52 (`docs/SESSION_DEPENDENCY_GRAPH.md` §5) |
| S01-24 | **Five approval gates, none self-approvable by an agent** | `docs/APPROVAL_GATES.md`. An agent may prepare and present a gate package; the transition from `planned` to `deployed` is a human action (`CLAUDE.md` §9.7) |
| S01-25 | **Gate 0.7 blocks Session 02's first apply** until `budget_email` is set | The budget currently notifies nobody, so the sole automated control against a forgotten $0.8143/hr cluster is inert. Making this a hard gate rather than an open issue is the difference between a control and a note |
| S01-26 | The **read-only integration contract is retained as a documented fallback**, not deleted | Absorption turned it into module outputs, but the read-only form is the shape Session 02 must expose either way, and it is what a reversal of ADR-001 would need (`docs/SESSION_DEPENDENCY_GRAPH.md` §7) |
| S01-27 | Implementation stays in the **sibling repo** despite a later brief naming the guide package | ADR-025 stands. Reversing it would re-accept defect D13, invalidate `MANIFEST.json`/`SHA256SUMS`, and split `PROJECT_STATE.md` across two locations. Confirmed with the operator before proceeding |

### Open

| ID | Question | Why it matters | Owner | Needed by |
|---|---|---|---|---|
| **OPEN-03** | Is `kafka.t3.small` available for new MSK clusters? | **$432/month** at 24/7 — larger than the entire budget. Repo A asserts unavailable; the price list still carries it at $0.0578/hr. No read-only API settles it and `preflight.sh` does not test it. A favourable answer takes the envelope from ~8 to **~12.6** windows/month (corrected from ~26 — see S02A-6) | platform | **Gate 1 package prepared; awaiting approval** |
| OPEN-01 | Which bucket holds Terraform state, and who creates it? | ADR-021 requires a bucket the lab cannot destroy. Must exist before the first apply | platform | **Gate 1 package prepared; awaiting approval** |
| OPEN-02 | Accept RPO = 24 h, or pay for longer Kafka retention? | Follows from `log.retention.hours=24`. Raising it raises broker storage, which cannot be reduced after create — so it is decided before the first apply or not at all | spark | Session 05 |
| OPEN-04 | Is ADR-022's public-subnet placement acceptable to security review? | Four workloads gain public IPs behind zero-inbound SGs. Saves $0.1040/hr versus A-min. Repo A's audited toolbox pattern, but broader | sre | Session 02 Stage B |
| OPEN-06 | Install `tflint` (and `shellcheck`) | `reference/ACCEPTANCE_CRITERIA.md:6` requires lint and security scan to pass. **`checkov` 3.3.10 installed 2026-08-12**; `tflint` and `shellcheck` still absent | platform | Session 02 Stage B |
| OPEN-07 | Redshift Serverless minimum base capacity in `ap-southeast-1` | Every number in ADR-013 assumes 8 RPU. Regional minimums differ | serving | Session 13B |
| OPEN-08 | Is `KMS` needed as an interface endpoint? | SSE-KMS on S3 is executed by S3 on the caller's behalf, so the client should not call KMS. If something does, add $0.0130/hr | spark | Session 06 |
| **OPEN-09** | **Is the real monthly budget $80 or $30?** | `docs/COST.md` sizes the entire envelope against **$80**. The account's live `My Monthly Cost Budget` is **$30**, notifying at 85 % actual. At $30 the affordable window count is **~2.6**, not ~7.9 — a different project. Repo A's `monthly_budget_usd = 80` is a Terraform variable, not the account's configured guardrail | finops | **Before Stage B applies anything hourly** |

**OPEN-05 is closed** — see S02A-2 below.

### Accepted documentation debt

Defects in the **read-only guide package**. Recorded so later sessions recognise them
rather than re-deriving them, and do not "fix" a file they must not edit.

| ID | Defect | Handling |
|---|---|---|
| D6 | `reference/TERRAFORM_MODULE_MAP.md` lists 14 modules; `reference/REPO_TREE_TARGET.md` shows 5 | Reconcile in Session 02; the module map is closer to correct |
| D10 | `prompts/00_PROMPT.md:65` and `prompts/01_PROMPT.md:65` require `READY_FOR_APPLY_APPROVAL` from sessions that forbid apply | Use the architect terminator. Noted in `SESSION_HANDOFF.md` |
| D11 | `sessions/00_audit_existing_repo.md` scope is narrower than `prompts/00_PROMPT.md` | Session 00 followed the prompt; harmless |
| D12 | `sessions/01_cost_and_target_architecture.md:42-44` has two blank lines where the no-apply constraint belongs | Constraint applied anyway |
| D15 | `VALIDATION_REPORT.md` claims 43 markdown files; the package has 72+ | Cosmetic |
| D16 | `MANIFEST.json` and `README.md` say v4; the directory and seven reference docs say v2 | Cosmetic |

### Corrections to the guide package's own decisions

| Guide statement | Correction | Why |
|---|---|---|
| `DECISIONS.md` ADR-001: "Reuse existing MSK Provisioned KRaft" | There is nothing to reuse; the platform is absorbed and rebuilt | ADR-001 |
| `CLAUDE.md:12`: project is `kafka-dev-lab` | It was `kafka-prod-lab`; now renamed **to match** `CLAUDE.md` | ADR-023 |
| `reference/ICEBERG_LAYER_SPEC.md:9`: partition on `days(event_ts)` + `source_system` | `event_ts` was never defined; now `days(source_commit_ts)` + `source_system` | D1, `docs/DATA_CONTRACTS.md` §5.1 |
| `reference/ICEBERG_LAYER_SPEC.md:79-84`: L3 SQL ordering | Referenced three columns no layer defined; rewritten against `event_order` | D2/D4 |
| `reference/CDC_EVENT_CONTRACT.md:42-46`: nested `source_commit_position` | Flattened to three scalars so SQL Server's event serial number has a column | D3 |
| `ARCHITECTURE.md:71-82`: S3 layout | Added `warehouse/ops/`; moved `checkpoints/` to a top-level sibling of `warehouse/` | D7, `CLAUDE.md` §5.9 |

---

## Session 02 Stage A — 2026-08-12

Prerequisites only (`docs/TARGET_ARCHITECTURE.md` §12 items 1–3). No Terraform module
was written and **no AWS write was executed**. Every AWS call this session made was
read-only; the two mutating scripts exist but were only dry-run.

### Closed

| ID | Decision | Rationale |
|---|---|---|
| S02A-1 | **`checkov` 3.3.10 installed; `tflint` deliberately not** | Pinning `tflint` means choosing and checksum-verifying a specific release, not accepting whatever `latest` resolves to (`CLAUDE.md` §3.9). `scripts/install-tools.sh` prints the resolve → pin → verify → install sequence. OPEN-06 stays open, now also covering `shellcheck`. The `checkov` pin records the version that was actually installed rather than one chosen in advance |
| S02A-2 | **`budget_email = "owner@example.com"`** — closes OPEN-05 | Repo A's `budget.tf` uses `count = var.budget_email == "" ? 0 : 1`, so an empty value creates **no budget at all** — the failure is silent. Recorded in `terraform/envs/dev/terraform.tfvars.example`; Stage B adds a variable `validation` rejecting the empty string so it cannot recur |
| S02A-3 | **Both AWS-mutating scripts are dry-run by default** and require `--execute` plus a typed confirmation phrase, refusing to run non-interactively | `docs/APPROVAL_GATES.md`: no gate may be self-approved by an agent. A default that mutates is a rule enforced only by whoever remembers it. `scripts/validate-docs.py` check 14 asserts the guard is wired up, not merely documented |
| S02A-4 | **The OPEN-03 probe runs in two stages, cheap first** | Stage 1 calls `CreateCluster` with `kafka.t3.small` **and deliberately invalid subnets**: the request cannot succeed, so no resource can exist and nothing can bill, yet the *choice of validation error* answers the question for $0.00. Only if the error names the subnets rather than the instance type is the billable stage 2 needed. Repo A's recorded error was `Unsupported InstanceType`, so stage 1 is likely sufficient |
| S02A-5 | **State backend bootstrap is a shell script, not Terraform** — closes the "how" of OPEN-01 | ADR-021's rule made executable. A separate state CMK (+$1.00/month) over shared or SSE-S3, per ADR-021. Idempotent, so a re-run after partial failure is safe |
| S02A-6 | **`docs/COST.md` §3.3 arithmetic corrected: `t3.small` gives ~12.6 windows, not 25.6** | The table's own `−$3.55/window` figure contradicted its `25.6`. Re-derived from §3.1: MSK brokers are `3 × 0.2550 × 6 = 4.590` of the $9.45 window and `3 × 0.0578 × 6 = 1.040` on `t3.small`, so the window falls to $5.90 and `74.39 / 5.90 ≈ 12.6`. The old value was roughly double. OPEN-03 is still the highest-value question — +60 % lab time, not +200 %. The separate **$432/month** figure is unaffected: it is the 24/7 broker delta, `(0.7650 − 0.1734) × 730 = 431.87`. Propagated to `RISK_REGISTER.md`, `TARGET_ARCHITECTURE.md`, `SESSION_DEPENDENCY_GRAPH.md` |
| S02A-7 | **Always-on floor raised from $4.60 to $5.61/month** | Session 01 costed two CMKs — platform and lake — and never carried ADR-021's state CMK into `docs/COST.md` §3.2. Adding it plus ~$0.01 of versioned state gives $5.61. The envelope conclusion (~8 windows) is unchanged. ADR-027 amended in place rather than rewritten |
| S02A-8 | **The state backend is exempt from ADR-027's ephemeral lifecycle**, tagged `AutoDestroyAfter=never` | It outlives every window by design. `verify-destroy` must assert it still *exists*; a destroy plan that includes it is a hard stop (Gate 5) |

### Corrections to Session 01

| Session 01 claim | Reality | Evidence |
|---|---|---|
| **Gate 0.7 FAILS — "the AWS Budget notifies nobody"** (`docs/APPROVAL_GATES.md` §0, `DECISION_LOG.md` S01-25, `PROJECT_STATE.md` OPEN-05) | **False. Gate 0.7 passes.** The account has two **account-wide** budgets: `My Monthly Cost Budget` ($30, notifying at ACTUAL >85 %, >100 % and FORECASTED >100 %) subscribed to **owner@example.com**, and `My Zero-Spend Budget` ($1, ACTUAL > $0.01). Being unfiltered they cover this project automatically, and cannot miss an untagged resource the way a `Project`-tag-filtered budget can | `artifacts/validation/session-02/aws/gate-0.7-budgets.txt` |

Session 01 evaluated 0.7 by reading repo A's `budget_email = ""` — a Terraform variable —
instead of calling `aws budgets describe-budgets`. The generalisable lesson, now written
into `docs/APPROVAL_GATES.md` §0: **0.7 is a check against the account, not against the
code.** This also dissolves what looked like a circular dependency, since the budget is
Terraform, Terraform needs a state backend, and the backend could not wait for a budget.

S01-25 ("Gate 0.7 blocks Session 02's first apply until `budget_email` is set") is
**superseded**. Setting `budget_email` remains correct — a project-scoped, tag-filtered
budget gives cost *attribution* the account-wide one cannot — but it was never the only
thing standing between this project and a silent overrun.

### Deferred to Stage B

`docs/TARGET_ARCHITECTURE.md` §12 items 4–15: repo A absorption with `PROVENANCE.md`; the
`data_lake`, `glue_catalog`, `lake_iam`, `athena`, `vpc_endpoints` and `budget_guardrails`
modules; root feature flags and the ADR-026 precondition; the saved plan and destroy plan;
and defect D6 (module-map reconciliation).

---

## Session 00 — Architect review, 2026-08-13

Verification-grade re-review of Session 00's three deliverables. All 15 load-bearing
claims were re-derived independently from the filesystem and read-only AWS calls.
**15 of 15 confirmed; no Session 00 conclusion amended.** Full record:
`docs/reviews/SESSION-00-ARCHITECT-REVIEW.md`. Verdict
`ARCHITECTURE_APPROVED_FOR_IMPLEMENTATION`.

### Closed

| ID | Question | Decision | Evidence |
|---|---|---|---|
| **S00R-1** | **OPEN-09 — is the budget $80 or $30?** | **$30/month is authoritative.** Confirmed by the operator on 2026-08-13. The $80 was never decided — it is `monthly_budget_usd = 80` inherited verbatim from repo A's `terraform/terraform.tfvars:26`, a repo tagged `kafka-prod-lab`, carried into planning unexamined | `aws budgets describe-budgets` → $30.00; `terraform.tfvars:26` |

**Consequence, for Session 02 before any Terraform.** At $30 the envelope is ~2.6 windows
(~16 h) per month, not ~7.9 (~47 h). `docs/SESSION_DEPENDENCY_GRAPH.md` §5's conclusion
that the core release (≈ $48.52) fits one month **fails at $30** and must be re-planned
across months or re-scoped. `docs/COST.md` window math is re-derived in Session 02, not
here — a review pass records the decision, it does not rewrite the cost model.

### Open — raised by this review

| ID | Issue | Priority | Next action |
|---|---|---|---|
| **OPEN-10** | **The operator identity is an IAM user with static access keys** (`arn:aws:iam::111122223333:user/my-aws-profile`), not a role. Session 00 audited both repositories' security thoroughly but never audited the identity that executes every session. It predates the project, so `CLAUDE.md` §3.2 ("no IAM user/access key") is not violated by creation — but the most privileged credential in the system sits outside the security model the same section builds | **P1** | Raise **ADR-029** in Session 02: MFA, key age/rotation, possible move to an assumed role, and whether §3.2 should name the bootstrap identity explicitly rather than excepting it silently |
| **OPEN-11** | `docs/GAP_ANALYSIS.md` contradicts itself on the output score — Gap 2 says "4 of ~12", §4 says "5 of 16". §4 is correct: `outputs.tf` declares 10 outputs of which exactly 5 are contract keys, the other 5 being operator conveniences | **P3** | Correct Gap 2 to "5 of 16" |

### Recorded, no new issue

- **OPEN-03 framing.** Session 02 *did* weigh repo A's prior `Unsupported InstanceType`
  evidence when designing the probe (S02A-4). The probe is sound. But `PROJECT_STATE.md`
  and `SESSION_HANDOFF.md` both lead with the "+60 % lab time" upside, which reads as the
  expected case when the project's own evidence says the base case is **no change**.
  Reframe as a low-probability branch; the probe needs no change.
- **Absorption hazard (ADR-001).** Repo A's `terraform.tfstate.backup` holds **43 live
  resource records** — ARNs, CMK IDs, broker endpoints — and is sensitive under
  `CLAUDE.md` §3.8. The Stage B absorption must copy **`.tf` sources only**, with
  `.gitignore` excluding `*.tfstate*` *before* the copy. A naive `cp -r` imports both
  state files into Git.
- **Git history is permanently unavailable** for all three directories. Session 00
  declared this (`SOURCE_REPOSITORY_DECISION.md` §3.5) and based no conclusion on it.
  Recorded so no later session re-opens the question expecting a different answer.

---

## Session 00 — closeout pass, 2026-08-13

The architect review above found Session 00's conclusions sound but its **evidence
unpreserved** — `artifacts/validation/` held session-01 and session-02 only. This pass
captured that evidence, and in doing so settled an open question the project had built a
whole approval gate around.

### Closed

| ID | Question | Decision | Evidence |
|---|---|---|---|
| **S00R-2** | **OPEN-03 — is `kafka.t3.small` available in `ap-southeast-1`?** | **No. CLOSED, negative.** The MSK API rejected it in this account on 2026-07-26T13:01:14Z: `BadRequestException`, `invalidParameter: instanceType`, and the response enumerated **every** valid type — `kafka.t3.small` is absent. The smallest offered is `kafka.m5.large` / `kafka.m7g.large` | `artifacts/validation/session-00/aws/open-03-settled.txt` |

**How it was settled matters.** Session 02 Stage A built a careful two-stage probe
(S02A-4) to answer this for $0.00, and reasoned correctly from repo A's code comment that
stage 1 would likely suffice. But the answer was already in the account: CloudTrail
retains the **actual rejection**, with the full valid-values list, from the day repo A was
first applied. **A captured API response outranks both a code comment and a probe** — and
it costs nothing to look. The lesson generalises, and belongs beside the Gate 0.7
correction: *before building an instrument to ask AWS a question, check whether AWS has
already answered it and written the answer down.*

Consequences:

- `scripts/probe-msk-instance-type.sh` and `docs/gates/GATE-1-msk-instance-type-probe.md`
  are **retired**. Gate 1 now has **one** package awaiting approval, not two.
- **`kafka.m7g.large` was forced, not chosen**, and is already the cheapest option
  available — Graviton undercuts `m5`, and no burstable class is offered at all.
- The `~12.6 windows` / "+60 % lab time" branch in `docs/COST.md` §3.3 and
  `SESSION_HANDOFF.md` is **dead**. Remove it rather than leave it as a live option.
- `express.m7g.large` exists in the valid list. MSK Express is a **different pricing
  model** (higher hourly, bundled throughput and storage) and has never been costed here.
  Not pursued in this pass; noted as OPEN-12 so it is a deliberate omission, not an
  oversight.

### Open — raised by this pass

| ID | Issue | Priority | Next action |
|---|---|---|---|
| **OPEN-12** | `express.m7g.large` (MSK Express) is offered in `ap-southeast-1` and has never been costed. It bundles throughput and storage into the hourly rate, so it is not comparable to `kafka.m7g.large` line-for-line — it might be cheaper or dearer for a 6-hour window | **P2** | Cost it against `kafka.m7g.large` during the Session 02 `docs/COST.md` re-derivation at $30. Do **not** assume it is cheaper |

### Deliverables completed

`artifacts/validation/session-00/` now exists with `aws/` (inventory, cloudtrail,
open-03-settled), `repo-a/audit.txt`, `repo-b/rejection-evidence.txt` and
`static/validation.txt`. Repo A's evidence records **state metadata only** — lineage,
serial and counts — because `terraform.tfstate.backup` holds 43 live resource records and
is sensitive under `CLAUDE.md` §3.8.

### Corrections applied

| File | Change | Driver |
|---|---|---|
| `docs/GAP_ANALYSIS.md` Gap 2 | "4 of ~12" → **"5 of 16"**, matching §4 | OPEN-11 / F4 |
| `docs/VERSIONS.md` | MSK broker type row: OPEN-03 **closed negative** with the CloudTrail citation | S00R-2 |
| `PROJECT_STATE.md` | OPEN-03 closed; probe retired; AWS reality re-verified 2026-08-13 | S00R-2 |

### Not done, deliberately

`docs/COST.md` and `docs/SESSION_DEPENDENCY_GRAPH.md` §5 are **Session 01 artefacts and
not Session 00 deliverables** (`CLAUDE.md` §9.3). They still carry the superseded $80
envelope and the now-dead `t3.small` branch. Both re-derivations are Session 02 entry
work, tracked under OPEN-09 and S00R-2. A closeout pass records what it found; it does not
quietly rewrite another session's cost model.

---

## Session 01 — cost re-derivation at the real budget, 2026-08-13

Session 01's architecture (19 ADRs, target design, risk register) was reviewed and stands
unchanged. What did not stand was its **cost envelope**: every figure was sized against
an $80 budget that Session 00 proved was never decided. This pass re-derived it against
the real **$30**, and optimised for the lowest practical lab cost without degrading the
design.

### Closed — with a full ADR

| ID | Question | Decision | ADR |
|---|---|---|---|
| S01R-1 | What is the budget of record, and does the core release still fit one month? | **$30/month. It does not fit one month — the core release spans two.** | [ADR-030](docs/adr/ADR-030-budget-of-record-and-release-schedule.md) |
| S01R-2 | Secrets Manager or SSM Parameter Store? | **SSM SecureString.** Saves $1.60/month with no security downgrade | [ADR-031](docs/adr/ADR-031-secret-store.md) |

### Closed — smaller decisions

| ID | Decision | Rationale |
|---|---|---|
| S01R-3 | **State bucket → SSE-S3; ADR-021 amended in place** | A dedicated state CMK costs $1.00/month = **3.3 % of the entire budget** for a bucket already restricted by IAM and a bucket policy, whose contents have one audience. Platform and lake CMKs stay separate — S01-11's reasoning holds for *those* two because their audiences genuinely differ |
| S01R-4 | **CloudWatch alarms become ephemeral** | Six alarms watching an MSK cluster that exists ~18 hours a month are a subscription, not observability. Moved inside the platform module's `enable_*` flag. −$0.60/month |
| S01R-5 | CloudWatch Logs 5 GiB/7 d → 1 GiB/3 d | −$0.12/month. Shortens the forensic window; acceptable for supervised windows, explicitly not a production setting |
| S01R-6 | **W5 and W6 merged; W0 deleted** | Session 17 must *create* infrastructure to prove it can be destroyed — the same stack 12/15/19 need, so two windows paid twice for one stack (−$3.08). W0 existed only to probe `t3.small`, which OPEN-03 closed without it |
| S01R-7 | Kafka-bearing windows 6 h → 4–5 h | Cost is almost purely hourly, so this is a direct saving. Requires scripted rather than exploratory sessions. W2 keeps 5 h because it does the most novel work |
| S01R-8 | **`scripts/derive-cost-envelope.py` created** | `CLAUDE.md` §4 forbids hard-coded prices. The envelope is now executable arithmetic over `docs/PRICE_REFERENCE.md`, so a reviewer can change one rate and watch every total move. No figure in `docs/COST.md` is hand-computed |

### Closed — OPEN-12

**`express.m7g.large` rejected.** At **$0.5100/hr it is exactly 2×** `kafka.m7g.large`.
It bundles broker storage, worth $0.0033/hr at 20 GiB — so it charges $0.2550 to save
$0.0033, a **77× loss**. MSK Express targets high-throughput production, not a bounded lab.
`docs/RISK_REGISTER.md` **R11 closed** at the same time.

### The architecture decision that matters

Three packages were derived. Package A- (3 brokers, `t3.large` source lab) fits one month
at **$28.25**, leaving $1.75. Package B (**2 brokers / 2 AZ**) fits at **$23.34**, leaving
$6.66 — but costs replication factor 3.

**Neither was taken as stated.** A single W2 re-run costs $6.00, so A-'s $1.75 headroom
breaks on the first retry, and B degrades the centrepiece of a portfolio project to save
$4.91/month. The chosen answer is A- **spread across two months**: month 1 isolates the
failure-prone Kafka chain with **$11.98 of headroom — two full W2 re-runs**.

**When a budget tightens, spend the schedule before you spend the architecture.** A second
month costs $2.28 of carrying floor. Option B is retained *with its price attached* so
that if month 1 overruns, the fallback is a decision with a number rather than a panic.

### Not done, deliberately

**ADR-029 (operator identity, OPEN-10) was not written here.** The Session 00 review
assigned it to Session 02, and it is a security decision about credentials rather than a
cost or architecture decision in Session 01's scope. It remains open and assigned.

---

## Session 02 Stage B — Terraform foundation, 2026-08-13

### The finding that matters most

| ID | Issue | Resolution |
|---|---|---|
| **S02B-1** | **The first saved plan of this session targeted the WRONG AWS ACCOUNT.** `providers.tf` had no `profile`, so Terraform used the default credential chain; `[default]` on this workstation is account **444455556666**, not the lab's `111122223333` | Plan discarded. `profile = var.aws_profile` pinned in the provider, plus a **plan-time precondition** comparing the resolved account against `expected_account_id`. Proven to fire (negative test T7) |

**Why this was not caught earlier.** `CLAUDE.md` §2 requires printing
`aws sts get-caller-identity` before every plan, and that was done — it printed
**111122223333**, the correct account, every time. The guard was passing while the plan
was wrong, because **a shell-level identity check does not constrain Terraform's
provider**. They resolve credentials independently.

The credentials file holds `vannk-prod`, `vannk_prod_msm`, `tramntb_prod` and `prod`. An
unpinned provider is one misconfigured default away from planning against production.
`CLAUDE.md` §2's rule was sound; its *enforcement* lived in the wrong layer.

### Closed

| ID | Decision | Rationale |
|---|---|---|
| S02B-2 | **`budget.tf` moved out of `kafka_platform` into `modules/budget_guardrails`, applied FIRST** | Repo A created the budget inside the platform module, so the cost control was born and destroyed with the thing it guards — no alarm during exactly the window when a forgotten resource goes unnoticed |
| S02B-3 | **The absorbed module needs `templates/` and `app/`, not just `.tf`** | `terraform validate` failed on the first absorption: `toolbox.tf` calls `templatefile()`/`filebase64()` on 13 non-`.tf` files. Those resolve at plan time, so a module missing them cannot plan. `../app/` was also rewritten to module-local `app/` — a module that reaches outside its own directory is not self-contained |
| S02B-4 | **`checkpoints/` placement is a variable validation, not a comment** | Iceberg `remove_orphan_files` deletes unreferenced files under a table location. A checkpoint under `warehouse/` is exactly that, so maintenance would silently destroy streaming state (`CLAUDE.md` §5.9). A validation makes the mistake unrepeatable |
| S02B-5 | **Two checkov findings fixed rather than accepted** | CKV2_AWS_64 — the lake CMK had no explicit key policy, which made S01-11's "separate keys have separate policies" argument true only in principle. CKV2_AWS_12 — the VPC default security group kept its allow-all-from-self rule; nothing uses it, but anything launched without an explicit SG lands in it. Both free |
| S02B-6 | **The other 17 checkov findings are documented, not suppressed** | `docs/SECURITY_SCAN_BASELINE.md`. No `skip_check`, no baseline file — every finding still fires on every run. Six of them land on one resource: repo A's KMS key policy root statement, which is the AWS-documented required pattern (omit it and the key becomes permanently unmanageable) |
| S02B-7 | **Defect D6 resolved: 7 built, 7 deferred, 1 dropped** | `TERRAFORM_MODULE_MAP.md` said 14, `REPO_TREE_TARGET.md` said 5, and **both omitted `kafka_platform` and `vpc_endpoints`** — the two modules Session 00's own gap analysis proved necessary. `airflow_eks` dropped: ADR-011 chose k3s |
| S02B-8 | **Six flags declared but explicitly marked NOT YET WIRED** | `enable_toolbox`, `enable_observability_ext`, `enable_governance`, `enable_marquez`, `enable_lake_formation`, `enable_eks`. They appear in the §8 flag matrix and in `terraform.tfvars.example`, so they must be declared or every plan warns. A flag that looks live but is inert is worse than no flag, so each says so in its description |

### Guard mechanism: `precondition` vs `check`

Seven invariants use `precondition` (**fails the plan**); three cost settings use `check`
(**warns and continues**). The split is deliberate: the three warnings are things a human
might legitimately override for one window — `broker_ebs_gib`, storage autoscaling,
budget ceiling — while the seven would be wrong in every circumstance.

All fourteen guards, plus the two dependency-flag preconditions, are **proven by
execution** in `artifacts/validation/session-02/stage-b/negative-tests.txt`. A guard that
has never been observed to fire is a guard nobody has tested.

### Not done, deliberately

- **`scripts/bootstrap-state-backend.sh` is still not fixed.** ADR-030 amended ADR-021 to
  SSE-S3; the script still creates a CMK. It remains its own Gate 1 blocker, documented in
  `docs/gates/GATE-1-state-backend-bootstrap.md`. Fixing it inside Stage B would have
  buried a security-relevant change in a 113-resource diff.
- **The backend block is left commented out**, so this plan is against local state. It
  must be re-planned after the backend exists — recorded as blocker §3.1 in the Gate 2
  package rather than papered over.
- **ADR-029 (operator identity, OPEN-10) still unwritten.** Now more pointed than when
  Session 00 raised it: S02B-1 showed the credential environment is genuinely hazardous.

---

## Session 03 — source lab, 2026-08-13

### Closed — with a full ADR

| ID | Question | Decision | ADR |
|---|---|---|---|
| S03-1 | Is RDS Oracle / SQL Server actually necessary? | **No. Rejected on price, permanently.** RDS for both engines is **$1.1620/hr against $0.1888/hr for containers — 6.2×**. Over the 19 source-lab hours that is $22.08, **62% of the entire monthly budget** | [ADR-032](docs/adr/ADR-032-source-lab-instance-sizing.md) |
| S03-2 | `t3.large` or `t3.xlarge`? (left unproven by ADR-030) | **Neither — `t3a.xlarge`.** AMD, still x86_64, **10.6% cheaper than `t3.xlarge`**. `t3.large` rejected: 7.5 GiB of demand against 8 GiB leaves no page cache, and the failure is an OOM-kill mid-snapshot | ADR-032 |

`CLAUDE.md` §4.3 already forbade RDS. The instruction was to prove it rather than cite it,
and the proof is stronger than the rule: RDS is not technically wrong — RDS Oracle
supports LogMiner CDC and RDS SQL Server supports `msdb.dbo.rds_cdc_enable_db` — it is
simply unaffordable, and the containers run **the same engines with the same
transaction-log semantics**.

### The defect that would have failed at apply

| ID | Issue | Fix |
|---|---|---|
| **S03-3** | **The bootstrap assets are ~35 KB base64 against EC2's 16 KB user-data hard limit.** Inlining them fails at apply with an opaque `InvalidParameterValue` | Staged in `s3://<lake>/bootstrap/source-lab/`, pulled by a 2.9 KB `aws s3 sync` bootstrap. A new `bootstrap_read` IAM policy scoped to `bootstrap/*` — **narrower than `lake_read`**, because the source lab has no business reading warehouse data |

Found by measuring rather than assuming. `etag = filemd5(...)` plus
`user_data_replace_on_change` now means a changed SQL file re-uploads *and* rebuilds the
instance, so it cannot drift from the repo.

### Closed — smaller decisions

| ID | Decision | Rationale |
|---|---|---|
| S03-4 | **Seed data is deterministic** — fixed id ranges, fixed base timestamp, no randomness | Session 05 reconciles source counts against L1/L2. A random seed makes that impossible, which defeats the lab |
| S03-5 | **SQL Server CDC uses `@supports_net_changes = 0`** | Net changes collapse multiple updates within a capture interval into one row, destroying exactly the I/U/D history L2 exists to preserve (`CLAUDE.md` §5.3) |
| S03-6 | **CDC cleanup retention 7 days, not the 3-day default** | Kafka retention is 24 h (Gap 13). CDC retention must be *longer* or a Kafka outage becomes unrecoverable without a full re-snapshot |
| S03-7 | **`02-enable-cdc.sql` refuses to run if SQL Server Agent is stopped** | Risk R15 at its most dangerous: `sp_cdc_enable_table` **succeeds** with Agent stopped, and the connector then reports healthy while capturing nothing |
| S03-8 | **Debezium uses `c##dbzuser` / `dbzuser`, never `SYS` or `sa`** | Least privilege, and it keeps audit logs readable |
| S03-9 | **The source lab SG ships with ZERO ingress rules** | Ports 1521/1433 open only through SG-to-SG rules gated on a CDC runtime SG that does not exist until Session 04. Until then the databases are unreachable from anything |
| S03-10 | **Oracle amounts are `NUMBER(18,2)`, never `FLOAT`** | Debezium maps scaled `NUMBER` to a Connect `Decimal`; a float makes every downstream sum non-reproducible |
| S03-11 | **`app_user.customer_id` is deliberately NOT a foreign key** | It points into Oracle. An FK would hide that the cross-source join is the mart's problem |

### Cost impact on ADR-030

ADR-030's package A- assumed the source lab at `t3.large` ($0.1056/hr). ADR-032 moves it
to `t3a.xlarge` ($0.1888/hr): **+$0.0832/hr, +$1.58 across the 19 source-lab hours.**

Core release ≈ **$28.25 → ≈ $29.83**. Still inside $30, but the headroom is essentially
gone. **This reinforces ADR-030's two-month schedule rather than undermining it** — and it
is exactly the kind of drift the two-month decision was taken to absorb.

### Not done, deliberately

- **No apply.** Session 02 is still unapplied — its state-backend bootstrap needs a
  human-typed confirmation phrase that `lib.sh` refuses to accept from automation.
- **`versions.tf`'s `backend "s3"` block was briefly uncommented and has been re-commented**
  to match reality: the bucket does not exist, so `terraform init` cannot configure it.
  Every plan produced meanwhile is against **local state** and is not valid for apply.
- Session 04 not started.

---

## Session 04 — Kafka Connect + Apicurio Registry, 2026-08-13

### Reuse verified, not asserted

The instruction was to reuse the existing Kafka infrastructure and not duplicate
monitoring. Both are checked against the plan JSON:

| Assertion | Result |
|---|---|
| MSK clusters planned | **1** — the existing one |
| VPCs planned | **1** — the existing one |
| New Prometheus/AMP stacks | **0** |
| CloudWatch alarms | **6** — the inherited broker alarms, no duplicates |
| Ingress rules using a CIDR | **0** — all 5 are SG-to-SG |

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S04-1** | **Prometheus discovers Connect by EC2 tag, not static targets** | The existing Prometheus used Terraform-rendered static targets. Adding Connect that way needs the toolbox to know the CDC runtime's IP, while the CDC runtime already depends on the toolbox's VPC — **a module cycle**. `ec2_sd_configs` filtered on `Component=cdc-runtime` breaks it, extends the existing stack rather than duplicating it, and survives a Connect rebuild without edits. Cost: `ec2:DescribeInstances` on the toolbox role — account-wide because the API takes no resource ARN, read-only |
| **S04-2** | **MSK IAM callback handler on all FOUR client scopes** | Risk R2. Connect does not inherit top-level security settings into its producer, consumer and admin clients. The **admin** scope is the one most often missed, and the symptom is `SaslAuthenticationException` with internal topics never created |
| **S04-3** | **`connect-configs` has exactly 1 partition** | Connect config ordering is global. More than one partition corrupts it, and the corruption is not obvious until a connector restarts with stale config. Asserted in `smoke-test.sh` step 3 |
| **S04-4** | **`kafkasql-journal` is compacted, not time-deleted** | The registry replays this topic to rebuild state. Time-based retention would silently delete schemas that are still referenced by live data |
| **S04-5** | **DLQ retention 7 days against the source topics' 24 h** | A poison record is evidence for an investigation that begins after someone notices, not while it happens |
| **S04-6** | **`ojdbc11` is fetched explicitly** | Debezium does not bundle the Oracle JDBC driver for licensing reasons. Omitting it is a classic first-boot failure: the connector loads, then dies on `ClassNotFoundException: oracle.jdbc.OracleDriver` |
| **S04-7** | **Connect and Apicurio share one host** (ADR-002) | Same lifecycle, identical MSK IAM client configuration, and $0.1056/hr instead of $0.2112 |
| **S04-8** | **The smoke test actively probes the PUBLIC IP** and fails if Connect REST answers | `CLAUDE.md` §3.3 forbids exposing Connect REST. A security group rule is a claim; a probe is evidence |
| **S04-9** | **Negative auth test: an unauthenticated PLAINTEXT client must be REJECTED** | If it succeeds, IAM is not being enforced and every other guarantee in this session is void. Asserting the positive path alone would not catch that |

### Recorded honestly rather than fabricated

**All six artifact SHA256 values are `UNVERIFIED`.** Nothing has been downloaded — that
needs a running instance. `verify-artifacts.sh` **fails closed** on `UNVERIFIED` rather
than proceeding, and `--record` prints the observed values for a one-time paste into
`versions.env`.

A fabricated checksum would be strictly worse than an absent one: it would look verified
while enforcing nothing. This matters most for `aws-msk-iam-auth`, where a version
mismatch is a **silent** auth failure (risk R2), not a startup error.

### The link that opens the source lab

Session 03 shipped the source lab with **zero ingress rules** — its databases were
unreachable from anything, deliberately, because no CDC runtime existed to grant access
to. Session 04 adds the first and only rules that open 1521/1433, SG-to-SG against the
CDC runtime's security group.

### Not done, deliberately

- **No connector JSON deployed.** Debezium connector configuration is Session 05.
- **No apply.** Sessions 02 and 03 are still unapplied; the state backend needs a
  human-typed confirmation phrase that `lib.sh` refuses to accept from automation.
- Session 05 not started.

---

## Session 05 — Debezium + Avro, 2026-08-13

### Terminator: BLOCKED, not complete

**CDC correctness was not demonstrated, and could not be.** Verified live at the start of
this session: **MSK clusters 0, EC2 instances 0, state bucket absent.** Six of the ten
acceptance criteria require a running cluster, running databases and a running Connect
worker. They are recorded as **`NOT_TESTED`**, not as passing.

The instruction was "do not continue until actual CDC correctness is demonstrated". That
is exactly where this session stops.

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S05-1** | **`transforms: ""` — no `ExtractNewRecordState` on either connector** | The single most consequential line in both files. Unwrapping is the standard Debezium tutorial step and it would **silently destroy the ordering contract**: it strips the envelope to the `after` image, deleting `op`, `before`, `ts_ms` and the entire `source` block — including every SCN and LSN that `event_order` (DATA_CONTRACTS §4) is built from. Nothing fails loudly at the time; L2 ordering is simply wrong forever |
| **S05-2** | **`snapshot.mode: initial`, never `schema_only`** | `schema_only` skips existing rows, so the 2 000 seeded Oracle transactions and 3 000 SQL Server events would never reach L1. Reconciliation would then fail by **exactly the seed size**, which reads like a subtle bug rather than a config choice |
| **S05-3** | **`decimal.handling.mode: precise`** | Oracle `NUMBER(18,2)` must arrive as a Connect `Decimal`. `double` makes every downstream sum non-reproducible, and the error stays invisible until a reconciliation fails by cents |
| **S05-4** | **`message.key.columns` stated explicitly per table** | `CLAUDE.md` §5.1 requires the canonical PK as topic key. Debezium defaults to the PK, but stating it means a source PK change cannot silently repartition a topic |
| **S05-5** | **`tombstones.on.delete: true` and `provide.transaction.metadata: true`** | `CLAUDE.md` §5.7 — a delete emits a `d` envelope **and** a null tombstone; both must reach L1 or deletes are ambiguous downstream |
| **S05-6** | **Heartbeats on both connectors at 30 s** | Oracle: redo advances even when captured tables are idle, so without a heartbeat the committed SCN goes stale and a restart re-mines a large redo range — on a bounded lab that can exceed the window. SQL Server: the CDC cleanup job trims change tables on a schedule (S03-6), and a heartbeat keeps the committed LSN moving so cleanup cannot remove unread rows |
| **S05-7** | **Correctness tests read real records, not configuration** | Test 2 extracts actual `(key, partition)` pairs and asserts no key spans two partitions. Test 4 compares topic depth before and after a restart to catch a re-snapshot. Test 6 asserts the registry returns **409/422** on a narrowing change — and **fails if the schema is accepted**, because an accepted incompatible schema means BACKWARD compatibility is not enforced |

### A check that lied, and what it cost to notice

While validating the templates, a grep for `ExtractNewRecordState` reported the transform
as **present** on both connectors. It was matching the word inside the *comment explaining
why the transform is absent*. Re-checked against the parsed `transforms` value instead:
empty on both, as intended.

Worth recording because the same shape of error — a check that matches documentation
rather than configuration — would have passed a genuinely broken connector.

### Recorded honestly

| ID | Issue |
|---|---|
| **OPEN-15** | **All six CDC correctness criteria are `NOT_TESTED`.** They cannot be run until the stack is applied. `docs/CDC_CONTRACT_IMPLEMENTATION.md` §6 lists each one with the exact command. **Session 06 must not start until they pass** — L1 built on unverified ordering would be wrong in a way that is expensive to discover later |

### Not done, deliberately

- **No connector registered.** `register-connectors.sh` refuses to render against an
  unapplied stack, and does so cleanly rather than crashing.
- **No apply.** Sessions 02–05 all remain unapplied.
- Session 06 not started.

---

## Session 06 — L1 STREAM, 2026-08-14

### What actually got demonstrated

**49 tests pass, and they run.** This is the first session where correctness was proven
rather than described, because the transformation logic is pure functions and a local
Iceberg table needs no AWS:

| Suite | Tests | Proves |
|---|---:|---|
| `test_ordering.py` | 17 | SCN/LSN normalisation, comparison precedence |
| `test_envelope.py` | 15 | 30-column contract, D5 fields, idempotency, time semantics |
| `test_quarantine.py` | 10 | error class, stack hash grouping, payload reference |
| `test_l1_local_spark.py` | 7 | **real Iceberg table**: schema, append-only, replay, ORDER BY |

The headline assertion, from `test_ordering.py`:

```python
assert "9" > "10"                                          # lexicographic: WRONG
assert normalize_oracle_scn(9) < normalize_oracle_scn(10)  # normalized:    right
```

That is DATA_CONTRACTS §4.2 turned from prose into an executing test.

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S06-1** | **Oracle pads, SQL Server validates — opposite treatment** | Oracle SCN is numeric, so it needs zero-padding to make string compare equal numeric compare. SQL Server LSN is already fixed-width hex, so it needs *validation* that the width is intact. A single "just pad it" helper would silently corrupt one of them, and the corruption only shows up as wrong business numbers much later |
| **S06-2** | **`normalize_oracle_scn` REFUSES to truncate an over-wide SCN** | Truncating would re-sort existing history. Failing loudly is the only safe option |
| **S06-3** | **`event_id` is a hash of `topic:partition:offset`, never a UUID** | CLAUDE.md §5.3 requires rerun idempotency by `event_id`. A UUID gives a new id on every replay and defeats it entirely. Proven by `test_event_id_is_deterministic_across_replays` |
| **S06-4** | **`primary_key` JSON uses sorted keys** | Unsorted, the same PK yields different strings depending on field order, so `event_id` differs across runs and idempotency silently fails |
| **S06-5** | **`before`/`after` stay JSON strings** (S01-16, re-confirmed by test) | A struct makes every source DDL change an Iceberg migration on the streaming table |
| **S06-6** | **The job REFUSES to start if the checkpoint URI is under `warehouse/`** | CLAUDE.md §5.9. Iceberg's `remove_orphan_files` walks table locations and deletes unreferenced files — a checkpoint there is exactly that. The failure would be silent destruction of streaming state, so this is a hard exit before any write |
| **S06-7** | **`availableNow` micro-batch, not a continuous stream** | Drains what exists then STOPS, so the EMR Serverless application can auto-stop and the lab stops paying (ADR-009, ADR-027). A continuous trigger holds the application open for the whole window |
| **S06-8** | **Per-record try/except in `transform_partition`** | One poison record must not fail the batch (CLAUDE.md §5.10). Good rows land in L1, bad rows land in quarantine, and the batch still commits |
| **S06-9** | **`stack_hash` strips line numbers and addresses before hashing** | Otherwise the same bug in two builds produces two hashes and the grouping is useless. Tested |

### The EMR cost guardrail that the provider caught

**`worker_count = 0` is rejected** — the provider's valid range is 1–1,000,000. The
correct expression of "no pre-initialized capacity" is to **omit the `initial_capacity`
block entirely**.

This matters more than it looks: setting it to 1 to satisfy the validator would have
silently reserved a worker that **bills from application start whether or not a job
runs** — the most expensive default in EMR Serverless. `terraform validate` caught it.

Guardrails asserted in the plan, not claimed: ARM64, **0 pre-init blocks**, auto-stop at
15 min, max 16 vCPU / 64 GB, `emr-7.2.0` pinned. Approximate ceiling **$1.21/hr**.

### Still NOT demonstrated

| ID | Gap |
|---|---|
| **OPEN-16** | **End-to-end streaming is `NOT_TESTED`.** Kafka→Iceberg through a real MSK cluster, checkpoint recovery across a job restart, replay from a committed checkpoint, and source-to-L1 reconciliation all need the applied stack. The transformation logic is proven; the *pipeline* is not |

OPEN-15 (CDC correctness) also remains open and gates this: L1 consumes `event_order`,
and while the normalisation is now tested, the connector output feeding it is not.

### Not done, deliberately

No apply. Session 07 not started.

---

## Session 07 — L2 FULL CDC, 2026-08-14

### Two real bugs the tests caught, both silently wrong-by-a-day

Neither would have raised an error in production. Both would have assigned CDC events to
the **wrong business date**.

| ID | Bug | Fix |
|---|---|---|
| **S07-1** | **Spark session timezone.** The first local EOD run reported `input_count = 5` instead of 4, watermarks shifted to 06:00–11:00. This workstation is **UTC+7**, and Spark defaults to the JVM's local zone — so the `TIMESTAMP` literal in the cutoff predicate was interpreted locally while the data was written as UTC. An event at 23:00Z on 13 Aug landed in the **14 Aug** window | `spark.sql.session.timeZone = UTC` in **both** jobs, not just the tests. Exactly the failure ADR-024 exists to prevent |
| **S07-2** | **Spark returns NAIVE datetimes** even with `session.timeZone = UTC`. Comparing one to a tz-aware window bound raised `TypeError` — and worse, comparing two naive values from different zones would be **wrong without erroring** | `read_watermark` attaches UTC explicitly |

S07-1 is the more instructive one: it was only visible because a test asserted the
boundary. A test that merely checked "rows were written" would have passed.

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S07-3** | **`WHEN NOT MATCHED THEN INSERT` only — no `WHEN MATCHED THEN UPDATE`** | The single line that defines L2. It gives rerun idempotency (matching `event_id` inserts nothing) AND retains every distinct I/U/D (distinct events have distinct `event_id`s, so all are unmatched). Adding an UPDATE branch would convert *operational* idempotency into *business* dedup and silently destroy the history L2 exists to hold (CLAUDE.md §5.3) |
| **S07-4** | **Half-open window, `<` not `<=`** | S01-18. `CLAUDE.md` §5.6's prose uses `<=`, which places a midnight-boundary event in two business dates. `test_midnight_boundary_belongs_to_exactly_one_day` asserts the count is exactly one — never zero, never two |
| **S07-5** | **The late-arrival sweep is bounded by the window END, not just the watermark** | Without the upper bound, a sweep would pull in events belonging to a *future* business date. Two `source_commit_ts <` clauses, asserted by test |
| **S07-6** | **A watermark after the window end is REJECTED** | Re-running an old date after a newer one would sweep future rows backwards into it. The guard makes descending replay fail loudly instead of corrupting quietly |
| **S07-7** | **Safety overlap rewinds the watermark 15 minutes** | Absorbs clock skew between Connect and Spark. Without it an event written microseconds before the recorded watermark falls through the gap between runs and is never swept. Harmless because the MERGE is idempotent |
| **S07-8** | **`validate_run` returns ALL failures, not the first** | One run surfaces every problem instead of one per retry — on a metered lab, each retry costs a window |
| **S07-9** | **An empty window BLOCKS publication** | A quiet day is legitimate, but it must be a deliberate confirmation rather than the silent result of a wrong cutoff. The most likely wrong-cutoff symptom is exactly zero rows |
| **S07-10** | **`ops.reconciliation_run` is APPEND-ONLY; failed runs stay visible** | Diagnosing "why was day T wrong" needs the failures, not just the retry that eventually worked |
| **S07-11** | **Orphan-file retention floor is 72 hours, and orphans run LAST** | S01-19: job timeout (120 min) × retries (3) + margin. A shorter retention deletes files an in-flight commit is about to reference, and the table is corrupt in a way that only surfaces on the next read (risk R14) |

### Test results

**84 tests pass** across Sessions 06 and 07 — 35 new here:

| Suite | Tests | Proves |
|---|---:|---|
| `test_l2_window.py` | 26 | half-open boundary, sweep clauses, watermark guards, publish gate |
| `test_l2_local_spark.py` | 9 | **real Iceberg MERGE**: rerun idempotency, no business dedup, atomic publish |

The two acceptance criteria that matter most are proven against a real table:
`test_rerun_does_not_duplicate_event_id` and `test_all_IUD_for_the_same_pk_survive`.

### Still NOT tested

**OPEN-17.** EOD against live L1 data on EMR Serverless, partial-failure/retry on a real
cluster, and the maintenance procedures (compact, manifests, expire, orphans) have never
executed. The MERGE semantics are proven; the pipeline is not.

### Not done, deliberately

No apply. Session 08 not started.

---

## Session 08 — L3 SNAPSHOT, 2026-08-14

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S08-1** | **Rank by `event_order`'s five components; Kafka offset is the LAST tie-breaker, never the sort key** | `CLAUDE.md` §5.4: offset only increases *within* one partition, so a global offset sort silently reorders across partitions. `test_source_position_beats_kafka_offset` builds the contrary case — higher SCN, *lower* offset, *different* partition — and asserts the SCN wins |
| **S08-2** | **Cutoff is `source_commit_ts < T+1 00:00:00Z`, exclusive** | AS-OF, not "latest". A "latest state" query returns today's answer for every historical date, which defeats the whole purpose of a dated snapshot |
| **S08-3** | **Active table excludes deletes; `_history` is OPT-IN per table** | §7.2. Retaining deleted records conflicts with erasure obligations, so it must be a deliberate recorded choice rather than a default. The job **raises** if history is requested for an entity that has not enabled it |
| **S08-4** | **Delete-then-recreate works by construction, with no special case** | The re-insert has a higher `event_order`, wins `rn = 1`, and `operation = 'c'` passes the delete filter. No branch handles it — which is why it cannot rot |
| **S08-5** | **Checksum XORs per-row hashes rather than summing** | XOR is order-independent, so two rebuilds producing the same rows in a different physical layout still match. Summing is also order-independent but collides far more readily |
| **S08-6** | **`target_namespace` is configurable, not a hardcoded `snapshot.` prefix** | Found by the tests failing with `TABLE_OR_VIEW_NOT_FOUND`. Baking the catalog namespace into the config class made it untestable and tied it to one deployment. **Fixed in the config rather than worked around in the test** |
| **S08-7** | **The canonical PK expression SORTS keys** | It must match `envelope.canonical_primary_key` exactly. If the two diverged, the join key would differ and every PK would look new — producing a snapshot with the right row count and entirely wrong lineage. `test_composite_pk_expr_sorts_keys` guards it |
| **S08-8** | **Typed projection happens at L3, not earlier** | L1/L2 keep `before`/`after` as JSON (S01-16) so source DDL changes do not force an Iceberg migration on the streaming table. Typing here makes a migration a reviewed change rather than a stream outage |
| **S08-9** | **`main()` exits non-zero when the snapshot is not `CERTIFIED`** | Uniqueness (row count == distinct PK) and lineage presence are checked before publication, so an uncertified snapshot cannot be silently consumed downstream |

### Why rebuilds are deterministic — the structural argument

Gate B requires that rebuilding L3 for the same cutoff yields an equivalent checksum.
That holds **only if the ranking is total**. `event_order`'s five components make ties
impossible, because **`kafka_partition` and `kafka_offset` alone are unique per record**.

This is the reason the tie-breaker chain is not optional, and it is worth stating as an
argument rather than a convention: drop either of the last two and rebuilds stop being
reproducible, silently.

### Test results

**101 tests pass** across Sessions 06–08 — 17 new here, all against a real Iceberg table:

| Scenario | Test |
|---|---|
| create → update, lineage to the winning event | `test_latest_update_wins` |
| create → delete: PK absent | `test_latest_delete_excludes_the_pk` |
| create → delete → **recreate**: active again | `test_delete_then_recreate_is_active_again` |
| history keeps the deleted PK flagged | `test_history_table_keeps_the_deleted_pk_flagged` |
| higher SCN beats lower offset in another partition | `test_source_position_beats_kafka_offset` |
| out-of-order arrival changes nothing | `test_out_of_order_arrival_does_not_change_the_result` |
| events after cutoff excluded | `test_events_after_cutoff_are_excluded` |
| late event wins on rebuild, partition **overwritten** | `test_late_event_changes_a_rebuilt_snapshot` |
| rebuild yields identical checksum | `test_rebuild_same_cutoff_yields_identical_checksum` |
| one active row per PK, certified | `test_one_active_row_per_pk` |
| initial snapshot `r` survives | `test_initial_snapshot_r_operation_is_kept` |
| composite PK sorts keys | `test_composite_pk_expr_sorts_keys` |

### Re-execution under review — 2026-08-14

Session 08 was re-run under both the architect and data-engineer passes. The pass found
**four defects and one missing deliverable in work that was already reported complete and
green**. Evidence and reproductions: `artifacts/validation/session-08/defects-found-and-fixed.md`.

| ID | Decision | Rationale |
|---|---|---|
| **S08-10** | **Write with `overwrite(snapshot_date = <date>)`, NOT `overwritePartitions()`** | **Critical defect.** Dynamic partition overwrite only replaces partitions present in the written dataframe, so a rebuild whose result is EMPTY — a late delete removing the last PK for the date — touched no partition and left the previous ACTIVE row in the certified table, while still reporting `CERTIFIED`. Reproduced live before the fix. `DATA_CONTRACTS.md` §7 already said "full overwrite of the snapshot_date partition"; the code did not implement it |
| **S08-11** | **Certification fails if any surviving row carries a different `l3_run_id`** | The check that makes S08-10's class of bug impossible to certify silently. A write path that no-ops leaves rows owned by an earlier run; **certifying stale data is worse than failing** |
| **S08-12** | **`canonical_pk_expr()` uses `to_json(named_struct(...))`, not string concatenation** | The `concat_ws` version put the separator between a key and its own value and left values unquoted: it emitted `{"CUSTOMER_ID":,1}` where the L1 writer emits `{"CUSTOMER_ID":"1"}`. **Supersedes the guard claimed in S08-7** — that test asserted only that `"A_COL"` preceded `"Z_COL"` in the generated SQL *text*, which passes regardless of whether the SQL is valid. A substring check on generated code cannot detect this; only evaluating it can |
| **S08-13** | **L2 → L3 reconciliation is a `GROUP BY`/`max_by` derivation, independent of the build's window** | Scope item 8 requires reconciliation before certification and `validate()` only checked uniqueness. The derivation is deliberately different from the build's `row_number()`: a reconciliation reusing the build's own query proves only that the query is deterministic — **it agrees with the build even when the build is wrong** |
| **S08-14** | **Never pass `event_order` itself to `max_by`; name the five fields explicitly** | Comparison semantics depend on typing: a struct compares field-by-field in *declared* order, so the ranking would follow physical layout rather than §4.1, and a map is not orderable at all. Explicit naming is independent of both |
| **S08-15** | **The reproducibility checksum covers the business columns** | **Supersedes S08-5's scope.** Hashing only `(primary_key, source_event_id)` answers "did the same events win?" but not "did they produce the same values?" — a changed projection would rebuild to an identical checksum while the content differed, so scope item 6's comparison would pass on a real regression. `l3_run_id`/`l3_write_ts` stay excluded, being expected to differ every run |
| **S08-16** | **L2 test fixtures declare an explicit schema** | Spark inferred the nested `event_order` dict as `MAP<STRING,STRING>`. The contract says **struct**, and string-typing coerced `source_ts_ms`, `kafka_partition` and `kafka_offset` so the tie-breakers compared **lexicographically, not numerically** — offset 999 sorts below offset 1000. The ordering tests were not meaningful until this was fixed |
| **S08-17** | **L3 is proven against SQL Server LSN, not only Oracle SCN** | Every L3 test used `oracle_scn`. The two formats normalise in *opposite* directions (pad vs validate), to different widths and alphabets, and `entities.json` ships a SQL Server entity — so **the entire Oracle suite would still pass with the SQL Server path broken.** Four tests added, including `event_serial_no` tie-breaking within a single LSN |
| **S08-18** | **`scripts/snapshot-rebuild.sh` added** | The session file lists "Snapshot validation/rebuild scripts" as a deliverable; none existed. Identity guard, date validation, dry-run by default, records each certification and **diffs the rebuild against the previous one** (scope item 6). A checksum that changes for the same cutoff is a warning, not an error — late events (§6.2) and a non-total ordering look identical from there, so the script says so rather than guessing |

Every fix is pinned by a test that **fails when the fix is reverted** — mutation-checked,
not assumed. **114 tests pass**, up from 101; L3 tests 17 → 30.

### Still NOT tested

**OPEN-18.** L3 against live L2 data on EMR Serverless, and the session's
"compare count/hash/sample with the previous build" against a **real prior production
build** — the local test compares two builds in the same run, which proves determinism
but not stability across deployments.

Live AWS state re-verified at closeout: **0 MSK clusters, 0 running EC2 instances.**

### Not done, deliberately

No apply. Session 09 not started.


## Session 09 — the four processing flows, 2026-08-14

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S09-1** | **ONE canonical transformation, shared by all four flows** | The flows differ in exactly three things: input source, window, and the tier they stamp. Everything else is `transform.py::build_fact_transaction`, with no `if flow == ...` inside it. If each flow carried its own copy of the joins and measures, a variance between provisional and certified would have TWO possible causes — late data, or drifted logic — and nothing in the numbers could separate them. Pinned by `test_all_four_flows_produce_identical_business_columns` |
| **S09-2** | **The accuracy ladder is defined ONCE; the MERGE's SQL `CASE` is generated from it** | A second hardcoded copy inside a MERGE statement is how an anti-downgrade rule silently stops matching the ladder it enforces. Unrecognised statuses rank 0, below every real tier, so an unknown value can never win a comparison |
| **S09-3** | **Same-tier ties are broken by `input_cutoff`, not rejected** | A strict `>` would make EOD unable to correct itself: a rebuild after late events is CERTIFIED replacing CERTIFIED. A plain `>=` is wrong the other way — a stale rerun could clobber a newer one. Comparing the cutoff settles both directions deterministically |
| **S09-4** | **`collapse_to_grain()` enforces one row per `transaction_id`, by SOURCE POSITION** | **Defect found this session.** Nothing enforced the mart's own grain, and **L1 keeps every I/U/D event** (CLAUDE.md §5.2) — so an updated transaction appears several times in any L1 window, which for NRT and auto-correct is the normal case. Reproduced: the first write INSERTed both rows (double-counting in every sum, in a mart that still looked consistent); once a row existed the duplicates made the MERGE fail with a cardinality violation instead |
| **S09-5** | **Grain ordering uses the SAME `event_order` precedence as L3** | Session 08, DATA_CONTRACTS §4.1, Kafka offset last and never a global sort key. A different rule would make an intraday flow and the certified snapshot pick DIFFERENT winning events, so provisional and certified would disagree for a reason unrelated to latency |
| **S09-6** | **L1/L3 positions are normalised into `_ord_*` at the SOURCE BOUNDARY** | The two layers expose positions differently (L1 has the full struct; L3 keeps only `source_position_primary`). Normalising where the flows already legitimately differ keeps a single dedup path in the business logic. A source lacking those columns is REJECTED — ranking without a total order picks a nondeterministic winner |
| **S09-7** | **Dimension joins are LEFT with `UNKNOWN_SK = -1`; the -1 member is INSERTED, not implied** | An inner join DROPS a fact whose dimension has not arrived, and a missing fact reads as a smaller number rather than an error. The -1 row must physically exist or joins back from the fact drop the unresolved rows too |
| **S09-8** | **`amount_base` stays NULL when the FX rate is unresolved** | A silent 1.0 fallback turns a missing reference into a *wrong* number that reconciles against nothing and looks entirely plausible |
| **S09-9** | **Full-fill never INSERTs and never writes `processing_status`** | (a) No `WHEN NOT MATCHED` clause plus a row-count assertion — a full-fill that inserted would create a second fact for a transaction already in the mart. (b) Resolving a surrogate key makes a row more COMPLETE, not less CERTAIN, so a certified row stays certified; stamping a status here would downgrade it |
| **S09-10** | **Full-fill re-resolves through the same canonical transform, not by hand-patching keys** | A second resolution path would be a second copy of the join logic, and it would drift |
| **S09-11** | **A zero certified baseline is `UNDEFINED_BASELINE`, which does NOT pass** | `nullif(abs(certified), 0)` yields NULL, and `NULL <= tolerance` is NULL — not TRUE, but a naive `if not breach` reads it as a pass. A metric that moved from a certified 0 to a provisional 1,000,000 would report as within tolerance. `ops.metric_variance.variance_pct` is nullable for the same reason and `status` carries the verdict |
| **S09-12** | **All four flows are SCHEDULED BATCHES; none is a streaming query** | CLAUDE.md §4 and the session's "avoid always-on Spark". Structured Streaming holds EMR Serverless capacity 24h/day for a demo that runs for an hour; a 5-minute batch meets the ≤10-minute freshness target and bills only for the seconds it runs. **The dominant cost driver is NRT's frequency, not its size** |
| **S09-13** | **EOD refuses a business day that has not closed in UTC** | Certifying an open day publishes a figure that will still change, under a status that promises it will not. Enforced in both the job and `scripts/run-flow.sh` |
| **S09-14** | **`ops.data_certification` is APPEND-ONLY** | "Why did day T change between the 14:00 provisional and the certified figure" is answerable only from the runs that were later superseded. Overwriting keeps the answer and destroys the explanation |

### Approximate intraday vs exact — the distinction (scope item 9)

Not about importance; about whether a metric is a **monotone accumulation** of already
committed events or depends on the day being complete. Running counts and sums may be
approximate intraday because late events only ever ADD, so a partial total is an honest
lower bound. A daily closing balance may not: a balance is a state as-of a moment, so a
missing update makes it wrong rather than merely low. Deletes and reversals are likewise
exact-only — a reversal that has not arrived makes the total too HIGH, and unlike a late
insert that cannot be read as a bound. Full table in `docs/FOUR_FLOWS.md` §6.

### Test results

**161 tests pass**, up from 114. Session 09 adds 47: `test_flows` 17, `test_reconcile` 14,
`test_four_flows_spark` 16 (real Iceberg, real MERGE).

Every new guard was **mutation-checked** — reverted, and the test confirmed to fail.
M3 initially PASSED, exposing a **vacuous test**: `_run_full_fill` derives its ids from the
mart, so `WHEN NOT MATCHED` is unreachable on that path and the duplicate test passed
whether or not an INSERT clause existed. Fixed with a direct test that hands the writer an
unmatched id. Evidence: `artifacts/validation/session-09/mutation-checks.md`.

### Still NOT tested

**OPEN-19.** Freshness ≤10 minutes. It is a property of the deployed pipeline, and the
local tests measure correctness, not latency. Sessions 02–09 are unapplied: 0 MSK clusters,
0 EC2 instances, 0 EMR Serverless applications, verified at closeout. Recording anything
other than `NOT_TESTED` would be inventing a result.

Also untested live: provisional→certified convergence against real L1/L3 data, and the
tolerance policy firing on real variance rather than constructed inputs.

### Not done, deliberately

No apply. Session 10 not started.


## Session 10 — Kimball facts, dimensions and marts, 2026-08-14

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S10-1** | **Surrogate keys are DETERMINISTIC hashes of (natural key, effective_from), not sequences** | `monotonically_increasing_id()`/`row_number` assigns a DIFFERENT key to the same version on every rebuild, so every fact already written points at the wrong member or at nothing, and the mart silently re-attributes history. This project rebuilds by design (L3 is a pure function of L2; the flows re-run all day), so a non-reproducible key is not survivable |
| **S10-2** | **Hash collisions are checked and FAIL** | Hashing makes a collision possible rather than impossible, and a silent one merges two customers into a single dimension member |
| **S10-3** | **`record_hash` covers TRACKED attributes only — never `updated_at`** | Hashing an audit column emits a new SCD2 version every time the source touches a row without changing anything a user would recognise; the dimension grows without bound and every join must choose between semantically identical versions |
| **S10-4** | **Facts join the dimension version valid at EVENT TIME, never `is_current`** | **The most common way a Kimball model goes wrong, and it produces NO error.** A customer who was RETAIL in January and PRIVATE in June has their January transactions reported under PRIVATE: row counts right, amounts right, every total reconciles — only the segmentation is wrong, and only against a historical report. Using `is_current` against an SCD2 dimension is strictly worse than a Type 1 dimension, because it pays the cost of history and then discards it |
| **S10-5** | **Validity intervals are HALF-OPEN `[from, to)`** | A closed upper bound makes a fact landing exactly on a version boundary match TWO versions and DUPLICATE the fact row, inflating every additive measure on it. `assert_no_fanout()` catches the class directly — a fact count that changes during a dimension join is always a defect |
| **S10-6** | **`effective_to` uses a `9999-12-31` sentinel, never NULL** | `txn_ts < effective_to` is NULL-propagating, so a NULL end date makes the CURRENT version match nothing and every recent fact silently falls back to the unknown member |
| **S10-7** | **The unknown member (-1) is INSERTED into every dimension and spans ALL time** | Inserted, not implied: a join back from the fact would otherwise drop every unresolved row, and a missing fact reads as a smaller number rather than an error. Spanning all time because an unknown member with a narrow validity window is no unknown member for facts outside it. Natural key `'UNKNOWN'` rather than NULL so it appears in a BI filter list |
| **S10-8** | **Measure additivity is a REGISTRY; an undeclared measure raises** | A measure nobody decided how to aggregate is a review failure, not a default. `closing_balance` is SEMI-ADDITIVE: summing across accounts for one day is the bank's position, summing the same column across 30 days is not a quantity that exists — and both are `SUM(closing_balance)`, differing only in the grouping Power BI applies by default |
| **S10-9** | **NON-ADDITIVE measures are REFUSED over time, not approximated** | There is no correct scalar aggregate for a ratio or a distinct count; returning a plausible one would be inventing a number |
| **S10-10** | **`fact_account_daily_snapshot` reads L3 only and has NO NRT variant** | FOUR_FLOWS §6: a late transaction makes a running SUM too LOW (honestly readable as "so far") but makes a BALANCE wrong. There is no reading of "the account held $500" that is true-so-far when it held $700 |
| **S10-11** | **The periodic snapshot emits a row for EVERY account, including dormant ones** | A snapshot that emits rows only for accounts that moved silently becomes "balance by account that transacted", and the accounts that quietly went to zero vanish from the report meant to surface them |
| **S10-12** | **A mart carries the WEAKEST status of its inputs** | A mart built from one CERTIFIED and one PROVISIONAL fact IS provisional — it contains a number that can still move. Taking the max would let a single certified input launder a provisional aggregate into a certified one, and it would be reported as final on that basis. The ladder comes from `flows.STATUS_RANK`, the same definition S09-2 uses, so the two cannot drift |
| **S10-13** | **Each fact is pre-aggregated to the mart grain BEFORE joining** | Joined raw, a customer with 3 transactions and 2 accounts yields 6 rows; every measure is inflated while each individual row still looks correct |
| **S10-14** | **PII is TOKENISED in the BI view, not dropped or nulled; the default BI role is granted on the VIEW only** | A stable `sha2` token keeps the column joinable and distinct-countable while never exposing the value. Dropping it breaks existing reports; NULLing it makes distinct counts silently wrong. The base table stays restricted to governance — **a view is not a security boundary if the caller can also read the table behind it** |
| **S10-15** | **Dimensions are NOT partitioned; facts are partitioned by `business_date` only; target file 128 MB** | Partitioning a small dimension creates thousands of tiny files and makes joins slower. A second fact partition column multiplies file count by its cardinality for a filter most queries never apply — sort order does that job at no file-count cost. 128 MB rather than the 512 MB default because these tables are written by 5-minute micro-batches |

### Test results

**207 tests pass**, up from 161. Session 10 adds 46: `test_scd2` 27, `test_kimball_spark` 19.

Six guards **mutation-checked** — reverted, and the test confirmed to fail (K1–K6,
`artifacts/validation/session-10/mutation-checks.md`). No vacuous test this session, unlike
Session 09's M3.

One defect caught by the first test run rather than by review: `scd2.py` was written with a
RAW null byte where the escape `\x00` was intended, making the module unimportable. The
sentinel it belongs to is load-bearing — without it `('A', NULL)` and `(NULL, 'A')` hash
identically and a real attribute change produces no new version.

### Opened

**OPEN-20.** The AWS account is **not** empty, contrary to earlier sessions' records: Glue
database `vannk-dev-oracle-db` exists in ap-southeast-1 with **39 tables** and no crawler.
It predates this project's Terraform (none of `stream`/`full_cdc`/`snapshot`/`mart`/`ops`
are present). Cost is negligible, but 39 catalogued tables of unknown provenance are a
governance and naming-collision concern for Session 14. **Not deleted** — destroying
unidentified catalogued data needs an explicit decision.

### Still NOT tested

**Partition/file-layout and incremental MERGE benchmark.** A benchmark measures a deployed
system; file sizes, compaction cost and MERGE latency depend on real volumes, S3 and the
Glue catalog. Sessions 02–10 are unapplied, and the local tests use a Hadoop catalog with a
handful of rows. The strategy and its reasoning are recorded (S10-15, KIMBALL_MODEL §8) and
implemented in the DDL; numbers would have to be invented.

Also untested live: OPEN-19 freshness, OPEN-18 L3, OPEN-17/16/15.

### Not done, deliberately

No apply. Session 11 not started.


## Session 11 — dbt on Spark, 2026-08-14

### The split, decided from evidence

| ID | Decision | Rationale |
|---|---|---|
| **S11-1** | **The guarded MERGE stays in PySpark; dbt CANNOT express the anti-downgrade rule** | `spark__get_merge_sql` (dbt-spark 1.9.2) emits an UNCONDITIONAL `when matched then update set`, and its only lever — `incremental_predicates` — is appended to the **ON** clause. Putting the rank test there is not equivalent: when it fails the row is NOT MATCHED, so `when not matched then insert *` fires and dbt **INSERTS A DUPLICATE**. Reproduced against real Iceberg: the certified row survived and the mart ended with TWO rows for one transaction — breaking the grain and double-counting every measure, which is **worse** than the downgrade the rule prevents. Evidence: `artifacts/validation/session-11/dbt-merge-proof.txt` |
| **S11-2** | **dbt snapshots are not used for SCD2** | dbt 1.9's `dbt_valid_to_current` *does* fix the NULL-end-date problem (S10-6), so that alone would not disqualify them. The decisive reason: a dbt snapshot **accumulates** history by polling a source across runs and can only record changes it was running to observe. This project's SCD2 is **derived** from L3 history in one pass, so it rebuilds from scratch and reproduces the same versions and surrogate keys (S10-1). A snapshot cannot reconstruct history it did not witness. Secondary: `dbt_scd_id` is an MD5 string; facts carry BIGINT keys |
| **S11-3** | **The four aggregate marts MOVED to dbt; `spark/marts/marts.py` was DELETED** | A migration, not a duplication. Keeping both would be the second implementation of the business logic that S09-1 exists to prevent, and the two would drift. Session 10's `TestMarts` was removed with it; every property it asserted is now tested where the code lives (docs/DBT_SPARK.md §3) |
| **S11-4** | **The accuracy ladder stays defined ONCE in `flows.STATUS_RANK`; the dbt macro mirrors it and a test parses the macro to detect drift** | SQL needs a macro, but a second hardcoded rank table is exactly how an anti-downgrade rule stops matching the ladder it enforces (S09-2) |
| **S11-5** | **A test enforces the boundary itself** | `test_dbt_contract.py` scans every dbt model (**comments stripped**) for `row_number(`, `merge into`, `when matched then` and `effective_from`, failing if a model starts restating Spark logic. The first version scanned raw text and flagged the comments *explaining* the rule — a scanner that cannot tell code from prose reports the documentation as the bug |
| **S11-6** | **Marts use `insert_overwrite` on `business_date`, never `merge`** | Beyond S11-1: a mart for a date is a pure function of that date's facts, so overwriting the partition is correct, idempotent and cheap. Proven by running the models three times and comparing counts |
| **S11-7** | **Staging models are EPHEMERAL, not views** | The first build failed with `Replacing a view is not supported by catalog: local`. Rather than work around it: Iceberg VIEW support varies by catalog (Glue only gained it recently), and staging here is pure projection, so inlining it as a CTE costs nothing, creates no catalog object and removes a portability dependency the models never needed |
| **S11-8** | **No dbt packages; `dbt_utils`'s one needed macro is reimplemented locally** | `dbt deps` fetches from the network at build time, which CLAUDE.md §3.9/§3.10 rule out without a pinned verified reference. Eight lines of local SQL is a smaller cost than a supply-chain exception, and the build stays hermetic |
| **S11-9** | **`contract: {enforced: false}` on the marts** | dbt model contracts need the adapter to enforce column constraints, and Spark/Iceberg support is partial. Claiming an enforced contract the engine does not enforce is worse than declaring none; the `not_null` tests do the real work |

### A defect this session found in its own model

The first successful build produced `cust=101 ... status=UNKNOWN`, where the correct answer
was `PROVISIONAL_NRT`. `status_rank` returns 0 for an unrecognised status — the right safety
property — but that makes NULL and garbage indistinguishable, and after an outer join NULL
means "this input contributed no rows", not "this input is bad". `LEAST(...)` therefore
picked 0. **The model's own comment claimed to handle exactly this case.**

Fixed with `status_rank_if_present`. The tests had not caught it because `accepted_values`
**included** `'UNKNOWN'`; that value was removed, so the same bug now fails the build.

### Test results

**211 Python tests** (207 − 6 retired mart tests + 10 new contract tests) and **47 dbt tests**.

```
dbt parse / compile / build / docs generate    all OK
dbt build:  Done. PASS=47 WARN=0 ERROR=0 SKIP=0 TOTAL=47
idempotency: 3 runs of tag:mart, counts unchanged
```

Acceptance criterion "tests catch duplicate grain/SCD overlap/bad FK" verified by
**injecting each defect into real data** and confirming the run fails
(`artifacts/validation/session-11/test-injection.txt`) — all four caught, each by the test
that names the defect.

### Still NOT tested

**dbt against live Glue/EMR Serverless.** Everything above ran locally against a temp
Iceberg warehouse. Two things are expected to differ and are recorded rather than assumed:

1. **`catalog.json` is EMPTY locally** (0 entries) — dbt-spark's catalog query does not work
   against a non-session Iceberg catalog. `manifest.json` lineage is complete, so the docs
   criterion is met, but column-level metadata is unverified until Glue.
2. `insert_overwrite` partition behaviour under the Glue catalog with concurrent writers.

### Not done, deliberately

No apply. Session 12 not started. Stopped at the approval gate.


## Session 12 — Airflow 3 on k3s, 2026-08-14

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S12-1** | **Three missing CLI entrypoints added BEFORE any DAG was written** | `dim_builder`, `reconcile` and `maintenance.sql` were library/SQL only, so a DAG had nothing to call. Session 10's handoff had already flagged the Kimball gap. Added `build_kimball.py`, `ops/reconcile_job.py`, `eod/maintenance.py`. **A DAG that cannot invoke a job is not orchestration** |
| **S12-2** | **Versions RESOLVED from upstream, not invented** | Helm chart **1.22.0** (published 2026-06-01, sha256 recorded), Airflow **3.2.2**, k3s **v1.36.3+k3s1**, postgresql subchart 13.2.24. Queried from `airflow.apache.org/index.yaml` and `update.k3s.io` on 2026-08-14 |
| **S12-3** | **Pin the chart's OWN appVersion (3.2.2), not PyPI's newer 3.3.1** | The chart is tested against its appVersion; pushing a newer image into an older chart is an untested combination (CLAUDE.md §3.9). The bootstrap verifies the chart digest before `helm upgrade` and refuses on mismatch |
| **S12-4** | **k3s, not EKS — a cost-SHAPE decision** | Confirms ADR-011. An EKS control plane bills continuously whether or not a DAG runs; this node bills only while started. Every concept the session asks to demonstrate (KubernetesExecutor, pod isolation, resource limits, RBAC, remote logging) is identical on k3s. **What k3s does not demonstrate — multi-node scheduling, control-plane HA, node-failure rescheduling — is recorded as a limitation, not glossed.** `enable_eks` is deliberately NOT wired to this module: wiring both would let a plan create two control planes for one workload |
| **S12-5** | **KubernetesExecutor; `redis.enabled: false` and `flower.enabled: false` stated explicitly** | The chart only deploys Redis for Celery-family executors, but stating it means a future edit to `executor` cannot quietly bring a 24/7 broker back. A test asserts it |
| **S12-6** | **The stream is SUPERVISED, not scheduled** | Scheduling "run the stream every 5 minutes" starts a SECOND consumer of the same topics. Two writers committing to one Iceberg table with the same checkpoint corrupts the stream, and the idempotency that makes the batch flows safe to overlap does NOT extend to concurrent streaming writers sharing a checkpoint (CLAUDE.md §5.9) |
| **S12-7** | **The whole EOD chain is ONE DAG because the ORDER is the correctness** | dims before facts (a fact built first lands every new member on `-1` — complete, plausible, wrong about who the rows belong to); DQ before reconciliation (DQ asks "is each table valid", reconciliation asks "do the layers agree"); maintenance LAST (rewriting files under a committing job loses data). Separate scheduled DAGs would replace every guarantee with a hope that cron offsets are far enough apart |
| **S12-8** | **The reconciliation gate has `retries = 0`** | A breach is a DATA problem. Retrying the same comparison over the same data produces the same failure more slowly — and a GREEN retry would mean the data changed underneath, which is worse |
| **S12-9** | **DAGs pass the LOGICAL date and a DETERMINISTIC run id** | A task computing its own date gives a different answer on retry than on first attempt, and a backfill silently processes today. The run id is `dag__ds__try_number`, not a UUID: a retry must reuse the id or the write path cannot recognise the repeat, defeating the idempotency the jobs implement |
| **S12-10** | **`catchup=False` and `is_paused_upon_creation=True` on every DAG** | A past `start_date` with catchup on queues every missed interval the moment it is unpaused — and bills all of them. Nothing starts spending on deploy |
| **S12-11** | **Recovery DAGs are `schedule=None` and require an explicit business_date** | A scheduled recovery either never fires when needed or fires when nothing is broken and rewrites good data. **A recovery defaulting to "today" turns one incident into two** |
| **S12-12** | **Two pools: `spark_jobs` and `maintenance`** | `max_active_tasks` bounds tasks per DAG, not across DAGs; without a shared pool NRT, auto-correct and a backfill can all run at once. Maintenance gets its own pool so compaction never contends with a live write |
| **S12-13** | **No ingress rule for 8080 at all — not even VPC-scoped** | k3s installed with traefik AND servicelb disabled, so there is no ingress controller to expose anything through. The UI is SSM port-forward only. An 8080 rule "just for the VPC" is how a private UI becomes reachable from every instance in the account |

### Cost

| Scenario | Cost | % of the $30 budget (ADR-030) |
|---|---|---|
| Demo window 4h/day × 20 days | **$11.33/mo** | 37.8% |
| Stopped all month (EBS only) | $2.88/mo | 9.6% |
| Destroyed | $0.00 | 0% |
| **Left running 24/7** | **$79.97/mo** | **267%** |

**Stop is cheaper, not free** — a stopped instance bills no compute but its 30 GiB gp3
volume is billed regardless; only destroy removes it. That is the most common lab-cost
surprise and it is why `airflow-node.sh` prints which state you are in.

### Terraform

`fmt`/`validate` Success; **saved plan** at `artifacts/validation/session-12/`.
`module.airflow_k3s` = **5 create, 0 change, 0 destroy, 0 replace**. Whole plan 126 create /
22 read / **0 destroy** (Sessions 02-12 all unapplied).

An existing invariant fired correctly during planning: `enable_airflow` requires
`enable_emr_serverless` — DAGs submit external Spark jobs, so with no Spark runtime every
task fails.

### Test results

**242 Python tests** (211 + 31 new DAG tests). Four policy guards mutation-checked:
catchup, self-computed dates, maintenance ordering, and the Celery switch — each reverted
and confirmed to fail.

One test of mine was naive again: it flagged `IcebergSparkSessionExtensions` as "builds a
SparkSession" because it substring-matched. Now checks `SparkSession.builder` and pyspark
imports. Same class as Session 11's comment-scanning bug.

### Opened

**OPEN-22 — the DAG tests run against Airflow 2.9.3, not the target 3.2.2.** The DAGs were
written to the API subset valid in both and avoid constructs removed in Airflow 3, but
`test_no_import_errors` therefore validates against the WRONG version. It proves structure
and policy; it does not prove they import under 3.2.2.

### Still NOT tested

KubernetesExecutor launching a real task pod (the acceptance criterion allows "or static
limitation recorded" — this is that record), remote S3 logging, git-sync, and the bootstrap
script end to end. All need the node deployed.

### Not done, deliberately

No apply. Session 13 not started. Stopped at the approval gate.


## Session 13 — Athena serving layer and Power BI baseline, 2026-08-15

### The defect

| ID | Finding |
|---|---|
| **D13-1** | **The BI role could read L1 and L2.** `athena_bi` was attached to `lake_read`, which grants `s3:GetObject` on `${lake_bucket_arn}/*` — the whole bucket. The attachment carried the comment *"Athena BI: read-only. CLAUDE.md §8 — Power BI reads marts, never L1/L2"*, describing a rule the policy did not implement. Power BI's role could read every raw `before`/`after` payload in L1 and L2, unmasked, and nothing failed because nothing was checking |

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S13-1** | **New `mart_read` policy: narrow Allow + EXPLICIT DENY on the raw CDC layers** | The Deny is not redundant. An explicit Deny cannot be overridden by any later Allow, in this policy or any other attached to the same principal — so widening the Allow, or attaching `lake_read` alongside, still cannot expose L1/L2. Without it the guarantee depends on nobody ever doing either, which is a hope, not a guarantee |
| **S13-2** | **The Glue CATALOG is denied too, not just S3** | With only object access denied, a BI principal can still read L1/L2 schemas — column names and partition keys — which leaks structure and volume even when the objects are unreadable |
| **S13-3** | **The BI `ListBucket` grant is scoped by `s3:prefix`** | An unscoped listing enumerates L1/L2 object keys, leaking table names, partition dates and data volumes without reading a single object |
| **S13-4** | **Serving and raw layer lists are named ONCE (`local.serving_layers` / `local.raw_cdc_layers`)** | So the Allow and the Deny cannot drift apart — the same single-source-of-truth reasoning as S09-2 and S11-4 |
| **S13-5** | **`ops.quarantine` is denied to BI even though `ops` is a serving namespace** | Poison records contain raw payloads; it is a PII path as much as an operational one |
| **S13-6** | **Certified and provisional are SEPARATE views, not one view behind a status column** | One view is one careless filter away from a report that mixes provisional and certified numbers and presents the total as final |
| **S13-7** | **`v_account_balance_daily` is NOT pre-aggregated** | `closing_balance` is semi-additive (S10-8). A view that pre-summed it across dates would bake the wrong answer in where no downstream query could see or fix it |
| **S13-8** | **Partition pruning is proven with NEGATIVE cases, not asserted** | `CAST(business_date AS VARCHAR)` and `year(business_date)` both return the CORRECT answer and scan the whole table. Results stay right, so nothing prompts anyone to look — the assertion is the RATIO between pruned and unpruned scans |
| **S13-9** | **The benchmark records BYTES SCANNED as primary and latency as secondary** | Athena bills per byte. A query taking 8s and scanning 200 GB costs far more than one taking 40s and scanning 200 MB, and only one appears on the invoice |
| **S13-10** | **Power BI Import is the default; DirectQuery is a gated decision** | A 10-visual DirectQuery page refreshing every 15 min issues ~960 queries/day and bills every one; the same page in Import costs one scan per refresh and nothing in between. A dashboard nobody is looking at still costs money in DirectQuery |
| **S13-11** | **Incremental refresh window is 3 days, not 1** | Late events change an already-published date (§6.2, S08-10). A 1-day window would never re-read the day a late event landed in, so the dashboard would keep showing the pre-correction figure — and it would look STABLE, which is worse than looking wrong |
| **S13-12** | **`certified_at` for change detection, not `built_at`** | `built_at` moves on every run including no-op reruns, forcing refreshes of unchanged partitions and billing the scan for nothing |
| **S13-13** | **The Power BI ODBC DSN leaves the S3 output location BLANK** | The workgroup sets it and `enforce_workgroup_configuration = true` overrides the client. A DSN with its own location is either ignored or, on a misconfigured workgroup, writes unencrypted results outside the lifecycle rule |
| **S13-14** | **No Power BI gateway is created** | A gateway must run continuously to serve a refresh; a lab refreshing once a day would pay for it 24/7. Documented as a requirement, not deployed |

### No Redshift, no Trino — precisely

The plan creates **zero engine resources**: no `aws_redshiftserverless_namespace`, no
workgroup, no Trino cluster. It does create **7 IAM objects** named for them — roles,
instance profile, policy attachments — which pre-date this session, come from
`modules/lake_iam`, are **free**, and exist so the engines can be enabled later without an
IAM change. "Zero Redshift/Trino resources" would be imprecise; **"zero billable
Redshift/Trino resources"** is accurate.

Flags as required: `enable_athena=true`, `enable_redshift_serverless=false`,
`enable_trino=false`.

### Test results

**268 Python tests** (242 + 26 new). Four guards mutation-checked, including reverting
`athena_bi` to `lake_read` — the test refuses the original defect.

Two of my own tests were wrong first: one matched `'effect = "Deny"'` literally and failed
on correct code after `terraform fmt` aligned the `=`; the other stripped only FULL-LINE SQL
comments, so a trailing comment mentioning `dob` was scanned as code and reported as a PII
leak. Both fixed. Same class as Sessions 09/11/12 — a scanner that cannot tell code from
prose reports the documentation as the bug.

### Still NOT tested

**Everything requiring a deployed stack.** No Athena query has been executed, so: bytes
scanned per query, the actual pruning ratio, Iceberg time travel through Athena, the BI
role's denial in practice, and every step in `docs/POWERBI.md`. The acceptance criterion
"Power BI Desktop connection steps reproducible" is met by steps written to be followed —
they have not been walked, and claiming otherwise would be inventing a result.

### Not done, deliberately

No apply. No Redshift, no Trino. Session 14 not started. Stopped at the approval gate.


## Session 14 — governance, quality and lineage, 2026-08-15

### The defect

| ID | Finding |
|---|---|
| **D14-1** | **A masked VIEW is not a masked TABLE.** Session 13 denied BI the L1/L2 prefixes — necessary, not sufficient. `snapshot.banking_customer` and `mart.dim_customer` hold unmasked `full_name`/`dob` and sit under `warehouse/snapshot/*` and `warehouse/mart/*`, prefixes the BI policy ALLOWS in full. An Athena view requires read access to the UNDERLYING table, so `v_dim_customer_bi` masked nothing: a caller who can query the view can query the base table. **Session 13 stated this exact principle — "a view is not a security boundary if the caller can read the base table" — and then relied on a view over a readable table.** The principle was right; its application was incomplete |

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S14-1** | **`governance/catalog/domains.yml` created at the path ADR-028 already chose** | It was referenced twice by DATA_CONTRACTS.md §2 and named by ADR-028 but never existed — a dangling contract for six sessions. Created there rather than at a new path |
| **S14-2** | **Every governance artefact DERIVES from the registry, and tests assert it** | A registry nothing checks against is documentation, not governance. The DQ SLAs, the uniqueness keys, the IAM deny list and the masked tables are all checked against it |
| **S14-3** | **Three DQ verdicts — PASS, FAIL, NOT_EVALUATED — never two** | A check that finds nothing looks exactly like a check that passes. Zero duplicate rows means "no duplicates" if the table has data and means NOTHING if it is empty. A suite that reports green in the second case actively asserts health for a pipeline that did not run |
| **S14-4** | **An ERROR check blocks on NOT_EVALUATED as well as FAIL** | "We could not tell" is not "it is fine". Publishing on an unevaluated uniqueness check certifies a number nobody verified |
| **S14-5** | **A check that RAISES is NOT_EVALUATED, never a pass** | A missing table or bad predicate must not read as health |
| **S14-6** | **Validity counts NULL as invalid** | `NOT (amount > 0)` is NULL when amount is NULL. Treating that as passing means the rule silently exempts exactly the rows most likely to be wrong |
| **S14-7** | **WARN never blocks; ERROR blocks; neither ever DELETES** | Acceptance: bad data blocks the certified publish but does not erase audit history. Quarantine holds COPIES; L1/L2 keep the originals because they are the audit record and the replay source |
| **S14-8** | **`after IS NULL` on L1 is WARN, not ERROR** | A delete legitimately has a NULL `after`. Blocking would fail every day containing a deletion |
| **S14-9** | **Layer-boundary reconciliation runs at tolerance 0** | Both L1 and L2 keep every event, so a difference is a lost or duplicated record and must be explained, never rounded away |
| **S14-10** | **IAM-only fix for D14-1: explicit Deny on the PII tables + a MATERIALIZED masked copy** | Scope item 5 keeps the IAM baseline functional with Lake Formation off, so column-level security is unavailable. `mart.dim_customer_bi` is written by the Kimball build with the PII already tokenised — real data that never held the raw values |
| **S14-11** | **No Deequ, no Marquez, no Glue DQ, no Lake Formation** | Deequ adds a JVM dependency and version matrix for six checks that are a few lines of Spark SQL. Marquez is a 24/7 service; `ops.lineage_event` is an Iceberg table that needs nothing running. All recorded as flags with reasons (CLAUDE.md §4.10) |
| **S14-12** | **Lineage edges carry an `evidence` marker: observed / declared / dbt_manifest** | Debezium does not emit OpenLineage. Marking that edge `observed` would claim telemetry that does not exist — the graph would look complete with one link an assumption |
| **S14-13** | **Lineage emission cannot fail the job it observes** | A lineage backend outage that takes down the pipeline has inverted the priority |
| **S14-14** | **OpenLineage covers the SPARK chain; dbt lineage is REFERENCED, not rebuilt** | dbt already emits 53 nodes for its models (S11-3). A second system restating it would drift |
| **S14-15** | **OPEN-20: ISOLATE the 39 orphan Glue tables — do not adopt, do not delete** | Not adopt: unknown provenance, so adopting asserts metadata nobody verified. Not delete: not ours to assume, and destroying 39 catalogued tables is irreversible. Already outside every grant because roles are scoped to this project's databases by name. **Owner action still required; not silently closed** |

### A test-isolation bug this session introduced

The first full-suite run showed **263 passed / 56 ERRORS**. Not a product defect: pytest
runs every file in one process, `SparkSession.builder.getOrCreate()` returns the EXISTING
session, and the new plain (non-Iceberg) fixture — alphabetically earlier than the Iceberg
suites — decided the session config for the whole run. The Iceberg tests lost their `local`
catalog.

Fixed by configuring **every** Spark fixture identically, so file ordering is irrelevant.
Recorded rather than quietly re-run, because the latent fragility predated this session:
the suite has always depended on which file built the session first.

### Test results

**319 Python tests** (268 + 51 new). Four guards mutation-checked — G1/G2 are the same
defect from two directions (the policy that NOT_EVALUATED blocks, and the detection that an
empty table IS NOT_EVALUATED); either alone restores the vacuous-green failure mode.

### Still NOT tested

Everything requiring deployment: the DQ suite against real data, a single lineage event, the
IAM deny producing `AccessDeniedException`, and quarantine round-tripping. Nothing is
applied.

### Not done, deliberately

No apply. Session 15 not started.


## Session 15 — observability, SLO and DR, 2026-08-15

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S15-1** | **EXTEND the existing Prometheus; never stand up a second** | The toolbox already runs prometheus/grafana/alertmanager on the `kafka-observability` docker network with MSK scraping and (S04) EC2 discovery by `tag:Component`. `scrape-additions.yml` contains ONLY `scrape_configs`; a test fails if a `global:` or `alerting:` block appears, because that would replace the existing config rather than extend it |
| **S15-2** | **Pushgateway for batch SLIs, not scrape and not CloudWatch** | Prometheus scrapes long-lived processes; an EOD run lasts ~20 min and a 30s scrape either misses it or catches it mid-flight. CloudWatch custom metrics are billed per metric/month for ~20 series × 12 datasets. The Pushgateway is one more container on a docker network that already exists — no new instance, no new EBS |
| **S15-3** | **Every push emits `cdc_lakehouse_pushed_timestamp_seconds`, and staleness alerts key off THAT** | Pushed metrics are STICKY. A job that pushes `freshness=3min` and never runs again leaves Prometheus reporting 3 minutes forever: the pipeline is dead and every dashboard is green. This is the observability form of the vacuous DQ pass (S14-3) — absence of signal read as health. It is the only signal separating "healthy" from "gone" |
| **S15-4** | **Exactly TWO severities: page and ticket** | A "warning" nobody has decided how to react to is noise wearing a severity label. 5 page, 8 ticket, and a test enforces that pages stay the minority — if most alerts page, none of them do |
| **S15-5** | **CDC completeness targets 100%, and it is the only DATA slo that pages** | Every other target is a percentage because latency and availability are continuous. A reconciliation difference means an event was LOST OR DUPLICATED (CLAUDE.md §5.3): "99.9% complete" is not a service level, it is a defect budget for data loss |
| **S15-6** | **NRT freshness only TICKETS, despite being the headline SLO** | NRT output is explicitly PROVISIONAL (S09); certified numbers come from EOD and nothing downstream treats intraday values as final. Paging at 03:00 for a provisional latency target is how a pager gets ignored — and then the completeness page is ignored too |
| **S15-7** | **Every alert carries a `runbook:` anchor, and a test verifies the anchor EXISTS** | A link to a section nobody wrote is worse than no link: it looks like the procedure already exists |
| **S15-8** | **Consumer lag comes from a broker-side exporter, not client JMX** | `kafka_consumergroup_lag` is log-end-offset minus committed-offset computed BY THE BROKER. Client JMX reports what the client believes about its own progress — exactly wrong when the client is wedged, because a stuck consumer reports a confident, unchanging position |
| **S15-9** | **Availability measures SCRAPE TARGETS, not job success rates** | A pipeline that is not running produces no failures at all, and a "0 failures" metric would report 100% availability for a stack that is switched off |
| **S15-10** | **`sli:dq_pass_ratio` counts NOT_EVALUATED in the DENOMINATOR only** | Excluding it from both sides would let a pipeline that produced nothing report 100% DQ — S14-3 reappearing one layer up |
| **S15-11** | **NO destructive action against managed MSK, ever** | Acceptance criterion. MSK holds the only copy of in-flight events at 24h retention; deleting a topic or terminating a broker to TEST resilience risks the real data loss the drill exists to prevent, and AWS already tests broker failover. Drills 5 and 6 exercise CONSUMER-side replay, which is reversible |
| **S15-12** | **Observability never fails the pipeline it observes** | `push()` swallows transport errors and `main()` exits 0 even when the gateway is unreachable. A Pushgateway outage that fails the EOD run has inverted the priority — same reasoning as OpenLineage's `fail_on_error: false` (S14-13) |
| **S15-13** | **`honor_labels: true` on the Pushgateway scrape** | Without it Prometheus overwrites the pushed `job` label with the scrape job's name, every pipeline metric collapses into one series called "pushgateway", per-job alerting silently stops, and the dashboard still renders — with one line where there should be twelve |

### Recovery: 10 drills, 4 demonstrated

All ten ran in dry-run and wrote evidence files, including those recorded `NOT_RUN` — a
drill that could not run is a recorded fact, not a gap.

Four recovery **properties** were executed against real Spark and Iceberg with no AWS:
L3 rebuild determinism, late-event-wins-on-rebuild, replay idempotency, and L2
window/watermark semantics. That is not the same as rehearsing the operational procedure on
deployed infrastructure, and `docs/DR.md` marks the distinction explicitly.

### RPO / RTO

Extends the existing table in TARGET_ARCHITECTURE.md. **Kafka's 24h retention governs every
RPO**: inside the window recovery is a replay (RPO 0, every write idempotent); outside it
the events are gone and only a Debezium re-snapshot remains (RPO 24h).

Recovery is **re-derivation, not restoration** — L2 is a pure function of L1, L3 of L2, marts
of L3 — so there are no data backups above L1 by design. A backup can be stale; a
recomputation cannot.

### What the lab deliberately does not do

No multi-AZ Airflow, no MSK multi-region, no standby EMR, no cross-region replication, no
automated failover — each with its cost stated. **The lab survives component failure well
and does not survive region failure at all.** For a $30 portfolio budget that is the right
trade; for production handling real money it would not be, and DR.md says so.

### Test results

**347 Python tests** (319 + 28). Four guards mutation-checked.

### Still NOT tested

**No SLI has been measured.** Nothing is deployed: no Prometheus, no Grafana, no pipeline.
Every threshold is reasoned from the architecture (24h retention, the EOD schedule, the
10-minute target) and no alert has fired. RTO figures are estimates, not measurements — the
first real drill is when they stop being estimates.

### Not done, deliberately

No apply. No HA infrastructure. Session 16 not started.


## Session 16 — AI/RAG assistant (OPTIONAL), 2026-08-15

### The decision that framed the session

**Is AI worth building here at all?** Asked first, because "add AI" is a proposal that has to
earn its cost, not a requirement.

**Finding: most of the value is retrieval and citation, not generation.** "Who owns X",
"what is downstream of X", "which runbook covers ALERT" are lookups with exact answers in
artefacts Sessions 14-15 already produced. A language model in front of a dictionary adds
cost, latency and a hallucination surface to an already-correct answer.

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S16-1** | **Two tiers; tier 1 (lookup + BM25) is FREE and DEFAULT, tier 2 (Bedrock) is flagged OFF** | The assistant is useful with the LLM switched off. That is the test of whether the AI is load-bearing or decorative — and why this is not AI-for-CV: the capability survives deleting the model. Tier 2 exists only for synthesis across chunks, which is a real but narrow gain |
| **S16-2** | **Routing is DETERMINISTIC, not model-decided** | Routing by an LLM puts a paid call in front of every free lookup, including the ones whose entire point is that they need no model |
| **S16-3** | **No LangChain** | ~150 lines of retrieval code versus a large dependency tree and a version matrix to pin (CLAUDE.md §3.9). Same reasoning as no Deequ (S14-11), no dbt_utils (S11-8), no Marquez (S14-11). Revisit if retrieval becomes genuinely complex — reranking, multi-hop, agentic loops |
| **S16-4** | **No embeddings; BM25 over ~700 chunks** | Jargon-dense prose written by one team, questions reusing that vocabulary — the regime BM25 is strongest in. Embeddings bill twice (index and query) and add a vector store to operate. **Trigger to revisit recorded**: a corpus containing user-written free text, where lexical overlap breaks down |
| **S16-5** | **Chunk by markdown SECTION, not fixed character count** | A fixed window splits a runbook procedure across two chunks, and half a recovery procedure is worse than none |
| **S16-6** | **The security boundary is CODE on the model's OUTPUT, never prompt wording** | Retrieved documentation is untrusted input; a chunk saying "ignore previous instructions and DROP TABLE" is data the model reads. A system prompt is a request addressed to a component whose job is producing plausible text. **If the only thing stopping an action is that we asked nicely, it is not stopped** |
| **S16-7** | **Statement ALLOW-list, not a deny-list** | A deny-list is a list of the destructive verbs someone thought of; SQL keeps adding more (MSCK, ANALYZE, COMMENT ON, COPY INTO) |
| **S16-8** | **The table allow-list is DERIVED from `governance/catalog/domains.yml`** | The assistant inherits the BI role's access rather than having its own. A separate hardcoded list would be a second access-control policy drifting invisibly from the first. Consequence: the assistant cannot reference L1, L2, `mart.dim_customer` or `snapshot.banking_customer` |
| **S16-9** | **Corpus EXCLUSION is the primary secret control; redaction is secondary** | Not indexing `terraform/`, `artifacts/`, tfvars or tfstate catches everything in them; pattern redaction catches only what it recognises |
| **S16-10** | **Redaction patterns are ordered most-specific-first** | The bare 12-digit account-id pattern was consuming the digits inside an ARN, leaving a half-redacted string that still named the role. A real ordering bug, found by a test |
| **S16-11** | **Credential-test fixtures are assembled at RUNTIME** | The repo's credential scanner flagged the test file. Weakening the scanner to tolerate test data is how it stops catching the real thing; building the string at runtime keeps it strict. Verified the scanner still fails on a real literal |
| **S16-12** | **The project remains complete without `ai/`** | No pipeline code, DAG or dbt model imports it, and a test asserts that so the dependency cannot creep in later. `rm -rf ai/` removes the assistant and nothing else |

### Evaluation

14 golden questions, measuring **routing**, **groundedness** and **content** — not string
similarity to a reference answer, because a paraphrase citing the right section is correct
and a fluent answer citing the wrong document is not. Routing is scored separately: a free
lookup silently becoming a paid retrieval is a COST regression no accuracy metric notices.

**First run: 9/14**, exposing three real bugs — route ordering (lineage questions matched the
generic "explain X" pattern first), `find_runbook` comparing the whole question to an alert
name instead of searching within it, and a 600-char truncation cutting the answering
sentence. One golden question was itself wrong. **Now 14/14 at $0.00.**

### Mutation checking found that the REASONING was wrong

**A1 and A4 initially PASSED** — two of four guards were not independently verified.

- **A1**: disabling the statement allow-list left every test green, because the deny-list
  caught them all. Fixed with cases no deny-list would catch.
- **A4**: comment-stripping does **not** catch a write hidden behind a comment; the
  allow-list already does. Its real job is the opposite — without it the deny-list sees
  `DROP` inside a comment and refuses a valid `SELECT`. A guard with false positives gets
  disabled by whoever hits it.

Four sessions of mutation checking have each found something. This is the first time it found
that the *documented reasoning* was wrong rather than the code.

### Test results

**400 Python tests** (347 + 53).

### Still NOT tested

**Tier 2 has never been invoked.** No Bedrock call has been made; the generation path is
written and unit-tested for graceful degradation, not exercised. Its answer quality is
unmeasured, and the ~$0.002/question figure is arithmetic from published pricing, not an
observed bill.

### Not done, deliberately

No apply. No AWS resource created — there is nothing to destroy. Session 17 not started.


## Session 17 — FinOps, lifecycle and destroy verification, 2026-08-15

### Audit performed LIVE against the account, not derived from Terraform

| ID | Decision | Rationale |
|---|---|---|
| **S17-1** | **`show-cost-resources.sh` splits drivers into THREE categories, not "running / not running"** | RUNNING (stops when you stop it), STOPPED-BUT-BILLING (EBS, EIP, snapshots), ALWAYS-BILLING (S3, KMS CMK, NAT, logs). A lab that "stopped everything" and still bills is almost always category 2 or 3, and a flat inventory cannot show that |
| **S17-2** | **The scripts report QUANTITIES, never hardcoded prices** | Acceptance: no fixed price promise. A price embedded in a cost tool goes stale silently, and a stale price in a cost tool is worse than no price. Rates stay in `docs/PRICE_REFERENCE.md` with a collection date |
| **S17-3** | **`stop-ephemeral.sh` can only STOP; it can never delete** | Stop and destroy have different blast radii. Keeping them in separate scripts means a daily shutdown cannot become a teardown by a typo, and the destroy path keeps its typed confirmation |
| **S17-4** | **MSK is deliberately excluded from the daily shutdown** | There is no "stop" for a managed MSK cluster — you keep it or delete it, and deleting loses in-flight events at 24h retention. That is a destroy decision with a data consequence, not a nightly habit |
| **S17-5** | **`verify-destroy.sh` checks SERVICE APIs, not `terraform state`** | Terraform reporting success means it removed what was in its state. It says nothing about resources created outside Terraform, asynchronous deletion (MSK takes minutes), or deliberately retained resources. Only the API answer appears on a bill |
| **S17-6** | **Retained-on-destroy resources are listed explicitly as non-failures** | S3 lake (`force_destroy=false`), the state backend (destroying it strands everything else), KMS keys (7-30 day waiting period, still listed), and the OPEN-20 orphan Glue database. A verify script that flagged these as failures would train people to ignore it |

### Findings

**This project costs $0.00/month.** Sessions 02–16 have never been applied; zero resources
carry `Project=kafka-dev-lab`. Compute is zero across MSK, EC2, EMR Serverless, EKS, RDS and
Redshift, and there are no NAT gateways, Elastic IPs, EBS volumes or VPC endpoints.

**The account costs ~$0.35/month** — 44 S3 buckets holding 13.67 GB from prior work
(YouTube analytics, Redfin, Snowflake tests, demos). Reported because a bill does not
distinguish between projects.

### Two alarms I raised and then corrected by checking

1. **"5 KMS keys = $5/month" — wrong.** All five are **AWS-managed** (`alias/aws/*`), which
   are free; only customer-managed keys bill. Verified via `KeyMetadata.KeyManager` after
   raising it. The script now prints the key manager explicitly so nobody repeats the error.
2. **"44 buckets" — real, but not ours.** The CDC lakehouse bucket does not exist, which
   independently confirms nothing was ever applied.

### The one actionable finding

**All 3 CloudWatch log groups have `retentionInDays = null`** — they never expire. ~1.5 MB
today and belonging to *other* projects (`/aws-glue/crawlers`, `/aws/codebuild/airflow-*`).
The command is documented and **deliberately not run**: changing another project's retention
is the owner's call, not this session's.

### Guardrails verified live

Two AWS budgets already exist ($30 monthly, $1 zero-spend — matching ADR-030). Flags are
`enable_athena=true`, `enable_redshift_serverless=false`, `enable_trino=false`, and the plan
creates **0 billable** Redshift/Trino resources (7 free IAM objects remain, which is the
precise rather than the flattering statement).

### Not done, deliberately

**Nothing was stopped or destroyed** — there was nothing running. No mutating AWS command was
executed. Session 18 not started.


## Session 18 — portfolio packaging, 2026-08-15

### Closed

| ID | Decision | Rationale |
|---|---|---|
| **S18-1** | **A four-label capability matrix is the FIRST document in the repository** | LIVE TESTED / IMPLEMENTED / DESIGN ONLY / OPTIONAL are grades of EVIDENCE, not quality. A README that describes what the code does without saying what has run is the standard way a portfolio project misleads — usually without meaning to |
| **S18-2** | **"Never deployed" is stated in the README's first paragraph, not buried** | It governs every other claim. Putting it late would make the reader discover it after forming an impression, which is the definition of misleading by structure |
| **S18-3** | **No throughput, latency, volume, user-count or uptime figure appears ANYWHERE** | Nothing was measured. Not softened, not estimated — absent. A swept grep confirms the only matches are inside the explicit "lines to avoid" table |
| **S18-4** | **Every number in the portfolio docs was re-verified before publishing** | 400 tests, 166 decisions, 22 ADRs, 11 modules, 8 DAGs — each re-counted, not quoted from an earlier session. A stale number in a CV is a question you cannot answer |
| **S18-5** | **The demo is split: Part A DESIGN ONLY, Part B runs today for $0.00** | The end-to-end demo cannot run — there is no MSK, no connector, no EMR. Presenting it as runnable would be the exact fabrication this session forbids. Part B exercises the layer that actually carries the correctness guarantees |
| **S18-6** | **CV_BULLETS.md includes an explicit "lines to avoid" table** | The tempting bullets ("processed X TB/day", "99.9% uptime") are listed with why each fails. A candidate who writes one cannot defend it, and the interview ends there |
| **S18-7** | **The interview guide answers "did you deploy it?" with "no", and treats that as the strong answer** | Every session ended at a spend gate rather than self-approving. That discipline is a more senior signal than an unverifiable deployment claim |
| **S18-8** | **The STAR story is about finding a defect behind a green test suite** | The most senior-signalling material in the project is not what was built but what the testing caught: a vacuous test, two unverified guards, and one case where the documented reasoning was wrong |

### What the labels resolved to

- **LIVE TESTED** — the transformation logic (real Spark + real Iceberg, not mocks), the dbt
  build, the FinOps account audit, four recovery properties, AI tier 1.
- **IMPLEMENTED** — DAG structure and policy, the governance registry, serving views, SLIs.
- **DESIGN ONLY** — **all** infrastructure, the **entire** CDC plane, every runtime
  behaviour. Nothing has processed a real CDC event.
- **OPTIONAL** — Redshift, Trino, Marquez, Lake Formation, Glue DQ, AI tier 2.

### Not done, deliberately

No apply. Session 19 not started.


## Session 19 — final acceptance test, 2026-08-15

### The result

**The end-to-end pipeline acceptance test could not be executed.** Nothing is deployed —
verified live: 0 MSK, 0 EC2, 0 EMR, 0 tagged resources, no Terraform state.

| ID | Decision | Rationale |
|---|---|---|
| **S19-1** | **Deployment-dependent criteria are BLOCKED, never NOT_TESTED** | BLOCKED means a specific named prerequisite is unmet; NOT_TESTED means testing was possible and did not happen. Calling a blocked item NOT_TESTED implies negligence; calling it PASS is a lie. The distinction is the whole point of the four labels |
| **S19-2** | **Schema evolution was BLOCKED for the WRONG reason — found and fixed** | It appeared in the design (registry compatibility, contract-change ledger) but had **no executable test at all**. The lake's tolerance for schema change needs no registry, broker or AWS: it is a property of the projection, which runs on local Spark. 5 tests added, mutation-checked |
| **S19-3** | **The projection TOLERATES schema change; the DQ gate REFUSES to certify it silently** | Confirmed by the new tests. A dropped column becomes NULL rather than crashing the build — one dropped attribute must not stop a day's pipeline — and the completeness check then fails and blocks the publish. Neither half is sufficient: tolerance without detection is silent data loss; detection without tolerance is a brittle pipeline |
| **S19-4** | **Verdict READY_FOR_PORTFOLIO, explicitly NOT "validated end-to-end pipeline"** | Two different claims. The portfolio claim is supported by 405 tests, 166 decisions and honest labelling; the pipeline claim is not supported at all, and the report says so in its first section rather than its last |

### Final tallies

- **Data scenarios: 11 PASS · 3 BLOCKED · 1 PARTIAL · 0 FAIL** (was 10/4/1 before the fix)
- **Correctness criteria: 9 PASS · 4 BLOCKED · 2 PARTIAL · 0 FAIL**
- **405 Python tests** (400 + 5 new), `make check` 14 passed

### No failures were hidden

Zero criteria are FAIL because every test that runs, passes. The honest weakness is not
failure — it is **absence of execution**: the entire CDC plane, every runtime behaviour and
every performance figure remain unexercised.

### Not done, deliberately

Nothing destroyed. **No spend approved** — no session has self-approved, including this one.

## 2026-08-22 — Session 34

- **Layer model is a FAN-OUT, not a chain.** FULL_CDC is canonical and written directly
  from Kafka; REALTIME and EOD are sibling derivations. Rationale: EOD must be a
  deterministic function of FULL_CDC and a cutoff, or a FULFILL of a historical date is not
  reproducible. An intermediate layer would have to be reconstructed to its state on that
  date, and nothing does that.
- **`kafka_dev_lab_dev_stream` is REALTIME**, not a landing zone. Physical databases are
  NOT renamed — they are deployed and hold data; `layers.yaml` carries the mapping.
- **`FULL_CDC_RAW` keeps its enum member but loses its binding.** There is no raw landing
  database any more. The member survives so `resolve()` refuses the layer BY NAME; deleting
  it would turn a clear ADR-033 prohibition into a KeyError.
- **Two identities on every FULL_CDC row.** `dv_event_id` (Kafka coordinates + broker
  timestamp) answers "same delivered record" and is the MERGE key; `dv_src_event_id`
  (source coordinates only) answers "same source change" and survives a cluster rebuild.
  Deliberately NOT the merge key — collapsing on it would erase a legitimate redelivery.
- **FULFILL stamps RECONCILED.** Previously CERTIFIED or PROVISIONAL_CORRECTED; both were
  wrong. RECONCILED sits above a correction and below a normally-closed day.
- **EMR logs go to S3, not CloudWatch.** The application runs in private subnets with no NAT
  and no `logs` endpoint; the S3 gateway is free and reachable.
- **Stuck executions are finalised FAILED, never SUCCEEDED.** A run whose outcome was never
  confirmed must not advance a watermark, even when its Spark job demonstrably succeeded.

## 2026-08-22 — Session 34, Phase 15

- **Failure semantics are tested in-process, not by breaking live AWS.** The properties are
  properties of the framework, and the most important window (post-commit, pre-finalisation)
  cannot be aimed at with a real engine failure. Live evidence from Phase 14 is
  cross-referenced for the three cases that produced it naturally.
- **A run whose outcome was never confirmed is FAILED, never SUCCEEDED.** Codified as the
  operator recovery for cases 9 and 10 after Phase 14 produced four such records.
- **An ERROR on a streaming deployment is not a RESTART.** Conflating them makes a
  crash-looping stream look like a healthy one that has restarted a lot (case 15).

## 2026-08-25 — Session 39, AI platform discovery and prompt pack

- **The AI platform extends `ai/`; it does not replace it.** Session 16 already built a
  working two-tier assistant — deterministic router, output-side guards, allow-list derived
  from the governance registry, and an eval harness that scores routing separately. Re-run
  live: 14/14, routing 14/14, $0.000000. Discarding a working, evaluated, $0 capability
  would need justification, and none of the four planes requires it.
- **LangGraph yes, LangChain no.** `docs/AI_USE_CASE.md` §2 rejected LangChain and named its
  own revisit trigger — "agentic tool loops". The approved architecture asks for exactly
  that, so the trigger has fired. This follows the recorded decision rather than reversing
  it. `AI_USE_CASE.md` is a session document, not an ADR, which is why it could be revisited
  at all — but its reasoning still governs retrieval, where nothing changed.
- **Embeddings stay conditional.** The corpus is still one team's technical prose, so
  `AI_USE_CASE.md`'s stated trigger for embeddings has NOT fired. S3 Vectors is the selected
  managed target, but it ships only if hybrid beats BM25 on a golden set expanded to ≥50
  questions. 14 questions cannot resolve a delta; 14/14 against 14/14 proves nothing.
- **DynamoDB over SageMaker Feature Store for the online store, and off by default.** $0
  idle and a pattern ADR-036 already proved in production here. No consumer in this project
  needs sub-100 ms lookup, and building an online store before a consumer exists is the
  failure the policy is meant to prevent.
- **The ML pilot is a forward-window label, not anomaly detection.** Anomaly detection is
  unsupervised and has no label horizon, so it would never exercise the leakage boundary the
  architecture makes mandatory. A 30-day forward label makes leakage possible and therefore
  makes the leakage tests meaningful. The pilot exists to prove the pipeline, not to score
  well.
- **Three blockers are facts, not opinions.** `s3vectors` and `bedrock-agentcore-control` are
  absent from botocore 1.35.79 while the AWS provider is pinned exactly at 6.56.0; Titan
  embeddings are not offered to this account in ap-southeast-1 (only Cohere), so a Bedrock
  KB assuming the usual default would fail at apply; and the repository has **no Python
  dependency manifest at all**, which `CLAUDE.md` §3.9 forbids and which blocks AI-P8.
- **Metrics fail open; the audit record fails closed.** Observability observes the work, so
  its outage must not break the agent — the same rule `openlineage.yml` already applies. But
  the audit row is part of the control, so an action that cannot be audited must not proceed.
- **One phase mutates AWS.** AI-P13, against a saved plan the operator approved by hash.
  Every other phase is static, local or plan-only, so cost and blast radius are reviewable
  before anything exists.

## 2026-08-26 — Session 40, rebuild after destroy: seven defects

- **`chown`, not `chmod`, for the Connect secrets.** The file is `0600 root`; Connect runs
  as `uid=1000`. Widening the mode to `0644` would have made the Oracle and SQL Server CDC
  passwords readable by every local user on the host. Ownership gives exactly one reader
  and keeps `0600`.
- **`cdc_reader`, not `db_owner`, for the SQL Server CDC user.** Debezium calls
  `sys.sp_cdc_help_change_data_capture`, which needs the capture instances' gating role —
  which is `cdc_reader`, the role `02-enable-cdc.sql` already sets. Verified sufficient by
  granting `db_owner`, confirming, then dropping it and re-testing. `db_owner` would hand a
  read-only CDC principal full control of the database for no extra capability.
- **Exactly one Oracle JDBC driver on the plugin path, enforced by a hard failure.** The
  Debezium 2.7 Oracle archive ships its own `ojdbc8` alongside the `ojdbc11` this repo
  installs. Two drivers in one plugin directory means the classloader picks arbitrarily,
  and when `ojdbc8` won, its O5LOGON against Oracle 23ai failed as **`ORA-01005` — null
  password** for a password that was provably correct. `verify-artifacts.sh` now deletes the
  bundled jar and aborts unless one remains, because a silent nondeterministic pick is worse
  than a build error.
- **Verify the capability, not the login.** The SQL Server health check tested `SELECT 1`,
  which any user who can open the database passes — including one with no CDC rights, which
  Debezium then rejects. It now calls the exact procedure Debezium calls, so a pass here
  means the connector passes too.
- **Reconcile principals explicitly instead of re-running an init script with its output
  discarded.** `-i 02-enable-cdc.sql >/dev/null 2>&1 || true` made a failed login creation
  indistinguishable from success. A server login with no database user is the specific trap:
  it authenticates, then fails with "Cannot open database", which reads as a wrong password.
- **A tainted-but-healthy instance is untainted, not rebuilt.** An `AuthFailure` during a
  33-minute apply left the Airflow node created, in state, and fully booted — k3s active and
  every pod Running — but tainted. Destroying a verified-working node to satisfy Terraform's
  bookkeeping costs a rebuild and gains nothing.
- **Updating SSM is not a rotation.** Both nodes read SSM only at boot, so a `put-parameter`
  leaves the old secret live everywhere that matters. Recorded in the runbook with the
  three-way md5 check that proves whether a rotation actually landed.

## 2026-08-26 — Session 40 (cont) — defects 8 and 9

- **Resolve a dependency chain, do not guess it one exception at a time.** The Apicurio
  converter needs `converter -> avro-serde -> serde-common -> schema-resolver -> client ->
  rest-client-jdk`. Fixing only `serde-common` moved the failure to the next missing class.
  The chain was resolved with `mvn dependency:copy-dependencies` against the converter POM
  and all 11 Apicurio-side jars pinned with SHA256.
- **Pin the chain, not the whole tree.** `kafka-clients`, `connect-api`, `connect-json`,
  `avro`, `jackson-*`, `slf4j` and the compression jars are deliberately NOT staged: Connect
  ships them, and a second copy in a shared plugin directory is a version-skew bug waiting
  to happen.
- **The build asserts the class, not the file list.** `verify-artifacts.sh` now fails closed
  unless `AbstractKafkaSerializer` is present in some staged jar. A missing dependency is a
  build error instead of a connector that reports RUNNING and moves no data (risk R15).
- **`reset` must STOP before deleting offsets.** Connect 3.6+ returns
  `400 Connectors must be in the STOPPED state`, so the old `DELETE /offsets` silently did
  nothing and the follow-up register returned 409 — a reset that reset nothing. `stop` is
  not `pause`: pause leaves the task assigned and the delete still fails.
- **`RUNNING` is not `flowing`, and the tool that says so must be the right one.**
  `kafka.tools.GetOffsetShell` moved package in Kafka 3.x; the old class raises
  `ClassNotFoundException` and a pipeline summing its output prints **0**. That produced a
  false "no data" reading twice while the snapshot had in fact completed with 2,529 records.
  The runbook now names `kafka-get-offsets` and records the expected first-run counts.

## 2026-08-26 — AI-P1, AI metadata / contract foundation

- **Contracts live in `aiplatform/`, not `ai/` and not `reporting/`.** `ai/` is deletable —
  no pipeline code imports it, and a test asserts that. Putting feature or ops contracts
  under `ai/` would force `spark/features/*` to import it and quietly destroy that property.
  `reporting/` is keyed on the five flow modes, which a knowledge source and an agent tool
  do not have. The reporting *pattern* is reused exactly; the tree is not.
- **Versions are content hashes, never counters.** A hand-incremented version is a field
  someone forgets to bump, and a stale version is worse than none because downstream trusts
  it. `generated_at` is excluded from `plan_hash`, or every compile would be a new version.
- **Both enums fail closed.** An unknown or absent classification resolves to RESTRICTED, so
  an unlabelled chunk is withheld rather than served. An unknown sensitivity is a reason to
  withhold, not to share.
- **The label-horizon test is the one that matters.** `feature_event_time <= label_event_time`
  does NOT catch a feature drawn from a forward label's outcome window — such a feature
  satisfies the inequality while encoding the answer. That is why the pilot is a
  forward-window label rather than anomaly detection: an unsupervised pilot has no horizon
  and would never exercise the boundary the ADR makes mandatory.
- **`created_at` is rejected as a feature and as an event-time column, in code.** Processing
  time diverges from event time under backfill and late arrival; a comment saying "do not
  join on this" is not a control.
- **`ops.feature_materialization_run` is declared but NOT created.** If feature groups
  register as reporting jobs, `ops.job_master_execution_hist` already records every
  materialisation with the same correlation ids. AI-P5 must justify a second table or reuse
  the first. Every ops table names its consumer, and the contract refuses one that does not.
- **Auditing cannot be disabled on a tool.** Metrics may fail open because they observe the
  work; the audit row is part of the control, so an unauditable action must not proceed.
- **`requirements.txt` records what is already installed.** AI-P1 clears the §3.9 blocker by
  pinning the four packages in use — it does not upgrade anything, because an upgrade is its
  own reviewed change.

## 2026-08-26 — AI-P2, RAG knowledge publisher

- **Allow-list the sources; do not walk-and-exclude.** A walk fails open — a new directory
  is indexed until someone remembers to exclude it. A registry fails closed. For a corpus a
  language model reads back to operators, failing closed is the only defensible default.
- **Quarantine whole documents; never redact-and-publish.** Rewriting the match and
  publishing the rest is a bet that the pattern list is complete, and it is not. An explicit
  refusal naming the file and the rule beats a silent leak.
- **The scanner must not quarantine documentation ABOUT secrets.** This corpus is mostly
  security prose; a scanner that cannot tell `${file:...}` or `$(DBZ_PASSWORD)` from a
  credential withholds the documents operators most need. Nine detection rules and six
  exemption contexts, all tested.
- **Chunk by structure, not by character count.** A fixed window splits a runbook procedure
  in half, and retrieval then returns step 4 of 7. Fenced code is never split — half a
  command is not a command — and a `#` comment inside a fence is not a heading.
- **One dbt document per model, never the manifest as a blob.** 688 KB as one chunk matches
  everything and answers nothing. Models with no description and no documented columns are
  skipped: a stub dilutes retrieval and teaches nothing the SQL does not already say.
  `refs`/`sources` are recorded as declared inputs and the text says so, because the
  authoritative execution order is the compiled plan.
- **`corpus_version` is a content hash, and a dirty tree is recorded not hidden.** Excluding
  `generated_at` from the hash is what makes two builds of the same tree identical, which is
  what makes "re-embed only what changed" decidable in AI-P3 instead of a guess.
- **Two silent-failure bugs found by the tests, not by reading.** `lstrip("./")` strips a
  character set rather than a prefix, so `.git/config` became `git/config` and dodged the
  root exclusion. And a bare `secret` path pattern excluded `ADR-031-secret-store.md` — an
  ADR operators need — while excluding nothing that held a credential.
- **The repo's credential scanner was narrowed, not weakened.** It was reading
  `.pytest_cache`, where pytest stores parametrise ids verbatim, so a case named
  `aws_access_key_id` read as a finding. Generated caches are now skipped; a planted secret
  in a real source file still fails the check, verified by probe.

## 2026-08-26 — AI-P3, RAG infrastructure (plan only)

- **Verify the provider schema, not the CLI.** B1 was carried through two phases as "S3
  Vectors cannot be addressed". The CLI gap was real; the provider was never blocked —
  6.56.0 ships `aws_s3vectors_*`, `aws_bedrockagent_knowledge_base` with an
  `s3_vectors_configuration` backend, and `aws_bedrockagentcore_agent_runtime`. Infrastructure
  capability is a property of the provider, and checking the wrong tool nearly forced an
  unnecessary substitution.
- **The approved path was implemented, not substituted.** Seven of the eight KB storage
  backends are always-on and priced per hour; against ADR-030 they are disqualified by
  arithmetic. `s3_vectors_configuration` is the eighth and the reason S3 Vectors was chosen.
  OpenSearch Serverless was never planned.
- **Pin the embedding model AND its measured dimension.** Titan is unavailable to this
  account in ap-southeast-1, and Bedrock KBs commonly default to it — that default would
  fail at apply. The module validates that the model is Cohere, and `embedding_dimension`
  is 1024 because an invocation returned 1024, not because a document said so.
- **Distance is converted to similarity at the backend boundary.** A higher score always
  means better regardless of backend; mixing conventions is how a fusion silently inverts
  its own ranking.
- **Hybrid degrades to BM25 on any dense failure, and never raises.** The catch is
  deliberately broad: the property being protected is that the assistant answers correctly
  with the vector index absent, unreachable, or flagged off. The degradation is visible in
  `backend` on every returned chunk rather than hidden.
- **Sync idempotence comes from the content-addressed corpus_version.** Re-syncing an
  unchanged corpus is decidably a no-op — objects already exist, no ingestion job starts —
  so a retry is not a second bill. Tested with injected clients that assert no upload.
- **`data_deletion_policy = RETAIN` and `force_destroy` left false.** The embedded vectors
  are the expensive artefact; destroying them must be a deliberate act, not a side effect.
- **Stopped at plan.** The repository has a dedicated apply phase (AI-P13) and bypassing it
  to "just apply a small module" would remove the review gate that exists for exactly that
  argument.

## 2026-08-26 — AI-P4, RAG evaluation

- **Ground the dataset in the corpus, and assert it.** Every `expect_source` must exist and
  every `key_facts` string must appear in that document, checked by test. Without that a
  golden set drifts into questions whose answers were invented, and it then measures the
  author's memory rather than the retriever.
- **Paraphrased questions are the only ones that can decide ADR-049.** BM25 is strongest
  exactly where question and corpus share vocabulary, so a set written in house jargon
  flatters it and resolves nothing. Measured: recall@5 **0.755 native vs 0.250 paraphrased**.
  That 50-point collapse is the evidence for embeddings, and it is asserted by a test so it
  cannot be quietly lost.
- **A score threshold was swept and REJECTED on the data.** The unanswerable "Istio mesh
  configuration" question scores 13.19 — above most genuine questions — because Kubernetes
  vocabulary really is in this corpus. Any cutoff that suppresses it destroys real answers,
  so `min_score` stays 0.0. Telling "we don't have this" from "we have something wordy about
  this" needs semantics, not a threshold. A tuning change that the measurement does not
  support is not a tuning change.
- **Retrieval and generation are scored separately.** A fluent answer from the wrong chunk
  and a terse one from the right chunk look similar end-to-end, and only one is fixable by
  changing the retriever. Generation is AI-P10's problem, after an agent exists.
- **The gate floor is recorded from a measured baseline, never chosen.** `--gate` refuses to
  run at all when no baseline exists rather than inventing a threshold: a gate set above
  what the system has ever achieved fails on day one and gets disabled, which is how a
  quality gate stops existing.
- **Source correctness and groundedness are kept distinct.** The right document with the
  wrong content is still wrong, and collapsing them hides which half broke.
- **A backend error is a RESULT, not a crash.** The evaluator records `error_rate` instead
  of aborting, so a partial outage produces a score with a visible hole rather than no score.

## 2026-08-26 — AI-P5, feature platform

- **Source policy is enforced in code, not documented.** `resolve_source_layer` refuses
  FULL_CDC and REALTIME for batch features. A batch job reading the change stream would make
  the feature store a second interpretation of those events, and two interpretations of the
  same events is exactly what ADR-052 exists to prevent.
- **`materialization_run_id` is stamped on every row but excluded from the merge key.**
  Including it would make every rerun insert new rows and destroy the idempotence the run id
  exists to audit. The merge key is the grain — `(entity_id, feature_event_time,
  feature_version)` — and nothing else.
- **A feature version bump writes a distinct row rather than overwriting.** Point-in-time
  reconstruction of an older training set depends on the old values still being there; an
  in-place update would make yesterday's model unreproducible.
- **The point-in-time join is a LEFT join.** A label with no feature must survive with
  nulls. An inner join silently shrinks the training set and moves the class balance, which
  shows up as a model that scores differently for no visible reason.
- **The online store default RAISES rather than returning None.** A caller that assumed an
  online store exists should find out at the call site, not silently receive no features and
  score against defaults.
- **The DynamoDB adapter's put is conditional on `feature_event_time`.** Out-of-order
  arrival is normal; a late write silently regressing a newer value is not.
- **Lineage reports what it cannot resolve, and that immediately caught a real defect.**
  The pilot's three references resolved as `false` because the resolver indexed dbt models
  only, while `fact_transaction` and `fact_account_daily_snapshot` are dbt SOURCES. A
  resolver that knows half the graph reports the other half as broken — worse than reporting
  nothing, because it looks like a registry error. Fixed and regression-tested.
- **The DDL is generated from the registry.** A hand-maintained schema beside a
  hand-maintained contract is two things to keep in agreement, and only one of them is tested.

## 2026-08-26 — AI-P6, ML pilot

- **The label is synthetic because the data says so, and the pilot says so too.**
  `txn_count`, `debit_amount` and `credit_amount` each have exactly ONE distinct value, and
  only 2 of 320 accounts have a balance that changes across the 4 available dates. A churn,
  activity or anomaly label on that is degenerate. Inventing one and reporting a metric
  would have produced a number with no meaning presented as a result.
- **AUC 1.0 is reported as meaningless.** The synthetic label is a threshold on a feature
  and balances barely move, so today's balance nearly determines tomorrow's tier. The metric
  is evidence the pipeline works end to end; it is not evidence of predictive skill, and the
  artifact carries `synthetic_label: true` so no downstream consumer can mistake it.
- **A single-class label is refused, not trained on.** A model fit to one class predicts the
  majority and scores perfectly; that is not a model.
- **numpy, not scikit-learn.** Pinning a large ML stack to train 960 rows is a real
  dependency decision — a version matrix and a supply-chain surface — taken to avoid writing
  thirty lines. If a later model genuinely needs sklearn, that is an ADR with a pinned
  version, not a side effect of this pilot.
- **Missing or null features are REFUSED at inference, never imputed.** An imputed feature
  produces a confident score from data that was never supplied, and nothing downstream can
  tell the difference between that and a real prediction.
- **Catalog recovery needed two fixes, both silent-failure shaped.** `register_tables.py`
  defaults to catalog `glue_catalog` while the EMR session configures `spark_catalog`; and
  the reporting role could not decrypt the LEGACY lake CMK that encrypts the pre-rebuild
  data. The second is now an inline IAM policy and is DRIFT from Terraform — recorded so it
  is removed when the old key is retired, rather than quietly becoming permanent.

## 2026-08-26 — AI-P7, safe read-only tool layer

- **One entry point, so the controls are structural.** `invoke()` is the only way to call a
  tool, and it validates input, enforces the timeout, caps the result and writes the audit
  row. If each tool did its own, adding a tool would mean re-deciding all four, and the one
  that forgets is the one that gets exploited.
- **The audit row is written on FAILURE too.** An action that cannot be audited must not
  proceed, so rejections, denials, timeouts and backend errors all produce a record. An
  audit that only covers the happy path documents the cases nobody needed to investigate.
- **The audit stores an input HASH, never the input.** A question can contain a customer
  name; its hash cannot.
- **The LIMIT is re-validated after injection.** A guard that checks the input and then
  rewrites it has validated a string that is no longer the one being run.
- **Timeout CANCELS the query rather than abandoning the wait.** An abandoned Athena query
  keeps scanning and keeps billing.
- **Unqualified table references are refused.** `SELECT 1` with no schema resolves against
  whatever the session default happens to be, and that is not a decision this tool may leave
  to chance.
- **Oversized results are refused, not truncated.** Silent truncation hands the model a
  partial answer it cannot see is partial.
- **Three candidate tools were left OUT rather than stubbed.** Reconciliation tables are
  empty, `feature_offline` is not materialised, and the only model carries a synthetic
  label. A tool that answers plausibly from nothing is worse than a missing tool, because
  neither the model nor the reader can tell.
- **A package name collision broke the working assistant, and only the old suite caught it.**
  `ai/tools/` shadowed `ai/tools.py`, so Session 16's `tools.call` resolved to the new
  package. Every new test passed while `ai/eval/evaluate.py` failed with AttributeError.
  Renamed to `ai/agent_tools/`, with a test asserting the directory does not come back.

## 2026-08-26 — AI-P8, LangGraph copilot

- **The model never selects a tool and never supplies an argument.** A deterministic router
  picks the intent, the intent maps to a fixed plan, and code extracts arguments from the
  question. There is no edge from the model back into tool selection, so "ignore previous
  instructions and query the raw CDC table" is text the router pattern-matches and refuses,
  not an instruction it obeys.
- **Plurals are not a stylistic detail in a deny pattern.** `\boffset\b` failed to match
  "offsetS", so "reset Kafka offsets" routed to KNOWLEDGE — a mutation request classified as
  a documentation question. Every noun in the deny pattern now carries `s?`, and the routing
  test covers plurals explicitly.
- **PREDICTION is ordered before PIPELINE_OPS.** "What is the status of the trained model"
  matches both, and a model question answered by the pipeline tools returns a watermark
  nobody asked for.
- **Generation degrades, never fabricates.** A model outage falls back to the tier-1 answer
  assembled from tool output and records why. The answer is less fluent and equally correct.
- **Tier 1 works with LangGraph uninstalled**, asserted by a test that compares both paths.
  That is the test of whether the framework is optional or load-bearing.
- **LangChain is not adopted.** `langchain-core` arrives only as a langgraph dependency; no
  business logic imports it, matching ADR-055.
- **A LangGraph node may not be named after a state key.** `answer` was both a node and a
  field and failed at compile. Renamed `compose`: the node is the step, the field is the
  result.
- **Bedrock access was VERIFIED, not assumed — and it is currently broken.** Anthropic chat
  needs a use-case form; Cohere embeddings now fail with INVALID_PAYMENT_INSTRUMENT despite
  working during AI-P3. Both are operator actions on the account. The architecture survives
  it because the deterministic path needs no model.

## 2026-08-26 — AI-P9, managed agent runtime

- **The capability objection to AgentCore was withdrawn; the requirements objection stands.**
  ADR-056 deferred it partly as "cannot be expressed today", which AI-P3 disproved. Removing
  a wrong reason strengthens the decision: it now rests on the fact that AgentCore's managed
  sessions, managed identity and per-session isolation have no consumer in a single-operator
  lab with no identity provider.
- **Nothing was deployed, and that is the finding.** Bedrock cannot be invoked from this
  account — Anthropic needs a use-case form, Cohere returns INVALID_PAYMENT_INSTRUMENT.
  Provisioning a hosted runtime whose entire purpose is to call a model, while no model can
  be called, buys cost and attack surface for something that cannot function. That applies
  to Lambda as much as to AgentCore.
- **The role is the boundary, not the network.** The function is deliberately given no
  `vpc_config`: it needs Bedrock, Athena, Glue and S3, all public AWS endpoints, so a private
  subnet would require a NAT Gateway (forbidden by CLAUDE.md 4.2) to gain nothing.
- **`reserved_concurrent_executions` is a COST ceiling, not a performance setting.** Without
  it a loop or a burst invokes without bound, and each invocation may call a paid model.
- **Log retention is set explicitly.** "Never expire" is the default and it is a cost driver.
- **IAM is asserted from the Terraform source, not from a deployed policy.** A policy
  assertion that only runs after apply is one that never gates the apply.
- **An assertion that forbade the string `aws_bedrockagentcore` was wrong.** It caught the
  comment explaining the deferral and would have pushed that reasoning out of the file. The
  test now forbids a RESOURCE declaration and requires the reasoning to be present.

## 2026-08-26 — AI-P10, agent evaluation

- **Injection is tested where it actually arrives: in retrieved content.** A scenario that
  types "ignore previous instructions" into the question only proves the router refuses a
  hostile question. The realistic attacker edits a DOCUMENT. The poisoned-chunk test asserts
  a malicious retrieved chunk produces no second tool call — which it cannot, because the
  plan is fixed by the router before retrieval runs and the model never selects a tool.
- **A P0 is not a percentage.** Any scenario allowing a mutation fails the gate outright
  rather than being averaged away by good behaviour elsewhere. A refusal regression is a
  security incident, not a quality dip.
- **`arguments_valid` must distinguish REJECTED from ERROR.** A tool that errors because the
  data does not exist is telling the truth; counting that as a bad argument blames the agent
  for reporting missing data. Only input-validation rejection counts.
- **My scenario expectation was wrong and the router was right.** A DQ question routes to
  PIPELINE_OPS, whose plan already includes the DQ tool. When an evaluation disagrees with
  the system, the evaluation is as likely to be wrong as the system, and checking which is
  part of the work.
- **The suite touches live Athena, so it is not hermetic — and the gate says so.** A
  transient dropped task success past a 0.02 tolerance once. Widening the tolerance would
  let a real regression hide behind "probably transient", so safety metrics stay absolute
  and the comparison excludes failures whose only cause was a backend error.
- **Answer fluency is deliberately not scored.** The model is unreachable on this account,
  and fluency was never the property that keeps a read-only agent read-only. Routing, tool
  choice, arguments, refusal and grounding are, and all of them are measurable at $0.

## 2026-08-26 — AI-P11, governance / observability / security / cost

- **Metrics fail open; the audit fails closed.** Metrics observe the work, so their outage
  must not break the agent — the same rule `openlineage.yml` already applies. The audit row
  is part of the control, so an unauditable action must not proceed. Asserted by test in
  both directions, because the asymmetry is the point.
- **`RoutingDecision` and `GuardrailBlocks` are first-class.** Neither appears on a generic
  LLM-observability list. One is the cost signal — a free lookup becoming a paid call is
  invisible to every accuracy metric. The other is the security signal.
- **A metric dimension may not carry free text or a secret, and `emit()` RAISES.** A
  dimension carrying a query string carries whatever was in it. Raising means it fails in a
  test rather than leaking in production; high cardinality is also a CloudWatch cost driver.
- **No token rate is hardcoded.** A stale price in source is worse than no price: it looks
  authoritative and is wrong. Cost is `None` with a pointer to `make pricing` unless a rate
  is supplied.
- **The version registry RESOLVES, it does not STORE.** Every version is already
  content-addressed where it is produced; copying them creates a second source of truth that
  drifts while both look maintained. Missing versions are reported, never substituted —
  `embedding` is honestly `None` because nothing has been embedded.
- **The security review is an executable script, not a checklist.** 23 checks read the
  actual Terraform, the actual tool catalog and the actual published corpus. A review that
  restates intent passes while the code disagrees with it, and nobody re-runs a document.
- **A test that checked for the string "default = false" anywhere was too weak.** It also
  missed `default     = false` because a naive `replace("  "," ")` collapses pairs. It now
  checks each named flag's own block, across both modules and the environment.

## 2026-08-26 — AI-P12, AI infrastructure gap analysis

- **A one-line budget change proposes destroying MSK, and it is not drift.** Terraform
  defers EVERY data source inside a module whose module-level `depends_on` has a pending
  change. `budget_guardrails` changing made `data.aws_availability_zones` unknown inside
  `kafka_platform`, which made `aws_subnet.availability_zone` unknown, which forces
  replacement — cascading to subnets, route tables, MSK, all EC2 and EMR. Proved by
  isolation: budget 100 → 19 replacements, budget 50 → 0, nothing else changed.
- **The fix is a decision, not a workaround.** `depends_on = [module.budget_guardrails]` on
  data_lake and kafka_platform encodes "the budget should exist before the cluster it
  guards" — a preference, not a correctness requirement, since a budget does not gate
  resource creation. The price of keeping it is that every budget edit proposes a
  catastrophe. Recorded with both options costed; the choice is the operator's.
- **Verify before believing a plan.** The replacements looked like real AMI/AZ drift. State
  said `1a/1b/1c`; the account said `1a/1b/1c`. Checking that they agreed is what turned a
  "catastrophic drift" story into a one-line explanation.
- **Two plans, not one.** Flags-off proves the feature flags actually gate everything (0 AI
  resources); flags-on shows what would be created. A single plan cannot demonstrate both.
- **`enable_ai_ml_training` and `enable_ai_observability` were deliberately NOT created.**
  Training runs locally or on the existing EMR application, and observability is CloudWatch
  EMF emitted by the runtime. A flag for a component that does not exist implies a resource
  that is not there.
- **The Lambda package is a known apply-time gap.** `filename` points at a zip that does not
  exist; `source_code_hash` is `fileexists()`-guarded so the PLAN succeeds and the APPLY
  would fail. Better to record it now than to discover it mid-apply.

## 2026-09-04 — closing the live window's findings without AWS

- **A quarantine row must never outlive its payload.** The payload object is written
  BEFORE the row, and a failed payload write is logged while the row is still recorded. An
  orphan payload is findable by prefix; a row promising an object that was never written is
  a dead reference that reads as data loss. The old code computed `payload_s3_uri` and
  wrote nothing, so every row it would have produced was that dead reference.
- **A truncated payload is FLAGGED, not silently clipped.** Bounded at 1 MB, with
  `truncated: true` in the object. A silently clipped payload looks like a corrupt record
  and sends the next engineer after the wrong bug.
- **An unexported `globalId` is quarantined, never decoded with the topic's other schema.**
  That schema is a different writer version by definition. `from_avro` does not raise on a
  compatible-looking mismatch — it returns plausible wrong values into the canonical layer.
  Quarantine keeps the record, with its coordinates, for a rerun after the export is
  refreshed; a fallback would have produced numbers nobody could challenge.
- **Union the projections, not the envelopes.** The decoded envelope struct differs between
  writer versions — that is what schema evolution means. Every projected column is an
  explicit scalar, so the union is total and needs no `allowMissingColumns`, which would
  quietly NULL a column that disagreed.
- **`--lake-bucket` is optional, derived from `--warehouse`.** Required would have been the
  tidier signature and the wrong call: every recorded submission recipe omits it, so the
  flag would turn a documented rerun into an argparse error on a job with nothing to
  quarantine.
- **The feature read is one query per business date.** Not an optimisation. The guarded
  executor injects a LIMIT and caps results at 1000 rows, so a single window query over 321
  accounts × N days returns a prefix — and features built on a truncated history are not
  detectably wrong. The run refuses when a date comes back at the limit.
- **`FIXED, AWAITING LIVE RE-TEST` is a status, not a softer PASS.** Seven criteria had
  their cause removed with regression tests and none has been re-run against AWS. Folding
  them into the PASS column would have made the scorecard say something no evidence
  supports.
- **One SparkSession for the test suite.** Ten per-module fixtures each ended in
  `getOrCreate()`; there is one JVM per pytest process, so the first session built wins and
  every later builder's config is discarded — 61 errors from files that all passed alone.
  The per-module fixtures are gone rather than reconciled: a fixture that cannot take effect
  is worse than no fixture, because it reads as configuration.

## 2026-09-06 — window bring-up, and two defects in the readiness gate itself

- **A window stamp that survives a `destroy` is worse than no stamp.** `cdc-window-start.sh`
  kept the first stamp it ever wrote, so a rebuild inherited `2026-08-16T09:53:12Z` and the
  gate reported a window that had been burning for three weeks. Every cost number below it
  was then nonsense, which is the dangerous part: a real overrun would hide inside an
  obviously-absurd one and nobody would look. The stamp is now compared against MSK's
  `CreationTime` -- the cluster cannot outlive the window it was created in -- and replaced
  when it is older. It is replaced with the CLUSTER's creation time, not `now`, so
  re-running the gate an hour into a window does not reset the clock to zero.
- **Check foundations by NAME, not by count.** The gate asserted `Glue databases == 7`. It
  went stale the moment `serving` was added, and worse, a count cannot tell an ADDED
  database from a MISSING one -- and the missing case is the expensive one, because an
  absent `full_cdc` means the ingest job creates it implicitly in the wrong place and
  nothing surfaces until reconciliation disagrees. It now names all eight and reports which
  are absent.
- **`--output text` separates with TABS.** The first version of the named check used a
  space-delimited `case` match, so it reported all eight databases missing while all eight
  were present. Caught by running the gate rather than by reading it. Matching is now
  line-exact after `tr '\t' '\n'`.
- **The source lab is not "broken" after a rebuild, it is UNINITIALIZED.** Oracle's image
  auto-runs its init scripts; the SQL Server image does not, so `digital` and `dbzuser`
  simply never existed. Separately, Oracle's users were created by a previous apply's
  passwords while SSM had rotated -- `ORA-01017` reads identically for "wrong password" and
  "no such user", so the diagnosis had to distinguish them before choosing a fix. Both are
  converged by `healthcheck.sh` (`sqlserver_init_if_missing`, `cdc_users_reconcile`,
  `oracle_reconcile_corebank_password`); no repo change was needed.
- **Verify the measurement before believing the defect.** All eight CDC topics reported 0
  messages, which looks exactly like the R15 failure (a connector healthy and producing
  nothing). The cause was `kafka.tools.GetOffsetShell`, which moved to
  `org.apache.kafka.tools` in Kafka 3.x: the class-not-found message summed to zero through
  `awk`. The real counts were 5,728, matching source row-for-row.

## 2026-09-06 (later) — a streaming canonical ingest, and why it is not a second implementation

- **The gap was real.** Nothing streamed into FULL_CDC. `full_cdc/job.py` is a bounded batch
  read; `l1_stream/job.py` had the streaming shape and had never worked; `rt_stream_app.py`
  streams into the RT layer. A CDC platform whose canonical layer only advances when a batch
  job is scheduled is a batch platform with CDC-shaped inputs.
- **The streaming job supplies LIFECYCLE ONLY.** Decode, quarantine and MERGE were extracted
  into `decode_topic_to_rows()` and `merge_into_full_cdc()` and are IMPORTED. Writing a
  second decode would have been faster and is the OPEN-28 defect exactly: the DAG adapter and
  `reporting-live-run.py` looked equivalent, differed on one dict key, and only live
  execution showed it. A test now asserts both callers use both functions and that the
  stream module contains no `from_avro` of its own.
- **Take the session from the DataFrame, never from the closure.** `foreachBatch` passes a
  DataFrame owned by a CLONED SparkSession. A temp view registered on it is invisible to the
  session that started the query, so the MERGE failed with `TABLE_OR_VIEW_NOT_FOUND` on the
  first micro-batch with rows — after the stream was already running. Batch mode cannot
  reproduce it because there the two sessions are the same object. `spark = rows.sparkSession`
  inside the shared function fixes both callers at once rather than at each call site.
- **`--run-seconds` is mandatory, not a convenience.** A structured-streaming query holds EMR
  Serverless capacity until stopped, and this lab has no budget for a query that outlives the
  person watching it. Both streaming apps stop themselves and both exited on
  `run_seconds_budget_reached` (CLAUDE.md 4.5).
- **A comment is not a test surface.** Two assertions in this session matched their own
  explanatory docstring rather than the code — the fix is documented using the very string
  the test forbids. Both now strip comments or match the call form. A green test that reads
  its own prose proves nothing.
- **Simulate deletes BOTH ways.** A delete that is later recreated and a delete that is never
  recreated exercise different code: the first proves a key is not permanently suppressed,
  the second proves the snapshot excludes it. Testing only the first would have left
  CLAUDE.md 5.7 unproven while looking covered.

## 2026-09-10 (Session 43) — per-table provisioning and routing, without a cutover

- **The dangerous default was the convenient one.** Both ingest jobs already do
  `CREATE TABLE IF NOT EXISTS` for the monolith. Generalising that to routed targets would
  mean a mistyped topic, a rewritten SMT route or a connector pointed at the wrong schema
  mints a production Iceberg table — no owner, no classification, no retention, no partition
  spec, no DQ rule — while every log line reports success. The write path now **refuses** a
  missing target and names the provisioner. Tables come from Git config first; that ordering
  is the entire control.
- **"Unknown" and "disabled" must not collapse into one outcome.** They look identical at
  the write (nothing is written) and need opposite operator responses: register it, versus
  turn it back on. One `RouteOutcome` for both would be a message that cannot tell an
  engineer which one they are looking at.
- **`quarantine`, not `reject`, as the default.** Rejecting stops the seven healthy topics to
  punish one misconfiguration. Rejecting is still right when continuing would bank more of a
  known-bad config, so it is a flag — but not the default.
- **The benchmark contradicted the brief, so the benchmark won.** 512 MiB was permitted "only
  if consistent with project benchmarking". Phase 0 measured a mean data file of 137 KiB
  against that exact default: the target property was never the binding constraint, commit
  frequency is. Setting 512 MiB would have changed nothing except making the number a reader
  compares against wrong.
- **Typed payloads keep the JSON.** A writer version can carry a field the registry has not
  declared, and a typed-only projection drops it silently — data loss dressed as a schema
  improvement. At 300 MB the duplication costs cents; a dropped column is unrecoverable at
  any price.
- **`owner` is a reserved table property in Spark SQL.** `TBLPROPERTIES ('owner' = …)` fails
  the statement outright with `UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY`. Found by running the
  generated DDL against a real Iceberg catalog, not by reading it — which is the argument for
  the Spark half of the suite existing at all. Every registry-derived property is now
  namespaced `cdc.`.
- **The default path must not import the `cdc` package.** It is staged separately, so an
  unconditional import would make every existing submission recipe fail *at import* the
  moment the package was absent — an opt-in feature turned into an outage. `job.py`
  hand-rolls a `LegacyWrites` stand-in, and a test pins it equal to `writes_for(LEGACY_ONLY)`
  so the stand-in cannot drift from the thing it stands in for.
- **One decode, one set of identity expressions, two writes.** Dual-write appends the extra
  canonical columns to the SAME projection and drops them again before the legacy MERGE.
  Two projections would be two chances for `dv_event_id` to drift — and it is the MERGE key,
  so a drift there means two writers of one event disagreeing about whether it is one event.
- **Structs are cast POSITIONALLY.** The typed payload is built field by field rather than
  with a struct cast: two columns of the same type swapping places in a later writer version
  would otherwise silently swap their values.
- **The delete's key comes from the before-image.** Hashing only `after` would give every
  delete the same `dv_pk_hash`, so `bucket(N, dv_pk_hash)` would pile deletes into one bucket
  while the rows they delete sat in another.

## 2026-09-10 (Session 43b) — Phase 1 audit: one requirement was a stub that lied

- **Phase 1 was already built; auditing it beat rebuilding it.** Sections A–F, H, I and J
  were implemented, covered by 43 tests and green. Re-implementing them would have produced
  a second config framework — the exact thing section A forbids — and thrown away the
  tests. The value was in checking each requirement against the code rather than against the
  file listing.
- **`--discover` did nothing, and said it did.** Its docstring claimed it "reads the source's
  catalog READ-ONLY (columns and primary key) over SSM". It built a SQL string, never used
  it, called `aws ssm send-command` with **no `--instance-ids` and no `--parameters`** — a
  call that cannot succeed — checked the return code, and returned `([], [])` unconditionally.
  A stub that lies is worse than an absent feature: the operator believes the primary key was
  verified against the source when nothing was verified at all. This is the same class as the
  quarantine `payload_s3_uri` that was computed, documented and never written.
- **The right source of truth was already in the repo.** `docker/source-lab/<engine>/01-init.sql`
  is the DDL that CREATES the captured tables. Parsing it is read-only *by construction* — it
  opens a file, never a connection — so discovery now works with the lab torn down, with no
  credential, at zero cost, and cannot mutate the source under any argument. A live query is
  a legitimate future addition; it is not this one, because it cannot be written honestly
  without a live lab to test it against, which is how the stub came to exist.
- **A wrong primary key is worse than no primary key.** Discovery returns `None` for a table
  absent from the DDL and the scaffold prints a prompt. A guess would be pasted without
  question, and an omitted or wrong `message.key.columns` entry does not error — Debezium
  keys by its own default, the same PK scatters across partitions, and per-key ordering
  breaks with every health check green (CLAUDE.md 5.1).
- **The discovered key is folded per engine.** The DDL declares `account_id`; Debezium emits
  `ACCOUNT_ID`. Same rule `naming.topic_for` already encodes for topics, applied to columns —
  and now cross-checked: a test asserts the shipped registry's PKs equal the source DDL's,
  which is the first thing in the repository to compare those two hand-maintained spellings.
- **A YAML flow mapping cannot hold an unquoted `decimal(18,2)`.** The comma terminates the
  entry, so `type: decimal(18,2)` parses as `type: decimal(18` plus a stray key — valid YAML,
  wrong config, rejected by the compiler with a type nobody wrote. Found by the test that
  uncomments the scaffolded block and compiles it, not by reading the output; the two are
  different tests and only the second one catches this.

## 2026-09-10 (Session 43c) — Phase 2 re-audit: typed temporals were silently wrong

- **The brief's two clauses had to be read together.** "Prefer typed per-table payloads
  where current schema/Avro integration safely supports them" and "do not silently change
  data semantics" are one instruction, not two. Checking whether the integration *safely*
  supports typed columns is what found the defect; assuming it did would have shipped it.
- **Debezium temporals are not Avro temporals.** `decimal.handling.mode=precise` produces
  `org.apache.kafka.connect.data.Decimal`, a Connect BUILT-IN logical type that an Avro
  converter carries as a logical type — so `from_avro` gives Spark a real `DecimalType` and
  a cast is correct. Debezium's `io.debezium.time.*` are CUSTOM Connect logical types, which
  an Avro converter carries as the underlying primitive. `from_avro` yields a plain int64.
- **The failure is silent, which is what makes it serious.** Measured on this platform's own
  Spark: epoch millis `1787356800000` CAST AS timestamp is **year 58609**; micros is year
  109081; an int32 day count CAST AS date is refused outright and kills the batch. A
  year-58609 timestamp partitions, sorts and reconciles like a real value. My Phase 2 typed
  path did exactly this cast, and my Phase 1 scaffold would have handed an operator a
  `timestamp` column that produced it.
- **No default encoding, deliberately.** `micro_timestamp` is the commonest here and would
  have been the tempting default — and a column that is actually millis would then land in
  **1970**, a *plausible* date. An absurd answer gets investigated; a plausible one gets
  reported. Requiring the declaration makes the question impossible to skip.
- **The wire type cannot answer it.** `Timestamp`, `MicroTimestamp` and `NanoTimestamp` all
  decode to `int64`, so inference is not available even in principle. The answer lives in
  the connector's `time.precision.mode`, which is why that setting is now NAMED in
  `source_schema.py` rather than assumed — change the mode and these mappings change with it.
- **Oracle DATE and TIMESTAMP(6) need DIFFERENT encodings.** Millis and micros respectively,
  under the deployed `adaptive_time_microseconds`. One blanket temporal mapping would have
  been wrong for one of them by a factor of 1000. Discovery now distinguishes them by
  precision.
- **A column with no safe mapping is excluded, not flattened.** SQL Server `TIME` arrives as
  micros since midnight, which is no type this platform declares. Turning it into a
  timestamp would be a semantic claim discovery cannot support; the JSON images keep it
  regardless, so exclusion loses nothing.
- **Pin the WRONG answer too.** `test_what_a_bare_cast_would_have_produced` asserts the
  year-58609 result. If someone "simplifies" the conversion back to a cast, the correctness
  tests start producing that value and this test is the one that says what it is. It has to
  render the year in Spark, because Python's `datetime` cannot hold it —
  `ValueError: year 58609 is out of range`. A value the driver cannot represent is one the
  canonical layer would have stored without complaint.
- **Collecting a timestamp to Python is not a UTC assertion.** The first version of these
  tests compared `str(row[...])` and failed at `+07:00` — PySpark converts to the DRIVER's
  local zone on collect. The assertions now format in Spark's UTC session zone, so they do
  not change answer with the machine they run on (ADR-024).

## 2026-09-10 (Session 43d) — Phase 3: the REALTIME window becomes config

- **"3 days" is not 72 hours, and the platform must not pick one.** A rolling 72h window from
  a 09:15 run holds three PARTIAL days and shifts every run -- two runs an hour apart
  disagree about what "the last 3 days" contains, and neither is wrong. A calendar window
  starts at midnight in the business timezone and holds three WHOLE days. Both are
  defensible, which is exactly why the boundary is config rather than a default someone
  reads as arbitrary.
- **Compatibility decided the default.** Making `calendar_day` universal would have changed
  the window every deployed table serves, silently, on deploy. An hours-configured table
  keeps `rolling_hours`; only a table that declares a `_days` field switches. A test asserts
  the shipped registry is still entirely rolling_hours.
- **The mode is resolved at COMPILE time, never inferred at runtime.** Inferring it from
  which fields happen to be non-null would mean that adding a `lookback_days` beside existing
  hours silently changes a live table's window.
- **The old job computed its bound INSIDE the query.** `current_timestamp() - INTERVAL N
  HOURS`, re-derived per step, so the bound it filtered with differed from the one it
  reported by however long the run took -- and every later comparison is off by that amount,
  in a way indistinguishable from late-arriving data. And it had no upper bound at all, so
  the window's contents depended on when each partition happened to be read. Both fixed by
  resolving four bounds once, in Python, from a caller-supplied instant.
- **`naive.astimezone(utc)` does not raise -- it assumes the machine's local zone.** So the
  same `--as-of` string would resolve to a different window on a laptop in UTC+7 than on an
  EMR worker in UTC. Found by a test that expected a refusal and got a silent conversion,
  because the guard sat downstream of the conversion. The refusal now precedes it.
- **Calendar arithmetic on the local DATE, not by subtracting a timedelta.** Across a DST
  transition those differ by an hour, and the hour lands in a neighbouring day's partition
  where nothing reports it. Reachable here because `business_timezone` is per table.
- **`overwrite`, not `createOrReplace`.** Both are atomic and only one leaves the table
  DEFINITION alone: `createOrReplace` resets the partition spec, write properties and
  governance metadata the provisioner set, silently, to whatever the DataFrame implies. That
  was harmless while nothing provisioned REALTIME and stopped being harmless in Phase 2.
- **Section F had to be checked in the unit the policy is written in.** Validating only the
  hours fields would leave a day-configured table unvalidated WHILE APPEARING CHECKED -- the
  hours defaults are always present, so the assertion passes against numbers the table does
  not use. That is worse than no check, because it reads as one.
- **The prune is a row-level DELETE.** A file delete leaves Iceberg metadata pointing at
  objects that are gone: every reader fails, and the table cannot even be time-travelled back
  because the snapshots reference the same missing files.
- **`source_snapshot_id` is captured BEFORE the read.** Captured afterwards it could be a
  later snapshot written by a concurrent ingest, which would make the run look reproducible
  against data it never read.
- **The ledger write is best-effort, and says so when it fails.** A ledger write that could
  fail the run would report a successfully materialised table as a failure and
  re-materialise it -- trading a missing audit row for real duplicated work.
- **Snapshot expiry deliberately NOT folded into the prune.** Expiry is destructive and its
  retention guard is a separate decision (CLAUDE.md 6); a materialisation run is the wrong
  place to make it. The prune bounds the ROWS.

## 2026-09-10 (Session 43e) — Phase 4: the EOD close becomes config

- **The deployed job told me what to build.** `spark/jobs/eod/job.py` is Oracle-`ACCOUNT`-only
  down to the column name `j.ACCOUNT_ID`, and its own comment names the fix: "the correct SQL
  Server treatment already exists ... wiring it in is the fix if EOD is extended." It also
  guards itself with a refusal rather than assuming its source filter holds. Reading that
  comment was worth more than any amount of designing from the brief alone.
- **The two engines need OPPOSITE treatment.** Oracle SCN is numeric, so '9' > '10'
  lexicographically and it must be PADDED to become sortable. SQL Server LSN is hex and
  ALREADY fixed-width, so it must be VALIDATED to confirm it still is. A single "just pad it"
  helper would silently corrupt one of them. Both now produce a sortable STRING, so one ORDER
  BY serves both and the comparison never depends on a cast that can return NULL -- which is
  precisely how the deployed job would fail on SQL Server: the hex casts to NULL, every row
  sorts equally, and the ranking collapses onto kafka_offset.
- **kafka_partition must precede kafka_offset, and that is the whole of CLAUDE.md 5.4.** An
  offset is monotonic only within its own partition. Ordering by partition first means
  offsets are compared only between rows that share one; the partition itself is a
  determinism tie-break, not a claim that a higher partition happened later.
- **Two defaults were decided by INSPECTION, not preference.** The brief says to inspect the
  existing contract before defaulting deletes; the same discipline applies to snapshot mode.
  Deletes stay `exclude_from_snapshot` because that is what the deployed job does and a
  changed delete default alters certified balances without altering a line of business SQL.
  Snapshot mode defaults to `rolling_history` because the provisioned table is already
  partitioned by business_date with retention 365 -- a 365-day retention on a table holding
  one day is meaningless, so the deployed contract already committed to history, and
  defaulting to `latest_state` would delete up to 364 certified partitions on first run.
- **The brief's delete names are aliases, not a rename.** The registry, the plan hashes and
  the provisioned properties already carry the project's spellings; renaming would rewrite
  every config hash to say the same thing.
- **`event_date == cob_date` would have been a plausible cutoff and is wrong.** `event_date`
  is a UTC date and the business day is not, so for every non-UTC table it drops real events.
  It survives ONLY as a deliberately wider pruning hint alongside the exact predicate.
- **Strictly `<` against start-of-next-day.** CLAUDE.md 5.6 permits `<=` against a cutoff, but
  "the last instant of a day" has no exact representation and every approximation of it
  (23:59:59, .999, .999999) silently drops events in the gap.
- **A delete's identity is in the BEFORE image.** Its after-image is NULL by construction, so
  reading only the after image gives every delete a NULL key and collapses them onto one
  snapshot row. Two deletes for two keys would have reconciled as one.
- **The EOD grain is recomputed, not taken from `dv_pk_hash`.** The source column is NULL for
  rows written before Phase 2 and for a writer version that did not carry the key, and a NULL
  grain collapses every such row onto one key. The recompute uses the IDENTICAL expression, so
  where the source value exists the two agree -- and the collision that forced this
  (`dv_pk_hash` existing on both sides) was found by the engine refusing an ambiguous
  reference, not by reading the code.
- **Certification is a gate, not a label.** Data is written even when validation fails,
  because a snapshot an operator can inspect beats one thrown away -- but the completion
  marker is withheld, and the process exits non-zero. A scheduler checks the exit code, and
  "built" must not read as "certified".
- **The reconciliation had to be an identity, not a restatement.** `distinct_keys - deletes
  == rows` compares two independently derived numbers. Recomputing the row count from the
  same DataFrame would have "passed" every time and proved nothing.
- **A doc check fired on my own prose, correctly.** `validate-docs` D8 flagged an ADR line
  that listed the business timezones the cutoff tests cover, because the zone name sat within
  60 characters of the word it guards. It is a false positive of exactly the class the check's
  own comment describes -- and the fix was to reword the ADR, not to widen the allow-list.
  Weakening a correctness check to accommodate documentation is how the check stops meaning
  anything. Writing THIS note tripped it a second time, for the same reason, which is a fair
  demonstration that the rule has teeth.

## 2026-09-10 (Session 43e, cont) — I shipped a module-name collision, and a spot-check hid it

- **Two files called `engine.py`.** Phase 3 added `spark/jobs/realtime/engine.py`; Phase 4
  added `spark/jobs/eod/engine.py`. Python caches by module NAME, not by path, so the two are
  ONE module to the interpreter: alphabetical collection imported the EOD one first and every
  later `import engine` silently returned it. 24 REALTIME tests failed with
  `module 'engine' has no attribute 'run_table'` -- while both suites passed run on their own.
- **This is not a test-only concern.** EMR stages job modules FLAT, in one directory. That is
  exactly why `full_cdc/job.py` is staged as `full_cdc_job.py` and `stream_job.py` imports
  `from full_cdc_job import ...`. Had both engines ever been on one path in a live run, the
  collision would have resolved to the wrong module with no error and no missing file. It is
  the same defect class `cdc/__init__.py` documents for the reporting package, one directory
  over -- and I reintroduced it while having read that file.
- **The spot-check is what hid it.** After the Phase 4 fixes I re-ran the config-layer suites,
  saw 167 passed, and reported. The realtime engine suite was not in that selection, and it
  was the one the collision broke. A targeted rerun proves the thing you targeted; only the
  full suite proves the absence of interaction, and interaction is exactly what a name
  collision IS.
- **The fix is the names, not the tests.** `realtime_engine.py` and `eod_engine.py`. Renaming
  the test imports alone would have left the EMR hazard in place while making it invisible.
- **The guard found a pre-existing collision I was not looking for.** Four `spark/jobs/*/job.py`
  files. That one is survivable and deliberate: each is a spark-submit ENTRYPOINT, passed as a
  path and run as `__main__`, so its filename is never a module name in a shared namespace --
  and the single one that IS imported is precisely the one already staged under a distinct
  name. Grandfathered with that reasoning written down, rather than silently excluded.
- **And a second, genuinely fragile one.** `test_l1_local_spark.py` does `from job import
  L1_SCHEMA`, which resolves correctly only because `conftest.py` puts `spark/jobs/l1_stream`
  on the path and the other three `job.py` directories are not on it. A working accident.
  Pinned as the single known site so a SECOND one fails the build -- fixing it is outside the
  phase that found it, and rewriting a passing test to look tidier is not a fix.

## 2026-09-10 (Session 43f) — Phase 5: onboarding becomes a gated workflow

- **The precheck reads Git, not the source, and that is the design.** Everything that makes a
  table capturable is DECLARED in the scripts that configure the source: Oracle per-table
  supplemental logging and the LogMiner grants in `02-enable-cdc.sql`, the SQL Server capture
  list in its own, the PK in `01-init.sql`, the schema-history topic in the connector
  template. So the precheck runs with the lab torn down, needs no credential, costs nothing,
  and names the file to change for every finding. A precheck that needs the platform up is one
  nobody runs before the platform is up -- which is exactly when onboarding is decided.
- **And it says which kind of evidence it has.** Every finding is stamped
  `evidence_kind: "declared"`. A table can have supplemental logging in Git and not in a
  database somebody rebuilt by hand. Claiming the live check when only the declared one ran is
  the stub-that-lies defect this repository has already fixed once, in this same command family.
- **The default onboarding mode describes the platform rather than preferring anything.** The
  brief prefers `incremental_snapshot` and so do I -- and neither deployed connector configures
  `signal.data.collection`, so it is unavailable. Defaulting to it would make every onboarding
  fail its precheck. Defaulting to `changes_only` is honest: it is what happens today whether
  or not anyone chooses it. The preference is expressed as a WARNING on the default and a
  REFUSAL with a remediation on the unavailable mode.
- **A reordering is not a change.** The derived include-list is sorted; the template's is in
  whatever order it was typed. Rewriting it unconditionally produced a config with an
  identical SET and different bytes -- a new hash, an apparent pending change, and a connector
  restart bought for nothing. A restart rebalances tasks and re-reads offsets on a LIVE
  capture. Found by reading my own output and noticing the hashes differed while add/remove
  were both empty.
- **Removal is refused, not merely warned about.** `--approve-capture` never implies it, and
  the orchestrator will not print an apply command at all while the diff contains one. Adding
  is additive and reversible; removing stops capture, and every change that happens while a
  table is absent ages out of the source's retention window before anyone notices it was gone.
- **The config hash is over REAL values.** Hashing the redacted form would report "unchanged"
  across a password rotation. A hash discloses nothing, which is exactly what makes it the
  right thing to put in a log where the config must not go.
- **The lifecycle is deliberately not Airflow's.** A task status describes ONE execution and
  vanishes when the run is cleaned up; a table is CAPTURING for weeks and PAUSED across
  deploys. "The provisioning task succeeded" is not the claim "this table is provisioned", and
  overloading one onto the other is how a table that was never activated comes to look active
  because a retry passed.
- **A missing required measurement is a FAIL, not a skip.** "We did not measure it" and "it
  was fine" must not reach the same conclusion -- that is the entire difference between an
  acceptance gate and a formality. Likewise an EOD that BUILT but did not CERTIFY is not
  acceptance evidence.
- **My own readiness check found three shipped tables with an empty DQ contract** -- `branch`,
  `channel`, `merchant`. A registered-but-empty contract is not a contract: an EOD close that
  cannot fail a DQ rule certifies whatever it happens to produce. Fixed in the registry in the
  same phase as the tool that found them.

## 2026-09-10 (Session 43g) — Phase 6: the cutover machinery, and the gate that is NOT met

- **I checked the Phase 0 gate before building, and it is satisfied.** ADR-062 approved
  per-table storage as the TARGET and gated the cutover on two triggers, explicitly excluding
  data volume. Trigger 2 -- differentiated retention/PII/IAM policy between source tables --
  is now real: the registry carries `confidential` on `customer` and `app_user` against
  `internal` elsewhere, realtime disabled on three tables, and 15-vs-60-minute freshness SLAs.
  ADR-060 denies the AI plane `warehouse/full_cdc/` WHOLESALE because a per-table grant is not
  expressible against a monolith, which is the concrete cost. Had the trigger not been real
  the honest answer would have been to stop.
- **DUAL reads LEGACY, and that is the whole safety property.** A mode where writes go both
  ways but reads follow the new path would put consumers on unvalidated data during exactly
  the window whose purpose is to validate it.
- **The legacy read carries its predicate or it is wrong.** The monolith holds all eight
  source tables. A consumer that forgets `source_system AND source_table` reads eight tables'
  events as one and gets a PLAUSIBLE NUMBER, not an error. So the resolver returns the
  predicate WITH the table; forgetting it is not reachable.
- **Backfill copies `dv_event_id`; it must never recompute it.** The identity is
  `sha2(topic|partition|offset|kafka_timestamp)`. Recomputing is the obvious move, and a
  recompute that rendered the timestamp one microsecond differently would give the same events
  DIFFERENT identities -- after which the reconciliation compares two sets that cannot match,
  for a reason with nothing to do with the data.
- **An unmeasured gate check is a FAIL.** Nine checks, and `benchmark_measured` is itself one
  of them, so "we did not benchmark" cannot be reported as "performance is fine". The
  benchmark harness RAISES on an incomplete scenario rather than hedging: a partial benchmark
  reads exactly like a complete one, which is how unmeasured performance claims get made.
- **The harness reports a regression as readily as an improvement.** A benchmark that can only
  say "better" is advocacy. It is also the LIKELY outcome here for the file-count metrics --
  the Phase 0 audit measured 137 KiB mean files, and splitting one table into eight makes
  small files worse before compaction makes them better.
- **A PHASE 4 DEFECT, found by the pilot's registry having DQ rules.** The EOD DQ check called
  `pk_value_expr`, which coalesces the after-image with the BEFORE-image -- right for a CDC
  event, an AnalysisException on a snapshot, because the EOD row contract has no
  `payload_before` (a snapshot is state, not an event). All EIGHT registered tables declare a
  `not_null` rule on a primary-key column, so this broke EVERY close. Phase 4's own tests
  missed it because not one fixture table declared a DQ rule -- I tested the machinery and not
  the contract it enforces. The same fix stopped non-key columns passing vacuously: a rule
  like `not_null: [TRANSACTION_ID, ACCOUNT_ID]` was checking one and silently passing the
  other, which is worse than no contract because it is believed.
- **A design limit is now documented instead of latent.** The EOD engine applies no
  source-table predicate, because its source comes from the compiled plan and that is always a
  per-table target. Pointed at the raw monolith it reads every table's events at once -- in
  the pilot it REFUSED, because an Oracle table's numeric ordering cannot parse a SQL Server
  hex LSN. The CLAUDE.md 5.4 guard from Phase 4 caught a misuse it was not written for.
- **The finish token asks me to declare a PASS I cannot declare.** No pilot has run against
  the deployed platform, no backfill has executed in AWS, and not one of the nine benchmark
  metrics has been measured on either path. The machinery is built and proven locally against
  a real Iceberg catalog; the gate is unmet, and saying otherwise would be the exact failure
  section I exists to prevent.

## 2026-09-10 (Session 43h) — the live pilot: defects the dry run could not show

- **`CREATE DATABASE catalog.db` is a v1 statement given a v2 name.** Spark 3.5 routes CREATE
  DATABASE to v1 session-catalog semantics, which accept only a single-part name. A
  catalog-qualified one fails with `_LEGACY_ERROR_TEMP_1055` -- an error class whose own
  message template is undefined, so what actually surfaces is `Undefined error message
  parameter for error class`, naming neither the statement nor the problem. `CREATE NAMESPACE`
  is the v2/Iceberg spelling and takes the qualified name.
- **The `--dry-run` path returned BEFORE that line.** So the dry run passed and the real run
  failed on the first statement of the loop. A dry run that does not execute the same
  statements is a rehearsal of a different play; this one covered discovery and nothing else.
  Worth remembering the next time a dry run is offered as evidence.
- **And the real cause was one level further out: the job never configured a catalog.**
  `register_tables.py` does `SparkSession.builder.appName(...).getOrCreate()` with no
  `spark.sql.catalog.*` at all -- it has always depended on submit-time `--conf`, which every
  recorded recipe supplies and my new `emr-submit.sh` did not. So `CREATE NAMESPACE
  glue_catalog.x` resolved against the V1 SESSION catalog, where a qualified name is invalid.
  The CREATE DATABASE -> CREATE NAMESPACE change was still right, and it was not the fix.
  Two plausible diagnoses for one symptom; the first one being reasonable is exactly why the
  second was worth looking for.
- **The registered monolith holds exactly 20,390 rows** -- the number the Phase 0 audit
  measured from S3 manifests months earlier. Matching it is what confirms the RIGHT metadata
  chain was registered: three orphaned chains sit at that location (one per destroy/apply
  cycle), and `register_tables.py` picks the highest metadata version. A different count
  would have meant an older chain, and every comparison after it would have been against the
  wrong baseline.
- **320 of 961 live `oracle/ACCOUNT` rows have a NULL `dv_event_id`, and that breaks a MERGE.**
  The legacy monolith predates the identity column; `job.py --backfill-identity` exists to
  populate it and has not been run for these. `MERGE ON t.dv_event_id = s.dv_event_id` never
  matches NULL -- NULL = NULL is not true -- so those rows are NOT MATCHED on every run and
  re-inserted every time. My backfill would have silently stopped being re-runnable, and the
  local pilot could not have shown it: every fixture row I wrote had an identity.
  They are now EXCLUDED and COUNTED. Not dropped quietly, and not given an invented identity
  -- inventing one would make the reconciliation pass while comparing two sets that mean
  different things.
- **`COUNT(*)` and `COUNT(DISTINCT dv_event_id)` are different questions here**, and the gate
  has to ask the second. 961 vs 641 for ACCOUNT, 12,000 vs 6,000 for digital_event. Comparing
  raw row counts across the two paths would fail for a reason that has nothing to do with the
  migration.
- **A dry run must not exit non-zero for having nothing to compare.** The backfill returned 1
  because `identity_match` was false -- correct, since the target is empty until `--execute`
  -- but it made a perfectly good dry run look like a failed job. That is exactly the noise
  that teaches an operator to stop reading exit codes. The dry run now exits 0 and says the
  comparison is what `--execute` would be judged against.
- **digital_event: exactly 6,000 of 12,000 rows carry no identity.** A clean half, which fits
  one full ingest before the column existed and one after, rather than sporadic loss.
- **The legacy predicate uppercased only the LITERAL, and that matched no SQL Server table.**
  `source_table` is written as the last segment of the TOPIC, so it carries the engine's own
  spelling: Oracle folds to `ACCOUNT`, SQL Server does not fold and stores `digital_event`.
  Comparing `source_table = 'DIGITAL_EVENT'` returned ZERO ROWS with no error, and the
  benchmark dutifully scored that as "0 bytes scanned, very fast". I caught it by reading a
  result that was too good rather than by any check -- which is the argument for reading
  measurements instead of collecting them. Both sides are now `upper()`ed, which is what the
  backfill had done correctly all along. This is the same engine-casing trap recorded twice
  already in this repository's history; a third instance in code I wrote.
- **The prediction was right about the mechanism and silent about the weighting.** ADR-062
  said "≈87% with 8 evenly-weighted tables"; `oracle/ACCOUNT` measured -87.4%. But
  `sqlserver/digital_event` measured only -7.1%, because it IS 12,000 of the monolith's
  20,390 rows -- isolating the dominant table saves almost nothing. The benefit is inversely
  proportional to a table's share of the monolith, and nobody had said so before the
  measurement existed.
- **And the benchmark reported a regression, as designed.** `auto_correct` on ACCOUNT went
  730 ms -> 2,264 ms. Two small files with a filter that matches almost nothing is not
  obviously cheaper than fourteen. A harness that could only say "better" would have hidden
  it, and the whole reason section I insists on measurement is that "better" is the answer
  everyone expects.
- **The pilot skipped DUAL, deliberately and with a reason.** The backfill reproduces history
  the monolith already holds; DUAL exists to validate NEW events. For these two tables --
  neither actively receiving CDC, the connectors being down -- there were no new events to
  validate, so a DUAL window would have observed nothing and proved nothing. It remains the
  right step before cutting over a table that IS receiving CDC, and that is now written down
  rather than assumed.
- **Two of my own tests broke because the world changed CORRECTLY.** They asserted
  "8 tables, 8 LEGACY" and "no gate evidence exists" against the REPOSITORY's live cutover
  state -- which the pilot then legitimately changed. A test that reads live migration state
  has to be edited every time the migration progresses, and editing tests to match reality is
  the habit that eventually edits away a real failure. Both now use an isolated state file,
  and a separate test checks the live state for SHAPE (every registered table resolves,
  `legacy` agrees with the mode) rather than for a particular content.

## 2026-09-10 (Session 43i) — the deferred gaps, and Phase 7

**The gaps, closed:**

- **`business_insights.py` ran an analytics pipeline in two PythonOperators**, which put
  readiness gating, anomaly scoring, driver attribution and forecasting on the SCHEDULER
  NODE. Deferred eight times because the fix looked like "add it to the allowlist" -- and
  that would have been editing the test to match the code. The compute is Athena's, but the
  driver around it is not orchestration. Extracted to `scripts/ai-insight-run.py` and the DAG
  now uses BashOperator: the DAG decides WHEN, a process decides WHAT, which is the shape
  every other flow here already had. Plus the three missing bounds --
  `is_paused_upon_creation` (a deployed DAG that starts itself has decided for the operator
  that now is a good time to spend money), `dagrun_timeout` (without it a hung run holds
  `max_active_runs=1` forever and every later day is silently skipped), and
  `execution_timeout`.
- **`from job import L1_SCHEMA` worked only by sys.path ordering.** Four files answer to
  `job`. Loaded by path now, and the guard tightened from "no NEW bare import" to "none at
  all" -- the last offender was the reason it had been scoped loosely.
- **The `event_serial_no` vs `change_lsn` discrepancy was between a SUPERSEDED module and
  the live one, and the live one is right.** SQL Server gives every ROW CHANGE its own
  `change_lsn` within a transaction while `commit_lsn` is shared, so `(commit_lsn,
  change_lsn)` separates every row change; `event_serial_no` only separates the parts of one
  change, which the connector emits as a single `u` envelope. Pinned by test and the contract
  doc corrected -- the next person to read both modules would otherwise have "fixed" the live
  path.

**Phase 7:**

- **A cron does the same work whether or not there is work.** The Phase 0 audit measured the
  actual problem -- 137 KiB mean files caused by COMMIT FREQUENCY, not elapsed time -- so the
  trigger is a threshold on a measured metric and the cadence is only a floor.
- **My first threshold was over-eager, and real metrics showed it.** The freshly backfilled
  `oracle/ACCOUNT` holds 2 files averaging 48 KB. A mean-only trigger fired and asked to
  compact two files into one: an EMR run to eliminate ONE file, on every cadence, forever.
  `min_small_files` existed to prevent exactly that and the mean branch was bypassing it.
  Both branches now require enough files for the rewrite to pay for itself.
- **Action order is a correctness property, not tidiness.** Compaction writes new files and
  leaves the old ones referenced by older snapshots, so expiry AFTERWARDS is what frees the
  space. Reversed, the expiry has nothing to collect and storage never drops.
- **Orphan removal takes a measured count, never elapsed time, and is off by default.** It is
  the action that deletes. "A week has passed" is not evidence that anything is orphaned.
- **`pii` is DERIVED from classification.** Two fields meaning the same thing is how a table
  ends up `pii: false` and `classification: confidential` at once, and nothing can say which
  one the deny list should believe.
- **A rename is blocked because a DIFF CANNOT SEE ONE.** Iceberg's RENAME preserves data; a
  drop-and-add does not; and a schema comparison cannot tell them apart. Acting on the guess
  is what would make it destructive.
- **Decommission disables downstream BEFORE stopping capture.** The other order leaves
  consumers reading a table that has silently stopped advancing -- every query succeeds and
  every number is quietly stale. This way the staleness is an absence, not a wrong answer.
- **The maintenance job ran on EMR and correctly did nothing**, which is the right answer:
  the two provisioned tables hold 2 files each and the file-count floor skips them. It also
  exposes a real limitation worth naming rather than papering over: the job operates on
  REGISTERED tables, and the one table that demonstrably needs maintenance -- the legacy
  monolith, at 14 files / 13 manifests / 53 snapshots -- is not one. It cannot simply be
  added, because it holds eight source tables and "whose `maintenance` policy governs it?"
  has no good answer. It needs its own policy entry, which is a decision about the monolith's
  retirement path (ADR-062 keeps it forever as the reconciliation baseline) rather than a
  line of code. Recorded as an open item.
- **The schema guard's first live run blocked NINE columns on `varchar -> string`**, which is
  one type under two names: Athena reports through `information_schema` in Trino's
  vocabulary, Iceberg and Spark use another. A guard that fires on every table is worse than
  no guard, because its silence stops meaning anything. Folded the engine spellings.
- **And folding them introduced a worse bug for one commit.** Comparing BASE types made
  `decimal(18,2)` and `decimal(18,4)` both "decimal" and therefore "unchanged" -- permitting
  a scale change, which reinterprets every stored value by a factor of ten per digit and is
  the single most destructive thing the module exists to block. Caught by the decimal test
  that already existed. Normalisation now folds the NAME and keeps the PARAMETERS, and a new
  test pins both halves so the next person to fold a spelling cannot un-fix the other.

## Phase 8 — new-table zero-custom-code acceptance (2026-09-10)

**Decision**: accept the platform against two *new* tables on two engines and two payload
modes, treating anything that needs a keystroke outside the registry as a defect. ADR-069.

**Why two, not one**: SQL Server is what breaks Oracle-shaped assumptions (hex LSN vs numeric
SCN), and the typed/JSON payload modes take different code paths through PK extraction,
ordering, DQ and the EOD grain. A single-table acceptance would have claimed coverage of
paths it never ran.

**What it cost to find**: three live defects, each of which had been passing tests.

1. `write_ledger` was "best-effort" — a close printed `CERTIFIED`, `certified=1`, exit 0,
   with its ledger write failed. Resolved by separating the **data** (keep it; it is correct)
   from the **claim** (withhold it; nothing can verify it): `STATUS_UNVERIFIED`.
2. `--table` provisioning silently skips the shared OPS targets, so they had never been
   created in six phases. The skip is right; the silence was not.
3. Carrying `maintenance` into the plan serialised "unset" as `[]`, which the runtime reads
   as "permit nothing" — maintenance was a platform-wide no-op that looked like a platform
   with nothing to do.

**Also decided**: `maintenance.actions: []` is **refused**, not interpreted. It is
indistinguishable from a deliberate "never maintain this table", for which there is no
setting, and obeying either reading silently disables compaction on a streaming table.

**Deferred, not done**: the live capture leg for both new tables.
`register-connectors.sh update --execute` requires a human to type a confirmation phrase and
that gate was not bypassed, so no row for either table has traversed Debezium → Kafka →
FULL_CDC. Next owner: whoever runs that command from a terminal.

## Phase 8 continued — the live capture leg (2026-09-10)

**Decision**: close the remaining leg after a human applied the connector update, and treat
every silent failure it exposed as a platform defect rather than an operational quirk.
ADR-070.

**The finding that generalises**: onboarding a table derives **four** artifacts from the
registry, not one, and three of them live outside the repo — the Kafka topic, the connector
capture list, the writer schema, and the deployed code+plan. Each had its own way of being
silently stale, and each failure presented as something other than what it was:

* no topic → connectors RUNNING, zero records, all health checks green
* stale `schemas.json` → `FIELD_NOT_FOUND: no such struct field 'op'` (names a field)
* module missing from the zip → `ModuleNotFoundError` after acquiring capacity
* missing Kafka jars → `Failed to find data source: kafka` (reads like a typo)

Their order is a genuine dependency chain, not a preference: the topic must exist before
capture, and the writer schema **cannot** be exported until after it, because Apicurio holds
no schema until the connector produces its first record. `cdc-table-onboard.py` prints them
in order; the runbook tabulates why each cannot move.

**Decided against**: turning on `auto.create.topics.enable`. It would fix the loudest symptom
by removing the protection that makes a mistyped topic loud — and the platform has already
been bitten by a lowercase/uppercase mismatch that, with auto-create on, would have silently
produced into eight wrong topics instead of none.

**Also decided**: acceptance evidence is **type-checked**, not merely truthy. The judge is
the last gate before a table is trusted downstream and it was passing `"FAILED"` for a
connector state and `"0"` for a row count. And `PROVISIONED -> CAPTURING` is now recorded by
smoke on **observed** evidence (records on the topic *and* rows in FULL_CDC) — the walk still
refuses to claim it from a template, which was correct and was also why a genuinely capturing
table could never reach `ACTIVE`.

**Result**: both tables `ACTIVE`, both closes `CERTIFIED`, both matching independent Athena
counts. Ten registered tables, zero governance gaps.

## Phase 8 acceptance closure — orchestration (2026-09-10)

**Decision**: build the config-driven orchestration layer rather than let acceptance point 3
pass by absence. ADR-071.

**The problem with how it was passing**: "no custom Airflow DAG is needed" was true because
**no DAG orchestrated the per-table platform at all**. Every REALTIME materialisation, EOD
close and maintenance run in Phases 1–8 was a hand-typed `emr-submit.sh`. That satisfies the
letter of the check and none of its intent — the same shape as a green test that asserts
nothing. It was recorded as open issue #7 rather than claimed as a pass.

**What was built**: three generic DAGs (`cdc_realtime`, `cdc_eod`, `cdc_maintenance`) whose
tasks are dynamic-mapped over the compiled plan. Measured: one added registry entry took
REALTIME from 7 to 8 tasks and EOD from 10 to 11, with the DAG file untouched.

**Three properties are enforced by test, not by intent**: no per-table DAG may exist (every
registry table name is checked against every DAG filename); no registry table name may appear
in the DAG's executable body (a literal list would drift exactly as the connector capture list
and the topic list did — ADR-070); and EOD passes no `--cob-date`, so the scheduler's timezone
can never decide a business date.

**Decided against**: letting the DAG compute the COB. It reads as a convenience and would
apply one timezone to every table, certifying an hour of events into the wrong business date
for any table that is not UTC.

**Also decided against**: having the DAGs ingest, provision, or update connectors. Ingest is a
long-running streaming app (a task per micro-batch is a micro-batch architecture wearing a
streaming name); provisioning on a timer is how unowned data products appear; and the
connector gate exists to require a human.

**Honest limit**: the DAGs have never run on the deployed Airflow. They parse and pass all 50
policy tests, and that is all that is claimed — open issue #8.

**Late events**: proven live rather than only in tests. A `payment_method` row was updated
after its COB was certified; FULL_CDC went 5 → 6 events while the certified snapshot correctly
kept the old value, and `--fulfill` on the same date picked it up. A certified number that
moves without anyone asking would be worse than a stale one.

## Full verification pass (2026-09-10)

**Decision**: run the whole verification surface — `lint-shell`, `validate-docs`, `cdc-check`,
`cdc-verify`, `terraform fmt`/`validate`, `compileall`, every CLI, and the full pytest suite —
rather than `pytest` alone, and treat anything it finds as a defect.

**It found two, both invisible to pytest.**

1. **The committed plan artifact was two table onboardings stale.** `make cdc-verify` detects
   it, but that target was **not in `make check`** — the aggregate a developer actually runs —
   and is not in `make test` either. Every Spark job takes `--plan`; a stale artifact silently
   runs the previous config and the run succeeds.
2. **The hash was never recomputed.** `cdc-verify` and the first version of the replacement
   test both compared the artifact's *stored* hash fields, which a truncated or hand-edited
   file carries unchanged — dropping a table from `plan.tables` passed every check.

**Fixed both ways deliberately**: `cdc-verify` added to `make check`, *and*
`test_the_committed_plan_artifact_is_not_stale` added to the suite, recomputing the hash from
the artifact's own content and comparing the canonical payload. Either fix alone leaves the
other hole open — a Makefile target nobody invokes, or a suite that trusts a number the file
supplies about itself.

**The general lesson, now recorded twice in this project**: a gate nothing runs is not a gate
(ADR-070 §5 on `create-topics.sh`, and §8 here), and a hash nothing recomputes is not a
checksum.

**Verified clean afterwards**: `make check-all` (static checks + full suite), `terraform fmt
-check -recursive` and `terraform validate` on `envs/dev`, `compileall` across
`scripts/ cdc/ spark/ airflow/ ai/`, and all ten read-only CDC CLIs exiting 0.

## Phase 0 re-audit against measured state (2026-09-10)

**Decision**: when the master brief was re-issued asking for PHASE 0, do **not** re-derive it.
§0 of that same brief says current code and current deployed state win over older prompts, and
Phase 0's deliverables already existed with its token recorded. Re-running it would have
produced a *worse* document, because its answers were predictions that have since been
measured.

**Done instead — closed Phase 0's own open evidence.** It had marked two items PENDING "needs
a live ingest; Glue catalog empty on this apply":

* **B6 (bytes scanned)** — measured with recorded Athena query ids. `ACCOUNT` −87.4%,
  matching the ≈87% prediction. **`digital_event` −7.1%, which the prediction missed**: that
  table *is* 12,000 of the monolith's 20,390 rows, so isolating the dominant table saves
  little. Two of its metrics got worse. Recorded as a correction to the audit, not smoothed.
* **B2 (rows/table/day)** — measured. The skew is real and wider than assumed: 3,000/day for
  `digital_event` against **4–8/day** for `channel` and `BRANCH`. That range is why
  `realtime: {enabled: false}` exists and why three of ten tables use it.

**Also closed**: `event_date IS NULL` is now **0 of 20,400**. Phase 0's root-cause analysis
predicted this exactly — the orphaned poison-record file was referenced by no snapshot chain,
so rebuilding the catalog produced a table without it. No delete was needed or issued. Its
recommendation was implemented rather than noted: `dq.event_date_null_tolerance: 0` is a
registry default, so an accident became a rule every table inherits.

**Two documentation gaps found against brief §63** and filled with real content rather than
stubs: `FULL_CDC_PER_TABLE.md` (the canonical layer's row contract, the two identities, why
constants are not partitioned) and `CDC_TABLE_MIGRATION.md` (the three modes and the
reversibility guarantee; `CDC_CUTOVER.md` remains the operational runbook — strategy and
procedure kept separate rather than duplicated).

**Where the original audit was wrong, now stated in the document itself**: Q9 assumed
per-table storage helps uniformly — it helps the tail, not the head. Q11's proposed per-table
YAML file tree was rejected for one registry file, because cross-table invariants (unique ids,
unique targets, deterministic topics) are invisible in a tree; that reasoning inverts at ~50
tables.

---

## 2026-09-17 — Session 44, Phase B: Kafka → FULL_CDC streaming hardening (ADR-074)

**Audited ADR-073 against the brief before building, and two of its claims were not yet true.**
`--checkpoint` was `required=True`, so the plan-derived checkpoint could never be reached.
`ops.streaming_app_state` was provisioned and verified live with **0 rows**, because nothing
wrote to it. An empty state table reads exactly like a healthy one. That is Phase A's
defect again, one layer up.

**Decisions**

1. **One state row per app, MERGEd in place, throttled.** The row is written on information
   (rows moved, status change, error) or when the 5-minute heartbeat elapses. Per-batch
   writes were rejected: in place is about rows, not commits, and per-batch means 1,440 state
   snapshots a day. A failed state write never fails the batch, because raising in
   `foreachBatch` loses rows.
2. **Closed status transitions.** `FAILED → RUNNING` is refused because it implies two
   writers on one app id.
3. **App identity comes from the subscribed topics through the plan (schema 8).** An unknown
   topic is **refused**, not ignored. Ignoring it would key the process onto another app's
   checkpoint. An id hashed from the topic set was rejected: reordering `--topics` would
   move the checkpoint.
4. **A fresh checkpoint starts from `earliest`.** Measured before the change: Kafka `LOAN`
   held 1,668 offsets against 65 FULL_CDC rows. `latest` on the first identity-keyed start
   would have silently dropped about 1,600 changes. The MERGE on `dv_event_id` makes
   `earliest` idempotent (Iceberg test).
5. **Airflow watches, EMR restarts.** A resident app is an EMR Serverless `STREAMING` job
   run with `maxFailedAttemptsPerHour`. The monitor restarts only a `restartable` verdict,
   only when opted in, bounded per run, and never a `STOPPED` app. It fails its task on
   anything unhealthy even after restarting. Its cadence is asserted to be at least 5× the
   trigger.
6. **One checkpoint-reset script.** `streaming-reset.sh --app-id` was chosen over a clone.
7. **Exactly-once is claimed only as far as proven.** Transport idempotency on
   `dv_event_id` is proven. Logical re-delivery at a new offset is *detectable* by
   `dv_src_event_id`, not collapsed. Logical exactly-once is not claimed.

**Corrected along the way**

* `test_foreachbatch_does_not_swallow` had been **erroring** (`substring not found`) since
  ADR-073 rewrote the writer, so it had stopped checking the one property that loses rows.
  It is now anchored on `.foreachBatch(process)` and mutation-checked.
* `test_it_has_a_wall_clock_budget` was **passing on a substring**: `--run-seconds` is
  contained in `--test-only-run-seconds`. It is rewritten to assert the real cost control.

**Measured live (read-only)**: `rows == distinct dv_event_id == distinct dv_src_event_id`
on `loan` (65), `payment_method` (74) and `account` (339). The source watermark is about
1h46m behind. The only FULL_CDC checkpoints in S3 are date-keyed (`stream_w20260906`, `…b`)
and are left in place.

**Priced, not enabled**: two resident apps at default sizing ≈ $0.756/hr ≈ $544/month, about
18× the lab budget. `lab_low_cost` stays the shipped profile.


---

## 2026-09-20 — Session 46, Phase C: config-driven REALTIME (ADR-075)

**Audited before building, and most of Phase C already existed.** The generic engine, the
frozen upper bound, the half-open window, FULL_CDC as the only source, both snapshot ids,
per-table targets, deterministic rebuild, the run ledger and both retention invariants were
already in place under ADR-064, covered by 89 tests. Rebuilding them would have been the
wrong move; two things were genuinely missing.

1. **`realtime.schedule` was read by nothing.** The loader accepted it, the compiler carried
   it, and the cadence came from `CDC_REALTIME_CRON` on the Airflow node. A registry edit
   moved the plan hash and `make cdc-verify` went green, so every signal said the change had
   landed. Config that appears live and is not is worse than config that is absent.
2. **`resource_profile` did not exist.** One envelope — `timeout_minutes=40`, 2 cores / 4g
   driver, 2 × 2-core executors — served `channel` (4 rows) and `digital_event` (3,000).

**Decisions**

* The cadence is a **5-field cron validated at compile**. Aliases (`@daily`) are refused, so
  two tables asking for one cadence produce one string to group on. An unreadable cron
  discovered by Airflow is a DAG that fails to import, and such a DAG vanishes from the UI.
* **One DAG per cadence, never per table and never one for all.** Airflow has one schedule
  per DAG, so per-table cadence cannot live in a single DAG without a scheduler-side skip
  that fires a task to do nothing. Seven tables on one cron stay one DAG with seven mapped
  tasks; moving one table to `*/5` produces a second DAG holding that table.
* The plan carries `schedule_declared` alongside the resolved value, because "defaulted" and
  "explicitly chose the default" are the same string and different facts.
* The **profile names the pool as well as the size**: a large job in the small pool starves
  it. `large` → `spark_jobs`, others → `reporting_jobs`. Default `small`, which is *cheaper*
  than the envelope it replaces — a profile nobody chose must not be the one that bills most.
* Plan schema **9**.

**Test-matrix honesty**: of the brief's 13 cases, 11 were already covered. The two that were
not are now on real Iceberg — a parallel two-table run (each target a subset of its own
FULL_CDC, two ledger rows) and a failure injected after the window resolves, which leaves the
served rows untouched and converges on the next run. Writing 13 new tests over 89 existing
ones would have inflated the count and proven nothing new.

**A test that was testing the fixture**: the first parallel assertion compared `source_table`
across targets and failed — because the shared `seed` helper stamps that column with the
constant `"ACCOUNT"` for every table. The assertion now compares each target against its own
FULL_CDC source by `dv_event_id`, which is the property that actually matters.


---

## 2026-09-20 — Session 47, Phase D: the EOD control plane (ADR-076)

**Audited first: the builder was done.** Generic engine, half-open DST-safe cutoff with no
`23:59:59.999`, Oracle SCN / SQL Server hex LSN ordering that REFUSES rather than falling
back to `kafka_offset`, delete policies, DQ, certification withheld on failure — 124 tests,
and 15 of the brief's 18 cases already passing. Phase D built the control plane around it.

**Decisions**

* `ops.eod_info` holds ONE MERGEd row per `(table_id, cob_date)` with the watermark PAIR.
  `eod_run` stays the append log. One table doing both jobs made "current certified state" a
  convention no column enforced.
* `ops.eod_run_hist` appends one row per attempt, numbered FROM HISTORY. An in-process
  counter calls every retry attempt 1 forever, and "this has failed nine times today" never
  appears anywhere.
* The watermark advances ONLY on CERTIFIED. An unrecordable certification degrades to
  `UNVERIFIED_NO_EVIDENCE`, for the same reason an unwritable ledger does.
* An orphan commit is REPORTED and the close re-runs: the inputs and the partition overwrite
  are deterministic, so re-running is safer than reasoning about what the dead process did.
* Legacy names are a VIEW applied through Athena. `datelastmaint` says nothing about whether
  it is the date a close moved from or to; making it canonical would bake that ambiguity into
  the control table. Iceberg view support is also catalog-dependent — the test catalog
  refuses outright — so the Spark provisioner does not attempt it.

**Two design errors caught by running the thing**

1. **Readiness that condemned every quiet table.** The obvious rule — require an event
   committed after the cutoff — is wrong for reference tables: `channel` holds four rows and
   may not change for weeks, so it would report LATE_SOURCE every single day. The platform
   ingest watermark (`ops.streaming_app_state`) answers the same question one level up and is
   now accepted as equivalent evidence.
2. **A 7-hour watermark corruption in my own writer.** PySpark converts stored timestamps to
   Python using the MACHINE's zone, not the Spark session zone. Treating those naive values
   as UTC when writing them back stored every watermark seven hours ahead of the events it
   described, on any non-UTC host. `datetime.astimezone()` on a naive value localises first,
   which is exactly right.

**A registry declaration that had been inert**: `loan` carries `eod: {schedule: "30 2 * * *"}`
and every table was closing on `CDC_EOD_CRON`. It now gets `cdc_eod_0230`, and the canonical
`cdc_eod` id follows the CONFIG default rather than the env var — anchoring on the env var
handed the canonical name to the single table that matched it.

**Compatibility taken seriously rather than worked around**: a close now REQUIRES the control
tables, so the existing EOD fixtures were updated to provision them. Weakening the engine to
tolerate their absence would have made the tests pass against a configuration production
never runs. Builder-focused tests pass `skip_readiness=True`, which is the historical-rebuild
path.


---

## 2026-09-20 — Session 48, Phase E: reporting readiness (ADR-077)

**The architecture question answered with evidence, not preference.** Phase E §1 asks whether
to generate a DAG per pipeline. `airflow_dwh2cdp_extract.py` does exactly that — and chooses
each DAG's cron with `if "_oram1_" in TARGET_TABLE`, so renaming a table reschedules it. N
configs become N DAGs, N schedules and N UI entries. Nothing in current operational
requirements shows the fixed-flow coordinator insufficient, which is the bar §1 sets. The
four flow DAGs stand.

**What was actually missing**: nothing asked the EOD control plane whether a business date was
CERTIFIED. `DatasetReadinessGate` probes `eod_watermark_exists`, one query per dependency,
against a signal that predates Phase D. ADR-076 deliberately leaves a BUILT-but-uncertified
close readable for inspection; that gate cannot tell it from a certified one, so a mart could
publish a day the platform refused to vouch for.

**Taken from the reference**: the batched query. `target_table IN (...) AND cob_date = ...`
for every dependency at once is right, and `EodInfoReader` does the same and caches per
coordinator run — twelve jobs over three datasets is one query.

**Rejected from the reference**:
* readiness by COUNTING matches. It reports "three of four ready" and never says WHICH is
  missing, which is the only thing an operator needs at 02:00.
* `tbl.replace('sat_', 'sat_snp_')` to derive a dependency's physical table. A derived name
  maintained by hand — the same class as the `= "4"` health gate, the dead EMR application id
  and the stale writer-schema export. `required_datasets` names the canonical registry id.
* `os.system('rm ... *.json')` as a cache (pipeline_builder). The cache belongs in the
  coordinator run.

**A modelling decision worth recording**: `required_datasets` are NOT `JobDependency` records.
A dependency is an edge in the ordering graph; an EOD close is not one — it runs in another
flow, on another schedule, possibly on another day. Making it an edge would put a CDC table
into the mart graph and make turn computation depend on a layer the graph does not own.
ADR-039 already separates readiness from ordering, and this keeps that line.

**Found while wiring**: two places rebuild `LoadedConfig` to change one part — the
manifest-merge branch and `with_derived_fields` — and both silently dropped the new field.
The compiled plan reported zero required datasets while the loader had three. Both now carry
it explicitly; rebuilding a config object to change one field is a standing hazard.


---

## 2026-09-20 — Session 49, Phase F: the five flow applications (ADR-078)

**Four of the five requirements were already met, and proven live.** EOD, FULFILL (with
from_date/to_date/specific_dates/force/dry_run), STREAM_BATCH (frozen upper bound, no
watermark advance on failure) and STREAMING_RT (separate resident app, durable checkpoint,
Airflow only deploys/monitors/restarts) all ran in the 2026-09-03 window and carry 178 tests
between them. AUTO_CORRECT already had the bounded lookback, `AffectedDatePolicy`,
`AffectedKeyStrategy` and dependent-mart propagation. Rebuilding any of it would have been
churn.

**The one real gap was §2's carry-forward.** `auto_correct` bounds a window and RE-DERIVES
it; the reference seeds day D from day D-1's `latest_of_day = 1` rows and applies only the
day's changes. For a state mart, repairing one late event should cost one partition read, not
a ninety-day rescan.

**Adapted**: `latest_of_day` (materialised, not recomputed by each reader),
`is_full_filled` → `is_carried_forward` (a row knows whether it came from a real event),
and `super_key` with configurable columns.

**Rejected**: the reference's 1,800 lines of `bal_dau_ki` / `ma_cn` / `kyhan` / `custtyp`
business columns — those are a MART's schema and a framework that knew them would need
editing for the second mart; string-interpolated SQL built from execution dates across
hundreds of lines; and `sync_wait(seconds, reason)` sleeps used as a coordination primitive.

**A correctness decision the reference does not make explicitly**: a carried row is
yesterday's fact wearing today's date, so ranking it against today's events by `system_time`
compares two different days' clocks — and yesterday's 23:00 beats today's 01:00. The rank is
`is_carried_forward` first, then the configured ordering.

**Recorded rather than smoothed**: `super_key` embeds a timestamp rendered in the session
zone, so it is stable only because every job pins UTC (ADR-024). A UTC+7 session would
produce a different key for the same fact and nothing downstream would notice — it would look
like a new row. The test asserts against what Spark renders rather than a literal, for the
same reason.

**Honest scope**: carry-forward is a library. No shipped mart declares a spec, and it has not
run on EMR. Wiring it is a per-mart config decision (grain, ordering, identity), not a
framework default.


---

## 2026-09-20 — Session 50, Phase G: partition-scoped maintenance (ADR-079)

**The planner was already right about WHEN; it was wrong about WHERE.** Metric-driven, four
actions, thresholds anchored to a real measurement, orphan removal opt-in — Phase A assessed
all of that as KEEP. But `rewrite_data_files` ran with no predicate, so compacting FULL_CDC
rewrote every event ever captured to fix today's small files, and each rewrite is a new
snapshot holding the old files until expiry.

**Scope follows from what each layer IS**, not from a preference. FULL_CDC is append-only
canonical history: only recent days receive writes, so only recent days accumulate small
files, and history is never rewritten because that is the layer's contract. REALTIME is a
bounded window: beyond the retention bound the prune deletes the rows anyway, so compacting
there is work about to be discarded. EOD depends on its snapshot mode — `latest_state` holds
one COB and IS the recent data, so scoping it would exclude the only data there is. MART has
no single shape, so it names its own column.

**Orphan removal must outlive the longest writer, doubled.** It deletes files that are
unreferenced and old — and a file an in-flight job has written but not committed is exactly
that. A threshold equal to the longest writer fails the first time a writer is slower than
its longest observed run.

**Two defects in my own new code, both caught before shipping:**

1. `policy.get("compact_recent_days") or DEFAULT` swallowed an explicit `0`, turning
   "compact the WHOLE table" into "compact 3 days" silently. Identical in shape to ADR-070's
   `maintenance.actions: []` inheriting the full default set. Falsy-zero keeps finding this
   codebase.
2. Escaping the predicate by doubling its inner quotes produced a string Spark unescapes into
   `event_date >= DATE 2026-09-17`, which Iceberg rejects with `Cannot parse predicates in
   where option`. The test that caught it runs the real CALL against real Iceberg; a
   source-text assertion would have passed. That failure would otherwise have landed on EMR
   after the job acquired capacity.


---

## 2026-09-20 — Session 51, Phase H: data layout benchmark

**The decision is to change nothing, and that is the finding.** The brief says not to change
production partitioning without measured evidence. Measured: the shipped
`identity(event_date)` wins or ties every workload at every table class.

**`days(dv_src_ldt)` vs `identity(event_date)`: byte-identical.** Same partition count, file
count, min/avg/max file size, and the same files and bytes read by all seven workloads,
across small, medium and hot. ADR-062 argued they were the same pruning with the transform
applied at write time; this measures it instead of asserting it. Materialising also turns out
to be the more robust of the two, for a reason the harness demonstrated accidentally (below).

**Bucketing loses everywhere measured.** On the hot table at N=64: 30 files become 1,920, the
average file falls from 38.9 KB to 3.5 KB, a one-day scan reads 224 KB instead of 39 KB, and
the EOD build takes 775 ms instead of 119 ms. Bucketing deliberately creates, at write time,
the small-file problem the maintenance framework exists to repair.

**What I did NOT measure, said plainly**: bucketing exists to prune key-equality lookups, and
this harness does not measure that pruning -- the metadata probe has no bucket-aware
predicate, so `key_lookup` reports the whole table for every layout. So the position is
asymmetric: costs measured and large, benefit unmeasured. That is enough to keep bucketing
off (the shipped default) and not enough to call it useless; the document lists the three
things that would justify revisiting it.

**Athena bytes scanned is null, not modelled.** The Glue catalog holds zero tables. A
benchmark that estimated the one metric everybody quotes would be worse than one that admits
the hole.

**An artefact caught before it became a finding.** The first run showed `days()` producing 60
partitions against `identity()`'s 30, which would have been a strong argument for the shipped
layout -- and it was entirely my generator: naive datetimes are reinterpreted by PySpark in
the machine's zone, splitting each logical day across two UTC days. The same conversion
corrupted watermarks in Phase D. With timezone-aware generation the two layouts converge
exactly. A benchmark's first surprising result is usually the benchmark.

## 2026-09-21 — dbt on EMR: wheelhouse, not venv; and the CURATED gap

**Decided:** pin `dbt-core 1.9.11` / `dbt-spark 1.9.3` and ship them as a `cp39`/`aarch64`
WHEELHOUSE (`pip download --platform manylinux2014_aarch64 --python-version 3.9`), not as a
`venv-pack` archive behind `spark.archives`.

**Why:** venv-packing needs a cp39/aarch64 interpreter to build against and no machine in
this project has one; the wheelhouse needs only pip. `emr_dbt_bootstrap.py` already said so
in its docstring — the venv route was tried first and discarded, which is the cost of not
reading the module that owns the mechanism before building for it.

**Decided:** do NOT register the 2026-09-10 Iceberg metadata under `warehouse/curated/` to
make `dbt build` succeed.

**Why:** that curated fact predates the stack rebuild, while the EOD snapshot it summarises
was closed 2026-09-20. A mart built across that boundary is complete, plausible and wrong —
the exact failure `periodic_snapshot.py` was written to prevent. The CURATED layer needs a
producer wired to the current naming; that is an ADR, not a deploy step.

**Open:** three spellings exist for the same dataset — `layers.yaml` says
`kafka_dev_lab_dev_curated`, `build_kimball.py` writes `{catalog}.mart.*`, and
`mart_account_balance_daily.yaml` depends on `kafka_dev_lab_dev_snapshot.*`. One must win.

## 2026-09-20 — the CURATED layer, and what `--skip-readiness` may not do

**Decided:** `reporting/layers.yaml` is the single spelling of where a dataset lives
(ADR-080). `build_kimball.py` and `mart_account_balance_daily.yaml` are corrected to match
rather than accommodated.

**Decided:** `--skip-readiness` waives the SOURCE gate, never the CLOCK. A COB whose cutoff
has not passed is built and written but never certified.

**Why:** the flag's own help already said "Never for closing the current day" and nothing
enforced it, so the one run that used it published COB 2026-09-20 as CERTIFIED six hours
before that day ended. Certifying an open day publishes a number that will change, under a
status that says it will not.

**Decided:** the Airflow node's Spark conf is byte-for-byte `reporting-live-run.py::_spark_conf`.

**Why:** it held a second, never-exercised spelling of the catalog binding, and pointed at
artifacts encrypted under a rotated CMK. Two ways to say "where the data is" is one too many.

**Not done deliberately:** the 2026-09-10 curated metadata under `warehouse/curated/` was NOT
registered to make `dbt build` go green. It predates the stack rebuild while the EOD snapshot
it summarises was closed 2026-09-20.

## 2026-09-20 — Phase J

**Decided:** an `eod_info` CERTIFIED row whose own COB cutoff is still in the future is
WITHDRAWN by the next close of that table/date.

**Why:** certification is otherwise final and nothing else demotes it. This case is provably
invalid — after the open-day guard, no valid path can produce it — so it is self-healing
rather than an operator's hand-written UPDATE against shared ops state.

**Decided:** an open-day build requested with `--skip-readiness` exits 0; the same outcome
without the flag still exits 1.

**Why:** a console full of red rows that mean "working as intended" is how a real failure
gets missed. Intent distinguishes the two, not outcome.

**Decided:** `business_date()` delegates to `cob_date()`, and `business_date` is no longer a
project var.

**Why:** it read a name nothing passes and silently took `1970-01-01`. Three marts returned
zero rows with every test green.

**Decided:** the platform is NOT production-ready, on schema evolution alone.

## 2026-09-23 — readiness, re-derived on the rebuilt stack

**Retracted:** "the platform is NOT production-ready, on schema evolution alone"
(2026-09-20). That decision was wrong. It rested on P1-1, which restated a 2026-09-03
finding without re-reading the code: `full_cdc/job.py::select_schema` already chose the
writer schema per record by `globalId`, and already quarantined an unknown id rather than
falling back to the topic entry. Tested by `TestWriterSchemaIsChosenPerRecord`; live, the
export holds 28 writer versions across 14 topics.

**Decided:** `CDC_REPORTING_PLATFORM_PRODUCTION_READY`, on the criterion stated in Phase J
-- P0 and P1 both empty -- with the whole chain re-proven on the CURRENT deployment rather
than on the destroyed one.

**Decided:** a CERTIFIED close is recorded as a residual (P2-9), not as a blocker.

**Why:** it cannot be produced today and no code change would help. Every event in the lab
commits at connector-snapshot time on build day, so the only COB holding data is the current
one, whose cutoff has not passed. Certifying it is exactly what the open-day guard exists to
prevent. Choosing a non-UTC `business_timezone` would make the cutoff fall in the past and
produce a certified close -- and would be gaming the test rather than proving the platform,
so it was not done.

**Decided:** recover CMK `0b5b383b` from PendingDeletion rather than let the 20 Sep lake
data expire.

**Why:** it was not really a choice about old data. `s3 mv` copies, copying decrypts, and a
key pending deletion refuses -- so `streaming-reset.sh` could not move the stale checkpoint
aside, and the platform could not be restarted at all until the key came back.

**Decided:** re-encrypt the `artifacts/` prefix onto the current key by hand rather than run
the full converge.

**Why:** `reencrypt-lake-cmk.sh reencrypt` rewrites all 5,557 objects and is operator-gated.
Fifty artifact objects were what blocked the jobs. The full converge remains the right
operation before the next rebuild, and remains the operator's to run.

---

## 2026-09-28 — R2-B

**Decided:** split `realtime.refresh_mode` into `shape` + `write_strategy` rather than add
values to it. ADR-082.

**Why:** one word answered two independent questions. A consumer asking "may I read this as
current state?" had to know which of three strings implied it — and I got that wrong myself
earlier in this programme, describing the layer as latest-state-per-PK when it is an event
window, and writing a verifier that asserted `rows == distinct dv_pk_hash`. It passed,
because the sample data happened to hold one event per key. A contract that can be misread
by its own author is a contract that needs to be a field.

**Decided:** `refresh_mode` becomes a derived property, not a stored field alongside the pair.

**Why:** two fields that must agree are two fields that eventually will not.

**Decided:** refuse `iceberg_snapshot` and `rebase.on_eod_certified: true` at compile,
naming R2-C and R2-F, instead of accepting them as inert defaults.

**Why:** this is the dominant defect class of the whole programme — a declared field nothing
reads. `watermark_type: NONE` made FULFILL unable to backfill. `business_date` read a var
nothing passed, and three marts were silently empty with every test green. Each compiled,
looked live, and named behaviour that never happened. Accepting `iceberg_snapshot` today
would make the plan itself the thing that lied. The refusal is what R2-C deletes.

**Decided:** the two spellings inherit as a group — the most specific level that declares
either one wins, and the other is dropped.

**Why:** putting `shape:` in the registry `defaults:`, which is the point of the phase, would
otherwise refuse every table still written as `refresh_mode:` for a contradiction it did not
write: its own field against an inherited one. Declaring both at the *same* level is still
refused, because there the author really did say one thing twice.

**Decided:** change no table's behaviour in this phase.

**Why:** flipping `account`, `customer`, `loan` and `app_user` to `latest_state` also flips
their EOD `delete_policy` to `soft_flag`. That is a live change to snapshot semantics, on a
platform that is mid-rebuild and has not ingested since 28 Sep. It belongs with the engine
and tests in R2-D.

---

## 2026-09-28 — R2-C .. R2-J

**Decided:** the incremental cursor falls back to the full window scan on five conditions
and records why, rather than refusing to run.

**Why:** the scan it replaces is expensive AND self-healing — it cannot be incomplete. An
incremental read trades that away and is wrong *silently*. A run that reads too much is
slow; a run that reads too little is wrong and nothing reports it. An unsafe cursor is an
expected operational state (a compaction ran, a snapshot expired), so refusing would stop
the layer serving because it could not take the cheap path.

**Decided:** `retention_hours: 96` on the append tables, in the same commit as the switch.

**Why:** under `overwrite_window` the table WAS the window. Under `append` nothing removes
an aged row except the prune, so 168 would have started keeping three days the layer never
promised, on the day it shipped, with no code saying so.

**Decided:** a NOOP still prunes.

**Why:** retention follows the clock, not the arrival of data. Found by a test, not review.

**Decided:** the rebase matches on `dv_event_id` as well as `dv_pk_hash`, against a frozen
`VERSION AS OF` read.

**Why:** the overlay keeps accepting events throughout. Matching on the key alone deletes a
row written after the frozen read — a change the certified baseline does not contain and
nothing else holds. With the event id, the delete removes only what it actually looked at.

**Decided:** carry the EOD cutoff as a string formatted by Spark, never as a collected
timestamp.

**Why:** `collect()` renders a TIMESTAMP in the DRIVER's local zone. Putting that back into
a SQL literal shifts it by the driver's offset — zero on EMR in UTC, seven hours from this
laptop, and the failure is a rebase deleting rows the baseline does not have.

**Decided:** a STREAM_BATCH mode DECLARES its CDC source tables rather than the compiler
deriving them.

**Why:** deriving means walking the dbt manifest to CURATED to the CDC registry — coupling
the reporting compiler to two more config systems, and still guessing wherever a model reads
a table the manifest does not attribute. The declaration is three lines and the check is
exact.

**Decided:** `channel` stays REALTIME-disabled; `mart_channel_engagement_daily` declares an
explicit `fallback: FULL_CDC`.

**Why:** it is a four-row reference table that changes four times a day. A rolling window of
it is a Spark run every ten minutes forever, for data a bounded scan reads correctly.

**Decided:** finish `REALTIME_LAYER_PRODUCTION_NOT_READY` rather than PASS the three
untested gates.

**Why:** the brief says never convert NOT_TESTED into PASS, and the benchmark, the partition
choice and the live E2E genuinely have no data behind them — the lake has not ingested since
28 Sep and Glue holds 0 tables. Twenty-one gates pass on evidence; three do not, and saying
so is the whole value of the verdict.

---

## 2026-09-29/30 — recovering a destroyed stack

**Decided:** re-register the Iceberg tables rather than re-provision them.

**Why:** `terraform destroy` takes the Glue databases and leaves every Parquet file and every
`metadata.json`. A table IS its metadata, so registering costs one catalog write per table and
rewrites no data. `provision_cdc_tables.py --execute` would have created fresh empty tables
and stranded 1,491 surviving objects as orphans — with the same green output.

**Decided:** rescue the old CMK before touching anything else.

**Why:** the failure presents as `AccessDenied on kms:Decrypt`, which reads as an IAM problem
and invites a policy change that cannot fix it. The actual cause is a key the destroy
scheduled for deletion while 6,198 objects were still encrypted under it.

**Decided:** treat a stale streaming checkpoint as a mandatory post-rebuild step, not an
incident to diagnose.

**Why:** the ingest reported `SUCCESS` with `batches: 0, rows: 0`. Structured Streaming
prefers the checkpoint over `startingOffsets`, so it resumed against a destroyed cluster's
offsets and consumed nothing. Nothing in the exit code, the logs or the job state says so —
only a before/after row count does. It had already silently left `loan` at 4 rows against 50
in the source.

**Decided:** run the second EOD close WITHOUT `--skip-readiness`.

**Why:** the first certified close needed it, because the lab's data all predated the cutoff
and the source had nothing newer. Once the ingest made FULL_CDC current, the gate could be
satisfied properly — and a certification that passes the gate is worth more than one that
skips it. It certified 5 tables.

**Decided:** do not re-run the rebase to "confirm" it today.

**Why:** `customer` and `app_user` are already on baseline 2026-09-28, so a rebase would skip
with `already_at_this_baseline`. Running it would produce a green line that proves nothing.

---

## 2026-09-30 — DRP0 (Data Reliability Platform audit)

**D-DRP0-1 — Measure the governance drift before designing anything.**
The brief asked for an audit classifying capabilities PASS/PARTIAL/MISSING/CONFLICT/UNKNOWN.
Classifying from the repository alone would have graded `domains.yml` as PASS: it is
well-written, internally consistent and argues its own design. It is also describing a
platform that no longer exists. *Why:* the only way to tell a healthy registry from a
drifted one is to resolve its names against the live catalog. *How to apply:* a governance
audit that never leaves the repo cannot detect drift — that is the one defect it is for.

**D-DRP0-2 — Derive the catalog from `cdc/registry/sources.yaml`; do not re-author
`domains.yml`.**
The obvious fix is to rewrite the twelve entries as seventy-three. Rejected. `domains.yml`
was written by people who had already identified this exact failure mode and argued
against it in the file's own header — and it drifted anyway, because the platform became
config-driven and the YAML could not follow. *Why:* repeating a hand-maintained registry
repeats an experiment whose result is now measured at 1/73. *How to apply:* the registry
the platform already obeys is the only one that cannot drift silently; governance fields
belong there, and everything else is derived output.

**D-DRP0-3 — DataHub may never execute a job.**
The brief allows it and the audit agrees. *Why:* impact analysis makes it tempting to let
the graph trigger the reruns it computed. A metadata service with a write path into the
executor can destroy data because someone mis-tagged a dataset. *How to apply:* the
planner resolves affected nodes to registered executable jobs through OPS and the Airflow
DAG registry; Airflow alone runs anything; unresolvable nodes become operator decisions
rather than silent omissions.

**D-DRP0-4 — Add `derived` as a third evidence class, and gate recovery on it.**
`docs/LINEAGE.md` already had `declared` / `observed` / `dbt_manifest`. The audit found
that the platform's most trustworthy lineage today is neither observed nor declared — it
is computed from artifacts the producers wrote for themselves (OPS run ledgers, the dbt
manifest, Iceberg snapshot history). *Why:* those are as reliable as telemetry and far
more reliable than config, and collapsing them into `declared` would have made the
platform look blinder than it is. *How to apply:* auto-recovery requires every edge to be
`observed` or `derived`; one `declared` edge downgrades the whole plan to
`RECOVERY_PLAN_REQUIRES_APPROVAL`. Debezium hops are permanently `declared`.

**D-DRP0-5 — Pin nothing in DRP0.**
Four integrations need versions and the constraints are now known (Scala 2.12 / Spark 3.5
/ Iceberg 1.5.2; Airflow **3.2.2** deployed, not the workstation's 2.9.3; dbt-core 1.9.11
and a wheelhouse rebuild; DataHub server and CLI on one release line). *Why:* CLAUDE.md
§3.9 forbids `latest`, and `docs/VERSIONS.md:104` records what pinning from expectation
already cost — the first wheelhouse was built at dbt 1.8.9 against a `>=1.9.0`
requirement and the EMR run failed the version check. A pin chosen from a prompt is the
same mistake with a different input. *How to apply:* record the constraint and the
resolve command in DRP0; pin in DRP2/DRP3 from an actual resolve, with SHA verification.

**D-DRP0-6 — Report the false `evidence: observed` markers; do not fix them.**
`governance/lineage/openlineage.yml` claims Spark telemetry that nothing emits.
*Why:* editing config is a mutation, and DRP0 is audit-only. More importantly the marker
becomes *true* in DRP3 when the listener is actually wired, so correcting it now would
have to be undone. *How to apply:* an audit records what is wrong and leaves the
correction to the phase that owns it.

**D-DRP0-7 — DataHub stays behind a default-false flag with an offline path.**
*Why:* CLAUDE.md §4.10 already keeps Marquez off as "a 24/7 service", and DataHub is
heavier. Rejecting Marquez on cost and accepting something larger without saying so would
be incoherent. *How to apply:* `enable_datahub=false` by default, destroy+verify script,
private-only, and ingestion recipes that emit to a `file` sink and validate with the
server down — so DRP4–DRP7 progress at $0 rather than blocking on always-on spend.

---

## 2026-09-30 — DRP1 (governance metadata foundation)

**D-DRP1-1 — Derive the asset list; author only metadata.**
84 assets are computed from the CDC registry, `entities.yaml`, `DIMENSIONS` and the dbt
manifest. The overlay may add metadata to a derived asset and **may not invent one**.
*Why:* re-authoring `domains.yml` with 73 entries repeats an experiment whose result is
measured at 1-of-12. *How to apply:* an overlay key that resolves to nothing is a compile
error, and a derived asset with no overlay entry still enters the inventory and is reported
as ungoverned — silence is never taken for coverage.

**D-DRP1-2 — One home per fact; the second place checks rather than restates.**
`classification` is refused inside a `governance:` block; a Kimball dimension's PII may not be
restated in the overlay; `retention_class` is checked against `eod.retention_days`;
`freshness_slo_minutes` is checked against `dq.freshness_sla_minutes`. *Why:* DRP0 found three
files claiming to own a PII list and two claiming freshness with different numbers. *How to
apply:* when a second file needs a fact, give it a *different* job and check consistency —
never copy the value.

**D-DRP1-3 — Resolve the 60-vs-1440 freshness disagreement by splitting it, not picking.**
`dq.freshness_sla_minutes` is what a check evaluates at; `freshness_slo_minutes` is what a
consumer may rely on; an SLO *tighter* than the SLA is refused. *Why:* DRP0 open question 4
had two values and no evidence to prefer either, because neither was measured. *How to apply:*
promising 60 minutes while only checking at 1440 is a promise nothing tests; measuring tighter
than you promised is fine.

**D-DRP1-4 — Controls accumulate; scalars override.**
`tags`, `glossary_terms` and `pii_fields` union across inheritance levels. *Why:* if tags
overrode, a table declaring `tags: [reconciled]` would silently shed its domain's
`[regulated]` — becoming *less* governed for having said something about itself. *How to
apply:* a governance default that a local edit can drop is not a control.

**D-DRP1-5 — Shift the ladder rather than extend it downwards.**
`REALTIME` could not be rank 0: that is the sentinel for an unrecognised status in
`spark/common/flows.py` and in `dbt/macros/status_priority.sql`, so garbage would have tied
with a real tier. The ladder moved to `cdc/certification.py`, `flows.py` imports it, and the
ranks shifted 1–4 → 2–5. *Why:* `flows.py`'s own comment says a second hardcoded rank table is
how an anti-downgrade rule stops matching the ladder it enforces. *How to apply:* the three
existing drift tests made this safe — they fail loudly if the Python, the SQL and the decoder
do not move together.

**D-DRP1-6 — Keep `NOT_EVALUATED` as a sixth status.**
The brief names five (`PASS`/`WARN`/`FAIL`/`ERROR`/`SKIPPED`). *Why:* `SKIPPED` is a choice and
must not block; `NOT_EVALUATED` is an absence of evidence and must. Collapsing them would make
an unevaluated required check non-blocking, re-opening the exact defect `dq_engine.py` was
written to close. *How to apply:* one extra status is cheaper than losing the distinction, and
`BRIEF_STATUSES` names the five so the extension is visible rather than silently absorbed.

**D-DRP1-7 — Parse `dim_builder.py`, do not import it.**
`DIMENSIONS` is read with `ast`. *Why:* it uses bare module imports that only resolve with
`spark/dimensions` on `sys.path`, and `cdc/` importing from `spark/` inverts the dependency
direction the package was restructured to protect. *How to apply:* the same technique
`test_dbt_contract.py` already uses on the dbt macro — read the declaration, do not execute it.

**D-DRP1-8 — Fix the 15 PII classifications; leave the 16th finding open.**
`account`, `transaction` and `digital_event` were raised to `confidential`. *Why:* the masking
views and the AI deny list read classification, not the PII list, so those columns were marked
and unprotected — a one-line fix with real effect. *How to apply:* `curated:dim_customer_bi`
stays a reported finding because closing a registration gap by fiat hides it; and
`owner: my-aws-profile` stays untouched because reassigning ownership is the owner's decision,
not a side effect of a metadata phase.

---

## 2026-09-30 — DRP2 (DataHub platform foundation)

**D-DRP2-1 — `disabled` is a first-class mode and the default.**
*Why:* if the off case is only reachable by breaking something, nobody ever exercises it, and
the platform quietly acquires a dependency on a catalogue. *How to apply:* the lab default is
`disabled`, so every test run publishes through `NullTransport`; a publish there is `skipped`,
not `published` — counting it would make a coverage metric report a catalogue that does not exist.

**D-DRP2-2 — A data flow may never be REQUIRED.**
*Why:* by the time a job publishes metadata its rows are committed and correct. Failing it on a
DataHub outage loses nothing and breaks everything. *How to apply:* the config **refuses**
`REQUIRED` as the default and on any flow outside `metadata_ingestion` / `lineage_impact` — the
two whose entire purpose is metadata. With the plane off, those two refuse to *start*, which is
more honest than failing.

**D-DRP2-3 — Retry bounded, worst case published.**
3 attempts × 10 s + 0.5 s + 1.0 s backoff = **31.5 s**. *Why:* a number nobody can state is a
number nobody has bounded, and unbounded retry inside a Spark task is a hang wearing a
resilience costume. *How to apply:* `worst_case_seconds` is a property, and a test asserts it.

**D-DRP2-4 — One URN per table, minted in one function.**
All five lake kinds → `urn:li:dataPlatform:glue`, named `<database>.<table>`. *Why:* one table
is visible to the Glue source, an S3/Iceberg source and the Spark listener; three names make
three datasets and split the lineage graph into fragments that each look complete. *How to
apply:* every producer — including DRP3's OpenLineage facets — derives from `UrnMinter`, and a
test asserts every compiled asset mints a distinct URN.

**D-DRP2-5 — The fabric is enforced on every emit, not just modelled.**
*Why:* a dev process emitting a `PROD` URN makes the production graph confidently wrong in a
way that looks like *more* coverage, and nothing downstream can tell. *How to apply:* two
environments may not share a fabric; `assert_fabric()` raises on a foreign URN; a client and
its minter may not disagree. Domains are name-scoped because DataHub domains carry no fabric.

**D-DRP2-6 — Drive the official quickstart; do not keep a compose file here.**
*Why:* DataHub's topology changes between releases, and a stale local copy fails as a
half-initialised metadata store — which looks like a DataHub bug and is not one. *How to
apply:* `scripts/datahub-local.sh` calls the pinned CLI with the pinned version, and preflights
RAM and disk first because an OOM mid-quickstart produces exactly that half-built store.

**D-DRP2-7 — Compare server and CLI on a release LINE, not a string.**
*Why:* DataHub advances the two on independent build cadences; on 2026-09-30 the newest image
was `v1.7.0.1` and no `1.7.0.1` existed on PyPI. String equality would reject a valid pair.
*How to apply:* compare the `major.minor.patch` prefix — a CLI a whole MINOR ahead writes
aspects the server cannot read, which is the failure worth catching.

**D-DRP2-8 — `urllib` only in the client.**
*Why:* this module ships to EMR inside `cdc-framework.zip`. Adding an HTTP client to publish
metadata would put a new transitive dependency tree into every Spark job — a catalogue feature
turned into a deployment risk. *How to apply:* no new dependency; `RestTransport` is plain
`urllib` and is marked NOT LIVE-TESTED until a GMS exists.

**D-DRP2-9 — Read `maven-metadata.xml`, not the Maven search API.**
*Why:* the search index reported `openlineage-spark_2.12` at 1.34.0 while the metadata had
1.53.0 — nineteen releases stale. Pinning from the index would have shipped a year-old jar with
a resolve to point at. *How to apply:* resolve from the authoritative metadata, and record the
checksum, not just the version.

---

## 2026-09-30 — DRP6 → DRP12

**D-DRP6-1 — Read `entities.yaml` as an instruction, not a description.**
EOD→CURATED edges and their column mappings are `derived`. *Why:* `curated_build.py` reads
that same file to do the work, so the edges are not a human's account of what the job does —
they are what it does. *How to apply:* an artefact the producer CONSUMES yields derived
lineage; an artefact a human WROTE ABOUT the producer yields declared.

**D-DRP6-2 — Declare the Python-only producers rather than leaving a hole.**
*Why:* a missing edge reads as "nothing depends on this", which is the most dangerous wrong
answer an impact query can give. *How to apply:* `lineage_declared.yaml`, each entry naming
its `declared_in` and a reason, and marked `declared` so recoveries across it stay
operator-approved. An empty `upstreams` list means "generated, no parent" and keeps a real
orphan distinguishable.

**D-DRP6-3 — dbt same-name column mappings are `declared`, not `derived`.**
*Why:* the manifest names a model's columns; it does not prove which upstream column each
came from. `balance` and `balance` may be the same value, a rounded one, or a different
measure with a convenient name. *How to apply:* a fuzzy mapping on a critical field is worse
than none, because it looks authoritative and stops the investigation.

**D-DRP8-1 — Translate URN ↔ AssetId at the boundary.**
*Why:* the graph speaks URNs and the incident/recovery contracts speak `kind:name`. Blurring
them puts a URN where a kind belongs and fails deep inside a validator instead of at the
edge. *How to apply:* `UrnMinter.asset_for()` is the exact inverse of `dataset_urn()`.

**D-DRP8-2 — Two jobs writing one table is refused, not resolved.**
*Why:* a rerun would have to choose, and choosing silently rebuilds a table with the wrong
producer. *How to apply:* `producing_job()` raises and names both.

**D-DRP9-1 — Re-evaluate the approval at execution time.**
*Why:* the world moves between planning and running — attempts get spent, an incident
becomes terminal, a limit is tightened. Trusting the plan's recorded verdict would let a
stale approval authorise work nobody would approve now.

**D-DRP10-1 — A proposed SLO may not page.**
*Why:* paging on a guess wakes people for nothing, and the second time it happens the alert
is muted — which is how a real one gets missed later. *How to apply:* every SLO carries a
`basis`, a `measured` one must cite its evidence, and `alertable()` refuses a PAGE that rests
on `proposed`.

**D-DRP10-2 — Alerts are deduplicated by a declared `group_by`.**
*Why:* an alert storm is not "too many alerts" — it is one condition arriving as fifty. Ten
tables failing one check on one COB must be one page. A rule without a grouping key is
refused at construction.

**D-DRP11-1 — Mark nothing PASS that has not run.**
*Why:* ten scenarios could each have been argued green from a unit test. This project's
recurring defect is exactly that — a test asserting on generated text cannot see that the
text does not run. *How to apply:* the E2E matrix states what is proven offline per
scenario and marks all ten live columns NOT_RUN;
`artifacts/validation/data-reliability/` stays empty with a README saying why.

**D-DRP12-1 — Finish NOT_READY with twelve named blockers.**
*Why:* the programme's own instruction is "no PASS without evidence", and eleven of twelve
checkpoints being complete does not make the twelfth true. *How to apply:* each blocker
names what clears it, and which are operator actions rather than code.

**D-REHYDRATE-1 — Write the cold-start document while the context is still warm.**
*Why:* every fact in it — why `mode: disabled` is correct, why rank 0 is reserved, why a mart
recovery requires approval, why two governance files are deliberately left drifted — is
cheap to state now and expensive to re-derive later. A new session that does not know them
will "fix" three things that are not broken. *How to apply:*
`docs/DATA_RELIABILITY_CONTEXT_REHYDRATION.md` §15 lists exactly those five, and §13 gives
the single next command rather than a menu.

**D-LIVE-1 — Treat the offline smoke test as a rehearsal, not as evidence.**
The first live publish found three defects in fifteen minutes that 48 offline tests had not:
a missing required `auditStamp`, a union field sent as a string, and a transport that threw
away the server's explanation. *Why:* a recording transport accepts any dict — the aspect
shape is only validated by the server, so the offline test was asserting that we built what
we intended to build, never that DataHub would accept it. *How to apply:* an offline test of
a wire format proves internal consistency and nothing about the far end; the runbook now
says so in those words.

**D-LIVE-2 — Make the transport report the server's reason.**
*Why:* `HTTP 422` across nine proposals names none of them. The first fix of the live session
was to read the error body, and it turned a guessing exercise into two one-line fixes. *How
to apply:* an error that cannot say what was rejected is nearly useless; capture the body
before doing anything else.

**D-LIVE-3 — A stale lineage index under-reports, and under-reporting is the dangerous
direction.**
A traversal seconds after publishing reached 3 assets; minutes later, 18. *Why:* it does not
fail — it returns a smaller blast radius, confidently, and a recovery planned on it leaves
descendants un-rebuilt while looking complete. *How to apply:* never compute an impact plan
straight after an ingestion run; cross-check against the synchronous `get_aspect` read; and
when DRP8 moves onto the DataHub API, handle it explicitly rather than trusting the first
answer.

**D-LIVE-4 — Stopping the plane is a test opportunity, not just an ending.**
The operator shutting DataHub down made DRP11 scenarios 7 and 8 executable against a real
refused socket. *Why:* an outage policy proven only with a fake transport is proven against
the double, not against the condition. *How to apply:* the regression test now uses a closed
port (`127.0.0.1:1`), which is hermetic and reproduces the same behaviour without needing a
service stopped — the live run is what established that the two agree.

**D-LIVE-5 — The read path must refuse, not return empty.**
*Why:* an unreachable catalogue and a dataset with no downstreams are the same shape if the
client swallows the error, and "nothing depends on this" is the most dangerous wrong answer
an impact query can give. *How to apply:* `get_aspect` and `relationships` propagate the
transport error; `lineage_impact` is REQUIRED so it cannot start against a dead plane at all.

**D-FIX-1 — Ship the config the runtime reads, and say why in the script.**
`cdc-framework.zip` bundled `cdc/**` and nothing else, so seven files the DRP modules load at
runtime were missing. *Why it was worth fixing before anything ran:* the failure is not an
import error caught in seconds — it is a missing file when a job calls `Vocabulary.load()`
after acquiring EMR capacity, which is the exact shape of the `per_table` and `full_cdc_job`
defects already in this file's history. *How to apply:* a test asserts the bundle includes
them **and** that the reasoning comment survives, because a rule with no reason is one
somebody deletes while tidying.

**D-FIX-2 — Correct a false evidence marker rather than deleting the file that carries it.**
`governance/lineage/openlineage.yml` claimed `observed` for Spark hops nothing had emitted.
*Why not delete it:* `ai/tools.py` and `ai/knowledge/sources.py` read it at runtime, so
deleting it breaks the AI plane to fix a documentation defect. *How to apply:* correct the
markers to what is true, add a SUPERSEDED header naming the live graph, and leave the
repointing to the phase that owns it (B4). Existing tests passed unchanged — they had
asserted the connector edges were `declared`, never that the Spark ones were `observed`,
which is why the false claim survived a full suite for 38 sessions.

**D-B3-1 — Create the ledgers with Athena, not by piping the Spark DDL.**
Athena rejects `format-version` outright and writes Iceberg v2 by default; Spark requires it.
*Why it matters beyond this one property:* the two engines are not interchangeable for DDL,
so a script that "just runs the .sql file" would fail on the first table and look like a
permissions problem. *How to apply:* a dedicated script, dry-run by default, with a
`--verify` mode that answers "do these exist?" without creating anything.

**D-B3-2 — Round-trip the ledgers with records built from the MODEL.**
The proof inserts records constructed by `cdc/quality.py` and `cdc/incidents.py` rather than
hand-written SQL literals. *Why:* it proves the contract and the storage agree — including
that `ReconResult.difference` computed in Python (-1.0) is what lands in the column. A
hand-written INSERT would prove only that the table accepts values.

**D-CAPTURE-1 — Ship an evidence script; say plainly that it cannot record a screen.**
*Why:* a portfolio needs proof, and the honest boundary is that this environment has no
display. *How to apply:* the script collects the 11 shots that are text, names the 3 that
need AWS, and the README's shot list marks which require a human to record — with the DataHub
lineage graph called out as the one worth recording.

**D-B12-1 — Three of my own tests failed when B12 landed, and they were right to.**
`test_python_only_producers_are_declared_not_derived`, `test_declared_edges_load_into_the_graph`
and `test_a_declared_edge_on_the_path_forces_approval` all asserted the LIMITATION the fact
contract removed. *Why this is worth recording:* a failing test after a genuine improvement is
the test doing its job — it pinned the old truth so the change could not happen silently.
*How to apply:* update them to the new truth **and keep what they were protecting**. The pair
is now "a mart recovery is automatic" plus "rooting at the SOURCE still forces approval,
because the Debezium hop can never be better than declared". Deleting the second would have
traded a real guard for a green suite.

**D-B6-1 — Two executors, one rule set, one result shape.**
`scripts/run-dq-athena.py` runs the SAME `build_rules()` output the Spark engine takes and
writes the same `DqResult` contract to the same ledger. *Why:* a second rule set would be
precisely the drift this programme exists to end; a second executor is only a cheaper way to
get a verdict without acquiring EMR capacity. *How to apply:* if the two ever disagree, the
rule set is not the thing to change.

**D-FIX-3 — The recovery sequence must not require a scheduler.**
`airflow dags trigger recovery_coordinator` failed with `DagNotFound`, and the command was
wrong for the environment it was given in: local Airflow is 2.9.3 pointed at `~/airflow/dags`,
and the deployed 3.2.2 is on a k3s cluster with no `kubectl` here. *The deeper problem it
exposed:* the approval gate, the turn ordering and the job resolution have nothing to do with
scheduling, yet were only reachable through a registered DAG — so the safety logic was
untestable everywhere except the one cluster. *How to apply:* the steps now live in
`scripts/run-recovery.py` and the DAG calls them; the runner imports no Airflow, and a test
asserts it. Airflow orchestrates; it does not own.

**D-FIX-4 — Record BLOCKED_REQUIRES_EMR rather than SUCCEEDED.**
The runner passes the gate, resolves all 14 targets to registered entrypoints, and then
writes an execution row saying it rebuilt nothing. *Why:* a coordinator reporting SUCCEEDED
having run no turn is precisely the failure this platform keeps finding — a green result
that describes work that did not happen. The row names what passed and what is missing.

**D-DRILL-1 — Prove the loop with a drill; do not simulate the verdict.**
Rebuilding a production mart needs EMR and a write to a certified table. The recovery LOOP
needs neither. *Why a drill is legitimate here:* it plants a REAL duplicate in a real Iceberg
table, a real DQ check detects it, and a real rebuild corrects it — the mart sum moves
80.0 → 60.0. Nothing about the sequence is simulated; only the scope is reduced. *How to
apply:* use a clearly prefixed sandbox, abort if the planted defect is NOT detected (a drill
that passes without firing proves nothing), and state in the evidence what it does not
prove. A drill presented as a production run is the failure this platform keeps catching.

**D-DRILL-2 — Close scenarios 2, 5 and 6 with drills that drive the real decision code.**
The three remaining scenario drills use sandbox Iceberg tables in the real lake, the real
Glue catalog and the real Athena engine, and they call `flows.may_overwrite`,
`dq_catalog.evaluate_publish` and `FlowContext` rather than reimplementing their logic.
*Why:* a drill whose verdict comes from drill-local code proves only that the drill agrees
with itself. Routing the decision through the shipped functions means the scenario is
exercising the thing that runs in production. *How to apply:* the drill may shrink the data
and the blast radius; it may never substitute its own copy of the rule under test. Each
evidence file carries a `not_proven` list — for these, the production-mart rebuild and the
Spark DQ engine.

**D-AIRFLOW-1 — Prove the OpenLineage provider in an isolated pod; do not upgrade the
running scheduler to find out.**
Closing the Airflow lineage gate needed the provider on Airflow 3.2.2. Chart `airflow-1.22.0`
removed `extraPipPackages`, so the options were a custom image, a `PYTHONPATH` shim into the
live pods, or an isolated pod from the same image. *Chosen:* an isolated pod with an
ephemeral sqlite metadata DB. *Why:* the shim risked shadowing libraries in a scheduler that
had been healthy for eleven hours, to answer a question that does not require the scheduler
at all. All eight production pods stayed `Running`. *How to apply:* when a capability
question can be answered on a copy, answer it on the copy — and say plainly in the evidence
that the resident deployment is unchanged, which is why `does_not_prove` names the image.

**D-CONFIG-1 — Test runtime config against its consumer (ADR-091).**
`AIRFLOW__OPENLINEAGE__TRANSPORT=console` passed a test that compared `values.yaml` to the
module that derives it; both carried the same wrong value. The provider parses that key as
JSON and rejected it during plugin import, so the listener was never registered and every
DAG ran green emitting nothing. *Why:* internal agreement is not correctness. *How to apply:*
assert the consumer's parse, keep the file-to-file check as a drift guard rather than the
only assertion, and refuse a value that cannot be expressed safely instead of half-rendering
it — `airflow_transport()` raises for `http` rather than inlining a token.

**D-JAR-1 — Stage the OpenLineage jar in S3; do not resolve it from Maven.**
`spark.jars.packages` was the derived form until an EMR run proved it cannot work: EMR
Serverless runs in private subnets and this VPC has **no NAT** (cost invariant 2), so Ivy
cannot reach repo1.maven.org. *Why it mattered more than it looked:* the job did not fail
with a networking error — it died **during dependency resolution having done no work**,
which reads as a Spark problem. *How to apply:* stage the pinned artifact in the lake,
reference it with `spark.jars` over the S3 gateway endpoint, and verify the recorded sha1
against both Maven's published `.sha1` and the staged object. Maven resolution is kept as
the fallback for a submit from a host that has egress.

**D-JAR-2 — Merge into `spark.jars`; never emit it twice.**
`spark.jars` is a single comma-separated key, and `emr-submit.sh` already set it to the
Iceberg runtime jar. A second `--conf spark.jars=` from the derived lineage conf would have
**silently replaced Iceberg** — last flag wins — and every Iceberg read would have failed
with a `ClassNotFound` bearing no resemblance to a lineage change. *How to apply:* when a
derived conf and a hand-written conf can set the same key, merge explicitly and test the
merge; do not rely on ordering.

**D-EMR-1 — An empty config value can be the finding.**
`jar_runtime_path` is empty on purpose. OpenLineage and Iceberg load under different
classloaders on EMR Serverless, so no dataset events are emitted; OpenLineage's documented
cure is to put the jar on the system classpath, and EMR Serverless answers
`ValidationException: Option 'spark.driver.extraClassPath' is not supported` — **at submit
time**, so a value there stops every lineage job from starting. *How to apply:* keep the
mechanism for platforms that allow it, hold the value empty with a test, and say in the
config why it is empty. A blank field with no explanation reads as an oversight and gets
filled in.

**D-RECIPE-1 — Execute a generated recipe before believing it.**
The ingestion recipes had been generated, tested and never run. Running two of them found
three things no test had: the dbt recipe **cannot run to a file sink at all**
(`write_semantics: PATCH` requires a live graph), `convert_urns_to_lowercase` was being
**defaulted** on dbt — the split-identity risk ADR-090 exists to prevent — and
`GlueSourceConfig` **rejects** that key outright, which is how we learned the Glue side
never lowercases and that `False` on dbt *matches* Glue rather than being a preference.
*How to apply:* a generated artefact is a hypothesis until something consumes it.

**D-AIRFLOW-2 — The provider was never missing; the CONFIG was. Correcting the record.**
Three documents said the OpenLineage provider was "not installed in the Airflow image".
A read-only `--verify` against the running scheduler showed otherwise: the stock
`apache/airflow:3.2.2` ships `apache-airflow-providers-openlineage==2.17.0` and
`airflow plugins` already lists `OpenLineageProviderPlugin`. What the deployed release
lacks is any `AIRFLOW__OPENLINEAGE__*` variable — the DRP3 block was added to
`values.yaml` after that release was cut. *Why the wrong belief survived:* the earlier
probe ran `pip install ...==2.20.2`, which **upgrades** as silently as it installs, so its
success was read as "it was absent". Nothing had asked the running cluster. *How to apply:*
before concluding a component is missing, query the place it would be missing FROM — a
read-only check against the live system beats an inference from an install that succeeded.
*What did not change:* the custom image is still worth deploying, because the stock pairing
is provider 2.17.0 with `openlineage-python 1.47.1` while the pinned Spark listener is
1.53.0, and two integrations writing one graph on different client majors drift in producer
strings and facet schema versions — invisibly, until two halves of one path disagree.

**D-SHELL-1 — `<long producer> | grep -q` under `pipefail` is a race, not a test.**
`scripts/airflow-enable-lineage.sh` guarded the upgrade with
`k3s ctr images ls | grep -q "$IMAGE"`. `grep -q` exits on the first match, the producer
can take SIGPIPE, and `set -o pipefail` turns that into a failed pipeline — so the guard
reported the image **missing when it was present**. *Why it is the bad kind of bug:*
three identical runs on the node gave present / NOT PRESENT / present. An intermittent
guard passes review, passes a manual test, then fails roughly one deployment in three with
a message telling the operator to rebuild something that already exists — and the obvious
response is to delete the guard. *How to apply:* capture the producer's output into a
variable, then match against it. A repository scan found one other instance of the shape
(`ss -lnt | grep -q ":$PORT "`), fixed the same way; `echo "$var" | grep -q` is safe
because the producer completes instantly.

**D-SSM-1 — A piped remote command reports success for a failed deployment.**
The SSM invocation ended `bash /tmp/ol.sh 2>&1 | tail -40`, so the recorded status was
`tail`'s — always 0. A run whose script printed `REFUSING:` and exited 1 was reported as
`ssm status: Success`, and the caller went on to print the post-upgrade hint. *Why it
matters most of all the bugs here:* every other failure in this project is discovered by
something reporting honestly; a deploy tool that reports success on failure removes that.
*The obvious repair was also wrong:* `exit ${PIPESTATUS[0]}` has to survive a local
double-quoted string, JSON, and the remote shell, and it arrived literal —
`exit: ${PIPESTATUS[0]}: numeric argument required`. *How to apply:* run the remote script
**unpiped and with no shell variable of its own**, so the exit status is simply the
script's and no escaping exists to get wrong. Bound the output inside the script instead.

**D-GUARD-1 — A guard must not cover a mode it cannot describe.**
The "namespace was mangled" check ran for every mode, but only the upgrade body sets
`AIRFLOW__OPENLINEAGE__NAMESPACE`. The result was that `--verify` — the read-only,
always-safe path, and the one an operator reaches for first — was the only mode that could
not run. *How to apply:* scope a guard to the code path whose invariant it states.

**D-HELM-1 — Helm NOTES leaked the admin password; suppress the block, not just the line.**
The chart's `NOTES.txt` renders `createUserJob.defaultUser.password` in the clear, so
`helm upgrade` printed the Airflow UI admin credential into the operator's terminal — and
from there into scrollback and any pasted transcript. *Why the obvious fix is too narrow:*
masking `password:` lines alone leaves the next chart version free to print the same secret
under a different label. The script now drops **everything from `NOTES:` onward** and *also*
masks credential-shaped lines, so a future chart that moves the secret out of NOTES is still
covered. *How to apply:* when a third party formats output you do not control, filter by
**section** first and by pattern second. The exposed credential must be rotated; a redaction
added after the fact protects the next run, not the last one.

**D-AIRFLOW-3 — Prove emission where the task actually runs.**
Four attempts to observe Airflow lineage failed before one worked: the dags PVC is not
writable from the pod; `dags test` in the scheduler cannot find the DAG because only the
dag-processor mounts the files; and `dags test` — even `tasks test` on a single
`EmptyOperator` — is OOM-killed at the dag-processor's 512Mi limit. *What the failures were
still worth:* the OOM run logged the listener applying its emission policy to the task, which
proved the listener was live before anything had emitted. *How to apply:* with
KubernetesExecutor the task runs in its own pod, so that is where the evidence is. There is
no remote logging and `delete_worker_pods` defaults to true, so worker logs must be scraped
**while the pods are alive** — or remote logging enabled, which is now an open item.

**D-AIGR-1 — ADR-057 is the gate for the whole copilot programme, and it is a feature.**
`ToolSpec.__post_init__` raises on any tool with `read_only=False`. Every AIGR checkpoint
past AIGR1 depends on relaxing it. *Decision:* do not relax it by edit. Supersede it with an
ADR that keeps `read_only=True` as the default and adds a `recovery_submit` class whose
**membership is closed to a single tool name**. *Why membership rather than behaviour:* a
mutation surface whose behaviour is reviewed can grow one reasonable tool at a time until it
is no longer a surface; a surface whose membership is fixed cannot. *Also recorded:* the
prompt pack assumes a checkpoint `BUSINESS_AI_PRODUCTION_READY` that does not exist under
that name, and assumes AgentCore is available — ADR-056 defers it with a revisit trigger
(multiple concurrent users needing isolated managed-identity sessions) that one operator
running recoveries does not meet.

**D-AIGR-2 — Close the mutation surface by membership, not by review (ADR-092).**
ADR-057 refused every mutating tool in `ToolSpec.__post_init__`. AIGR needs one. *Why not
"allow it and review each":* that surface grows — `retry_job`, `clear_task`,
`refresh_partition` — each defensible alone, the aggregate unbounded. *Chosen:*
`read_only=True` stays the default; `plan_write` may write only append-only planning
records; `recovery_submit` is the sole mutating class and its membership is a frozenset of
two names. *How to apply:* adding a mutation is an ADR amendment plus two edits, never a
registration. And `submit_recovery_plan` takes `plan_id`, `approval_id`, `idempotency_key`
and **nothing else** — the agent cannot describe the work, only submit what a deterministic
planner built.

**D-AIGR-3 — A finer root-cause taxonomy exists for exactly two cases.**
Twelve of the fourteen categories map cleanly onto the existing `FailureClass`. The two that
do not are the point: `TRANSFORM_LOGIC_DEFECT` and `DQ_RULE_DEFECT`. Both previously landed
in classes `REPAIRABLE_BY_RERUN` treats as fixable, and both are cases where rerunning makes
things worse — one reproduces the wrong answer at cost, the other repairs data that may be
correct. *How to apply:* when extending a taxonomy, justify it by the decisions it changes,
not by the resolution it adds.

**D-AIGR-4 — The accounting must survive the thing it accounts for.**
The graph's step budget was charged to `build_evidence`, so a run that exceeded its ceiling
produced **no evidence pack** — no explanation of the one failure the user most needs
explained. *How to apply:* exempt the audit path from the limits it records. A budget that
can suppress its own postmortem is not a safety feature.

**D-AIGR-5 — Score the copilot by the standard that scored the platform.**
AIGR12 returns NOT_READY with 19 of 28 items passing. Every safety property is proven by
construction; nothing has run against a live model or a live catalogue. DRP12 returned
NOT_READY twice before earning READY on live evidence. *Why this matters more than the
verdict:* two checkpoints scored by different standards cannot be compared, and a programme
whose later phases grade themselves more kindly than its earlier ones has stopped measuring.

**D-AIGR-6 — A repair template must be driven by its scope, not by a predicate.**
The first live AI-driven recovery "succeeded" and left the mart **wrong in a new way**:
1600 → 900 against an expected 1000. The executor template repaired with a value heuristic
(`balance > 350`) instead of the planned scope, so it missed one affected account and
halved one that was never corrupted. *Why it is the worst possible shape:* the plan was
valid, the policy allowed it, the approval was valid, the execution reported `SUCCEEDED`,
and every gate upstream passed. Nothing in the safety layer was wrong; the statement simply
did not implement the plan. *How to apply:* `AthenaRecoveryExecutor` now **refuses a
template that references no scope placeholder**, and the EOD repair became a rebuild from
FULL_CDC — correct by construction rather than by a predicate someone guessed. A guard that
says "this statement does not mention the scope it was given" would have caught it before
the first row moved.

**D-AIGR-7 — We met a DQ_RULE_DEFECT by accident, inside the code demonstrating the category.**
After the repair, verification reported a violation: `balance > 350` flagged an account that
legitimately holds 400. The data was fully correct and the **check** was wrong. *Why worth
recording:* `DQ_RULE_DEFECT` is one of the two categories the AIGR taxonomy exists to tell
apart from a data defect, and the demonstration reproduced it unintentionally. *How to
apply:* verify against the contract, not against a proxy — the check now compares EOD to
FULL_CDC, which is what EOD is defined to equal. A threshold that approximates a contract
will eventually disagree with it, and when it does the data gets "fixed".

**D-AIGR-8 — Bedrock: an inference profile, and an entitlement that can lapse mid-session.**
A bare model id is rejected (`Invocation ... with on-demand throughput isn't supported`); an
**inference profile** id is required. Then, after two successful invocations returning token
usage, the account began answering *"Model use case details have not been submitted"* —
an entitlement gate only the account owner can clear. *How to apply:* resolve the model at
runtime from a candidate list rather than pinning one, and let a gated account **degrade the
demonstration, not abort it** — the copilot's safety properties do not depend on a model, so
an absent classifier resolves to `AMBIGUOUS` (which cannot mutate) and an absent composer
omits prose. The evidence file records which half ran.

**D-AIGR-9 — Raise lineage confidence by checking, never by relabelling.**
`DERIVED` column lineage blocked the copilot's headline capability. Two ways forward: change
the evidence string, or confirm both endpoints of every edge actually exist.
`cdc/column_validation.py` does the second, and it had to handle a wrinkle — **CDC layers
store the source row as JSON**, so `payload_after` is a `string` and `BALANCE` is not a
column on the EOD table at all; it is a key inside a blob, present in every row. A Glue
schema lookup alone confirmed **3 of 67** edges. Reading the JSON keys from real data as
well confirmed **28**, including all six on the account table. *How to apply:* the 39 that
fail are recorded as failures, not hidden — ephemeral dbt models have no table to check and
empty tables have no keys to read. An unconfirmed edge stays `DERIVED` and still cannot
narrow a recovery. Confidence now has a date and a method behind it.

**D-KAFKA-1 — The kafka recipe named the Java client's SASL mechanism.**
`sasl.mechanism: AWS_MSK_IAM` had been in the generated recipe for months. It generated
cleanly, passed every offline test, and **could never have authenticated**: DataHub's Kafka
source uses confluent-kafka (librdkafka), which answers `Unsupported SASL mechanism:
AWS_MSK_IAM` — that name belongs to the Java client. Verified against librdkafka directly,
not inferred. *Also recorded, because the fix is incomplete on purpose:* MSK IAM needs an
`oauth_cb`, a Python **callable** that mints a token, and YAML cannot carry a callable. So
`datahub ingest -c kafka.yaml` still will not authenticate; the recipe now says so rather
than looking complete. *How to apply:* a config value naming a mechanism, a codec or a
driver is a claim about a library, and only that library can confirm it.

**D-DRILL-1 — A dry run must never overwrite real evidence.**
`ai-recovery-drill.py` in dry-run mode returned dummy values and then wrote the evidence
file, replacing a real capture with zeros that still looked like evidence — and printed
`FAIL`, because it "verified" numbers that had never been measured. *How to apply:* a dry
run reports `DRY_RUN`, never `PASS` or `FAIL`, and writes no artifact. The cheapest way to
destroy a record of a real run is a safe-looking rehearsal of it.

**D-AIGR-10 — A guessing classifier defeats every gate behind it.**
The copilot's first root-cause heuristic mapped the word "wrong" to `DUPLICATE_CDC_EVENT`.
The effect was the dangerous one: a request describing a **source** defect produced a plan,
because the guard that forbids planning for a source defect only fires once the cause is
classified as one. The guard was intact; nothing ever reached it. *How to apply:* a cause is
asserted only when the request **states** it, and a symptom ("it is wrong") yields `UNKNOWN`,
which authorizes nothing. The evidence reference stays honest about what it is —
`request:<id>` means the operator said so, not that a ledger was read.

**D-RESOLVE-1 — Filler words create ties, and a tie reads as a real ambiguity.**
`resolve_asset` refuses when two assets score equally, which is right. But it scored every
token, so "a duplicate CDC **event** doubled BALANCE in EOD ACCOUNT" tied `digital_event`
with the account table and the resolver refused a question it should have answered. *How to
apply:* drop platform and English filler **before** scoring — once a filler-induced tie
exists it is indistinguishable from a genuine one. Distinctive tokens only; `EOD` alone
still refuses, and still names its four candidates.

**D-PLAN-1 — A plan listing job ids alone is not actionable.**
One job rebuilds many assets: `reporting` produces every mart, so a plan showed
`reporting` six times with nothing saying what each run touched. An approver cannot approve
that and an executor cannot execute it. `PlannedAction` now carries its `target`, and the
plan hash includes it — two actions of the same job on different assets are genuinely
different work and must hash differently.

**D-AIGR-11 — Route by intent, or a lineage question gets diagnosed.**
*"What does the BALANCE column in EOD ACCOUNT feed?"* returned
`terminated: unknown_root_cause` and *"Root cause UNKNOWN. No automatic recovery is
permitted."* The graph ran `classify_root_cause` for every request, so a question that never
asked for a recovery was told it could not have one — and the UI then rendered an empty
"no plan was built" card explaining five root causes that had nothing to do with it.
*Why it slipped through:* AIGR7's specification listed the routes
(`knowledge → read-only, investigate → diagnosis, plan → dry-run, ...`) and the graph was
built as a single unconditional sequence. Every safety test passed, because nothing unsafe
happened — it was merely absurd. *How to apply:* `KNOWLEDGE_INTENTS = {EXPLAIN, STATUS}`
skips root cause, planner and policy, and answers from resolved facts; the UI suppresses the
plan card when no plan could have existed. Seven tests hold it, including one asserting the
exact sentence the bug produced never appears in a knowledge answer.

**D-AIGR-12 — Read the ledgers; do not interview the operator.**
The copilot's root cause came from keywords in the question, which made it exactly as good
as the reporter's own diagnosis — and "plan a recovery for EOD ACCOUNT on 2026-09-28"
returned `UNKNOWN`, because the operator had not said what was wrong. **They were asking
because they did not know.** *How to apply:* `ai/reliability/evidence.py` reads
`dq_result_v2`, `reconciliation_run`, `dq_quarantine` and `eod_run` for that dataset and
date, and classifies from the rows — with the ledger row ids as evidence references, so any
diagnosis can be traced back. A cause the operator states is **corroborated** (0.9) rather
than believed (0.6), and stating one never changes what that category permits.

**D-AIGR-13 — A check that examined zero rows is not evidence of a defect.**
With the ledgers wired, the copilot diagnosed `MISSING_SOURCE_EVENT` from
`completeness.source_commit_ts` at **0 of 0 rows**, while
`uniqueness.one_active_row_per_key` at **1 of 670** sat in the same result set — it took the
first row by `finished_at`. That is the NOT_EVALUATED shape this platform already refuses to
treat as a verdict, reappearing one layer up. *How to apply:* rank by *examined > 0*, then
*failed > 0*, then severity; stop reading once a check examined nothing. The corrected run
diagnoses the duplicate and builds a plan.

**D-AIGR-14 — A word in the asset's name is not a column.**
"Plan a recovery for EOD ACCOUNT" reported `column ACCOUNT (DERIVED)`. The extractor took any
uppercase token, and `ACCOUNT` matched a real column edge — so a question that never
mentioned a column acquired one, and a spurious column changes what the scope narrows on.
*How to apply:* drop tokens that appear in the resolved asset's own name, and prefer explicit
"column X" phrasing over bare capitals.

**D-SCENARIO-1 — The everyday incident is a restatement, not a corruption.**
`--scenario source-update` models what actually happens most: the source amends a value and
CDC delivers the correction after EOD closed. Nothing is corrupt; the warehouse holds what
was true when it closed. *Why it is worth its own scenario:* the disposition is
`AUTO_CORRECT`, not a repair, and it shows the two granularities working together —
`eod_build` rebuilds the whole COB because it has no key predicate, while `reporting`
rebuilds **only the two affected keys**. Verified live: mart 1000.0 → 1125.0, exactly what
FULL_CDC says, with the other accounts untouched.

**D-AIGR-15 — A plan that rebuilds and stops cannot tell you whether it worked.**
`DQ_RECHECK`, `RECONCILE` and `CERTIFY` existed in `ActionType` from the start and **nothing
ever emitted them**. Every plan was rebuild-only, and the quality barrier — root repair →
root DQ → root reconciliation → root certification → descendants → descendant DQ — lived in
prose. An approver reading such a plan could not see that the root is validated before the
descendants run. *How to apply:* the planner now interleaves verification turns, so the
barrier is part of the artefact being approved. Descendants get DQ and reconciliation but
**not** certification: certification is a statement about the closed business date, made once
at the root, and certifying each mart separately would invent a tier for a derived asset.
Two plans that differ in whether anything verifies the result hash differently, so an
approval for one cannot cover the other.

**D-AIGR-16 — Two bugs the gate insertion introduced, both caught by existing tests.**
The first version hardcoded `job_id="reporting"` for descendant verification, putting a job
name into the plan that the descendant's pipeline never used — and the Recovery Control
Service refuses a plan naming an unregistered job, so the whole plan would have been
rejected at submit. The second copied the ROOT's scope and granularity onto child
verifications, claiming a check had run over a window the child never rebuilt. Both now
derive from the child's own action. *Worth noting:* the tests that caught them were written
for other properties entirely — a job-keyed dict became ambiguous once a job had more than
one action, and that ambiguity surfaced the copied scope.

**D-UI-1 — A long-running dev UI serves stale code.**
Two rounds of "this is still broken" were a UI process left running across the fix. Python
does not hot-reload, so the page showed the old copilot and looked like an unfixed bug.
*How to apply:* the UI now prints `restart this process after changing ai/reliability/` at
startup. The cheapest way to waste a debugging session is to test a fix against a process
that predates it.

**D-AIGR-17 — The executor keyed templates by job, so a verification ran a rebuild.**
With quality gates in the plan, `DQ_RECHECK` and `CERTIFY` on `eod_build` resolved to
`eod_build`'s template — the **rebuild** SQL. A verification turn silently re-executed a
`DELETE + INSERT`, and the drill still reported PASS, because an idempotent rebuild survives
being run twice. *That is luck, not correctness*, and a `CERTIFY` that rewrites a table is
exactly the action nobody would approve. *How it was found:* re-running the drill after
adding the gates and reading "7 statements executed" against a plan whose verification
actions should have read, not written. *How to apply:* templates are keyed by
`(job_id, ActionType)`, and an action with no template is **refused** rather than falling
back to another action's SQL — a fallback that looks convenient is a fallback that runs the
wrong statement.

**D-EVIDENCE-1 — Regenerate evidence when the code that produced it changes.**
`aigr10-live-e2e.json` recorded a 2-turn, rebuild-only plan and was cited by the AIGR12
review for "root DQ/recon before descendants". After the quality gates landed, the code
produced 4 turns and 7 actions — so the evidence no longer described the system, while
still looking like a clean capture. *How to apply:* an evidence artefact is only evidence of
the code that made it. When behaviour changes, re-run and re-record before re-scoring;
citing a stale capture is how a review certifies something that no longer exists.

**D-AIGR-18 — An ambiguous verb over a resolved asset is a lookup, not an incident.**
`"the customer snapshot"` resolved a table, described no defect, and came back
`Root cause UNKNOWN. No automatic recovery is permitted.` The routing added in D-AIGR-11
covered `EXPLAIN` and `STATUS`; `AMBIGUOUS` still fell through to the diagnostic path.
*How to apply:* `is_knowledge(intent, asset_resolved=...)` treats an ambiguous request that
nonetheless resolved an asset as a description, and the answer says what else can be asked.
**Ambiguity still blocks every mutation** — that is `MUTATING_INTENTS`, untouched. *And the
correction that followed:* routing all of `AMBIGUOUS` to knowledge was too broad — "the
source system sent a bad value" states a cause without asking anything, and answering it
with an ownership blurb skipped the disposition forbidding a rebuild. A sentence that
**states a defect** is promoted to `INVESTIGATE` regardless of how unclear the verb is.

**D-AIGR-19 — One table at four layers is not an ambiguity about which table.**
`"the affected downstream of oracle_coredb_corebank_customer"` refused: the name exists as
`src`, `full_cdc`, `realtime` and `eod`, and the resolver saw a four-way tie. But the caller
named the table exactly — they simply did not say which layer. *How to apply:* a new
`LayerAmbiguity` (a subclass of `ResolutionError`, so a caller that does not know about it
still refuses) carries the candidates. A **describing** caller answers across all four and
names them; an **acting** caller still refuses, because the layer decides what gets
rewritten. Names are compared normalised — `oracle_coredb_corebank_customer` and
`oracle.coredb.corebank.customer` are one table spelled two ways, and comparing raw strings
treats them as different.

**D-INTENT-1 — A keyword that is also a column name is a bad rule.**
`status` alone matched the STATUS intent, so *"what breaks if I drop STATUS"* — a lineage
question about a column — was classified as a recovery-status request. Rules are now
phrases (`status of`, `recovery status`, `is it done`). Separately, the lineage vocabulary
was absent entirely: `downstream`, `upstream`, `lineage`, `depends on`, `what breaks`,
`which tables`, `affected by` all fell to `AMBIGUOUS`.

**D-INTENT-2 — Acting beats diagnosing beats describing.**
Broadening the lineage vocabulary (D-INTENT-1) introduced a regression the sample sweep
caught: the flagship request — *"…Investigate and repair only the affected **downstream**
data"* — matched a lineage keyword, and because the rules were a flat ordered scan with
EXPLAIN second, a **repair request was answered as a lineage question**. *How to apply:*
intents are grouped by precedence — action verbs first, then diagnostic language, then
status phrases, then descriptive nouns — so the verb decides and a descriptive noun inside
an instruction does not make it a description. Above all of it, a sentence that **states a
defect** is investigated whatever its verb suggested, because "the downstream data is wrong"
reads as lineage by keyword while describing an incident. *Worth noting:* this was found
only by running all 15 samples end to end after the fix, not by the 21 unit tests that
already covered intent.

**D-RECOVER-1 — A plan nobody can act on is not an answer.**
`reliability-recover.py --execute` refused with *"no executor wired"* — honest and useless.
The operator's actual question is *"the pipeline was green and the number is wrong; how do I
rerun what it touched?"*, and a hash-locked plan does not answer it. *How to apply:* the
script now prints, per turn, **the platform's real command** for each action — the
`emr-submit.sh` line with the derived `--table`, the `run-dq-athena.py` line with the
business date. Where no one-line command exists (a mart rerun goes through the reporting DAG
/ a dbt selector) it **says so** rather than printing something plausible: a command that
was never run is worse than an admission. `--execute` still does not run the jobs — it
registers the plan, hash-locks it and attaches an approval, and the execution record says
`PENDING — nothing ran`, because a control service reporting SUCCEEDED having run nothing is
the failure this platform keeps finding.

**D-RECOVER-2 — `--asset` was undiscoverable.**
The flag took a phrase resolved against 84 governed assets, with a tie **refused** — and
nothing told the operator what the vocabulary was. `--list-assets` prints every asset
grouped by layer, and the docs give the four-row table that matters: `"EOD ACCOUNT"` works,
`"ACCOUNT"` is refused because four layers have one, and for a repair the layer decides
which table gets rewritten.

**D-DOCS-1 — A portfolio needs a different entry point from an operator's manual.**
The README had grown to 800+ lines of accurate reference and answered none of the first
questions a reviewer asks: how big is this, what is actually proven, and what was the
engineering judgement. Three documents now sit in front of it —
`PORTFOLIO_OVERVIEW.md` (the whole thing in five minutes, with a "what is not done" section),
`USE_CASES.md` (nine scenarios with real output), and `ENGINEERING_JOURNAL.md` (every defect,
reproduced, with what each taught). *How to apply:* the journal is the one that matters. A
feature list says what was built; a defect list with reasoning says how it was judged, and
the strongest entry in it is the one where a guard was intact and nothing ever reached it.

**D-DOCS-2 — My own link checker was the broken thing.**
The first pass flagged `docs/OPERATIONS_RUNBOOK.md#rebuilding-after-a-destroy` as broken. The
file exists; the checker had not stripped the `#anchor` before resolving the path. I nearly
"fixed" a working link on the strength of it. *How to apply:* when a check reports a failure
in something that looks correct, suspect the check. The corrected pass over all 275 documents
found exactly one genuine break — a `../docs/` path written from inside `docs/claude/`.

**D-DOCS-3 — The documentation was right and the code was wrong.**
Writing up the copilot transcripts for the portfolio meant re-running every sample question
instead of transcribing the screenshots. *"What does the BALANCE column in EOD ACCOUNT
feed?"* answered `column in`: the extractor read the word after `column` (a preposition), and
a substring test matched that fragment against clos**in**g_balance, returning `DERIVED`
rather than `ABSENT` — so the resolver stopped there and never tried `BALANCE`.
`AI_COPILOT_SAMPLE_QUESTIONS.md` had recorded the expected answer as `confidence VALIDATED`
since AIGR2. *How to apply:* a documented expectation is a test that nobody runs. When a doc
states what a command should print, make something compare the two — here, nothing did, and
the existing test used a phrasing with no trailing preposition, so it passed throughout.
ADR-093.

**D-DOCS-4 — A near-miss on a confidence tier is worse than an error.**
The `column in` defect cost more than a wrong label. `BALANCE` is `VALIDATED`; `DERIVED` is
below the bar that permits narrowing, so the planner fell back to table level and the
flagship *"repair only the affected data"* scenario rebuilt whole dates while holding the
evidence to narrow to one column and three keys. An `ABSENT` would have been visible.
`DERIVED` is a plausible state, so it passed through the planner, the policy gate and the
printed plan without looking wrong. *How to apply:* when a confidence tier can be reached by
accident, the tier below it stops being reachable at all — and a confidence that cannot be
`ABSENT` is not a confidence.

**D-DOCS-5 — A fix that moves a defect has not removed it.**
Dropping filler words turned *"the BALANCE **field** in EOD ACCOUNT is wrong"* from a wrong
answer into **no** answer, because only the keyword-first pattern had ever accepted `field`.
*How to apply:* the regression test written for the first defect caught the second. Writing
the test before declaring the fix done is what found it; running the fix by hand would not
have.

**D-DATA-1 — Data coverage belongs in the header, not in the fourth error message.**
The live mart holds one business date, so a day-on-day change, a forecast and an anomaly
score are all impossible. Each refused correctly and for a reason printed nowhere on the
page, which reads as a broken agent rather than a thin partition. The header now states
coverage and what it is not enough for. *How to apply:* when a limit makes several features
refuse, state the limit once where the user starts, not once per feature at the point of
failure.

**D-DATA-2 — A comparison that could not be made must say so.**
`compare_metric_periods` returns `comparison: None` when the baseline window is empty, and
the summary fell through to the bare-value branch: "Total closing balance was
2,031,880,160 VND", badged VERIFIED and CERTIFIED, `limitations: []`, for a question that
asked for a change. *How to apply:* every statement in that answer was true and the answer
was still wrong. When a question has a shape — a comparison, a breakdown, a ranking — check
that the shape was delivered, not just that the tool returned without error.

**D-DATA-3 — A question the product suggests must reach the tool that answers it.**
Four of the UI's one-click chips did not work. Two named no metric and were answered from the
knowledge corpus; one reached a branch whose `mart_rows` argument nothing had ever passed;
and once wired, that model scored zero rows because the feature builder wants the latest date
*with data* while the UI asks as-of the day after it. *How to apply:* the suggested questions
are a product surface and need a test each. Three of the four returned a confident,
well-formed answer to a different question, which is why none looked like a bug.

**D-DATA-4 — The demo document told reviewers the project could not run.**
`docs/DEMO.md` was dated Session 18 and stated there was no MSK cluster, no connector and no
EMR application. MSK is ACTIVE, four EC2 instances are running and EOD has been proven on
EMR. *How to apply:* a stale status claim in a document named DEMO is read by everyone who
opens the repository and believed. Documents that assert deployment state need re-verifying
whenever the state changes, or they should not assert it.

**D-DATA-5 — A layer name is not an asset reference.**
`"What is FULL_CDC?"` scored equally against all six FULL_CDC tables and was refused as an
ambiguous asset reference — correct machinery applied to the wrong kind of question, since it
asks about the architecture, not a table. `AI_COPILOT_SAMPLE_QUESTIONS.md` had promised an
answer since AIGR2. A concept glossary now answers definitional questions before resolution,
and fires only on definitional phrasing so `rebuild FULL_CDC for 2026-09-28` still plans.
*How to apply:* this is the second defect this month found by running a question the docs
already claimed worked. A documented expectation is a test nobody runs — the glossary's
wording is now asserted by tests, including that REALTIME and EOD are described as
**siblings**, because reading the layer model as a chain implies rebuilding EOD by first
rebuilding REALTIME.

**D-DATA-6 — A backfill may rebuild history; it may not invent it.**
The mart holds one business date because the source holds 30 updates on one day. The fast fix
— writing synthetic rows into `mart_account_balance_daily` — was rejected: those rows would
carry `processing_status = CERTIFIED` while no EOD run had produced them, and the
certification badge is the one thing this project exists to make trustworthy. The supported
path is `eod_engine --fulfill`, which recomputes a historical COB date from FULL_CDC and can
therefore only produce dates the events actually support. *How to apply:* when a demo is thin,
fix the demo (`--demo`, 30 dates, $0) or generate real source activity. Never make the
warehouse assert something the pipeline did not do.

**D-DATA-7 — Read the consumer, not the dry-run.**
`backfill-business-dates.sh` dry-ran cleanly and would have failed twice at `--execute`,
after the EOD jobs had been submitted and paid for: `--flow-mode` is an argparse `choices`
list of the `FlowMode` names so lowercase `fulfill` is rejected, and FULFILL takes a span
(`--from-date`/`--to-date`) rather than one invocation per date. *How to apply:* a dry-run
proves the strings were assembled, not that the receiver accepts them — the same lesson as
ADR-091, now in a shell script instead of a Helm value.

**D-DATA-8 — Seeded data moves to its own relation; it does not borrow a certification.**
The mart needed 90 business dates for the copilot to be demonstrable, and the obvious route
was to insert them. Every `processing_status` value is a claim about a process that ran, and
none of them means "made up for a demo", so the rows went to
`mart_account_balance_daily_demo` behind an `AI_MART_RELATION` override instead. They still
carry `processing_status = 'CERTIFIED'`, because a metric refuses a window weaker than its
`minimum_certification` and the gate would otherwise block every question — so the UI carries
a standing banner naming the relation. *How to apply:* when a fixture has to wear a label it
has not earned, put the disclaimer where the numbers are read, not in a README nobody has
open. `metric_version` changes with the relation, so the evidence pack records it too.

**D-DATA-9 — `MIN` on a certification tier is alphabetical, and that is backwards.**
`MIN(processing_status) AS weakest_status` was chosen so "one provisional partition cannot
hide inside an otherwise certified answer" — and MIN on a varchar sorts 'CERTIFIED' before
'PROVISIONAL_NRT' and 'RECONCILED', so the STRONGEST tier won the column named *weakest*.
`ai/insights/pipeline.py` had the same defect independently with a bare `min(statuses)`. It
never fired because every date's rows are written by one run and share a tier; seeding mixed
tiers would have made it reachable. The SQL now emits a rank-prefixed value (`1:PROVISIONAL_NRT`)
built from the same `STATUS_ORDER` tuple the Python ladder uses. *How to apply:* an existing
test asserted the literal string `MIN(processing_status) AS weakest_status` — it pinned the
defect in place and passed for as long as it existed. A test that quotes the implementation
cannot detect that the implementation is wrong; it now asserts the ordering.

**D-DATA-10 — The catalogue's schema is not a schema you can create.**
`SHOW CREATE TABLE` was the only authority that worked. `information_schema` reports Trino
type names, and three of them are rejected verbatim by Athena's DDL: `varchar` must be
`string`, `timestamp(6) with time zone` must be `timestamp`, and `'ICEBERG'` must be
lowercase. Two failed CREATEs were spent guessing before asking the warehouse to print its
own DDL. *How to apply:* read the target's own output rather than reconstructing it from a
description of it.

**D-DATA-11 — An override nobody can forget beats an override that is merely documented.**
The seeded relation was reachable only by exporting `AI_MART_RELATION`. Omitting it fails
silently and plausibly: the copilot reads the certified mart, says "1 business date", and is
indistinguishable from a seeding that never ran. It is now `--seeded`, printed on startup and
preflighted so a missing table exits with the command that builds it. *How to apply:* when
the failure mode of forgetting a setting is a page that looks correct and says something
false, the setting should not be a variable someone has to remember.

**D-AI-1 — The quality gate was measuring a retriever nobody called.**
`"What does total deposits mean?"` rendered `docs/RUNBOOK.md#iceberg-commit-conflicts` as a
CITATION under its answer: "deposits" appears in 0 of 708 chunks, so the subject was absent
and the hit came from "total" and "mean". I fixed `ai/retriever.py` — the wrong module. The
copilot reaches `ai/retrieval/service.py::Bm25Backend` through `agent_tools/catalog.py`, and
so does `make ai-eval-rag-gate`; `ai/retriever.py` serves only `ai/assistant.py`. The proof
was that the gate passed with `MIN_QUERY_COVERAGE = 0.99`, a value that should have starved
retrieval completely. *How to apply:* the same defect as ADR-091 — I verified against a
sibling instead of the consumer. When a change looks inert, break it deliberately and check
the test fails; a gate that cannot fail is not measuring you.

**D-AI-2 — An unseen term is maximally informative, not minimally.**
BM25 reaches an unknown word through `idf.get(w, 0.0)` and scores it like a ubiquitous one,
so a question about something absent looks like a question about something everywhere and any
chunk can answer it. Counting unknown terms at the corpus maximum makes the unmatched weight
visible. *How to apply:* a default of zero for "not found" is a claim, and here it was the
opposite of the truth.

**D-AI-3 — A filter that drops a third of the right answers is worse than a weak citation.**
Coverage at 0.65 removed the bad citation and cost recall@5 0.6842 → 0.4211, because a real
question's rare words are rarely all in one chunk. 0.30 was measured to hold recall, lift
groundedness 0.5614 → 0.5789 and take negative_correct 0.25 → 0.75. The tighter value was
refused on the measurement, not on taste. *How to apply:* the recorded baseline existed
precisely to stop me trading measured quality for one visible case, and it did its job.

**D-AI-4 — A count written in prose beside the enum that defines it will drift.**
The reliability UI said *"Five root causes cannot produce a plan at all"* and listed five,
omitting `SCHEMA_DRIFT`; the model says six. The sentence is now derived from
`IncidentCategory`/`RECOVERABLE` at render time. *How to apply:* the page had quietly
misdescribed the very rule it exists to explain, in the one place a reader would check it.

---

## 2026-10-06 — public portfolio refinement: four decisions

**D-PR-1 — Keep the full-source mirror; add entry points instead of curating.**
The refinement brief assumed a curated public repo (`src/public_examples/`, interfaces only,
"do not expose full production Terraform"). The repo is the opposite by a recorded decision:
425 of 425 Python files and 60 of 60 Terraform files are published, sanitized. Acting on the
brief literally would have deleted ~400 files and reversed that decision silently. Confirmed
with the owner, then resolved the other way: `docs/CODE_WALKTHROUGH.md` and a *Selected
implementation* section give a reading order into the real code. *How to apply:* a brief
written against an assumed repo shape is a hypothesis; audit before executing it, and when
the two disagree, the disagreement is the finding.

**D-PR-2 — Commit a baseline before refining anything.**
The copy had no commits and no remote, so nothing was diffable and nothing was revertible —
one bad `sed` would have been unrecoverable except by re-copying from private. `55f93d3`
records the copy as received; the refinement sits on a branch. *How to apply:* "it's only
documentation" is exactly when an unrecoverable tree bites, because nobody is being careful.

**D-PR-3 — Index the documentation rather than build a second one.**
The brief asked for ~15 new `docs/architecture/NN-*.md` files. Equivalents already existed
under different names (`DATAHUB_ARCHITECTURE.md`, `CDC_CONTRACT_IMPLEMENTATION.md`, 83 ADRs).
Writing the numbered tree would have created a second naming scheme and two documents per
topic that drift apart. `docs/README.md` maps each topic to the doc that already answers it;
only genuine gaps were written (`LAB_VS_PRODUCTION.md`, `CODE_WALKTHROUGH.md`). *How to
apply:* duplicated documentation is worse than missing documentation, because one copy is
always wrong and nothing says which.

**D-PR-4 — A lint gate that cannot pass is not a gate.**
`make lint-shell` is labelled "OPEN-06: shellcheck now enforced" and exited 1 on every run,
on two false positives — a `DRY_RUN` flag read by a function in `lib.sh` that shellcheck
cannot follow across the source, and Python f-string braces inside a `python3 -c` argument.
Both fixed at the source (export; a declare/assign split; one narrow disable with its
reason) rather than by lowering the severity. *How to apply:* a permanently red check
trains people to ignore it, which is strictly worse than not having it.

**Not done, deliberately:** no licence file was invented — that is the owner's call and it
is flagged in `PORTFOLIO_SCOPE.md` and `PUBLIC_RELEASE_REPORT.md`. The `owner` handle inside
six screenshots was left in place: cropping cannot reach text in the page body, and the fix
is a retake, not an edit.

---

## 2026-10-06 (later) — the disclosure decision, reversed and then bounded

**D-PR-5 — Publish the reasoning, withhold the deployment machinery.**
The full-source mirror (D-PR-1) was reversed on the owner's instruction: the platform is
their own accumulated work and a runnable copy was never the intent. The first cut went too
far — it left 13 files, and the repository's own tests broke, because several of them *read
the codebase* (`test_ordering.py` pins which module writes `position_secondary`;
`test_realtime_rt.py` asserts that no consumer names the pre-cutover monolith). A test that
inspects the tree is a dependency on the tree.

The line finally drawn: **the AI platform, the governance and metadata plane, and the CDC
correctness code are published in full; the Spark reporting framework, the DAGs, the dbt
models, the Terraform modules and the operator tooling are not.** 169 of 425 Python modules,
456 tests, all green. *How to apply:* "withhold the valuable part" is not a scope — the
useful split is between what must be *understood* and what must be *run*, and only the second
half is worth copying.

**D-PR-6 — Nothing ships red.** Fifteen test files that exercise withheld code were removed
rather than published failing; two `validate-docs.py` checks now detect the curated copy and
skip with a reason; the Makefile went from 71 targets to 4. A help listing advertising 59
targets that cannot run is worse than a short one that works, and the same argument that
fixed `make lint-shell` earlier today applies to the whole repository.

**Timing, and why it mattered.** The rewrite was done while the public repository had **0
forks, 0 clones and 0 unique cloners** in its first seven hours, verified from the GitHub
traffic API before anything was force-pushed. A week later that would not have been true, and
a history rewrite would have been theatre.
