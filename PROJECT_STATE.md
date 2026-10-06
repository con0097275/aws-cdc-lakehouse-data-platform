# PROJECT_STATE

## Identity

- AWS profile: `my-aws-profile`
- Account: `111122223333`
- Region: `ap-southeast-1`
- Environment: `dev`
- Project tag: `kafka-dev-lab` (ADR-023 — renamed from repo A's `kafka-prod-lab`)
- Deployment profile: `lab_low_cost`
- Last updated: **2026-10-06** (Portfolio refinement of the PUBLIC copy — documentation,
  README structure, image audit, link checker. No platform code changed, no AWS call, $0.
  The public copy is now a git repository: baseline `55f93d3`, work on
  `docs/portfolio-refinement`, no remote, nothing pushed.)
- Previous: **2026-09-20** (Session 52 — Phase I, live E2E. **CDC leg PROVEN LIVE**; layers above NOT run. Checkpoint is PARTIAL, not a PASS.)
- Previous: **2026-09-20** (Session 51 — Phase H, data layout benchmark. Checkpoint `DATA_LAYOUT_BENCHMARK_PASS`: measured on real Iceberg, **no config change justified**; Athena bytes pending a populated catalog.)
- Previous: **2026-09-20** (Session 50 — Phase G, partition-scoped Iceberg maintenance, ADR-079. Checkpoint `ICEBERG_MAINTENANCE_READY` at **STATIC_PASS + LOCAL_PASS**; no maintenance run against the live catalog.)
- Previous: **2026-09-20** (Session 49 — Phase F, carry-forward state, ADR-078. Checkpoint `REPORTING_FLOW_APPS_READY`: the five flows were proven LIVE on 2026-09-03 and are unchanged; carry-forward is new and `LOCAL_PASS` only.)
- Previous: **2026-09-20** (Session 48 — Phase E, reporting EOD readiness, ADR-077. Checkpoint `REPORTING_READINESS_ORCHESTRATION_READY` at **STATIC_PASS + LOCAL_PASS**.)
- Previous: **2026-09-20** (Session 47 — Phase D, EOD control plane, ADR-076. Checkpoint `EOD_CONTROL_PLANE_READY` at **STATIC_PASS + LOCAL_PASS**; no close against the live catalog, which holds 0 tables.)
- Previous: **2026-09-20** (Session 46 — Phase C, config-driven REALTIME cadence and resource profile, ADR-075. Checkpoint `REALTIME_MATERIALIZATION_READY` at **STATIC_PASS + LOCAL_PASS**; no REALTIME run on the deployed Airflow, and the Glue catalog is empty.)
- Previous: **2026-09-17** (Session 44 — Phase B, Kafka → FULL_CDC streaming hardening, ADR-074. Checkpoint `FULL_CDC_STREAMING_PRODUCTION_READY` at **STATIC_PASS + LOCAL_PASS**; resident EMR run and live restart evidence PENDING. Platform LIVE and billing, verified read-only.)
- Previous: **2026-08-25** (Session 39 — AI architecture approved; AI implementation prompt pack `AI-P1`..`AI-P16` created. **Track C state lives in `AI_PLATFORM_STATE.md`, not here.** Platform APPLIED and still billing.)
- Previous: **2026-08-24** (Session 36 — reporting test suite unblocked, **933 Python tests passing**; platform APPLIED and billing)

  Session 36 correction to the line above: the "875 passing" figure was never reproducible
  as written. `make test-reporting` could not finish — see the Session 36 block below.
- Verdict: **READY_FOR_PORTFOLIO** — see `FINAL_ACCEPTANCE_REPORT.md`
- Deployment: **NOTHING IS APPLIED.** Terraform state holds **3 resources** (caller
  identity, the budget, the lake bucket). Corrected in Session 31 from evidence — the line
  this replaces described the 2026-08-15 cheap tier and was never updated after what
  followed it:
  - the **full platform, MSK included, was applied 2026-08-16T09:21Z and destroyed
    2026-08-16T10:58Z** (CloudTrail `CreateCluster`/`DeleteCluster`; the state object went
    from 587 KB to 16 KB across that window). `enable_kafka_platform=true` was honoured.
  - live footprint today: 2 S3 buckets, 2 KMS CMKs in `PendingDeletion` until 2026-08-23
    (~$2/month until then), 1 **TERMINATED** EMR Serverless application ($0).
  - **no drift**: state and reality agree.

## Query feature flags

```text
enable_athena=true
enable_redshift_serverless=false
enable_trino=false
allow_multiple_optional_query_engines=false
```

## Current session

- Session: **52 — Phase I: full platform end-to-end (2026-09-20)**
- **LIVE RESULT: 4 of 7 sections pass.** CDC, REALTIME, EOD and maintenance proven with
  recorded Athena results and EMR job-run ids. Reporting flows (4), Airflow execution (5)
  and datamart validation (7) did NOT run. `FULL_DATA_PLATFORM_E2E_PASS` is NOT claimed.
- Live numbers: FULL_CDC 350/350 (unchanged after compaction) · REALTIME 350 rows, frozen
  bound 16:26:25 · EOD snapshot 320 rows/320 keys · `eod_info` COB 2026-09-20 CERTIFIED ·
  `eod_run_hist` 4 attempts including 3 retained failures.
- **Six live defects found and fixed**: KMS decrypt on the 09-10 CMK (5 Kafka jars, since
  re-encrypted, plus `reporting-framework.zip`); the framework zip missing `full_cdc_job.py`;
  my own Phase B `json.dumps(datetime)`; EMR dynamic allocation exhausting the 100 GB disk
  cap; `--skip-readiness` declared but never passed; maintenance correctly refusing without
  `--metrics`.
- **Next session starts here**: the mart layer. `mart` and `curated` hold 0 tables, so dbt
  must build them before sections 4 and 7 can run. `reporting-framework.zip` is now readable
  by the Spark roles; a dbt entrypoint still has to be deployed (no `run_dbt_job.py` in
  `artifacts/code/`). Then unpause the flow DAGs for section 5.
- Checkpoint: **PARTIAL** — `docs/validation/FULL_CDC_REPORTING_E2E.md`. I am NOT emitting
  `FULL_DATA_PLATFORM_E2E_PASS`: sections 2-7 did not run.
- **PROVEN LIVE**: Kafka -> FULL_CDC. 2,664 events across five Oracle tables, rows ==
  distinct dv_event_id == distinct dv_src_event_id on every table, `cdc_event_index` 2,664
  in agreement. Restart from the identity-keyed checkpoint added **0 rows**.
  `ops.streaming_app_state` holds a live row with `restart_count = 2` read back from the
  table -- the Phase B control plane working in production for the first time.
- **Three live defects found and fixed**:
  1. KMS -- the spark-stream role cannot Decrypt with the 2026-09-10 lake CMK, which
     encrypts the five Kafka jars. Re-encrypted them; the multi-CMK divergence REMAINS.
  2. Packaging -- the framework zip lacked `full_cdc_job.py`; `cdc-deploy-code.sh` now
     takes `path:zip_name` because an import name and a file name are different facts.
  3. **Mine**: Phase B put a datetime in `stats` and the summary line does `json.dumps`.
     The job committed its data and then failed on its own log line.
- **NOT run**: REALTIME, EOD close, reporting flows, Airflow execution, maintenance,
  datamart. Blocked at the EOD submit by `ApplicationMaxCapacityExceededException
  [disk: 100 GB]` -- a capacity/sequencing constraint, not a code defect.
- Cost: 6 EMR job runs. Budget $51.77 actual / $95.58 forecast of $100.

- Previous session record:

- Session: **51 — Phase H: data layout and performance benchmark (2026-09-20)**
- Checkpoint: **`DATA_LAYOUT_BENCHMARK_PASS`**. `docs/DATA_LAYOUT_BENCHMARK.md`,
  harness `scripts/layout-benchmark.py`, raw data
  `artifacts/benchmark/layout-2026-09-20.json`.
- **Outcome: no production partition change.** The brief forbids changing layout without
  measured evidence; the evidence says the shipped layout already wins.
- Measured on real Iceberg (3 classes x 6 layouts x 7 workloads, 30 daily commits each):
  * `identity(event_date)` and `days(source_commit_ts)` are **byte-identical** on every
    metric and every workload — ADR-062's claim, now measured rather than asserted.
  * Bucketing loses at every scale: hot table 30 files -> **1,920** at N=64, average file
    38.9 KB -> 3.5 KB, one-day scan 39 KB -> 224 KB, EOD build 119 ms -> 775 ms.
  * Unpartitioned reads 32x the bytes of the shipped layout for a one-day scan.
  * Write amplification 1.00 everywhere, so the comparison is partitioning, not write path.
- **Stated as unmeasured, not smoothed over**: Athena bytes scanned (the Glue catalog holds
  0 tables) and bucketing's key-equality pruning BENEFIT. Its costs are measured; its
  benefit is not, and that asymmetry is written down.
- A harness artefact was caught and fixed before it became a finding: naive datetimes made
  `days()` look twice as fragmented as `identity()` because PySpark reinterprets them in the
  machine's zone — the same conversion that corrupted watermarks in Phase D.
- Tests: `make check-all` exit 0 — **2,637 passed**. Phase H: 14 new, asserting the
  findings against the raw JSON rather than restating them.

- Previous session record:

- Session: **50 — Phase G: Iceberg maintenance framework (2026-09-20)**
- Checkpoint: **`ICEBERG_MAINTENANCE_READY`** — `STATIC_PASS` + `LOCAL_PASS`.
- Existed (Phase A assessed KEEP): metric-driven planner, four actions, thresholds anchored
  to the Phase 0 measurement, cadence floor by temperature, orphan removal opt-in,
  `min_small_files: 8`, and the `cdc_maintenance` DAG dynamic-mapped over the plan.
- Built: **partition-scoped compaction per layer** (FULL_CDC recent event_date, REALTIME
  active window, EOD by snapshot_mode, MART configurable), orphan age tied to writer
  duration (2x floor, refusal names what the delete would destroy), MART as a layer.
- Two defects found in my OWN new code before shipping:
  1. `policy.get(...) or DEFAULT` swallowed an explicit `compact_recent_days: 0` — the same
     falsy-zero shape as ADR-070's `maintenance.actions: []`.
  2. Doubling the predicate's inner quotes produced `event_date >= DATE 2026-09-17`, which
     Iceberg rejects — a failure that would have landed on EMR after acquiring capacity.
- Tests: `make check-all` exit 0 — **2,623 passed**. Phase G: 24 new, including compaction
  on a REAL Iceberg table (file count down, avg size up, every row preserved, out-of-scope
  partition untouched, manifests rewritten, snapshots expired safely).

- Previous session record:

- Session: **49 — Phase F: the five reporting flow applications (2026-09-20)**
- Checkpoint: **`REPORTING_FLOW_APPS_READY`**. The five flows were already built and proven
  LIVE in the 2026-09-03 window (EOD #16, STREAM_BATCH #17, AUTO_CORRECT #34, FULFILL #35,
  STREAMING_RT #36, dbt build on EMR #15) with **178 tests**. Phase F changed none of them.
- Audited against the brief: §1 EOD, §3 FULFILL (every flag: from_date/to_date/
  specific_dates/force/dry_run), §4 STREAM_BATCH frozen bound + watermark discipline,
  §5 STREAMING_RT lifecycle + checkpoint, §6 dbt-spark, §7 source resolution — all present.
  §2's affected-date and affected-key strategies already existed as config enums.
- Built the one genuine gap: **carry-forward state** (ADR-078) —
  `spark/reporting/carry_forward.py`, adapted from the reference mart's D-1 seeding. A real
  event outranks a carried row whatever the clocks say; only yesterday's CURRENT rows carry;
  frames align by NAME, never position.
- Rejected from the reference: 1,800 lines of `bal_dau_ki`/`ma_cn`/`kyhan` business columns,
  interpolated SQL across hundreds of lines, and `sync_wait()` sleeps as coordination.
- Documented: `super_key` embeds a rendered timestamp, so it is stable only because every
  job pins the session zone to UTC (ADR-024).
- Tests: `make check-all` exit 0 — **2,599 passed**. Phase F: 19 new.
- **Not claimed**: no mart declares a carry-forward spec and it has not run on EMR.

- Previous session record:

- Session: **48 — Phase E: reporting readiness + orchestration review (2026-09-20)**
- Checkpoint: **`REPORTING_READINESS_ORCHESTRATION_READY`** — `STATIC_PASS` + `LOCAL_PASS`.
- §1 decision, with evidence: **the fixed-flow architecture stands.** The reference
  `airflow_dwh2cdp_extract.py` generates one DAG per config file and picks its cron by
  substring-matching the table name; nothing in current requirements shows the coordinator
  is insufficient. Full KEEP/ADAPT/REJECT in
  `docs/REPORTING_ORCHESTRATION_REFERENCE_COMPARISON.md`.
- Built: `EodTableReadyGate` (READY/WAITING/FAILED/NOT_APPLICABLE, naming WHICH dataset is
  missing), `EodInfoReader` (ONE batched query, cached per coordinator run),
  `required_datasets` in job YAML + schema + compiled plan, coordinator wiring (opt-in).
- Kept from the reference: the batched `eod_info` query. Rejected: readiness by COUNTING
  matches (never says which one is missing) and `tbl.replace('sat_','sat_snp_')` name surgery.
- Found: two places rebuilt `LoadedConfig` and silently dropped the new field —
  `with_derived_fields` and the manifest-merge branch. Both now carry it explicitly.
- Tests: `make check-all` exit 0 — **2,580 passed**. Phase E: 29 new.

- Previous session record:

- Session: **47 — Phase D: EOD control plane + config-driven closing (2026-09-20)**
- Checkpoint: **`EOD_CONTROL_PLANE_READY`** — `STATIC_PASS` + `LOCAL_PASS`. Phase E NOT started.
- Audited first: the BUILDER was already complete with 124 tests (generic engine, half-open
  DST-safe cutoff, source-native ordering with a refusal, delete policies, DQ,
  certification withheld). **15 of the brief's 18 test cases already passed.**
- Built: `ops.eod_info` (one MERGEd row per table+COB with the watermark PAIR),
  `ops.eod_run_hist` (one row per attempt, failures retained, attempt counted from history),
  source readiness (`WAITING_SOURCE` → `LATE_SOURCE` at the SLA), orphan-commit detection,
  the Athena legacy view, EOD cadence + resource profile. Plan schema **10**.
- Found and fixed:
  1. **A timestamp bug in my own control-plane writer**: PySpark converts stored timestamps
     using the MACHINE's zone, so treating naive datetimes as UTC stored every watermark
     **7 hours ahead** on this UTC+7 host.
  2. **Readiness that would have condemned every quiet table**: requiring a table's own
     post-cutoff event puts `channel` (4 rows) into LATE_SOURCE daily. The platform ingest
     watermark is accepted as equivalent evidence.
  3. `eod.schedule` was inert — `loan` declared `30 2 * * *` in the registry and every table
     closed on the env var. It now has its own DAG, `cdc_eod_0230`.
  4. DagBag tests depended on the environment; they are pinned to the committed plan.
- Tests: `make check-all` exit 0 — **2,551 passed**, 14/14 static, shellcheck clean.
  Phase D: 22 + 5 new; existing EOD fixtures now provision the control plane.

- Previous session record:

- Session: **46 — Phase C: config-driven REALTIME materialization (2026-09-20)**
- Checkpoint: **`REALTIME_MATERIALIZATION_READY`** — `STATIC_PASS` + `LOCAL_PASS`.
  Phase D (EOD control plane) NOT started.
- Audited first: the generic engine, frozen upper bound, half-open window, FULL_CDC-only
  source, snapshot capture, per-table targets, rebuild and `ops.realtime_run` already
  existed with **89 tests**. Phase C closed the two real gaps.
- Found and fixed:
  1. `realtime.schedule` compiled into the plan and was **read by nothing** — the cadence
     lived in `CDC_REALTIME_CRON`, so a registry edit moved the plan hash and changed
     nothing. Now a validated cron; an unreadable one fails at compile.
  2. `resource_profile` did not exist: one hardcoded envelope (`timeout_minutes=40`,
     2 cores/4g) served a 4-row reference table and a 3,000-event one alike.
- Built: plan schema **9** (resolved `schedule`, `schedule_declared`, `resource_profile`);
  `schedule_groups()`; **one DAG per cadence** dynamic-mapped over its tables (never per
  table); sizing + Airflow pool from the profile; `small` default is cheaper than the old
  hardcoded envelope.
- Tests: `make check-all` exit 0 — **2,524 passed**, 14/14 static, shellcheck clean,
  plan `cdccfg-d2b45f36c02a5e5c` not stale. Phase C: 28 + 6 new.
- The brief's 13 test cases: 11 already covered; the 2 that were not (parallel two-table
  run, failure does not corrupt target) are now covered on real Iceberg.

- Previous session record:

- Session: **44 — Phase B: Kafka → FULL_CDC production streaming hardening (2026-09-17)**
- Checkpoint: **`FULL_CDC_STREAMING_PRODUCTION_READY`** — `STATIC_PASS` + `LOCAL_PASS`.
  **Not live-tested**: no resident run, no restart on EMR. Phase C NOT started.
- Deployment (read-only, 2026-09-17T16:3xZ): MSK `kafka-dev-lab-dev` **ACTIVE**, 4 EC2
  **running** (cdc-runtime, source-lab, airflow, toolbox), EMR Serverless **STARTED** (ARM64,
  emr-7.2.0). **Billing ~$1.2–1.5/hr.** No AWS mutation this session.
- Found and fixed:
  1. `ops.streaming_app_state` was provisioned with **0 rows and no writer**; the ingest now
     MERGEs one throttled row per app (`cdc/streaming_state.py`).
  2. The plan-derived checkpoint was **dead code**: `--checkpoint` was `required=True`.
  3. `--starting-offsets latest` on a fresh identity-keyed checkpoint would have discarded
     ~1,600 LOAN events (Kafka 1,668 offsets vs FULL_CDC 65, measured); default now `earliest`.
  4. A Phase A guard test had stopped evaluating (`substring not found`); repaired and
     mutation-checked. A second guard was passing only on a substring of a renamed flag.
- Built: plan schema **8** (`ingestion.apps`, heartbeat), `scripts/cdc-stream.py`
  (apps/status/health/submit — read-only), `full_cdc_streaming_{start,monitor,stop}` DAGs
  (paused, flag-gated, EMR `STREAMING` job runs, bounded opt-in restart),
  `streaming-reset.sh --app-id`, `SubmissionRequest.job_run_mode`.
- Tests: `make check-all` exit 0 on the finished tree — **2,464 passed, 0 failed**,
  validate-docs 14/14, shellcheck clean, plan not stale (`cdccfg-1c0796dd6d3b65b0`).
- Live read-only evidence: `artifacts/validation/phase-b/`.
- Runbook: `docs/FULL_CDC_STREAMING.md`. Cost: `docs/COST.md` §7c — 2 resident apps
  ≈ $0.756/hr ≈ $544/month, OFF.

- Previous session record:

- Session: **42 — cost/safety gate, E2E live test plan, PLATFORM APPLIED (2026-09-03)**
- Status: **`FULL_E2E_INFRA_RUNNING`. Applied by the operator. BILLING ~$1.2340/hr.**
- Deployment: **APPLIED AND LIVE from 2026-09-03T09:06:04Z.** Terraform state holds
  **292 resources**. MSK ACTIVE (3 × kafka.m7g.large), 4 EC2, EMR Serverless CREATED,
  Airflow pods Running, 4 DynamoDB tables, 7 Glue databases, Athena workgroup ENABLED,
  **NAT 0**. Both Debezium connectors RUNNING. Drift: **0 add, 1 change, 0 destroy**
  (the known non-converging `logs/airflow/` object).
- **MSK cannot be stopped, only destroyed, and is 62% of the burn. `auto_destroy_after`
  is expired (`2026-08-25`), so a wall-clock alarm is the only teardown trigger.**
- Two blockers before any Spark job — lake CMK divergence and an empty Glue catalog; both
  detailed in `SESSION_HANDOFF.md` and
  `artifacts/validation/final-e2e/infra/50-infra-running-verified.txt`.
- Saved plan: `terraform/envs/dev/tfplan` — **248 create / 0 change / 0 destroy /
  0 replace**, 1 VPC, 1 MSK, 0 NAT/EKS/Redshift/OpenSearch/SageMaker/EIP/RDS/SecretsManager.
  Verified fresh 2026-09-03T08:36Z; sha256 `c70ba8a7…`.
- Live bring-up was requested and **did not proceed** — see `SESSION_HANDOFF.md` and
  `artifacts/validation/final-e2e/infra/`. Two operator edits are outstanding
  (`auto_destroy_after` expired; `monthly_budget_usd = 100` fails its check), and the
  apply gate refuses non-interactively by design.
- Governance review completed the same day: `docs/FINAL_PRODUCTION_GOVERNANCE_REVIEW.md`
  — 24 findings, 7 fixed, including two P0s (missing UTC session zone on all three
  canonical EMR jobs; an unqualified table reference that bypassed both SQL allow-lists).
- Idle cost floor: **8 customer KMS CMKs** (~$8/mo), 4 of them `PendingDeletion` and
  draining to ~$4/mo by 2026-09-05.

- Previous session record:

- Session: **31 — Phase 12, reporting infrastructure gap analysis**
- Status: **REPORTING_INFRA_PLAN_READY_FOR_REVIEW. Planned, NOT applied. Spend $0.00.**
- Branch: `session-02-prerequisites`

**The `enable_kafka_platform` discrepancy is resolved: the flag was honoured and the
documents were stale.** See `docs/REPORTING_INFRA_GAP_ANALYSIS.md` §2 and
`artifacts/validation/session-31/kafka-platform-discrepancy.txt`.

**ADR-036 is now real Terraform.** `terraform/modules/reporting_ops` — four
`PAY_PER_REQUEST` DynamoDB tables, SSE with the lake CMK, PITR off, TTL only where the
adapter stamps one, plus one EMR job role and two policies granting no `Scan`, no
`DeleteItem` and no wildcard. **Idle cost $0.00.** Gated behind `enable_reporting_framework`,
**default false**, because ADR-036 requires operator sign-off.

**The finding that matters most:** EMR Serverless, `lake_iam` and Airflow are all
`count`-gated on `enable_kafka_platform`, and `lake_iam` takes the MSK cluster ARN as a
*required* input. **No dbt-on-EMR reporting job can run without creating an MSK cluster** at
~$0.7650/hr — for a workload that never touches Kafka. Three options are costed in §5 of the
gap analysis; the decision is the operator's and was not made here.

**Two plans, neither applied.** Plan A (current tfvars, i.e. what `tf.sh plan` produces
today): 164 creates including MSK, **$1.1244/hr**, and **zero** reporting tables. Plan B
(reporting-only overlay): 42 creates, **$0.00/hr**, 4 tables. **Both are `0 to change, 0 to
destroy` — there is no unexpected destruction anywhere.**

**Tests: 875 pass** (12 new, cross-checking the Terraform key schema against the adapter's
actual `Key=` names — the mismatch class that only fails against real AWS).

**2026-08-20 ~03:56Z — the operator applied the PLATFORM.** 198 resources in state: MSK
(3 × `kafka.m7g.large`, ACTIVE), 3 EC2, EMR Serverless, lake, Glue, IAM. **Burn rate
~$1.1244/hr — 24 h is $26.99, ~90% of the $30 monthly budget.** `auto_destroy_after` on
every one of them is `2026-08-16T18:00:00Z`, already expired.

**`module.reporting_ops` was NOT part of that apply — 0 resources, 0 DynamoDB tables.** So
the reporting framework still has no runtime state store, and Phase 14 cannot produce
durable `execution_id` / watermark / `attempt_number` evidence: the Airflow adapter falls
back to the in-memory repository, which is process-local.

**Two blockers found before touching anything** —
`artifacts/validation/session-32/pre-phase14-blockers.txt`:

1. **Changing `auto_destroy_after` replaces the MSK cluster.** A/B: with the new timestamp,
   `27 add, 112 change, 18 destroy`; with the old one, `9 add, 1 change, 0 destroy`. The
   replacements are all 6 subnets (planned `availability_zone` is **unknown**, not
   different), cascading into MSK, EMR and all 3 EC2 instances. `auto_destroy_after` feeds
   provider `default_tags`, so changing it changes the provider config and defers the
   `aws_availability_zones` read. **ADR-027 requires updating this value every window —
   obeying it today would destroy the Kafka cluster.** Left unfixed: pinning AZs changes
   network identity and needs a decision.
2. **The untargeted plan silently revokes two MSK ingress rules** — port 9098 from EMR
   Serverless and from the CDC runtime. `aws_security_group.msk` uses **inline `ingress`**
   while those rules are separate `aws_vpc_security_group_ingress_rule` resources in other
   modules; the provider forbids mixing the styles and the inline block revokes what it does
   not know. They are not re-added in the same apply. That is the exact path Phase 14's CDC
   pipeline needs. Left unfixed: it is a live SG in front of a running cluster.

**Safe path prepared, not applied:** `terraform plan -target=module.reporting_ops -out=tfplan`
→ **9 create, 0 update, 0 delete**, $0.00/hr, nothing outside the module. Apply is gated:
`bash scripts/tf.sh apply --execute`.

**State locking was broken and is fixed (2026-08-20).** `bash scripts/tf.sh plan` failed
with a 403 on the `.tflock` object. **Two independent defects**, both proven against the
live bucket, both fixed — `artifacts/validation/session-31/state-lock-failure-root-cause.txt`:

1. **The state bucket policy denied Terraform's lock write.** `DenyUnencryptedObjectUploads`
   required an `x-amz-server-side-encryption: AES256` **request header**; Terraform's
   `.tflock` write does not send one (`encrypt = true` governs the *state* object). The
   bucket has default AES256 encryption, so the object was encrypted regardless — the policy
   was rejecting a request shape, not an unencrypted object. An explicit Deny beats the
   operator's `AdministratorAccess`, which is why admin did not help. Fixed with a `Null`
   condition so the Deny fires only when the header is present and wrong; **re-verified that
   an `aws:kms` upload is still denied**. Live policy updated; generator fixed; prior policy
   preserved.
2. **The backend was authenticating as the wrong account.** `backend.hcl` had no `profile`,
   and the S3 backend resolves credentials through the default chain independently of
   `providers.tf` — which on this workstation is account **444455556666**, the OPEN-13
   hazard. `providers.tf` pins the *provider* and carries a long warning about exactly this;
   the backend was never given the same treatment. Every command that worked in this session
   had `AWS_PROFILE=` set explicitly; `tf.sh` does not export it, and `require_identity`
   checks the CLI, not Terraform. Fixed in `backend.hcl`, the generator and the example.

`env -u AWS_PROFILE bash scripts/tf.sh plan` now succeeds; the lock is acquired and released
(`head-object` → 404). State unchanged at 3 resources.

**⚠ `terraform/envs/dev/tfplan` now holds a FRESH MSK-creating plan** — 164 add, **1 MSK**,
**0 DynamoDB**, $1.1244/hr — because the flags are still `enable_kafka_platform=true` and
`enable_reporting_framework=false`. No longer stale, still not the approved plan.

**Pre-apply verification run 2026-08-20: STOPPED, nothing applied.** Four blockers, in
`artifacts/validation/session-31/pre-apply-verification.txt`:

1. **`terraform/envs/dev/tfplan` — the file the wrapper applies — is NOT the approved
   plan.** It creates **1 MSK cluster**, 1 EMR application and 3 EC2 instances, and **zero**
   DynamoDB tables. It is also structurally stale: its prior state holds 46 resources
   against the 3 that exist, and a saved plan is applied as-is without re-checking the
   config, so it would ignore `modules/reporting_ops` entirely.
2. **`auto_destroy_after` is `2026-08-16T18:00:00Z`** — four days past. Every resource
   would be tagged already-expired (ADR-027, OPEN-34).
3. **ADR-036's required operator sign-off is not recorded**, and OPEN-32/33 are open.
4. **The gate cannot be self-approved**: `scripts/tf.sh apply` calls `confirm_destructive`,
   which refuses without a terminal. Bare `terraform apply` is what that control exists to
   prevent, and was not run.

The approved Plan B itself verifies clean — 42 create, **0 change, 0 destroy**, no network
resources at all, no MSK/EMR/Airflow, 4 DynamoDB tables, **$0.0000/hr**. It is the sequence
around it, not the plan, that is not ready.

- Previous session record:

- Session: **30 — Phase 11, STREAMING_RT**
- Status: **REPORTING_STREAMING_RT_READY. Shipped OFF. No AWS mutation. Spend $0.00.**
- Branch: `session-02-prerequisites`

The long-running Spark Structured Streaming application — the fifth and last reporting mode.
It has never run: `is_enabled: false` in the job config and `ENABLE_STREAMING_RT=false` in
the environment, both of which must be changed deliberately because this is the one workload
that bills for as long as it is up.

**Source decision reconfirmed, not reopened:** ADR-041's `FULL_CDC_APPEND` — an Iceberg
incremental read of the canonical layer. `KAFKA_DIRECT` stays expressible and unselected,
and its second obligation (a mandatory reconciliation job against FULL_CDC) is now enforced
in code rather than only written in the ADR.

**Tests: 863 pass, 0 fail** (42 STREAMING_RT). The checkpoint derives to
`s3://<lake>/checkpoints/reporting/dev/<job>/STREAMING_RT/` — top-level, never under
`warehouse/`, where `remove_orphan_files` would delete it.

**The failure contract is the design.** With `foreachBatch`, a handler that catches and
returns tells Spark the batch succeeded, so the offsets are committed and the rows are gone.
The reference implementation records losing ~100K offsets exactly that way. This
implementation **raises**, so Spark does not commit and the batch is retried.

**Three defects found and fixed:** a delete that dropped its row let a later-arriving older
update resurrect the key (fixed with soft-delete tombstones, which the job config already
specified, and the same flaw was in the real MERGE); a failed batch would have inflated
`restart_count`, which is the one number separating "one bad batch" from "the process keeps
dying"; and two DAG policy violations caught by the repository's own tests on first run.

**Checkpoint deletion is structurally impossible on the start path** — `CheckpointInspector`
has no delete method, and a test asserts it never grows one. `scripts/streaming-reset.sh` is
the only route: dry-run by default, refuses non-interactively, typed confirmation phrase,
moves rather than deletes, audited, fixed teardown order.

**Phase 11 evidence:** `artifacts/validation/session-30/` — a ten-step lifecycle with twelve
asserted invariants. `make reporting-streaming-rt-demo` regenerates it for $0.

**Untouched, as instructed:** `enable_kafka_platform`, DynamoDB, EMR Serverless, Airflow
deployment, and Kafka. Nothing was submitted anywhere.

- Previous session record:

- Session: **29 — Phase 10, STREAM_BATCH**
- Status: **REPORTING_STREAM_BATCH_READY. No AWS mutation. Spend $0.00.**
- Branch: `session-02-prerequisites`

STREAM_BATCH — the Airflow-scheduled **finite** micro-batch, not streaming (ADR-041) — is
implemented, unit-tested and wired to Airflow. Freeze the upper bound once, read
`[watermark − 2m, frozen)` from REALTIME, transform, guarded MERGE, validate, then advance
the watermark to the frozen bound. A failed run does not move it.

**Tests: 821 pass, 0 fail** (388 reporting, of which 45 are STREAM_BATCH). `dbt compile`
renders all four modes; `reporting/compile.py --check` passes. No AWS call was made.

**Two defects found and fixed, both silent by construction:**

1. The compiled STREAM_BATCH predicate compared `business_date` (a `DATE`) against the
   timestamp window, so Spark promoted the date to midnight and the model selected
   **nothing on every run** — then succeeded and advanced the watermark past data it never
   wrote. The macro test could not catch it: reading macro text proves the SQL was written
   a certain way, not that it selects anything.
   `spark/tests/test_reporting_stream_batch_sql.py` now evaluates both predicates in real
   Spark.
2. `source_layer_substituted` was promised by `reporting/layers.yaml`, present on the
   record, deserialised by the DynamoDB adapter — and **written by nothing**. REALTIME does
   not exist yet; it stands on the full_cdc database, and no run was recording that it had
   read a stand-in.

**Phase 10 evidence:** `artifacts/validation/session-29/` — a six-step run trace (cold
start, busy window, quiet window, engine failure, retry, overlap refusal) with the watermark
before and after each, plus the four modes' compiled SQL. `make reporting-stream-batch-demo`
regenerates it for $0.

**Untouched, as instructed:** `enable_kafka_platform`, the DynamoDB tables, EMR Serverless
and the Airflow deployment. STREAMING_RT (Phase 11) was not started.

- Previous session record:

- Session: **22 — Phase 3, reporting metadata foundation**
- Status: **METADATA_FOUNDATION_READY. No AWS mutation. Spend $0.00.**
- Branch: `session-02-prerequisites`

Built the metadata layer for the reporting/datamart framework: 8 domain entities, the
10-state execution machine, the dependency graph resolver, the deterministic config
compiler, the runtime-state contract with two implementations, and the watermark, history
and streaming-app-state contracts. **No flow executes**, no DAG was written, no dbt model
was invoked, no Terraform was applied.

**Tests: 570 pass, 0 fail** — 149 new reporting tests plus the 421 pre-existing ones,
which are unchanged. `validate-docs.py`: 14 pass, 0 fail. The reporting suite runs in
~1 second with no Spark, no credential and no network.

**DYNAMODB_CODE_READY / DYNAMODB_INFRA_NOT_APPLIED.** The adapter exists and is tested
against a hand-written fake; no table exists in the account and `modules/reporting_ops` was
not written or planned.

**One approved decision corrected under implementation evidence.** ADR-042 said
`partition_spec` may not contain any primary-key column. The compiler rejected the pilot on
its first run — correctly, under that rule — but the rule rejected the repository's own
deployed DDL: `mart_account_balance_daily` has grain `(account_sk, business_date)` and is
`PARTITIONED BY (business_date)` (`spark/common/ddl/kimball.sql:182`). The rule was stated
over PK membership instead of cardinality. Corrected in ADR-042 with the original wording
retained; see `docs/REPORTING_FRAMEWORK.md` §7.

**Untouched, as instructed:** `enable_kafka_platform` and `dim_date.is_working_day`. Both
are recorded as Phase 4 prerequisites and neither was modified.

- Previous session record:

- Session: **20 — cheap-tier foundation APPLIED and VALIDATED**
- Status: **FOUNDATION_READY_PENDING_F1_PROPAGATION**

Gate 1 and Gate 2 were both passed by the operator typing the confirmation phrase at an
interactive terminal. 32 resources applied; a post-apply plan returns
`detailed-exitcode=0` — **zero drift, zero replace, zero destroy**.

**Live and proven:** S3 lake (SSE-KMS, versioned, TLS-only, 13 prefixes), 7 Glue
databases, Athena workgroup (10 GiB cutoff **enforced**, proven by a discarded client
override), lake KMS CMK with rotation, 6 lifecycle rules, tag-filtered $30 budget.
Athena executed live against the real workgroup: all 7 databases visible, results
written to `query-results/athena/` and **encrypted with the lake CMK** — which proves
the KMS grant to `athena.amazonaws.com` works against live AWS, not just on paper.

**Two defects found and fixed this session:**

- **F1 — cost-allocation tag inactive.** `Project` (and every other tag) was `Inactive`
  for billing, so the tag-filtered budget would have read $0.00 forever and none of its
  three alerts could ever fire — exactly risk R7. The Terraform string was *correct*;
  activation is an account-level Billing setting Terraform does not manage, so no code
  review could have caught it. Six tags activated. **BLOCKED pending ~24h propagation**
  (verify after 2026-08-16T12:21Z) — deliberately NOT marked PASS.
- **F2 — smoke queries could never have passed.** `serving/athena/queries/*.sql`
  hardcode unprefixed schemas (`stream`, `mart`) but the deployed databases are
  `kafka_dev_lab_dev_*`. Proven: unprefixed → `SCHEMA_NOT_FOUND`, prefixed →
  `TABLE_NOT_FOUND`. Fixed by `scripts/athena-smoke.sh`, which substitutes the prefix
  from `terraform output` rather than hardcoding env names into version control.

**Deferred to the CDC window by design:** Iceberg table creation and every
table-dependent query. Spark creates those tables on first write, so they cannot gate
the window that runs Spark.

- Previous session record:
- Session: **20 — pre-deployment hardening; deployment attempted and BLOCKED**
- Branch: `session-02-prerequisites` · commits `60de297`, `751482f`
- Status: **NOT DEPLOYED. Spend $0.00. Blocked at the Gate 1 confirmation phrase.**

Deployment was approved by the operator and attempted. `scripts/bootstrap-state-backend.sh
--execute` passed its identity guard and then refused:

```
REFUSING: refusing to run non-interactively; a human must type the confirmation phrase
```

That is `confirm_destructive()` in `lib.sh:161` behaving exactly as designed (S02A-3) —
it refuses when stdin is not a TTY *specifically so no automation can satisfy it*, and
`scripts/validate-docs.py:515` asserts that refusal still exists. It was not bypassed. The
same gate guards seven further steps (`APPLY THE SAVED PLAN`, `ENABLE CDC`, `SEED SOURCE
LAB`, `RUN WORKLOAD n`, `RECORD CHECKSUMS`, `CREATE TOPICS`, `REGISTER CONNECTORS`), so
the whole end-to-end chain needs a human at a terminal, not one confirmation.

**Everything reachable without that phrase was completed:**

| Done | Evidence |
|---|---|
| Sessions 03–19 committed — 331 untracked files, no remote (review finding M1) | `60de297` |
| **OPEN-22 CLOSED** — DAGs parsed under Airflow 3.2.2; all 8 import | venv; 31 pass on 2.9.3 **and** 3.2.2 |
| **OPEN-06 CLOSED** — shellcheck installed and enforced at severity=warning | `make lint-shell` |
| **Defect: `tf.sh` cost preview never ran** — bash ate the apostrophes in `d['resource_changes']` (NameError) *and* the f-string had a backslash (SyntaxError <3.12) | moved to `scripts/plan_cost.py`, 12 tests |
| **Defect: state bucket would be created untagged** — 6 tag vars declared, none applied | fixed before the bucket exists |
| Cheap-tier plan produced and costed | `artifacts/validation/session-20/` |

The cost preview's **first ever execution** against a real plan: **32 to add,
$0.0000/hr, $1.00/month** (one lake CMK), 0 NAT, 0 Secrets Manager. The cheap tier has
no hourly billing at all.

Prior state, retained for history:

- Session: **08 — L3 SNAPSHOT as-of T-1**
- Branch: `session-02-prerequisites`
- Status: **PARTIAL — snapshot semantics PROVEN, pipeline NOT.**
  **101 tests pass and they run** — 17 new this session, all against a real Iceberg
  table. Delete lifecycle (create/update/delete/**recreate**), late events, composite PK,
  out-of-order arrival and **deterministic rebuild by checksum** are demonstrated.
  **L3 against live L2 and comparison with a prior production build are `NOT_TESTED`
  (OPEN-18).** MSK 0, EC2 0.

**Session 02 remains UNAPPLIED.** Its state-backend bootstrap needs a human-typed
confirmation phrase; `lib.sh` refuses non-interactive execution by design (S02A-3).
`versions.tf`'s `backend "s3"` block is therefore still commented, and **every plan so far
is against LOCAL state and is not valid for apply.**
- Previous: **00 closeout** — `ARCHITECTURE_APPROVED_FOR_IMPLEMENTATION`
  (`docs/reviews/SESSION-00-ARCHITECT-REVIEW.md`). One Gate 1 package remains (state
  backend); the MSK probe package is retired.

Prior state, unchanged by this pass:

- Session: **02 — Terraform foundation and data lake**
- Stage: **A of B — prerequisites** (`docs/TARGET_ARCHITECTURE.md` §12 items 1–3)
- Status: **STAGE A COMPLETE. No AWS mutation.**
- Stage B (not started): §12 items 4–15 — repo A absorption, six lake modules, root
  wiring, saved plan. Ends at `READY_FOR_APPLY_APPROVAL`.

Session 01 (target architecture, 19 ADRs, cost envelope, dependency graph, approval
gates) is **COMPLETE** — document-only, no AWS mutation, no Terraform.

## Verified AWS reality

Re-verified read-only on **2026-08-13** by the Session 00 closeout. Evidence:
`artifacts/validation/session-00/aws/inventory.txt`,
`.../cloudtrail.txt`, `.../open-03-settled.txt`; previously
`artifacts/validation/session-02/aws/post-stage-a-inventory.txt` and `.../gate-0.7-budgets.txt`.

The 2026-08-13 sweep adds **EMR Serverless `[]`** and **Redshift Serverless `[]`**, and
reconstructs the repo A lifecycle from CloudTrail: `CreateCluster` rejected at
13:01:14Z (`kafka.t3.small`), `CreateCluster` succeeded at 13:19:09Z
(`kafka.m7g.large`), `DeleteCluster` at 19:09:46Z — all on 2026-07-26.

| Check | Result |
|---|---|
| MSK clusters | **0** |
| Non-default VPCs | **0** |
| EC2 instances (non-terminated) | **0** |
| EBS volumes | **0** |
| NAT gateways | **0** |
| VPC endpoints | **0** |
| KMS aliases `aws-cdc*`/`kafka*` | **0** |
| EMR Serverless applications | **0** |
| Redshift Serverless workgroups / namespaces | **0** / **0** |
| Glue databases | 1 — `vannk-dev-oracle-db`, unrelated prior work |
| Athena workgroups | 1 — `primary` only |
| **S3 buckets** | **44 — all unrelated prior work.** No `aws-cdc-lakehouse-tfstate-*` exists yet |
| **AWS Budgets** | **2 — see below. This corrects Session 01.** |

**No lab resource exists**, and Stage A created none. Repo A was applied then destroyed
on 2026-07-26; nothing from it can be consumed at runtime.

### Gate 0.7 — corrected

Session 01 recorded precondition 0.7 as **FAILING** ("the AWS Budget notifies nobody")
by reading repo A's `budget_email = ""`. That inspected a Terraform variable, not the
account, and it was wrong. Live:

| Budget | Limit | Filter | Notifications | Subscriber |
|---|---:|---|---|---|
| `My Monthly Cost Budget` | **$30.00** | none — account-wide | ACTUAL >85 %, >100 %; FORECASTED >100 % | **owner@example.com** |
| `My Zero-Spend Budget` | $1.00 | none — account-wide | ACTUAL > $0.01 | — |

**Gate 0.7 PASSES.** Gate 1 is not blocked. The $30 limit is now the **budget of record**
and `docs/COST.md` is re-derived against it (ADR-030). OPEN-09 is closed.

## Component status

Status vocabulary per `SESSION_WORKFLOW.md:16`.

| Component | Status | Version/Config | Evidence | Notes |
|---|---|---|---|---|
| Kafka platform (MSK) | **PLANNED** | `3.9.x.kraft`, 3 × `kafka.m7g.large`, 20 GiB | `terraform/modules/kafka_platform/` + `PROVENANCE.md` | **Absorbed 2026-08-13** (ADR-001). 52 resources in the saved plan. Outputs 5 → 16 |
| Terraform state backend | **BLOCKED** | S3 + **SSE-S3** + `use_lockfile` | ADR-021 (amended by ADR-030), `docs/gates/GATE-1-state-backend-bootstrap.md` | **Script still writes a CMK.** Backend block is commented out in `versions.tf`, so the Stage B plan is against LOCAL state and must be re-planned once the backend exists |
| Target architecture | **DESIGNED** | — | `docs/TARGET_ARCHITECTURE.md` | 19 ADRs |
| CDC column contract | **DESIGNED** | normative | `docs/DATA_CONTRACTS.md` | closes D1–D5, D7–D9 |
| Cost envelope | **STATIC_VALIDATED** | 3 profiles, **$30 budget** | `docs/COST.md`, `scripts/derive-cost-envelope.py`, live price list | floor **$2.28** (was $5.60); **≈18.1 lab-hours/month**. Reproduce: `python3 scripts/derive-cost-envelope.py --budget 30` |
| Risk register | **DESIGNED** | R1–R15, **R11 closed** | `docs/RISK_REGISTER.md` | 3 rated H/H after R11 retired |
| Session dependency graph | **DESIGNED** | Sessions 00–19 | `docs/SESSION_DEPENDENCY_GRAPH.md` | 5 corrections to `MASTER_PLAN.md` |
| Low-cost implementation sequence | **STATIC_VALIDATED** | **5 batched windows, 2 months** | ibid. §5 | core release **≈ $30.53** — month 1 $18.02 (headroom $11.98), month 2 $12.51 (headroom $17.49) |
| Approval gates | **NORMATIVE** | 5 gates + universal preconditions | `docs/APPROVAL_GATES.md` | **Gate 0.7 PASSES** — corrected 2026-08-12 against the live account |
| Operator script library | **STATIC_VALIDATED** | `scripts/lib.sh` | `bash -n` clean; validator check 14 | identity guard, dry-run default, typed-phrase confirmation |
| Static-analysis tooling | **PARTIAL** | `checkov` **3.3.10** | `docs/VERSIONS.md` | `tflint` and `shellcheck` still absent — OPEN-06 |
| MSK instance-type probe | **RETIRED — not needed** | was 2-stage, $0.00 then ~$0.06 | `artifacts/validation/session-00/aws/open-03-settled.txt` | OPEN-03 was settled from CloudTrail on 2026-08-13 without running it. Gate 1 approval **no longer required**; script kept for provenance |
| MSK broker type | **LIVE_TESTED — forced, not chosen** | `kafka.m7g.large` | CloudTrail `CreateCluster` 2026-07-26 | `kafka.t3.small` rejected by the API; `m7g.large` is the cheapest offered |
| Budget recipient | **DECIDED** | `owner@example.com` | `terraform/envs/dev/terraform.tfvars.example` | closes OPEN-05. Applied by `budget_guardrails` in Stage B |
| Source lab host | **PLANNED** | **`t3a.xlarge`**, encrypted gp3 50 GiB, IMDSv2, no key pair | `terraform/modules/source_lab_ec2/`, ADR-032 | 23 resources. **RDS rejected on price: 6.2x** ($1.1620 vs $0.1888/hr). SG has ZERO ingress until Session 04 |
| Debezium connectors | **STATIC_VALIDATED — NOT DEPLOYED** | Oracle LogMiner + SQL Server CDC, Avro | `connectors/`, `docs/CDC_CONTRACT_IMPLEMENTATION.md` | `transforms:""` (no unwrap, S05-1), `snapshot.mode=initial`, `decimal=precise`, tombstones on. **No connector has ever run** |
| Kafka Connect | **PLANNED** | distributed, 1 worker, `cp-kafka-connect:7.7.1` | `terraform/modules/cdc_runtime_ec2/`, `docker/cdc-runtime/` | MSK IAM handler on **all 4 client scopes** (risk R2). REST binds the private IP; smoke test probes the public IP and fails if it answers |
| Schema Registry | **PLANNED** | Apicurio KafkaSQL `2.6.2.Final` | ibid. | Risk R3: health lies, so the smoke test WRITES a real schema. `kafkasql-journal` compacted and created explicitly |
| Oracle source | **PLANNED** | Oracle Free 23.5.0.0 container, x86_64 | `docker/source-lab/oracle/`, ADR-005/032 | ARCHIVELOG + supplemental logging ALL COLUMNS; `c##dbzuser` least-privilege |
| SQL Server source | **PLANNED** | 2022-CU14 Developer container, x86_64 | `docker/source-lab/sqlserver/`, ADR-005/032 | CDC enablement REFUSES to run if Agent is stopped (risk R15); 7-day retention |
| S3 / KMS / Glue | **PLANNED** | separate lake CMK + explicit key policy | `terraform/modules/{data_lake,glue_catalog}/` | 23 + 7 resources planned. 7 layers incl. `ops` (D7). `checkpoints/` placement enforced by validation |
| VPC endpoints | **PLANNED** | S3+DynamoDB gateway (free), Glue interface gated | `terraform/modules/vpc_endpoints/` | 2 free gateway endpoints planned; Glue interface NOT created (`enable_emr_serverless=false`) |
| L1 STREAM | **PARTIAL — logic proven, pipeline not** | 30-column contract, Iceberg v2 | `spark/jobs/l1_stream/`, `spark/tests/` | **49 tests pass** incl. 7 against a real Iceberg table. Job refuses to start if the checkpoint is under `warehouse/` (S06-6). E2E streaming `NOT_TESTED` (OPEN-16) |
| L2 FULL CDC | **PARTIAL — MERGE proven, pipeline not** | `MERGE ON event_id`, INSERT-only | `spark/eod/`, `docs/L2_FULL_CDC.md` | **35 tests** incl. 9 real-MERGE. Rerun does not duplicate; all I/U/D for a PK survive. Atomic publish withholds the marker on failure. E2E `NOT_TESTED` (OPEN-17) |
| L3 SNAPSHOT | **PARTIAL — semantics proven, pipeline not** | rank by `event_order`, cutoff `< T+1`, deletes excluded | `spark/snapshot/`, `docs/L3_SNAPSHOT.md` | **17 tests** on a real table. Delete→recreate works by construction. Rebuild determinism proven by XOR checksum. E2E `NOT_TESTED` (OPEN-18) |
| Four flows | NOT_STARTED | — | — | Session 09 |
| Kimball / dbt | NOT_STARTED | — | — | Sessions 10–11 |
| Airflow / Kubernetes | NOT_STARTED | Airflow 3, `KubernetesExecutor`, k3s | ADR-010, ADR-011 | Session 12 |
| Athena core | **PLANNED** | 10 GiB cutoff, `enforce_workgroup_configuration=true` | `terraform/modules/athena/` | Session 02 creates, 13 uses |
| Workload IAM | **PLANNED** | 8 roles, 6 reusable policies | `terraform/modules/lake_iam/` | Gap 9 closed. Secrets need `ssm:GetParameter` AND `kms:Decrypt` — fails closed (ADR-031) |
| Budget guardrails | **PLANNED** | tag-filtered, $30, applied FIRST | `terraform/modules/budget_guardrails/` | Split out of `kafka_platform` (S02B-2) so it is not destroyed with what it guards |
| Security scan | **STATIC_VALIDATED** | checkov 3.3.10 | `docs/SECURITY_SCAN_BASELINE.md` | 314 passed, 19 failed — 2 fixed, 17 documented, **0 suppressed** |
| Redshift Serverless | **DISABLED** | flag `false` | ADR-013 | $3.60/hr — one benchmark only |
| Trino | **DISABLED** | flag `false` | ADR-014 | ADR-026 mutual exclusion |
| Power BI | NOT_STARTED | Import from Desktop | ADR-019 | Session 13 |
| Governance / lineage / DQ | NOT_STARTED | — | — | Session 14 |
| Observability / SLO | **PARTIAL** | 6 Kafka alarms (ephemeral) + **2 new Prometheus scrape jobs** | `kafka_platform/templates/prometheus.yml.tftpl` | Session 04 **extended** the existing Prometheus via EC2 service discovery — **no second stack** (S04-1). Full SLO work is Session 15 |
| Cost / destroy | **DESIGNED** | metered windows | ADR-027 | Session 17 verifies |
| CI/CD | NOT_STARTED | — | Gap 16 | Session 15/18 |

## Live resources / cost drivers

**None.** Verified 2026-08-12.

| Resource | ARN/ID | Started at | Stop/destroy command | Owner |
|---|---|---|---|---|
| *(empty)* | | | | |

Verify with:

```bash
aws sts get-caller-identity --profile my-aws-profile --region ap-southeast-1
aws kafka list-clusters-v2  --profile my-aws-profile --region ap-southeast-1 --query 'length(ClusterInfoList)'
aws ec2 describe-instances  --profile my-aws-profile --region ap-southeast-1 \
  --query 'Reservations[].Instances[?State.Name!=`terminated`].InstanceId'
aws ec2 describe-volumes    --profile my-aws-profile --region ap-southeast-1 --query 'Volumes[].VolumeId'
```

## Cost envelope

| | |
|---|---|
| **Budget of record** | **$30/month** — ADR-030 |
| Always-on floor | **$2.28/month** — was $5.60; ADR-030 cut it 59 % |
| Full-stack hourly burn | **$1.5323/hr** — MSK is **50.6 %** of it |
| **Affordable lab time** | **≈ 18.1 hours/month** |
| **Core release** | **≈ $30.53 across TWO months** — month 1 $18.02, month 2 $12.51 |
| MSK at 24/7 | $594.45/month — **19.8× the budget**; gone in 39 hours |
| Always-on floor | **~$5.61/month** (was $4.60; + state CMK, ADR-021) |
| Per 6-hour metered window | ~$9.45 |
| **Affordable windows at $80** | **~7.9/month (~47 hours)** |
| **Affordable windows at $30** | **~2.6/month (~16 hours)** |
| MSK at 24/7 | $594.45/month — **7.4× the $80 plan, 19.8× the $30 budget** |

Full derivation: `docs/COST.md`. Unit prices: `docs/PRICE_REFERENCE.md`.

**OPEN-09 is resolved and fully propagated (2026-08-13).** `docs/COST.md` and
`docs/SESSION_DEPENDENCY_GRAPH.md` §5 are re-derived against $30. The core release now
spans **two months** rather than degrading the architecture to force one (ADR-030).
Reproduce every figure with `python3 scripts/derive-cost-envelope.py --budget 30`.

## Last successful commands

```bash
# 2026-08-13, Session 02 Stage B — all read-only, no apply
terraform fmt -recursive -check       # clean
terraform validate                    # Success
terraform plan -out=tfplan            # 113 to add, 0 to change, 0 to destroy
checkov -d terraform                  # 314 passed, 19 failed (all triaged)
bash scripts/tf.sh apply              # DRY RUN — refused to mutate
make validate-docs                    # 14 passed

# 2026-08-13, Session 00 closeout — all read-only
aws sts get-caller-identity                      # 111122223333, user/my-aws-profile
aws kafka list-clusters-v2                       # 0
aws ec2 describe-{vpcs,instances,volumes,nat-gateways,vpc-endpoints}   # all empty
aws emr-serverless list-applications             # []
aws redshift-serverless list-workgroups          # []
aws budgets describe-budgets                     # $30.00 + $1.00
aws cloudtrail lookup-events EventName=CreateCluster   # settles OPEN-03
make validate-docs                               # 14 passed, 0 failed
make lint-shell                                  # 5 scripts, bash -n clean

# 2026-08-12, Session 02 Stage A — all read-only or local
make identity                                    # account guard 111122223333 OK
bash scripts/install-tools.sh                    # checkov 3.3.10 installed
make lint-shell                                  # 5 scripts, bash -n clean
make validate-docs                               # 14 passed, 0 failed
bash scripts/probe-msk-instance-type.sh          # DRY RUN — no API call
bash scripts/bootstrap-state-backend.sh          # DRY RUN — no API call
aws budgets describe-budgets --account-id 111122223333 --profile my-aws-profile
```

## Open issues

| Priority | Issue | Owner | Next action |
|---|---|---|---|
| ~~P0~~ | **OPEN-09 — CLOSED 2026-08-13.** Budget = **$30/month**; `docs/COST.md` and `docs/SESSION_DEPENDENCY_GRAPH.md` §5 fully re-derived (ADR-030) | finops | **Done.** Core release spans two months: $18.02 + $12.51 |
| ~~P0~~ | **OPEN-03 — CLOSED, negative, 2026-08-13.** `kafka.t3.small` is **not offered** in `ap-southeast-1` | platform | **No probe needed.** CloudTrail holds the actual `CreateCluster` rejection (2026-07-26T13:01:14Z, `BadRequestException`, `invalidParameter: instanceType`) with the full valid instance list. `kafka.m7g.large` is already the cheapest available. **`scripts/probe-msk-instance-type.sh` and its Gate 1 package are retired** — evidence: `artifacts/validation/session-00/aws/open-03-settled.txt` |
| **P1** | **OPEN-10 — the operator identity is an IAM user with static access keys**, not a role | sre | **ADR-029 still unwritten.** More pointed after S02B-1: the credential environment is genuinely hazardous. Cover MFA, key rotation, assumed-role, and OPEN-13 together |
| **P0** | **OPEN-01 — state backend not bootstrapped, and its script contradicts ADR-030** | platform | Script still creates a KMS CMK; ADR-030 requires SSE-S3. **Gate 1 BLOCKED.** Stage B's plan is against LOCAL state and must be re-planned after the backend exists |
| **P1** | **OPEN-13 — the workstation's `[default]` AWS profile is a different account (`444455556666`), and four prod profiles sit alongside it** | sre | Fixed in Terraform (provider profile pin + account precondition, S02B-1). But every *script* and every future `terraform` invocation outside `scripts/tf.sh` carries the same hazard. Fold into ADR-029 |
| **P1** | R4 — lean endpoint set unproven; failure mode is a hang, not an error | spark | Trivial Iceberg job with only S3 gateway + Glue interface |
| **P1** | R2/R3 — MSK IAM client config for Connect and Apicurio | cdc | `kafka-topics --list` from the Connect host before any connector |
| **P1** | OPEN-06 — `tflint` and `shellcheck` still absent (`checkov` 3.3.10 done) | platform | `bash scripts/install-tools.sh` prints the pinned, checksum-verified tflint sequence |
| **P1** | **OPEN-18 — L3 against live L2 is `NOT_TESTED`** | spark | Snapshot semantics proven locally. The session's "compare with the previous build" is proven only *within one run* — stability across deployments is unverified |
| **P1** | **OPEN-17 — EOD against live L1 is `NOT_TESTED`** | spark | MERGE semantics proven locally; the pipeline is not. Partial-failure retry on a real cluster and the maintenance procedures (compact/manifests/expire/orphans) have never executed |
| **P1** | **OPEN-16 — end-to-end L1 streaming is `NOT_TESTED`** | spark | Transformation logic is proven by 49 passing tests; the pipeline is not. Kafka→Iceberg, checkpoint recovery across restart, replay from checkpoint and source-to-L1 reconciliation all need the applied stack |
| **P0** | **OPEN-15 — all six CDC correctness criteria are `NOT_TESTED`** | cdc | Cannot run without a live stack. `docs/CDC_CONTRACT_IMPLEMENTATION.md` §6 has the exact commands. **Session 06 must not start until they pass** — L1 built on unverified ordering is wrong in a way that is expensive to find later |
| **P1** | **OPEN-14 — six Connect/Debezium artifact checksums are `UNVERIFIED`** | cdc | Nothing has been downloaded; a fabricated checksum would be worse than an absent one. `verify-artifacts.sh` **fails closed**. One-time fix: `bash scripts/cdc-runtime.sh record-checksums --execute`, paste into `docker/cdc-runtime/versions.env`, re-apply |
| **P2** | R13 / OPEN-04 — public-subnet placement needs explicit security sign-off | sre | `docs/SECURITY.md` in Stage B |
| **P2** | OPEN-02 — RPO of 24 h follows from Kafka retention; accept or pay | spark | Decide in Session 05 before the first apply |
| **P2** | D6 — module map lists 14 modules, target tree shows 5 | platform | Reconcile in Stage B |
| ~~P2~~ | **OPEN-12 — CLOSED 2026-08-13, negative.** `express.m7g.large` is **2×** `kafka.m7g.large` ($0.5100 vs $0.2550) and saves only $0.0033/hr of bundled storage — a 77× loss | finops | **Rejected.** `docs/RISK_REGISTER.md` R11 closed at the same time |
| ~~P3~~ | **OPEN-11 — CLOSED 2026-08-13.** `docs/GAP_ANALYSIS.md` Gap 2 said "4 of ~12" against §4's "5 of 16" | platform | Gap 2 corrected to "5 of 16" |
| **P3** | D10/D11/D12 — prompt/session-file defects in the read-only guide | — | Accepted documentation debt; see `DECISION_LOG.md` |

**Opened 2026-08-20 (Session 31):** OPEN-32 — **EMR Serverless is unreachable without
MSK.** `emr_serverless`, `lake_iam` and `airflow_k3s` are all `count`-gated on
`enable_kafka_platform`, and `lake_iam` takes `msk_cluster_arn` and the platform CMK as
*required* inputs. So running a dbt-on-EMR reporting job — which reads Iceberg through Glue
and never touches Kafka — forces a 3-broker MSK cluster at ~$0.7650/hr, the whole $30 budget
in about 39 hours. Three costed options in `docs/REPORTING_INFRA_GAP_ANALYSIS.md` §5;
decoupling is an architecture change and was NOT made unilaterally. OPEN-33 — **ADR-036
diverges from the implementation in two places** (the `summary_config` sort key, and two
GSIs where the code uses one). Terraform follows the code because that is what the tests
exercise and what works against real DynamoDB; the ADR needs amending. OPEN-34 —
**`auto_destroy_after` is `2026-08-16T18:00:00Z`, in the past.** Every resource an apply
creates would be tagged already-expired (ADR-027). Reset it before any apply.

**Opened 2026-08-20 (Session 30):** OPEN-30 — **STREAMING_RT has never processed a real
event.** No EMR Serverless application, no Kafka, no DynamoDB, no Airflow deployment, and
both flags are off. Throughput, latency, restart behaviour on a real cluster and the actual
hourly cost are unmeasured; the failure contract is proven against fakes, which is the right
proof for the contract and no proof at all of performance. OPEN-31 — **the reconciliation
job that `KAFKA_DIRECT` would require does not exist.** It is not needed today because the
pilot ships `FULL_CDC_APPEND`, and `resolve_stream_source()` refuses KAFKA_DIRECT without
one — so the gap is enforced rather than latent, but it does bound the choice.

**Opened 2026-08-20 (Session 29):** OPEN-27 — **AUTO_CORRECT does not record
`source_layer_substituted`.** It reads `EOD_PLUS_REALTIME`, so it touches the substituted
REALTIME layer exactly as STREAM_BATCH does, and `record_source_layer()` now exists for it
to call. Left alone deliberately: Phase 8 code, one call plus one test, outside this
session's scope. OPEN-28 — **the AUTO_CORRECT and FULFILL Airflow adapters still raise
`NotImplementedError`.** Both flows are implemented and unit-tested; only
`_gate_and_run_callable`'s dispatch is missing, and wiring a mode without a phase to test it
under is how an untested path reaches a scheduler. OPEN-29 — **STREAM_BATCH has never run
against anything real.** No EMR Serverless application, no DynamoDB table, no Airflow
deployment, and REALTIME is a substitution, not a layer: the cadence, the cost per run and
the reconciliation gap rate are all estimates.

**Opened 2026-08-15 (Session 17):** OPEN-26 — 3 CloudWatch log groups have NO retention
(never expire). ~1.5 MB, and they belong to OTHER projects; the fix command is documented in
docs/FINOPS.md §1 but deliberately not run. Session 17 also confirmed LIVE that this project
costs **$0.00/month** (0 resources tagged Project=kafka-dev-lab) and the account ~$0.35/month
(44 pre-existing S3 buckets). Two cost alarms were raised and corrected by verification: the
5 KMS keys are AWS-managed and therefore FREE, and the 44 buckets are prior work, not ours.
**Opened 2026-08-15 (Session 16):** OPEN-25 — the AI assistant's tier 2 (Bedrock generation)
has never been invoked; only tier 1 (free retrieval + lookup) is exercised and evaluated.
The module is OPTIONAL and default-off; `rm -rf ai/` removes it with no effect on the
platform, and a test enforces that no pipeline code imports it.
**Opened 2026-08-15 (Session 15):** OPEN-24 — no SLI has been measured. Prometheus rules,
13 alerts and the Grafana dashboard are static-validated only; nothing is deployed, so no
alert has fired and every RTO is an estimate. 10 failure drills ran in dry-run with evidence
recorded; 4 recovery properties were demonstrated against real Spark/Iceberg.
**Opened 2026-08-15 (Session 14):** defect **D14-1** — a masked VIEW is not a masked TABLE.
Two curated tables holding unmasked PII sat under BI-allowed prefixes; an Athena view cannot
mask data the caller can read directly. Fixed with an explicit Deny plus a MATERIALIZED
`mart.dim_customer_bi`. **OPEN-20 decided: ISOLATE** the 39 orphan Glue tables — not adopted
(unknown provenance), not deleted (not ours to assume); owner action still required.
**Closed 2026-08-15 (Session 13):** defect **D13-1** — the `athena_bi` role was attached to
`lake_read` and could read L1/L2 in full, while the comment beside it claimed the opposite.
Fixed with a `mart_read` policy carrying an explicit Deny on the raw CDC layers at both the
S3 and Glue-catalog level.
**Opened 2026-08-15 (Session 13):** OPEN-23 — the entire Athena/Power BI path is
`NOT_TESTED`; no query has been executed and no BI connection made.
**Opened 2026-08-14 (Session 12):** OPEN-22 — the DAG tests run against locally installed
Airflow **2.9.3**, not the target **3.2.2**. The DAGs use only API valid in both, but the
import test therefore validates against the wrong version. Session 12 also added three
missing CLI entrypoints (Kimball build, reconciliation, Iceberg maintenance) that the DAGs
had nothing to call without.
**Opened 2026-08-14 (Session 11):** OPEN-21 — dbt against live Glue/EMR is `NOT_TESTED`.
Locally `catalog.json` is EMPTY (dbt-spark's catalog query does not work on a non-session
Iceberg catalog); manifest lineage is complete. Session 11 also **deleted** `spark/marts/`:
the four marts moved to dbt (S11-3), a migration rather than a duplication.
**Opened 2026-08-14 (Session 10):** OPEN-20 — the account is **not** empty. Glue database
`vannk-dev-oracle-db` exists in ap-southeast-1 with **39 tables** and no crawler, predating
this project's Terraform. Cost negligible; a governance/naming-collision concern for
Session 14. Not deleted — destroying unidentified catalogued data needs an explicit decision.
Session 10 also records the partition/file-layout and MERGE **benchmark** as `NOT_TESTED`:
a benchmark measures a deployed system, and nothing is applied.
**Opened 2026-08-14 (Session 09):** OPEN-19 — freshness ≤10 min `NOT_TESTED`. It is a
property of the deployed pipeline; the local tests measure correctness, not latency.
Session 09 also found **one defect**: nothing enforced the mart's grain, and because L1
keeps every I/U/D event, an updated transaction produced two facts (double-counted in every
sum) or a MERGE cardinality violation. Fixed by `collapse_to_grain()` using the same
source-position ordering as L3. A **vacuous test** was also found and replaced — see
`artifacts/validation/session-09/mutation-checks.md` and S09-1..S09-14 in `DECISION_LOG.md`.
**Opened 2026-08-14 (Session 08):** OPEN-18 — L3 pipeline untested.
**Re-executed 2026-08-14 (Session 08 review):** four defects found in work already reported
complete and green — a late delete emptying a partition left stale ACTIVE rows and still
certified (critical); `canonical_pk_expr()` emitted malformed JSON; no L2→L3 reconciliation
despite scope item 8; the reproducibility checksum ignored the business columns. Zero SQL
Server LSN coverage in L3 and the missing rebuild-script deliverable were also closed.
101 → 114 tests. See `artifacts/validation/session-08/defects-found-and-fixed.md` and
S08-10..S08-18 in `DECISION_LOG.md`.
**Opened 2026-08-14 (Session 07):** OPEN-17 — EOD pipeline untested.
**Opened 2026-08-14 (Session 06):** OPEN-16 — E2E streaming untested.
**Opened 2026-08-14 (Session 05):** OPEN-15 — CDC correctness undemonstrated.
**Opened 2026-08-13 (Session 04):** OPEN-14 — six artifact SHA256 values are `UNVERIFIED`;
`verify-artifacts.sh` fails closed until they are recorded from a live fetch.
**Closed 2026-08-13 (Session 03):** the `t3.large` vs `t3.xlarge` question ADR-030 left
open (answer: **`t3a.xlarge`**), and RDS necessity (**rejected, 6.2x**).
**Closed 2026-08-13 (Session 02 Stage B):** defect **D6** (module map reconciled — 7 built,
7 deferred, 1 dropped). **Opened:** OPEN-13 (default-profile account hazard).
**Closed 2026-08-13 (Session 00 closeout):** OPEN-03 (negative — `kafka.t3.small` not
offered), OPEN-09 (budget = $30), OPEN-11 (Gap 2 output score).
**Closed 2026-08-12 (Session 02 Stage A):** OPEN-05 (budget recipient).
**Opened 2026-08-13:** OPEN-10 (operator IAM-user identity), OPEN-12 (MSK Express uncosted).

## Next session prerequisites

Session 02 **Stage B** — `docs/TARGET_ARCHITECTURE.md` §12 items 4–15.

Decisions needed from the operator first — **two, down from four:**

- [x] ~~OPEN-09 — confirm the real monthly budget~~ **RESOLVED: $30** (2026-08-13)
- [x] ~~Gate 1 for the MSK probe~~ **RETIRED** — OPEN-03 settled from CloudTrail, no probe needed
- [ ] **Approve or decline Gate 1 for the state backend** — `docs/gates/GATE-1-state-backend-bootstrap.md`.
      Stage B cannot apply anything without it (ADR-021). **This is now the only open gate**
- [ ] **OPEN-04 — accept ADR-022's public-subnet placement, or take the A-min fallback**
      at +$0.1040/hr

Then, before writing modules — **do these first, they change the numbers:**

- [ ] **Re-derive `docs/COST.md` at $30** (~2.6 windows, ~16 h/month) and **delete the
      dead `t3.small` branch** from §3.3. Re-sequence `docs/SESSION_DEPENDENCY_GRAPH.md`
      §5 — its "core release ≈ $48.52 fits one month" **fails at $30**
- [ ] Cost `express.m7g.large` against `kafka.m7g.large` (OPEN-12) while you are in there
- [ ] Raise **ADR-029** for the operator identity (OPEN-10)

- [ ] Install `tflint` (and ideally `shellcheck`) — `bash scripts/install-tools.sh`
- [ ] Read `docs/TARGET_ARCHITECTURE.md` §12 items 4–15 — the ordered checklist
- [ ] Read `docs/APPROVAL_GATES.md` §0 and §1 — note 0.7 now **passes**
- [ ] Apply `modules/budget_guardrails` **first**, so the project gets tag-scoped cost
      attribution on top of the account-wide budget

## Session 34 — 2026-08-22 — PHASE 14 COMPLETE, architecture corrected

**Architecture corrected to the fan-out model.** Kafka → **FULL_CDC** (canonical, written
directly) → **REALTIME** and **EOD** as SIBLINGS. The previous docs drew a three-step chain
(`L1 STREAM → L2 FULL_CDC → L3 SNAPSHOT`) and hung flows off the raw landing — which
ADR-033 forbids and `source_resolver.resolve()` refuses, so the documented pipeline could
not have run. `reporting/layers.yaml` had recorded the ambiguity without resolving it.
Updated: `docs/TARGET_ARCHITECTURE.md`, `reporting/layers.yaml`, `docs/FOUR_FLOWS.md`,
`docs/LINEAGE.md`, `docs/DATA_CONTRACTS.md`, `docs/ARCHITECTURE_AND_TEST_GUIDE.md`;
`docs/L2_FULL_CDC.md` → `docs/L1_FULL_CDC.md`; new `docs/L2_REALTIME_STREAM.md`.

**Phase 14: all 8 scenarios have live evidence** —
`artifacts/validation/session-34/PHASE14-RESULT.md`.

Gaps closed since Session 33:
- **G1** real `dbt build` on EMR Serverless (`dbt=1.9.4`, `PASS=3 WARN=0 ERROR=0`), from a
  pinned cp39/aarch64 wheelhouse installed offline.
- **G2** STREAM_BATCH frozen upper bound computed and committed —
  `watermark_ts = 2026-08-22T12:00:00+00:00`, previously always null.
- **G3** a correction LANDED: +999.99 on one Oracle account traversed the whole pipeline
  and moved the mart 2,031,880,160.00 → 2,031,881,159.99.
- **G5** checkpoint read from S3 by the new `S3CheckpointInspector`, not a fixture.
- **G6** the real flow modules ran (`eod_flow`, `auto_correct_flow`, `fulfill_flow`,
  `stream_batch_flow`); Session 33's driver had bypassed them with an inline MERGE.

Still open: **G4** Airflow orchestrates none of it (`enable_airflow=false`, OPEN-28); the
REALTIME layer is bound but not materialised; STREAMING_RT processed zero events.

**18 defects found and fixed** — `artifacts/validation/session-34/DEFECTS-FOUND.txt`. Every
one was a silent failure. The most consequential: a missing `<topic.prefix>.transaction`
topic blocked ALL post-snapshot CDC while connector and task reported RUNNING; `event_id`
derived from Kafka coordinates collided across a cluster rebuild and discarded 5,728 fresh
events as duplicates; the Oracle ARCHIVELOG restart was unconditional and stranded the
database twice behind a HEALTHY container.

## Session 34 (cont.) — 2026-08-22 — PHASE 15 RECOVERY TESTS PASS

`spark/tests/test_phase15_recovery.py` — 20 controlled failure injections against the REAL
framework modules (coordinator, flows, runtime state, status machine), plus 4 cross-cutting
checks. **24 passed.** Matrix:
`artifacts/validation/session-34/PHASE15-RECOVERY-MATRIX.md`.

Run in-process rather than against AWS deliberately: every property under test (detection,
runtime state, final history, watermark advancement, retry, turn topology, attempt_number,
audit trail) belongs to this code, not to EMR — and a real Spark failure cannot be aimed at
"after the Iceberg commit but before history finalisation", which is the window that
matters most (case 9).

The invariant all 20 defend: **a watermark advances only behind a run that completed AND
validated.** Cases 8, 9, 10 and 13 attack it from four directions; it holds in all four.

Three structural guarantees confirmed, not conventions:
- illegal status transitions RAISE (`RUNNING -> SUCCEEDED` refused; must pass VALIDATING)
- `CheckpointInspector` has no `delete()` and must not gain one
- unknown dbt vars are rejected before dbt starts, and a dependency cycle fails at compile

Cases 9, 14 and 17 additionally carry LIVE evidence from Phase 14, where each occurred for
real. Not covered: Airflow-level retry backoff, pools and scheduler concurrency
(`enable_airflow=false`, OPEN-28).

## Session 34 (cont.) — 2026-08-22 — PHASE 16 NEW DATAMART TEMPLATE READY

A normal new mart now needs **three files and no DAG**:

    dbt/models/marts/<job_id>.sql        the SQL
    dbt/models/marts/schema.yml          the tests (one entry)
    reporting/jobs/<job_id>.yaml         the registration

Created: `templates/datamart/{model.sql,schema.yml,job.yaml}.tmpl`,
`scripts/create-datamart.py` (stdlib only, dry-run by default), `docs/NEW_DATAMART.md`,
`make create-datamart`, and `spark/tests/test_phase16_datamart_template.py` (8 tests).

**Validated by building a real mart through the normal workflow** —
`mart_channel_engagement_daily`, all five flows, STREAMING_RT shipped off. The chain
compile -> dbt parse -> dependency-sync -> validate passes end to end, and a
modification-time check confirms the only files touched were the three above: **zero**
under `spark/reporting/`, **zero** under `airflow/dags/`.

The scaffolder is ~300 lines of `@@NAME@@` substitution deliberately, not a
code-generation framework. It uses text insertion rather than a YAML round-trip because
`yaml.dump` would discard every explanatory comment in schema.yml, which is the most
valuable content in it.

One defect found and fixed while validating: the first version appended the schema entry to
the END of schema.yml, which lands it inside the `exposures:` block. dbt rejected it with a
message about exposures' allowed properties -- pointing at the columns rather than at where
the text went. `insert_into_models()` now finds the next top-level key after `models:` and
inserts before it.

## Session 34 (cont.) — 2026-08-22 — GUIDE V2 REPORTING UPDATE

**Prompt review.** All 24 prompt files in the guide package classified:
13 ACTIVE, 6 UPDATED, 3 SUPERSEDED, 1 DEPRECATED. Nothing deleted — a superseded prompt is
the best record of why the original approach looked right.
`aws-cdc-lakehouse-claude-guide-v2/prompts/PROMPT_STATUS.md`.

SUPERSEDED: prompts 06 and 07 (the L1→L2 staging chain, removed by the fan-out correction)
and prompt 09 (four flows; there are five modes with an accuracy ladder, ADR-040/042).
DEPRECATED: prompt 16 (AI/RAG) — out of scope, and the decision to skip it is worth keeping.

**The largest divergence:** prompts 00–19 stop at the four-flow model. ADR-033..045 — five
modes, accuracy ladder, dependency engine, turn coordinator, DynamoDB runtime state,
dbt-Spark bootstrap, streaming lifecycle — have **no prompt** in that package.

**Created:** `docs/EXECUTION_ORDER.md` (16 stages, each with an exit condition and status)
and `docs/runbooks/` (12 runbooks + index).

**Guide package integrity:** it is read-only, checksummed, and NOT a git repository. Only
two files were ADDED (`V2_ADDENDUM.md`, `prompts/PROMPT_STATUS.md`); no checksummed file was
modified. `sha256sum -c SHA256SUMS` still reports 70 OK / 3 FAILED — the three failures are
the pre-existing Session 01 amendment (ADR-025), not this work.

**Validation:** `make validate-docs` — 14 passed, 0 failed. ADR index confirmed correct by
the repo's own checker: 35 ADR files, all indexed, no orphans.

## Session 34 (cont.) — 2026-08-22 — PHASE 18 PRODUCTION REVIEW: NOT READY

Audit only; no code modified. `artifacts/validation/session-34/PHASE18-PRODUCTION-REVIEW.md`.

34 areas audited. The DATA platform passes everywhere that matters — layer semantics,
accuracy ladder, idempotency, watermark invariant, late data, failure recovery — all proven
against live infrastructure and pinned by 698 passing tests plus 20 injected failures.

**Three mandatory gates fail:**

- **P0-1 Nothing is scheduled.** `enable_airflow=false` (OPEN-28). Every live result came
  from `scripts/reporting-live-run.py`, which replaces the SCHEDULER, not the flows. Retry
  backoff, pools, scheduler concurrency and runtime task expansion are all unproven.
- **P1-1 Credentials materialised at rest.** The SSM config provider is commented out, so
  the CDC passwords sit in the connector config, the `connect-configs` topic and REST
  responses. All four SSM parameters are still Version 1 — never rotated since exposure.
- **P1-2 No alerting.** All 6 CloudWatch alarms are MSK broker health; zero on reporting
  runtime state. A failed EOD or stale watermark is silent until a human queries DynamoDB.

Also open: **P1-3** REALTIME not materialised (correct results, wrong cost — STREAM_BATCH
scans full history every 10 min); **P1-4** STREAMING_RT has processed zero events;
**P2-1** `auto_destroy_after` 6 days stale (cannot be changed without forcing replacement of
subnets/MSK/EMR/EC2); **P2-2** 9 Spark-session test files unexercised in the fast loop.

No fixes applied — the fix policy requires approval, and none of the P0/P1 items is a
zero-cost code change.


---

## Track C — AI / ML platform

**State for the AI track is `AI_PLATFORM_STATE.md`, not this file.** It is kept separate so
an AI phase and a platform session cannot overwrite each other's status.

| | |
|---|---|
| Last completed AI checkpoint | **`AI_TARGET_ARCHITECTURE_APPROVED`** (2026-08-25) |
| Current AI phase | **AI-P1 — AI Metadata / Contract Foundation**, NOT STARTED |
| Approved architecture | `docs/AI_TARGET_ARCHITECTURE_PROPOSAL.md` (905 lines, 25 sections) |
| Normative target | `docs/AI_TARGET_ARCHITECTURE.md` — **still the 8-line stub**; AI-P1 promotes it |
| Prompt pack | `../aws-cdc-lakehouse-claude-guide-v2/prompts/ai-platform/` — 16 phases |
| AI infrastructure | **none exists**; only AI-P13 mutates AWS, and only on operator approval by plan hash |
| AI spend to date | **$0.00** |

Open blockers carried into AI-P1/P3/P8: no Python dependency manifest (§3.9);
`s3vectors` and `bedrock-agentcore-control` absent from botocore 1.35.79 with the AWS
provider pinned exactly at 6.56.0; **Titan embeddings not offered in `ap-southeast-1`** —
Cohere only; Bedrock listed but never invoked by this repo.

The three tracks use separate numbering and never collide: **A** CDC/Lakehouse `00`-`19`,
**B** Reporting stages in `docs/EXECUTION_ORDER.md`, **C** AI `AI-P1`-`AI-P16`.


---

## Session 40 — rebuild defects fixed (2026-08-26)

The lab was destroyed and rebuilt. Seven defects surfaced, all fixed in the repository:
connector-payload quoting, Connect secrets ownership, SSM parameter escaping, a plaintext
password echo, a SQL Server login with no database user, a missing `cdc_reader` grant, and
**two Oracle JDBC drivers on the plugin path** (the `ORA-01005` root cause).

**Next rebuild: follow `docs/runbooks/rebuild-from-scratch.md`.** It carries the working
order, the benign errors not to chase, and a regression signature for each defect.

Two standing cautions:

- `terraform destroy` schedules the **lake CMK** for deletion on a 7-day fuse. It has done
  this twice. Check before applying; cancel it if you want the data.
- Updating an SSM parameter is **not** a rotation — both nodes read SSM only at boot, so the
  old secret stays live until user-data re-runs.

---

## Session 42 — post-live defect closure (2026-09-04)

The platform is **torn down**; nothing in this session touched AWS. It closes, in code and
Terraform, the defects the 2026-09-03 live window found and could not fix inside it.

**Nothing here is a PASS.** Seven scorecard criteria moved from FAIL/BLOCKED to
`FIXED, AWAITING LIVE RE-TEST`. The PASS count is unchanged at 28 and stays there until
`artifacts/validation/final-e2e/FINAL_TEST_SUMMARY.md` § *What to run tomorrow* produces
evidence.

| Finding | What changed |
|---|---|
| **G-P1-1** | `spark/jobs/full_cdc/job.py` decodes `PERMISSIVE`, quarantines the row **and writes the payload object** the module had always only referenced. Two error classes: `AvroDecodeError` (payload broken) vs `SchemaNotExported` (our export is stale) |
| **E2** | The writer schema is chosen per record by `apicurio.value.globalId`, not pinned per topic. An unexported id is quarantined, never decoded with a different version — `from_avro` returns plausible wrong values on a near-miss rather than raising |
| **E2 (tooling)** | `scripts/export-registry-schemas.py` + `cdc-runtime.sh export-schemas` export **every** schema version, keyed by globalId. A latest-only export cannot decode a topic's own history |
| **item 41** | `scripts/ai-feature-run.py` materialises the feature store from the live mart and scores the unsupervised anomaly pilot. One Athena query per business date, with a **refusal on truncation** — a trailing average over 9 of 30 days is a plausible number with the wrong name |
| **test harness** | `spark/tests/conftest.py` owns one session-scoped `SparkSession`. Ten modules each called `getOrCreate()`, which in one JVM returns the FIRST session built — so 61 tests errored on a full-suite run while passing individually. Now `1693 passed / 0 errors` in one run |

Also carried in from the same window: B2 (agent dimension substitution), OPEN-28
(AUTO_CORRECT/FULFILL in the scheduled path), R4 (reporting role logs), G1/G2/G3
(observability endpoints + KRaft alarm gating), and the `serving` Glue database.

**Still open:** E5 (EOD MERGE has no retract arm), G-P1-3 (source-connector DLQ is inert for
records that fail conversion *before* Kafka), G4 (workload hosts publicly addressable),
item 42 (Bedrock — `INVALID_PAYMENT_INSTRUMENT`, a billing state, not a defect).

**Standing caution, unchanged and urgent:** CMK `2fd510a7` is `PendingDeletion 2026-09-10`
and holds 1,875 of 2,758 lake objects. Cancel it **with the profile**:
`aws kms cancel-key-deletion --key-id 2fd510a7… --profile my-aws-profile --region ap-southeast-1`.

---

## Session 43 — CDC Phase 2: per-table provisioning and the event router (2026-09-10)

**Nothing touched AWS. Cost $0.00.** No plan, no apply, no connector call. The provisioner
is dry-run by default and the ingest jobs still default to `--migration-mode legacy_only`,
which writes the monolith and nothing else — exactly as before this session.

Phase 1 left a compiled registry that nothing consumed. This session makes it drive
provisioning and routing:

| | |
|---|---|
| `cdc/catalog.py` | layer → physical Glue database, read from `reporting/layers.yaml` (ADR-033), carried into the compiled plan so runtime and compile cannot disagree |
| `cdc/rowspec.py` | ONE canonical row contract; the provisioner, the writer and the drift check all derive from it |
| `cdc/provision.py` | generic DDL generator — one generator, 21 layer targets plus the optional event index. A test asserts no per-business-table DDL exists anywhere in `spark/` |
| `cdc/router.py` | normalized event → table_id → config → the one target it names; migration modes; routing counters |
| `spark/jobs/full_cdc/per_table.py` | the routed write, shared by BOTH ingest jobs (the OPEN-28 rule) |
| `spark/ops/provision_cdc_tables.py` | dry-run-by-default apply + drift verify |

**Three refusals, and they are the point.** An event whose table is not registered is
quarantined with a counter naming what it claimed to be — never written to a default target,
and above all **never allowed to create a table**. A registered-but-disabled table is a
*different* outcome from an unknown one, because the operator response differs. A topic and
an envelope that name different tables are refused rather than guessed.

**Decided against 512 MiB.** The phase brief permitted Iceberg's default "only if consistent
with project benchmarking". Phase 0 measured a mean file of **137 KiB** against it — file
size is set by commit frequency, not by the property — so 128 MiB stays and compaction is
the remedy.

Tests: **51** pure (no Spark) + **24** against a real local Iceberg catalog. Full suite
1,872 passed. `make check` exit 0.

**Not done, and not claimed:** no cutover. ADR-062's gate stands — a zero-diff
full-outer-join on `event_id`, computed independently in Athena.

**Found, out of scope, unfixed:** `airflow/dags/business_insights.py` fails four DAG policy
tests (no `is_paused_upon_creation`, no `dagrun_timeout`, no `execution_timeout`, and a
`PythonOperator` carrying transformation logic). It predates this session — committed at
`1d3cf5b` and untouched here. The first of those means **the DAG starts on deploy**.

---

## Session 43b — CDC Phase 1 audited against its brief (2026-09-10)

**Cost $0.00.** No AWS call, no connector mutation, no Terraform.

Phase 1 (registry, compiler, scaffolding) was already implemented and is what Phase 2 was
built on this session. Audited requirement by requirement rather than rebuilt:

| Section | State |
|---|---|
| A config model / B table id / C target names / D inheritance | implemented, unchanged |
| E semantic validation | all 14 listed checks present, unchanged |
| F compiled plan | implemented; schema version 2 since Phase 2 |
| G scaffolding CLI | **one real defect, fixed — see below** |
| H plan command | all 12 required sections present, unchanged |
| I tests | all 16 listed cases covered; **43 → 63 tests** |
| J docs | both exist; extended for the fix |

**The defect: `--discover` was a stub that reported success.** It claimed to read the source
catalog over SSM; it built a SQL string, never used it, called `aws ssm send-command` with no
instance and no parameters, and returned empty every time. Replaced with `cdc/source_schema.py`,
which reads columns and primary key from `docker/source-lab/<engine>/01-init.sql` — the DDL
that creates the captured tables. Read-only **by construction**: it opens a file, never a
connection.

That also connects Phase 1 to Phase 2: the scaffold now emits a ready-made (commented-out)
`payload: {mode: typed, columns: [...]}` block, and a test uncomments it and compiles it.

A new cross-check asserts the shipped registry's primary keys equal the source DDL's — the
first thing in the repo to compare those two hand-maintained spellings of one fact.

---

## Session 43c — CDC Phase 2 re-audited: typed temporals were silently wrong (2026-09-10)

**Cost $0.00.** No AWS call, no connector mutation, no Terraform. Migration mode is still
`legacy_only`; production still writes only the monolith.

Phase 2 (provisioner, router, migration modes) was built earlier this session. Re-auditing
it against section B's *"prefer typed per-table payloads where current schema/Avro
integration safely supports them"* — read together with *"do not silently change data
semantics"* — found that it did **not** safely support them.

**The defect.** Debezium's `io.debezium.time.*` are CUSTOM Kafka Connect logical types, so
an Avro converter carries them as the underlying primitive and `from_avro` yields a plain
int64/int32. The typed writer cast that to `timestamp`. Measured on this platform's Spark:

```
epoch millis 1787356800000  CAST AS timestamp  ->  +58609-…       silently wrong
epoch micros                CAST AS timestamp  ->  +109081-…      silently wrong
epoch days (int32)          CAST AS date       ->  batch dies
```

A year-58609 timestamp partitions, sorts and reconciles like a real value. Phase 1's
`--discover` would have handed an operator exactly such a column.

**The fix.** `payload.columns[].encoding` — `timestamp_millis` | `micro_timestamp` |
`nano_timestamp` | `date_days` | `zoned_timestamp` | `none`. The writer converts rather than
casts; the compiler **refuses** a typed `timestamp`/`date` column that declares none. No
default: a wrong default lands values in 1970, which is *plausible* and therefore worse than
absurd. Discovery derives the right encoding per column from the source DDL plus the
connector's `time.precision.mode` — distinguishing Oracle `DATE` (millis) from
`TIMESTAMP(6)` (micros), which one blanket mapping would have got wrong by 1000x.

Tests: **62** pure + **34** real-Iceberg (was 51 + 24), including one that pins the
year-58609 bare-cast result as a regression guard.

---

## Session 43d — CDC Phase 3: config-driven REALTIME layer (2026-09-10)

**Cost $0.00.** No AWS call, no connector mutation, no Terraform. Nothing has run on EMR.

REALTIME was one job with three fixed arguments and a bound computed *inside the query*.
It is now a generic engine driven by the compiled registry.

| | |
|---|---|
| `cdc/realtime.py` | window resolution — four bounds, two boundary modes, the run-ledger schema. Pure; no Spark |
| `spark/jobs/realtime/realtime_engine.py` | THE engine: `--table <id>\|ALL --as-of --rebuild`. Reads FULL_CDC, never Kafka |
| `cdc/provision.py` | provisions `ops.realtime_run`; per-table REALTIME partition override |
| `spark/jobs/realtime/job.py` | kept for every recorded recipe, now DELEGATES its window to `cdc.realtime` |
| `docs/REALTIME_LAYER.md` | the layer, and §3 separating it from the STREAM_BATCH watermark |
| `docs/adr/ADR-064` | the decision |

**The crux: "3 days" is not 72 hours.** A rolling window from a 09:15 run holds three
*partial* days and shifts every run; a calendar window starts at midnight in the business
timezone and holds three *whole* days. Both are defensible, so the boundary is config —
and an hours-configured table keeps `rolling_hours`, so **no deployed table's window
changes**. A test asserts the shipped registry is still entirely rolling_hours.

**Three defects fixed in the old job:** the bound moved during the run; there was no upper
bound at all; and `createOrReplace` would have reset the partition spec and governance
properties Phase 2 provisions.

Tests: **32** pure window + **25** against a real Iceberg catalog. Plan schema 2 → 3.

**Not proven:** no REALTIME table has been materialised in AWS by this engine and no ledger
row exists in Glue. The live gate is one `--rebuild` whose row count matches an independent
Athena count of FULL_CDC over the same bounds.

---

## Session 43e — CDC Phase 4: config-driven EOD close (2026-09-10)

**Cost $0.00.** No AWS call, no connector mutation, no Terraform. Nothing has run on EMR.

EOD was one job that closed one table — Oracle `ACCOUNT`, hard-coded down to the column name
`j.ACCOUNT_ID` — with an Oracle-only ranking that its own comment warned would silently
collapse onto `kafka_offset` if pointed at SQL Server. It is now a generic builder.

| | |
|---|---|
| `cdc/eod.py` | cutoff arithmetic, per-engine ordering, delete/snapshot policy, ledger schema. Pure |
| `spark/jobs/eod/eod_engine.py` | THE builder: `--table <id>\|ALL --cob-date --fulfill`. Reads FULL_CDC, never REALTIME |
| `cdc/provision.py` | provisions `ops.eod_run` |
| `docs/EOD_LAYER.md`, `docs/adr/ADR-065` | the layer and the decision |
| `make cdc-table-eod TABLE=… COB_DATE=…` | preview the cutoff and the ordering, $0 |

**The ordering fix.** Oracle SCN is numeric (`'9' > '10'` as strings) and must be padded;
SQL Server LSN is hex and already fixed-width, so it must be *validated*. Both now yield a
sortable string, so one ORDER BY serves both engines and no comparison depends on a cast that
can return NULL. `kafka_partition` precedes `kafka_offset` — CLAUDE.md §5.4.

**Two defaults decided by inspection, not preference:** deletes stay `exclude_from_snapshot`
(what the deployed job does), and snapshot mode defaults to `rolling_history` (the provisioned
table is partitioned by `business_date` with `retention_days: 365`, so the contract already
committed to history; `latest_state` would delete up to 364 certified partitions on first run).

**Certification is a gate.** `CERTIFIED` needs DQ *and* an independent reconciliation
(`distinct_keys − deletes == rows`) to pass. Data is written either way; the marker is
withheld and the process exits non-zero.

Tests: **41** pure + **28** against a real Iceberg catalog. Plan schema 3 → 4.
Full suite **2,048 passed / 4 failed** — the 4 are the pre-existing `business_insights.py`
DAG-policy failures, untouched since `1d3cf5b`.

**A defect I shipped and then caught late:** both new engines were named `engine.py`, and
Python caches by module *name*, so they were one module — 24 REALTIME tests broke while both
suites passed standalone. EMR stages job modules flat, so this was a live hazard, not a test
artifact. Renamed to `realtime_engine.py` / `eod_engine.py`; a regression test now requires
unique flat names under `spark/jobs/`. It was hidden by my re-running only the config-layer
suites after a fix — a targeted rerun proves the thing you targeted, and a name collision is
by definition an *interaction*.

**Not proven:** no EOD snapshot has been built in AWS by this engine and no `ops.eod_run` row
exists in Glue. The live gate is one close whose ledger `row_count` matches an independent
Athena count of distinct keys in FULL_CDC below the same `cutoff_utc`.

---

## Session 43f — CDC Phase 5: safe table onboarding workflow (2026-09-10)

**Cost $0.00.** No AWS call, no connector mutation, no Terraform. No table was onboarded.

Onboarding a new table was a sequence of hand-performed steps with no gate between them and
no record of which had been done. It is now a gated workflow:

```
SCAFFOLD → VALIDATE → PLAN → APPROVE → APPLY → SNAPSHOT/CATCH-UP → VALIDATE → ACTIVE
```

| | |
|---|---|
| `cdc/precheck.py` | source precheck, read-only — reads Git, so it runs with the lab torn down |
| `cdc/connector_plan.py` | capture diff derived from the registry; onboarding modes; redaction + config hashing |
| `cdc/lifecycle.py` | 11 states, closed transitions, evidence on every one |
| `cdc/onboarding_checks.py` | the acceptance checks and the judgement, pure |
| `scripts/cdc-table-{validate,provision,onboard,status,smoke}.py` | the section-H commands |
| `docs/CDC_TABLE_ONBOARDING.md`, `docs/adr/ADR-066` | the workflow and the decision |

**Six of the seven commands mutate nothing.** The two that change the world —
`provision_cdc_tables.py --execute` and `cdc-runtime.sh update-connector --execute` — are
*printed*, never invoked.

**Capture removal needs its own approval.** `--approve-capture` never implies it, and the
orchestrator refuses to print an apply command while the diff contains one.

**`incremental_snapshot` is refused**, not defaulted: neither connector configures
`signal.data.collection`, so there is no channel for an execute-snapshot signal. The default
`changes_only` describes what the platform actually does today.

**The readiness check found three shipped tables with an empty DQ contract** — `branch`,
`channel`, `merchant`. Fixed in the registry; an EOD close that cannot fail a DQ rule
certifies whatever it produces.

Tests: **57**, no Spark, no AWS. Plan schema 4 → 5. Full suite **2,105 passed / 4 failed**
(the 4 pre-existing `business_insights.py` DAG-policy failures).

**Not proven:** no table has been onboarded live. No connector updated, no snapshot taken, no
smoke check executed against the platform.

---

## Session 43g — CDC Phase 6: cutover machinery built, cutover NOT performed (2026-09-10)

**Cost $0.00.** No AWS mutation. Read-only describes only, to establish state.

**The Phase 0 gate was checked first and is satisfied.** ADR-062 gated per-table cutover on
two triggers; trigger 2 (differentiated retention/PII/IAM between source tables) is now real —
`customer` and `app_user` are `confidential`, three tables disable realtime, freshness SLAs
differ 15 vs 60 min. ADR-060 denies the AI plane `warehouse/full_cdc/` wholesale because a
per-table grant is not expressible against a monolith.

| | |
|---|---|
| `cdc/cutover.py` | `LEGACY`/`DUAL`/`PER_TABLE` per table, groups, and the resolver |
| `cdc/reconcile.py` | the 9-check gate and the benchmark harness, both pure |
| `spark/jobs/full_cdc/backfill.py` | legacy → per-table, identity copied not recomputed |
| `scripts/cdc-cutover.py` | status / gate / plan / set |
| `docs/CDC_CUTOVER.md`, `docs/adr/ADR-067` | the runbook and the decision |

**All eight tables remain `LEGACY`.** Nothing changed for any consumer.

**A Phase 4 defect surfaced and is fixed:** the EOD DQ check coalesced the after-image with
the *before*-image, which the EOD row contract does not carry — every one of the eight tables
declares a `not_null` rule on a PK column, so this broke **every** close. Phase 4's tests
missed it because no fixture table declared a DQ rule. The fix also stopped non-key columns
passing vacuously.

Tests: **18** pilot (real Iceberg: backfill, identity, EOD/REALTIME equivalence both paths)
+ **34** gate/benchmark/CLI + **4** new Phase 4 DQ regressions.

**NOT DONE, and not claimed: no pilot has run against AWS.** No backfill executed, no `DUAL`
window observed, and **not one of the nine benchmark metrics measured on either path**. No
table passes the gate.

**The platform is UP and billing** (~$1.12–1.53/hr): MSK ACTIVE, EC2 running, EMR Serverless
CREATED — but the Glue catalog is **empty** (tables destroyed; S3 Iceberg data survives, 51
parquet files under `warehouse/full_cdc/cdc_events/`). A live pilot needs
`spark/ops/register_tables.py` first. `docs/runbooks/stop-and-resume.md`.

---

## Session 43h — CDC Phase 6 pilot PASSED LIVE (2026-09-10)

**2 of 8 tables are cut over.** Account 111122223333, profile `my-aws-profile`,
`ap-southeast-1`. Evidence: `artifacts/validation/session-43/CUTOVER_PILOT_RESULT.md`.

| step | result |
|---|---|
| register 15 surviving Iceberg tables into the empty Glue catalog | SUCCESS — monolith at **20,390 rows**, matching the Phase 0 audit exactly |
| provision the two pilot targets | SUCCESS |
| backfill (dry run, then execute) | `identity_match=True` on both |
| benchmark, both paths, 9 metrics × 5 scenarios | complete |
| gate | **9/9 on both tables** |
| cutover | `oracle.coredb.corebank.account`, `sqlserver.digital.dbo.digital_event` |
| **legacy monolith after** | **20,390 rows — unchanged** |

**Measured headline:** EOD source scan for `ACCOUNT` fell **89,193 → 11,231 bytes (-87.4%)**,
matching ADR-062's "≈87%" prediction. Data files 14 → 2 (-85.7%) on both.

**The other half, reported as readily:** `digital_event`'s EOD scan improved only **-7.1%** —
it *is* 12,000 of the monolith's 20,390 rows, so isolating the dominant table saves little.
`auto_correct` on ACCOUNT got **3× slower**. The pruning benefit is inversely proportional to
a table's share of the monolith; nobody had stated that before the measurement.

**Four defects the live run found that the local pilot could not:**
1. `register_tables.py` configured **no catalog** — it relied on submit-time `--conf`.
2. Its `--dry-run` returned *before* the failing statement, so the dry run passed.
3. **320/961 `ACCOUNT` and 6,000/12,000 `digital_event` rows carry a NULL `dv_event_id`** —
   `MERGE ON dv_event_id` never matches NULL, so the backfill would silently stop being
   re-runnable. Now excluded and counted.
4. **The legacy predicate uppercased only the literal** — matched every Oracle table and no
   SQL Server one. The legacy read returned **zero rows with no error**.

**Not done:** no `DUAL` window was observed (the backfill reproduces existing history; `DUAL`
validates *new* events — run it before cutting over a table actively receiving CDC). 6 tables
remain `LEGACY`. The identity-less rows exist only in the monolith.

**The platform is UP and billing** (~$1.12–1.53/hr). `docs/runbooks/stop-and-resume.md`.

---

## Session 43i — deferred gaps closed, and Phase 7 (2026-09-10)

### The gaps

| gap | status |
|---|---|
| `business_insights.py` failed 4 DAG-policy tests (deferred **8 times**) | **FIXED** — pipeline extracted to `scripts/ai-insight-run.py`, DAG now BashOperator-only, plus `is_paused_upon_creation`, `dagrun_timeout`, `execution_timeout`. **All 43 DAG tests pass** |
| `test_l1_local_spark.py` imported a bare `job` | **FIXED** — loaded by path; the guard tightened from "no new one" to "none at all" |
| `event_serial_no` vs `change_lsn` in `position_secondary` | **RESOLVED** — the discrepancy was between the *superseded* L1 module and the live path; the live path is right. Pinned by test, contract doc corrected |

The DAG fix was deferred so long because the obvious move was to add the callable to the
allowlist — which would have been editing the test to match the code. The compute is
Athena's, but the driver around it was running on the scheduler.

### Phase 7 — table operations

| | |
|---|---|
| `cdc/maintenance.py` | metric-driven action selection, temperature, cadence floor, bounded batch |
| `cdc/schema_guard.py` | ALLOW / PLAN / BLOCK per change, with a remediation on every block |
| `cdc/decommission.py` | the 6-step offboarding sequence, governance record, observability surface |
| `scripts/cdc-maintenance.py` | measure / plan / governance / observe / schema / decommission |
| `spark/ops/cdc_maintenance_job.py` | applies a plan; dry-run default; `--metrics` required |
| `docs/adr/ADR-068` | the decision |

**Measured live:** the monolith at **14 files, 140,546 B mean, 13 manifests, 53 snapshots** —
matching the Phase 0 audit's 14 files / 137.3 KiB. All 8 tables report **zero governance
gaps**.

**A threshold defect real metrics found:** the backfilled `ACCOUNT` holds 2 files averaging
48 KB, and a mean-only trigger asked to compact two files into one — an EMR run to eliminate
one file, every cadence, forever. Both rewrite branches now require enough files to pay for
themselves.

Tests: **52**, no Spark, no AWS.

**Two defects the live run found in the schema guard:** it BLOCKED nine columns of the
cut-over ACCOUNT table on `varchar -> string` — one type under two engine names, and a guard
that fires on every table is one nobody reads. Folding the spellings then introduced a worse
bug for one commit: comparing *base* types made `decimal(18,2)` and `decimal(18,4)` both
"decimal" and therefore "unchanged", permitting the single most destructive change the module
exists to block. Normalisation now folds the name and keeps the parameters; both halves are
pinned by test.

**Known gap:** the maintenance job operates on *registered* tables, and the one table that
demonstrably needs maintenance — the legacy monolith at 14 files / 13 manifests / 53
snapshots — is not one. It holds eight source tables, so "whose policy governs it?" needs a
decision about its retirement path, not a line of code.

---

## Phase 8 — new-table zero-custom-code acceptance (ADR-069)

The claim "a new table needs no new code" had never been *executed*. Every table in the
registry pre-dated the platform, so every code path had only ever run against tables it was
designed around — which proves the fit, not the generality.

**Two tables, two engines, two payload modes**, onboarded through the shipped commands only:

| table | engine | payload | why this one |
|---|---|---|---|
| `oracle.coredb.corebank.loan` | Oracle | JSON | numeric SCN, `calendar_day` window |
| `sqlserver.digital.dbo.payment_method` | SQL Server | **typed** | hex LSN, `micro_timestamp`, `confidential` |

Each is **one registry entry**. No Spark job, no DAG, no `CREATE TABLE`, no EOD code.

### Live evidence (account 111122223333, `ap-southeast-1`)

* `corebank.loan` created in Oracle via SSM; precheck **BLOCKED** until declared in the Git
  DDL — the `declared`-evidence contract working, not a bug — then 0 blocked.
* All six targets provisioned on EMR Serverless (`00g8lp0dlt18h827` for the SQL Server table).
* **EOD close of `oracle.coredb.corebank.account`, COB 2026-08-21: 320 rows, 320 distinct
  keys, 0 null keys** — matching an **independently computed** Athena expectation of 320,
  derived from FULL_CDC below the same `cutoff_utc` rather than from the job that wrote it.

### Three defects the live run found

1. **A certification with no evidence.** The close passed both gates, printed
   `status=CERTIFIED`, `certified=1`, exited **0** — and its ledger write had failed with
   `TABLE_OR_VIEW_NOT_FOUND`. `write_ledger` was "best-effort" for a real reason (failing the
   run would rebuild correct data), but the ledger row *is* the completion marker. New
   `STATUS_UNVERIFIED = "UNVERIFIED_NO_EVIDENCE"`: the data stays written and readable, the
   **claim** is withheld, the exit code is non-zero.
2. **The root cause.** `plan_targets` emits the shared OPS tables only when no `--table`
   filter is given — correct, but every provisioning run in Phases 2–8 had used `--table`, so
   the OPS ledgers had never been created at all. The skip stays; a filtered run now prints
   `CDC_PROVISION_NOTE` naming the unfiltered command.
3. **Maintenance was a platform-wide no-op.** Carrying `maintenance` into the plan (schema 6)
   serialised the "not declared" sentinel `()` as `[]`, which `_enabled_actions` reads as
   "permit **only** these" — i.e. nothing. The job ran, selected its batch and did nothing,
   indistinguishable from having nothing to do. The plan now emits `null`, and an explicitly
   empty list is **refused**.

### A test-hygiene defect worth naming

Onboarding `loan` broke eleven tests that used `loan` as "a table that does not exist" and
`8` as "every table". **Four still passed while asserting the opposite of their intent** —
routing a now-known table and checking it was refused. Counter-examples are now guarded names
(`ABSENT_TABLE`, `UNREGISTERED_TOPIC`) with a test asserting they stay absent; set membership
(`SHIPPED_TABLE_IDS`) replaces counts, so adding a table is a line in a diff.

### Also shipped

* `scripts/cdc-deploy-code.sh` — the deploy was a hand-typed `zip` for six phases, so EMR ran
  whatever the last ad-hoc upload happened to include. It compiles the plan rather than
  copying it, so the plan on S3 always matches the registry at the deployed commit.
* Eight runbooks plus `docs/CDC_TABLE_PLATFORM_ARCHITECTURE.md` (the mermaid overview).
  `docs/CDC_TABLE_QUICKSTART.md` is the entry point.

Tests: **2,283 passed**.

**The orchestration gap.** No Airflow DAG submits any generic entrypoint — a grep over
`airflow/` for `eod_engine`/`realtime_engine`/`per_table.py`/`cdc_maintenance_job` returns
nothing, and `dag_eod_pipeline.py`'s hardcoded table list is the **legacy** path. "A new table
needs no new DAG" therefore passes for a weaker reason than it looks: nothing schedules the
per-table platform, and every run this phase was a manual `emr-submit.sh`. Open issue #5.

### The live capture leg — closed, and it cost six more defects

`register-connectors.sh update --execute` was applied by a human. The full path now runs end
to end for **both** tables: source → Debezium → Kafka → FULL_CDC → REALTIME → EOD, both
`CERTIFIED`, both `ACTIVE`.

| | `loan` (Oracle) | `payment_method` (SQL Server) |
|---|---|---|
| FULL_CDC | 4 rows — c=2 u=1 d=1 | 5 rows — c=3 u=1 d=1 |
| position | `3287944` numeric SCN | `0000002c:00006d50:003a` hex LSN |
| window | `calendar_day` → midnight 2026-09-06 | `rolling_hours` → 14:01:09, 72h back |
| EOD | 2 keys − 1 delete = **1 row**, CERTIFIED | 3 − 1 = **2 rows**, CERTIFIED |

Each EOD count matches an Athena expectation computed from FULL_CDC below the same
`cutoff_utc` — not read back from the job that wrote it. `dual_write` put the same events in
the legacy monolith (20,390 → 20,400, the last being the late event below) and the
per-table counts match exactly.

**Six defects, every one failing silently or misleadingly** (ADR-070):

1. **No Kafka topic.** `auto.create.topics.enable=false`, so Debezium cannot create one —
   both connectors reported RUNNING and produced nothing, every check green. The host's topic
   list was a hardcoded bash array under *"Keep these in sync with `table.include.list`"*.
2. **Stale `schemas.json`**, which genuinely cannot be exported before capture starts.
   The ingest decoded with a fallback and died with `FIELD_NOT_FOUND: no such struct field
   'op'` — a field name, not the cause.
3. **`per_table` missing from the deploy zip** — EMR downloads only the entrypoint, so this
   was a `ModuleNotFoundError` *after* acquiring capacity.
4. **Kafka jars absent for the `stream` role** — `Failed to find data source: kafka`, which
   reads like a typo in the format name.
5. **The acceptance judge accepted anything truthy** — `"FAILED"` passed a connector state,
   `"0"` passed a row count, `"probably"` passed a boolean. This is the *last* gate before a
   table is trusted downstream. Evidence is now type-checked per check.
6. **A genuinely capturing table could never reach ACTIVE** — the walk rightly refuses to
   claim `CAPTURING` from a template, and the confirmation gate forces the update out of
   band, so the ledger stuck at `PROVISIONED`. Smoke now advances it on observed topic
   records **and** FULL_CDC rows, and only on those.

### Orchestration: tables became data (ADR-071)

The Phase 8 point "no custom Airflow DAG is needed" was passing for the wrong reason: **no DAG
orchestrated the per-table platform at all**, so it was satisfied by absence rather than by
genericity. Now `airflow/dags/cdc_table_platform.py` adds three generic DAGs — `cdc_realtime`,
`cdc_eod`, `cdc_maintenance` — whose tasks are **dynamic-mapped over the compiled plan**.

Measured: one added registry entry moved REALTIME from **7 to 8** tasks and EOD from **10 to
11**, with the DAG file untouched. Three near-static tables get no REALTIME task at all,
because the mapping reads each table's own policy.

Enforced by test, not intent: no per-table DAG may exist (every registry table name is checked
against every DAG filename); no registry table name may appear in the DAG's executable body;
and EOD passes no `--cob-date`, so the scheduler's timezone can never decide a business date.
The DAGs deliberately do not ingest, provision, or touch connectors.

**A defect in my own DAG, caught by binding it to reality**: the first version called
`submit(name=..., entry_point=..., arguments=...)`. That reads perfectly and is wrong — the
submitter takes one positional `SubmissionRequest`, so it would have raised `TypeError` the
first time a task ran, which for an off-by-default DAG could be weeks after merge. Every other
check in the DAG suite reads the file and none would have caught it;
`test_the_submit_call_matches_the_real_submitter_signature` now binds the parameter list, the
request's field names and the handle attribute via `inspect.signature`.

**Off by default and never run on the deployed Airflow** — that is open issue #8.

### Running the FULL verification surface found two more

`pytest` alone had been green throughout, so both of these had been invisible.

1. **The committed plan artifact was two table onboardings stale.** `make cdc-verify` catches
   it — but `cdc-verify` was **not in `make check`**, the aggregate target a developer
   actually runs, and it is not part of `make test` either. Every Spark job takes `--plan`, so
   a stale artifact silently runs the previous config: old window, old cutoff, old delete
   policy, old table list. The run succeeds and certifies numbers built to a configuration
   nobody is looking at any more.
2. **The hash was never recomputed.** Both `cdc-verify` and the first version of the new test
   compared the artifact's *stored* hash fields — which a truncated file carries unchanged.
   Dropping a table from `plan.tables` left the recorded `plan_hash` correct and passed
   everything. The test now recomputes from the content.

Fixed by adding `cdc-verify` to `make check` **and**
`test_the_committed_plan_artifact_is_not_stale` to the suite, so neither a forgotten
recompile nor an edited artifact can pass. A gate nothing runs is not a gate; a hash nothing
recomputes is not a checksum.

**Still not claimed:** LOAN's EOD snapshot does not equal its source table and should not —
3 of its 4 live rows pre-date capture, which is what `changes_only` means. Both tables still
read the legacy monolith until someone runs the cutover gate. Both connector templates are
rendered and committed; applying them needs
`bash scripts/register-connectors.sh update --execute`, which requires a human to type the
confirmation phrase. That gate was not bypassed.


---

## Phase 0 re-audit — closing its own open evidence (2026-09-10)

The master brief was re-issued asking for PHASE 0. It was not re-derived: §0 of that brief says
current code and deployed state win over older prompts, both Phase 0 deliverables existed, and
its token was recorded. Its answers were **predictions**; they have since been measured, so the
useful act was closing them.

| Phase 0 item | status then | measured now |
|---|---|---|
| B6 bytes scanned | **PENDING** — "Glue catalog empty on this apply" | `ACCOUNT` **−87.4%** (matches the ≈87% prediction); `digital_event` **−7.1%** (missed) |
| B2 rows/table/day | **PENDING** — same | `digital_event` 3,000/day … `channel`/`BRANCH` **4–8/day** |
| `event_date IS NULL` | a live defect with a diagnosed root cause | **0 of 20,400** |

**Where the audit was wrong, and it is now written into the document**: it assumed per-table
storage helps uniformly. It does not. `digital_event` *is* 12,000 of the monolith's 20,390
rows, so isolating the table that dominates the file saves ~7%, and two of its metrics get
worse at this volume. The decision still holds — seven of ten tables are small and gain a lot,
and the architectural benefits were never contingent on scan bytes — but the headline number
applies to the tail, not the head.

**Where it was right**: the `event_date=null` root cause. It predicted the orphaned file was
referenced by no snapshot chain, so rebuilding the catalog would produce a table without it.
That is what happened; no delete was needed or issued. Its recommendation became a registry
default (`dq.event_date_null_tolerance: 0`), so every table — including the two onboarded in
Phase 8 — inherits the control.

**Two doc gaps found against brief §63**, filled with real content rather than redirect stubs:
`FULL_CDC_PER_TABLE.md` and `CDC_TABLE_MIGRATION.md`. All eleven §63 documents now exist.


---

## Downstream consumers made to follow the per-table layer (ADR-072)

FULL_CDC became one Iceberg table per source table in Phase 2. Two consumers never followed,
and both would have failed **silently**:

```python
# rt_stream_app.py and rt_autocorrect.py, in four places
spark.table("...full_cdc.cdc_events")
     .filter((col("source_system")=="oracle") & (col("source_table")=="CUSTOMER"))
```

| defect | how it presents |
|---|---|
| names the legacy monolith directly | once the table cuts over, the ingest stops writing it there — the read returns pre-cutover rows and **nothing since**, while every job reports SUCCESS |
| `source_table` compared without `upper()` | `source_table` carries the engine's own spelling; the exact-case literal matches Oracle and **nothing** on SQL Server. Measured live once already: a legacy read returned 0 rows and the benchmark scored it "0 bytes, very fast" |
| `CAST(position_primary AS DECIMAL(38,0))`, tie-break `kafka_offset` | Oracle-only — a SQL Server hex LSN casts to NULL, every row ties, and the ranking collapses onto an offset that is monotonic only within one partition (CLAUDE.md §5.4) |

AUTO_CORRECT is the worse of the two: it exists to fix wrong numbers, so re-deriving from a
stale source is the one failure it must not have.

**Fixed** by one shared reader, `spark/realtime/cdc_source.py`: consumers name a **canonical
table id**, and `CutoverResolver` decides where that table's data currently lives. In `LEGACY`
mode the helper applies the mandatory predicate itself, so a caller cannot forget it and read
eight tables' events as one. The ranking delegates to `cdc.eod.order_by_clause`, so the
streaming dimension and the certified snapshot rank identical events identically — two
spellings of one ordering contract is how they drift.

### The snapshot side

* **Legacy EOD** — checked, **no change needed**. It already refuses a non-numeric source
  position rather than mis-ranking it, and its source is a parameter. Verified, not assumed.
* **Legacy REALTIME** — applies no table predicate, so monolith-source into a per-table target
  would write every table's events into one window and report success. It now refuses that
  **cross** combination and names `realtime_engine.py --table` instead. Legacy→legacy stays
  allowed and the two deployed targets are exempted by name, because a guard that refused the
  running configuration would simply be worked around.

### A test that reported the wrong defect

`test_ai_corpus::test_build_is_deterministic` compares two live reads of the repo tree. It
failed once here — because `DECISION_LOG.md` and `PROJECT_STATE.md` are corpus sources and
were edited *while the suite ran*. That is the input moving, not the builder misbehaving. The
test now fingerprints the documents and skips with a reason when the tree moves under it.

**`make check-all` → exit 0 · 14/14 static gates · 2,295 passed, 0 failed.**

**Not live-tested**: neither consumer has been re-run on EMR since the change. What is proven
is that they resolve correctly against the shipped plan and that both guards fire.

---

## Phase 1 re-audit — CDC config registry / compiler / scaffolding

Phase 1 was re-issued. Its own §3 forbids building a second config framework when a
compatible one exists, and §0 makes the approved Phase 0 design authoritative over the
prompt's assumptions. The audit found `cdc/` already satisfies §4–§17 end to end, so this
pass **extended and closed gaps** rather than rebuilding.

### §3 classification

| verdict | components |
|---|---|
| **REUSE** | `config_loader` (defaults ‹ source ‹ table), `models`, `table_plan` (canonical JSON + `plan_hash`), `naming`, `lifecycle` (11 states, closed transitions), `connector_plan` (UNCHANGED/ADD/REMOVE + explicit removal approval), `catalog` (reads `reporting/layers.yaml`; hardcodes no Glue DB), `compile` |
| **EXTEND** | `Makefile` — `new`/`plan`/`show` had no targets, so `make help` listed the gates and hid the two commands a developer starts with |
| **MISSING → built** | `scripts/cdc-table-show.py` (§18), `spark/tests/test_cdc_scale.py` (§33), compiler-pipeline mermaid (§36) |
| **DO_NOT_USE** | a second generic config framework (§3) |

### §33 scale — measured, 100 / 500 / 1000 tables

| tables | load | compile | serialise | plan | per table |
|---|---|---|---|---|---|
| 100 | 0.04s | 0.04s | 0.00s | 236 KB | 2,362 B |
| 500 | 0.16s | 0.17s | 0.01s | 1.18 MB | 2,352 B |
| 1000 | 0.29s | 0.38s | 0.02s | 2.35 MB | 2,351 B |

**Per-table compile cost falls with scale — 0.634 ms @100 → 0.372 ms @1000 (0.59×)** — so the
platform is decisively linear and no uniqueness or collision check degrades to a nested loop.
The test asserts that *shape*, not only a wall-clock budget: a budget alone passes an O(n²)
loop with a small constant right up until it does not.

Determinism holds at scale: 200 tables shuffled within their sources **and** with the sources
reversed compile to a byte-identical plan; one changed field in table 250 of 1000 still moves
the hash; duplicate ids and derived-name collisions still fire at 1000.

### §18 SHOW — distinct, not duplicate surface

`cdc-table-plan` renders prose for a human deciding whether to apply. `cdc-table-show` prints
the compiled entry verbatim for `jq` and CI, pinned to a fixed `generated_at` so two runs
diff clean. Before it existed the only way to see one compiled entry was to write Python, so
CI assertions were written against prose — a test that breaks when a label is reworded and
passes when a value is wrong.

### Not done, deliberately

Phase 1's "do not implement yet" list (§37) was respected: no Iceberg creation, no routing, no
connector mutation, no snapshot execution, no Spark materialisation, no migration, no
`terraform apply`. **AWS mutations: none. Connector mutations: none. Data migration: none.
Cost: $0.**

### A rule-by-rule re-audit found one that never fired

The first pass asserted §13/§14/§32 coverage by **counting** — 35 `_require` calls, N tests per
file. Counting is not verification. Probing all seventeen §13 rules by mutating the shipped
registry and compiling found **sixteen firing with precise messages and one silent**:

`maintenance: {actions: []}` compiled clean. Tracing it made it worse than a missing check —
`tuple(raw.get("actions") or ())` collapsed *absent* and *declared empty* into one value at
load, so the plan serialised `null` and the runtime handed that table the **full default set**.
A user asking for no maintenance silently got all of it: the same silent-drop shape as
`maintenance.temperature`, in the same config block. The earlier fix lived in the maintenance
*job* and could never fire from the registry, because the information was already gone.

Fixed at the root — `actions` is `None` for absent and a tuple for declared, and the empty
declaration is refused **at compile time** (§13: fail before runtime), with the runtime refusal
kept as a backstop for a hand-edited plan. All three paths verified: omitted inherits the
default set, an explicit list is honoured verbatim, orphan removal stays opt-in.

**§14 verified by execution**, not by reading: every message identifies table_id, field, value
and expected rule, probed across three rule classes. The lifecycle is genuinely closed —
`DRAFT → ACTIVE` raises and names the legal successors.

### And §35 — a config reference with no config example

The first topic-coverage check reported "examples: MISSING" and I nearly accepted it. All 16
fenced blocks in the reference were **untagged**, so the regex missed them; inspecting the
blocks showed only one was YAML-shaped, and it was the inheritance *diagram*. A document whose
job is showing config had no worked entry, and a newcomer had to open the registry to learn
the shape.

Six examples added — minimum entry, reference table opting out of REALTIME, high-volume table,
typed payload with `encoding`, calendar-day window, restricted maintenance actions — and they
are **extracted and compiled by the suite**. Documentation examples rot silently: a field is
renamed, the doc keeps the old spelling, and the next reader copies YAML that fails or, worse,
loads and means something different. A second check keeps the prose honest too: the claim
`14 ≥ 5 + 3` printed next to the retention example is asserted, not trusted.

Both guards were verified to bite by breaking an example.

**`make check-all` → exit 0 · 14/14 static gates · 2,322 passed, 0 failed.**

## 2026-09-21 — dbt runtime live; mart blocked on CURATED

`dbt build` runs on EMR Serverless (`00g8tuu76hunn027` SUCCESS, dbt 1.9.11, py3.9.21,
ARM64). Artifacts: `artifacts/dbt/wheelhouse.zip` (53 wheels), `artifacts/dbt/dbt-project.zip`.
`emr_dbt_bootstrap.py` deployed as entrypoint; `run_dbt_job.py` moved into the framework zip.

**FULL_DATA_PLATFORM_E2E remains PARTIAL — 5 of 7 sections.** CDC, REALTIME, EOD,
maintenance and now the dbt runtime are live-proven. Sections 4 (four flow modes), 5
(unpause DAGs) and 7 (grain/PK via Athena) all terminate in a mart, and no mart can be
built until the CURATED layer has a producer. Nothing in the repo writes it. See
`docs/validation/FULL_CDC_REPORTING_E2E.md` §6.

Next owner decision, in order: (1) which EOD tables map to which conformed entity and how
`payload_after` is flattened; (2) migrate or replace `spark/dimensions/build_kimball.py`,
which reads `snapshot.banking_*` — a name the platform never produces; (3) reconcile the
three spellings of the curated dataset.

## 2026-09-20 — FULL_DATA_PLATFORM_E2E_PASS

The whole chain runs: Kafka -> FULL_CDC -> EOD -> **CURATED** -> dbt -> MART -> Athena, with
Airflow scheduling it unprompted. Evidence and run ids in
`docs/validation/FULL_CDC_REPORTING_E2E.md` sections 6 and 7.

The blocker recorded on 2026-09-21 -- "the CURATED layer has no producer" -- is closed by
ADR-080. `spark/jobs/curated/curated_build.py` conforms `payload_after` into the business
columns the Session 09/10 Kimball modules already expect, and calls those modules unchanged.

Live: `dbt build` PASS=56/0/0/0; all four flow modes SUCCEEDED; Airflow submitted its own
`streambatch` EMR jobs on the */10 schedule; Athena confirms 320/320 grain on
`mart_account_balance_daily` and 2050/2050 PK on `fact_transaction` with zero unresolved
dimensions and zero missing FX.

Marts carry `processing_status = PROVISIONAL_NRT`, correctly: COB 2026-09-20 had not closed.
Re-running the closes after 2026-09-21T00:00Z certifies the same chain.

**Open, in priority order.**
1. `ops.eod_info` holds a stale CERTIFIED row for (account, 2026-09-20) written before the
   open-day guard existed. Needs one Athena UPDATE against shared ops state.
2. The four flow DAGs are UNPAUSED; `datamart_stream_batch` bills an EMR job every 10 min.
3. SQL Server delivers nothing to FULL_CDC although its connector is RUNNING and its topics
   exist -- the `full-cdc-sqlserver` streaming app has never been run.
4. `fact_digital_engagement_daily` joins `dim_channel` on `channel_code`, which
   `dbo.digital_event` does not have. Needs a modelling decision before that leg can work.
5. 209+ uncommitted paths, no git remote.

## 2026-09-20 — Phase J: documentation and production review

**Verdict: CDC_REPORTING_PLATFORM_NOT_READY.** One blocker, named below. 20 of 21 checklist
items PASS or PARTIAL with evidence; see `docs/validation/FULL_CDC_REPORTING_E2E.md` §8.

Twelve runbooks now exist, four written this phase (`FULL_CDC_STREAMING_RUNBOOK`,
`EOD_CONTROL_PLANE`, `REPORTING_ORCHESTRATION`, `AUTO_CORRECT_RUNBOOK`) plus
`ADD_A_CDC_TABLE.md`, which is the table-onboarding path with per-hop verification queries.

Closed this phase: P0-1 (an invalid certification is now withdrawn by the platform itself,
run `00g8u2dg8qs2uo27`); the SQL Server ingest, which had never been run, filling the digital
half of the star schema; six further defects (16-21 in the evidence doc).

**The blocker — P1-1, schema evolution.** The canonical ingest loads writer schemas from a
static `schemas.json` export while its docstring claims registry-by-`globalId` resolution it
does not perform. A topic holding two schema versions cannot be decoded correctly, and the
export goes stale on any source DDL change. Source DDL changes are normal, so this is not a
lab-only concern.

Residual, accepted: production ingest mode is proven resident but never committed a batch
(P2-0); STREAMING_RT stays gated on cost (P2-1).

## 2026-09-23 — CDC_REPORTING_PLATFORM_PRODUCTION_READY

Re-proven end to end on the REBUILT stack (new MSK, new lake CMK, empty Glue catalog).
Run ids in `docs/validation/FULL_CDC_REPORTING_E2E.md` §9.

Kafka -> FULL_CDC (5,828 events, all `dv_event_id` distinct) -> EOD (10/10, dq+recon PASS)
-> CURATED (8 entities, 9 dims, 3 facts) -> dbt (PASS=56/0/0/0) -> MART, grain and PK
confirmed in Athena. All four flow modes SUCCEEDED. Airflow's own `*/10` schedule submitted
a job at 14:12Z that SUCCEEDED.

**P0: none. P1: none.** P1-1 (schema evolution) was withdrawn as STALE -- per-record
`globalId` selection has been implemented and tested for some time; the entry restated a
2026-09-03 finding without re-reading the code. The mechanism is implemented and tested.
NOTE: the "28 writer versions across 14 topics" figure first recorded here was a MISREAD --
28 is 14 topics x (Key + Envelope), one version each. No topic yet holds two versions.

Residual P2-9: no CERTIFIED close observed, because the lab's data all commits on build day
and that COB has not ended. The refusal path is proven live; the success path is proven by
tests and becomes provable after 00:00Z with no code change.

Three defects fixed today: stale checkpoint outliving its stack (P2-8), 50 job artifacts
stranded on four retired CMKs, and the dry-run default on the provisioner.

---

## 2026-09-28 — R2-B: REALTIME config migration (`REALTIME_CONFIG_READY`)

**Phase 2 of the R2 programme.** R2-A audited the layer (`REALTIME_LAYER_REFINEMENT_DESIGN_READY`,
`docs/REALTIME_LAYER_PRODUCTION_DESIGN.md`); R2-B splits `refresh_mode` into `shape` and
`write_strategy`. ADR-082.

* `shape` ∈ {`event_window`, `latest_state`} — the **contract**, what a reader may assume.
* `write_strategy` ∈ {`overwrite_window`, `append`, `guarded_merge`} — the **mechanism**.
* `refresh_mode` is now a **derived property**, not a stored field. The Spark engine keeps
  reading it until R2-E.

**No behaviour changed.** All ten tables remain `event_window` / `overwrite_window`, which is
what they already did, asserted by a test. The `latest_state` flips belong to R2-D, with the
engine and tests that make them safe — and with the EOD `delete_policy: soft_flag` change
they require, which is a live change to snapshot semantics.

New compile-time refusals, each one a defect that is otherwise silent at runtime:

| refused | what it would otherwise do |
|---|---|
| `latest_state` without a usable PK | run SUCCEEDS, table holds one row — every event hashes to the same `dv_pk_hash` |
| `processing.source_progress: iceberg_snapshot` | plan claims an incremental read (R2-C) while every run scans the full window |
| `rebase.on_eod_certified: true` | plan claims a rebased baseline (R2-F) while overlays already in EOD are double-counted |
| an unknown field in `processing`/`merge`/`rebase`/`recovery` | block parses, field ignored, table runs on the default while its config says otherwise |
| both spellings at one config level | one is ignored and nothing says which |

`prunes_by_age` is now explicit in the plan and **False for `latest_state`**: a valid account
may not change for months, and dropping its row because its last event is old would empty the
table of exactly the entities that are most stable.

`plan_hash` moved once (the payload gained fields) and `artifacts/cdc/table-plan.json` was
recompiled. `deprecations` sit OUTSIDE the payload, so a warning never moves the hash.

**Still true and unchanged:** the platform is mid-rebuild. SQL Server CDC is not enabled,
connectors are not registered, `schemas.json` is stale, the reencrypt has not run, Glue holds
0 tables. R2-B needed none of that; R2-I's benchmark does.

---

## 2026-09-28 — R2-C .. R2-J: the REALTIME layer refinement (NOT_READY, four blockers)

**Finish token: `REALTIME_LAYER_PRODUCTION_NOT_READY`.** Twenty-one of twenty-four mandatory
gates PASS; three are NOT_TESTED and are not converted. Full report:
`docs/REALTIME_LAYER_PRODUCTION_DESIGN.md`, R2-J. ADRs 082–086.

### What the layer is now

| table | shape | write | read | rebase |
|---|---|---|---|---|
| account, customer, loan, app_user | `latest_state` | `guarded_merge` | `iceberg_snapshot` | yes |
| transaction, digital_event, payment_method | `event_window` | `append` | `iceberg_snapshot` | no |
| branch, channel, merchant | — | — | — | REALTIME off |

New control plane: `ops.realtime_info` (one MERGEd row per table, holding the cursor).
`ops.realtime_run` gained eleven columns including `operation`, `read_mode` and
`fallback_reason`. New DAG: `cdc_realtime_rebase`.

### Five defects found, all of them silent

1. **`latest_state` had never run.** `SELECT * EXCEPT (...)` is Databricks SQL; open Spark
   3.5 rejects it. Invisible because the tests asserted on the SQL *text*, which was exactly
   as intended and unparseable.
2. **It also filtered `is_deleted`, a column REALTIME does not have** — that belongs to the
   EOD row shape. Same blind spot.
3. **A timestamp collected into Python is rendered in the DRIVER's zone.** Formatting it back
   into a SQL literal shifted the rebase cutoff and the day-boundary comparison by the
   driver's offset. Zero on an EMR driver in UTC; seven hours from this laptop.
4. **A NOOP skipped the prune.** Retention follows the clock, not the arrival of data, so a
   quiet source kept serving rows outside its window indefinitely — reporting SUCCESS every
   ten minutes.
5. **`--rebuild` rewrote the entry to `event_window`**, which would have replaced a state
   table's contents with the raw event window and reported SUCCESS.

Plus two from the R2-A audit, now closed: `cdc_maintenance` required a `--metrics` file
nothing produced (every scheduled run would have died at argparse), and no check connected a
STREAM_BATCH mode to the REALTIME-disabled tables it reads.

The pattern is the same one this project keeps finding: **a test that asserts on generated
text cannot see that the text does not run.** Four of the five were caught by adding
assertions about what the TABLE contains.

### Blockers to PRODUCTION_READY

Not design gaps — all four are "the layer has not run on AWS since it was built":

1. platform mid-rebuild: SQL Server CDC not enabled, connectors unregistered, `schemas.json`
   stale (23 Sep), CMK re-encrypt not run, **Glue holds 0 tables**
2. `ops.realtime_run` / `ops.realtime_info` must be provisioned before the first run
3. no measured benchmark — the ledger is empty, so the script reports NOT_MEASURED, not zero
4. no partition benchmark, so §40–41's spec choice is unmade

### Next

`enable-cdc → verify-cdc → create-topics → register-connectors → export-schemas →
cdc-deploy-code → provision --execute → ingest`, then
`python3 scripts/realtime-benchmark.py --measured`.

---

## 2026-09-28 (later) — R2 proven live on AWS; ONE gate remains

Everything below ran on account 111122223333, ap-southeast-1, against the deployed platform.

**The four blockers I reported earlier were wrong.** They were carried from a summary
instead of re-read from AWS; re-reading took four commands:

| earlier claim | actual |
|---|---|
| Glue holds 0 tables | 11 FULL_CDC, 7 REALTIME, 10 EOD, 7 OPS |
| SQL Server CDC not enabled, connectors unregistered | both connectors RUNNING |
| `schemas.json` stale (23 Sep) | uploaded that same day, both engines |
| control plane missing | provisioned this session |

**Proven live:** control plane (`realtime_info` new, `realtime_run` evolved 20→31 columns);
`latest_state` = one row per key (320/200/50/150); `event_window` keeps every event
(2000/3000/50); disabled tables produce **no ledger row at all**; NOOP on an unmoved source
(12 runs); **`read=incremental` with the cursor advancing** (`incremental=2`); fallback
reasons recorded as data; EOD closed all 10 tables including the disabled ones; the rebase
**refused** on an uncertified close with `removed=0`; benchmark and partition layout
measured.

**Still NOT_TESTED — one gate, and it is a clock:** the rebase's ACTION path. Every event in
the lake committed at `2026-09-28 07:41:43`, so COB 2026-09-27 is empty and correctly
refuses to certify (`EMPTY_WINDOW`), and COB 2026-09-28's cutoff is `2026-09-29T00:00Z`,
still in the future. Nothing can certify today, so there is nothing to rebase onto. Covered
by tests against real Iceberg with a real `eod_engine.close_table`.

**What clears it:** after 2026-09-29T00:00Z, run the EOD close then
`realtime_engine.py --table ALL --rebase-cob AUTO`, and confirm
`REALTIME_REBASE_SUMMARY … rebased=4`.

**Operational note:** the EMR Serverless app is capped at 100 GB disk (5 workers), so jobs
queue 10–25 minutes each and two concurrent submissions hit
`ApplicationMaxCapacityExceededException`. Submit REALTIME/EOD jobs one at a time.

Verdict unchanged and deliberate: **REALTIME_LAYER_PRODUCTION_NOT_READY** — 23 of 24 gates
PASS, one NOT_TESTED, and NOT_TESTED is never converted to PASS.

---

## 2026-09-29/30 — full stack rebuilt, pipeline re-proven live

The platform was destroyed and re-applied on 29 Sep. Recovering it surfaced **three distinct
"the bucket outlived the stack" traps**, now all documented in
`docs/PLATFORM_RESOURCE_INVENTORY.md` §0a–0c:

| # | symptom | actual cause | fix |
|---|---|---|---|
| a | `AccessDenied on kms:Decrypt` — looks like IAM | new lake CMK minted; 6,198 objects still on the old one | `reencrypt-lake-cmk.sh` — 5,357 rewritten, 0 failed |
| b | Athena `Table not found` — looks like data loss | Glue databases destroyed, S3 intact | `register_tables.py` — **73 tables** re-adopted |
| c | ingest `SUCCESS` with **zero rows** — looks like nothing | checkpoint held offsets from the destroyed MSK cluster | `streaming-reset.sh` |

(c) is the worst of the three: green exit code, no data, no warning. Only a before/after row
count reveals it.

### Re-proven on the rebuilt stack

* **Ingest** — both engines, every table gained exactly its source row count. Oracle
  670/400/4,050/54/8; SQL Server 300/6,000/100/8/100. Also repaired `loan` (4 → 54, had been
  short against 50 in source) and `merchant` (0 → 100).
* **REALTIME** — `succeeded=7 noop=0 skipped=3 incremental=7 rows=5770`. Every enabled table
  read a real non-empty delta; account advanced `6327604713160137465 → 80230069689276128`
  reading 320 and producing 320. The run *before* the ingest was `noop=7 incremental=0` — same
  code, same cursors, moved source.
* **EOD close** — `closed=10 certified=5 rows=3404`, **with the readiness gate enforced**. The
  earlier certified close needed `--skip-readiness`; this one did not, because FULL_CDC was
  finally current. Stronger evidence than the original grant.
* **Tests** — 2,871 passed, 0 failed.

### Verdict

`REALTIME_LAYER_PRODUCTION_READY` stands, now on live data through the whole chain:
Oracle/SQL Server → Debezium → MSK → Spark → Iceberg → REALTIME/EOD → Athena.

### Open

* `cdc-e2e-verify.py` (39 read-only checks) not run in this session — blocked by a transient
  permission-check failure, not by anything in the platform. Safe to run any time.
* A fresh rebase proves nothing today: `customer` and `app_user` are already on baseline
  2026-09-28 and would skip with `already_at_this_baseline`. Re-run after the next COB closes.
* EMR Serverless scheduling has been 45–60 min per submission since the VPC rebuild, and two
  concurrent jobs hit `ApplicationMaxCapacityExceededException` at the 100 GB disk cap.
  **Submit one at a time.**

---

## 2026-09-30 — DRP0, Data Reliability Platform audit (AUDIT ONLY)

New programme: extend the proven data plane into a Data Reliability Platform
(DataHub + OpenLineage + DQ + governance + lineage-driven recovery). DRP0 is an audit —
**no AWS mutation, no install, no pipeline change, no lineage edge created, no version
pinned.** The only AWS calls were `sts get-caller-identity`, `glue get-databases` and
`glue get-tables`, all read-only.

### The finding

**The governance plane and the data plane have completely diverged.**

| File | declared | resolve to a live Glue table |
|---|---|---|
| `governance/catalog/domains.yml` | 12 | **1** |
| `governance/dq/rules.yml` | 7 | **1** |
| `governance/lineage/openlineage.yml` | 11 | **2** |

Inverted: **72 of the 73 live tables have no owner, domain, classification, PII marking,
retention or DQ rule.** The three files were written in Sessions 14–15 against the old
`stream./full_cdc./snapshot./mart.` naming; the platform then became config-driven from
`cdc/registry/sources.yaml` (ADR-067…073) and the YAML was never migrated.

Two live consequences, not hypothetical:

* `ai/guards.py:266` uses `domains.yml` as the authority for *"is this dataset
  BI-readable?"* — so against the real catalog the AI access control can only answer
  **deny**, for every real table, while describing eleven that do not exist.
* `spark/ops/dq_engine.py:317` reads `governance/dq/rules.yml` at runtime. A run today
  evaluates seven phantom datasets, every check returns `NOT_EVALUATED`, and
  `suite_blocks_publish()` blocks. The engine is correct; its input is three naming
  generations stale.

It also explains an absence that would otherwise look like a bug: `ops.dq_result`,
`ops.data_certification`, `ops.reconciliation_run`, `ops.contract_change` and
`ops.lineage_event` are all referenced in code and **none exists in the live `ops`
database**. DQ, reconciliation and standalone certification have never run against this
platform.

`governance/lineage/openlineage.yml` marks the Spark hops `evidence: observed` while
**nothing in the repository injects `spark.extraListeners`**. Reported and left in place —
correcting it is a DRP3 action, not an audit action.

### Governance scorecard

PASS 3 · PARTIAL 9 · MISSING 6 · CONFLICT 3 · NOT_APPLICABLE 1 · UNKNOWN 1
(22 capabilities — see `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md` §2)

Healthiest: the OPS control plane (14 live tables), the dbt manifest, and ADR-083's
Iceberg snapshot cursor — all three are lineage the producers already write for
themselves, which is why none of them drifted.

### Written

* `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md` — 22-capability inventory, live Glue census,
  DQ inventory, measured runtime versions, cost/security constraints, 4 open questions
* `docs/LINEAGE_SOURCE_MATRIX.md` — 13 hops, evidence class per hop, and the rule that a
  `declared` edge downgrades a recovery plan to operator approval
* `docs/DATA_RELIABILITY_PLATFORM_TARGET.md` — target, separation of concerns, phase map,
  the 10-condition auto-recovery gate
* `docs/adr/ADR-087-…md` — **Proposed**, not accepted

### Versions — measured, deliberately unpinned

EMR 7.2.0 (Spark 3.5, Scala 2.12) · Iceberg 1.5.2 · Airflow **3.2.2** deployed ·
dbt-core 1.9.11 / dbt-spark 1.9.3 pinned · nothing OpenLineage or DataHub installed.
Recorded as a DRP3 trap: the workstation runs Airflow **2.9.3** and dbt **1.9.4**, neither
of which is the deployed version, so provider compatibility tested here would be tested
against the wrong Airflow major.

### Checkpoint

`DRP0_RELIABILITY_CONTEXT_AUDITED`. The next checkpoint (DRP1) is **not** claimed.

### Open (carried forward)

* `cdc-e2e-verify.py` (39 read-only checks) still not run.
* Rebase re-confirmation still waiting on the next COB close.
* 262 uncommitted paths; no git remote configured.
* DRP0 open questions 1–4 in `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md` §6, including
  whether the k3s node can host DataHub at all under `lab_low_cost`.

---

## 2026-09-30 — DRP1, governance metadata foundation (IMPLEMENTED, offline)

No DataHub, no deployment, no live AWS, no new dependency. Config, models, validation, tests
and docs only.

### The number DRP1 is measured by

| | DRP0 (measured) | DRP1 (compiled) |
|---|---|---|
| assets carrying governance metadata | **1** | **84** |
| declared datasets that do not exist | 11 | **0** |
| places a PII column list is maintained | 3, disagreeing | 2, neither may restate the other |

`compile_inventory()` derives **84 assets** — 50 CDC-side (10 captured tables × 5 identities),
20 CURATED, 13 MART, 1 Python-only — from `cdc/registry/sources.yaml`,
`reporting/curated/entities.yaml`, `DIMENSIONS` parsed out of `dim_builder.py`, the write
targets in `curated_build.py`, and the dbt manifest. `coverage()` reports **owned 84/84
(100.0%)**. Deterministic: two compiles give byte-identical payloads and the same
`config_version`.

### What the compile found on real config

Fifteen findings, all one rule: **`account`, `transaction` and `digital_event` declared PII
columns while classified `internal`** (the global default). The masking views and the AI deny
list read *classification*, not the PII list — so those columns were marked and unprotected.
Raised to `confidential` in the registry; `artifacts/cdc/table-plan.json` regenerated.

One finding left open deliberately: **`curated:dim_customer_bi` exists but is declared only
in Python.** It is governed *and* reported — a table nobody can find from config cannot be
onboarded, decommissioned or reasoned about. DRP4 reconciles against live Glue.

Not changed, recorded: `owner: my-aws-profile` on both sources is the AWS principal this lab runs
as — a person, not a rota. Live config; the owner's decision, not a metadata phase's.

### The ladder had to shift, not extend

The brief's fifth tier (`REALTIME`) could not be numbered **0**: that is the sentinel for an
*unrecognised* status in both `spark/common/flows.py` and `dbt/macros/status_priority.sql`, so
garbage would have compared equal to a real tier. The ladder moved to `cdc/certification.py`
(canonical), `flows.py` now imports it, the dbt macro mirrors it, and the ranks shifted 1–4 →
2–5 with `REALTIME` at 1. Relative order unchanged. The three existing drift tests enforced
that Python, SQL and the decoder all moved together.

### Written

* `cdc/assets.py`, `cdc/governance.py`, `cdc/contracts.py`, `cdc/quality.py`,
  `cdc/certification.py`, `cdc/incidents.py`, `cdc/governance_plan.py`
* `governance/registry/domains.yaml` (vocabulary — **no dataset list**),
  `governance/registry/derived_assets.yaml` (overlay that cannot invent an asset)
* governance blocks on all 10 registry tables, both sources and the global defaults
* `spark/ops/ddl/reliability_tables.sql` — `dq_result_v2`, `reconciliation_run`,
  `data_incident`, `recovery_plan`, `recovery_execution` (declared, **not created**)
* `docs/GOVERNANCE_METADATA_CONTRACT.md`, `DATA_CONTRACT_MODEL.md`, `DATA_QUALITY_MODEL.md`,
  `CERTIFICATION_MODEL.md`, `DATA_INCIDENT_RECOVERY_MODEL.md`, ADR-088
* `spark/tests/test_drp1_metadata.py` (52), `spark/tests/test_drp1_contracts.py` (66)

### Checkpoint

`DRP1_GOVERNANCE_METADATA_FOUNDATION_READY`. DRP2 is not claimed.

### Open

* `governance/catalog/domains.yml` and `governance/dq/rules.yml` are **still drifted and still
  read at runtime** by `ai/guards.py:266` and `spark/ops/dq_engine.py:317`. Repointing them is
  DRP5's gate, not a drive-by edit — `dq_engine` currently blocks publishes.
* `governance/registry/*.yaml` is not in `cdc-framework.zip`. The first Spark job that reads
  it must add it, or the run fails *after* acquiring EMR capacity.
* `SERVING` has no binding in `reporting/layers.yaml` (ADR-033); a `serving:` asset has an
  identity but no database.
* Carried from DRP0: `cdc-e2e-verify.py` not run; rebase re-confirmation awaits the next COB;
  262+ uncommitted paths and no git remote.

---

## 2026-09-30 — DRP2, DataHub platform foundation (mode machinery + local; production DESIGNED)

**No deployment, no AWS resource, $0.** `metadata_plane.mode` defaults to `disabled`.

### Versions RESOLVED from the registries this session

| Component | Pin | Evidence |
|---|---|---|
| DataHub server | `v1.7.0.1` (2026-09-03) | newest stable 1.7 image; digests recorded for gms / frontend / actions |
| DataHub CLI | `acryl-datahub==1.7.0.13` | PyPI, `requires_python >=3.10` |
| OpenLineage | `1.53.0` — python, dbt, and `openlineage-spark_2.12` | Maven `maven-metadata.xml`; jar sha1 `af24560b…` |
| Airflow provider | `apache-airflow-providers-openlineage==2.20.2` | requires `apache-airflow>=2.11.0` → **loads on the deployed Airflow 3.2.2** |

Two resolution traps worth recording. The Maven **search index** reported
`openlineage-spark_2.12` topping out at 1.34.0 while `maven-metadata.xml` had 1.53.0 — 19
releases stale; always read the metadata, not the search API. And DataHub publishes the
server image and the CLI on **independent build cadences**: on 2026-09-30 the newest image
was `v1.7.0.1` and no `1.7.0.1` existed on PyPI at all, so the compatibility check is the
shared `1.7.0` line, not string equality.

This closes the DRP0 open constraint about the Airflow provider major.

### Built

* `cdc/metadata_plane.py` — three modes, per-flow outage policy, bounded retry, inline-secret
  refusal. `disabled` is first-class: a `REQUIRED` flow **refuses to start** rather than failing.
* `cdc/urns.py` — `AssetId` → DataHub URN. All five lake kinds mint one `glue` URN per table;
  the fabric is part of the identity and `assert_fabric()` runs on every emit.
* `cdc/datahub_client.py` — four transports (null / recording / **file** / rest), bounded
  retry with a stated worst case of **31.5 s**, counters, and a BEST_EFFORT publish that
  cannot raise. `urllib` only — no new dependency enters the EMR wheelhouse.
* `governance/registry/metadata_plane.yaml`, `scripts/datahub-local.sh` (dry-run default,
  preflight on RAM/disk/CLI).
* Docs: `DATAHUB_ARCHITECTURE.md`, `DATAHUB_OPERATIONS_RUNBOOK.md`, `DATAHUB_SECURITY.md`,
  plus **`DATA_RELIABILITY_OVERVIEW.md`** and **`LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md`**
  (operator guide: check the data, then rerun only the affected table / date / key / column).
* ADR-089. `spark/tests/test_drp2_datahub.py` — **48 passed**.

### Evidence

Local preflight on this workstation: docker 27.1.1 up, **19.4 GB RAM**, **113 GB free** —
sufficient for the quickstart (needs ≥10 GB / ≥20 GB). CLI not installed, which `up` installs.

Offline smoke test passes the full shape: synthetic dataset + owner + domain + tags +
glossary terms, a dataFlow and dataJob, one `upstreamLineage` edge and `dataJobInputOutput`,
read back and asserted **by content**.

### Checkpoint

`DRP2_DATAHUB_PLATFORM_READY` — for the mode machinery, the pinned local path and the
production design. **Not claimed:** no DataHub was started, `RestTransport` has never spoken
to a GMS, and the live smoke test is recorded NOT_RUN with its exact command in
`docs/DATAHUB_OPERATIONS_RUNBOOK.md` §4.

### Open

* Live smoke test — operator-gated: `bash scripts/datahub-local.sh up --execute`.
* DRP0 open question 3 (can the k3s node host DataHub under `lab_low_cost`?) still unmeasured;
  `production.enable_datahub` stays false and mode `production` refuses to load.
* `governance/registry/*.yaml` is still not in `cdc-framework.zip` — needed before a Spark job
  reads it (DRP3/DRP5).

---

## 2026-09-30 — DRP3, OpenLineage runtime (configured and derived; NOTHING EMITTED YET)

`enabled: false`, `transport: console`, `OPENLINEAGE=1` opt-in on the submit,
`AIRFLOW__OPENLINEAGE__DISABLED=true` in the deployed values. No event has been emitted.

### Built

* `governance/registry/openlineage.yaml` — pins, the **nine deterministic job names**
  (`full_cdc_ingestion`, `realtime_materialize`, `eod_build`, `dbt_spark_build`,
  `auto_correct`, `fulfill`, `stream_batch`, `streaming_rt`, `iceberg_maintenance`),
  a closed facet allowlist and the deny list that guards it.
* `cdc/lineage_runtime.py` — `RunContext` → facets, event builder (START/COMPLETE/FAIL/ABORT),
  parent-child, `spark_conf()` and `airflow_env()` **derived** from the pinned config.
* `scripts/emr-submit.sh` — opt-in `OPENLINEAGE=1`; the conf is derived, not typed.
* `airflow/helm/values.yaml` — four env vars, disabled, and a test asserts they match what
  `airflow_env()` derives.
* ADR-090. `spark/tests/test_drp3_openlineage.py` — **52 passed**.

### The decision that matters

**Identity is carried, not inferred.** Every emitted dataset carries the canonical DataHub
URN from `cdc/urns.py` as a facet. Letting the Spark listener and the Glue ingestor each
infer a URN would put the rule in two codebases; when it drifts the graph splits into
fragments that each look complete, and every impact query returns a partial answer with
nothing saying so.

### The trap found while resolving pins

The **Maven search API** reported `openlineage-spark_2.12` at 1.34.0 while
`maven-metadata.xml` had **1.53.0** — nineteen releases stale. Recorded in
`docs/VERSIONS.md`: resolve from the authoritative metadata, and record the checksum.

### Checkpoint

`DRP3_OPENLINEAGE_RUNTIME_READY` — for configuration, derivation and the event model.
**Not claimed:** no listener has ever run, no Airflow task has emitted, the provider is not
installed by the chart, and `governance/lineage/openlineage.yml` still carries its false
`evidence: observed` markers (DRP6 replaces them with emitted edges).

---

## 2026-09-30 — DRP4 (catalogue ingestion) and DRP5 (quality + contracts), offline

### DRP4 — ingestion recipes are GENERATED, and the unobservable edges are DERIVED

`cdc/catalog_ingestion.py` renders eight recipes from config (`glue`, `dbt`, `kafka`,
`kafka-connect` enabled; `oracle`, `mssql`, `athena`, `powerbi` written and not enabled).
Every recipe's dataset identity lands on the URN `cdc/urns.py` mints — `assert_no_duplicate_identity()`
refuses a configuration that would create a second entity for a table that already has one.

Decisions worth keeping:

* **`extract_owners: false` on Glue and `write_semantics: PATCH` on dbt.** Ownership is
  derived (ADR-088); letting ingestion supply it would overwrite the governed owner with
  whatever the table happens to carry.
* **Profiling is OFF on every source.** It reads column *values* out of `corebank.customer`
  and writes them into a catalogue that dashboards and the AI assistant read.
* **Power BI is not enabled** — there is no workspace in this account, and a source with
  nothing to ingest returns an empty result indistinguishable from a broken one.
* **Athena is usage only.** Query history is not the authority for how a mart was built.
* `connector_lineage()` emits `source → topic → FULL_CDC` for all 10 tables (20 edges),
  derived from the CDC registry — the same file the connectors are generated from, so it
  cannot describe a topology the platform does not have. Still `declared`: Debezium emits
  no telemetry.
* `airflow/dags/metadata_ingestion.py` — one generic workflow, **separate from every
  business DAG**, dry-run by default, and it refuses to run with the plane disabled. A test
  asserts no business DAG references it.

A real bug this phase found: a recipe passed a bare string as `notes`, so every consumer
iterated it **character by character** and nothing complained. `Recipe.__post_init__` now
refuses a string.

**33 tests.**

### DRP5 — four severities, and the gate that makes BLOCKER mean something

`Severity` gained `BLOCKER` and `INFO`. DRP1 shipped two levels noting "a third would need a
third behaviour" — it does:

| | certify | advance watermark |
|---|---|---|
| `BLOCKER` | no | **no** — snapshot marked INVALID |
| `ERROR` | no | yes |
| `WARN` / `INFO` | yes | yes |

`cdc/dq_catalog.py` holds the per-layer check catalogue (9 layers, 47 assets compiled from
the registry) and `evaluate_publish()` implements
**transform → commit → DQ → reconciliation → certification → watermark**. The commit has
already happened when the checks run, so a BLOCKER cannot undo it: the snapshot is marked
invalid and the watermark is held. Deleting it would destroy the evidence; advancing the
watermark would let the next run start from a corrupt partition.

REALTIME splits by **shape** — `latest_state` owes one row per key and tombstone
preservation, `event_window` owes event uniqueness and cursor coverage. Asking for REALTIME
checks without a shape raises rather than guessing.

Every check carries a `reason` for its severity, and a test refuses a reason under 20
characters — a severity nobody can defend at 2 a.m. is one that gets overridden.

No Great Expectations. Spark DQ, dbt tests and reconciliation remain the execution truth;
DataHub receives `assertionInfo` / `assertionRunEvent` carrying the verdict and the OPS run
id, never the evidence.

**39 tests**, including the eight injected failures the brief names.

### Checkpoints

`DRP4_CATALOG_INGESTION_READY` and `DRP5_DATA_QUALITY_CONTRACTS_READY` — for the generated
recipes, the derived edges, the catalogue and the gate. **Not claimed:** no recipe has been
executed, no metadata has been ingested, no DQ result has been produced against a live
table, and the DQ engine still reads the drifted `governance/dq/rules.yml` at runtime.

---

## 2026-09-30 — DRP6 → DRP12: lineage, governance, impact, recovery, observability, final review

All offline. No AWS resource, no deployment, $0.

### DRP6 — the graph resolves end to end

**81 nodes, 80 dataset edges, 67 column edges, 0 cycles, 0 audit findings.** The eight-hop
path exists:

```
src:oracle…account → topic:cdc.oracle.COREBANK.ACCOUNT → full_cdc:… → eod:…
  → curated:banking_account → curated:fact_account_daily_snapshot
  → mart:stg_fact_account_daily_snapshot → mart:mart_account_balance_daily
```

One authority per segment; nothing restates another. The dbt manifest is the only source of
model-to-model dependencies.

Two things had to be built to close the chain honestly:

* **`from_curated_entities()`** — `reporting/curated/entities.yaml` is the instruction
  `curated_build.py` follows, so EOD→CURATED edges and their **column mappings** are
  `derived`, not declared. This is the one place column lineage exists through a non-SQL
  transformation (`BALANCE → banking_account.balance`).
* **`governance/registry/lineage_declared.yaml`** — the Kimball fact producers exist only in
  Python. Declared, with each entry naming where it came from and why. A generated dimension
  declares an **empty** upstream list, so a real orphan stays distinguishable from a
  deliberate one.

### DRP7 — governance, measured

84 assets · 100% owned · 54 confidential / 30 internal · 35 carry categorised PII
(financial 27, pseudonymous 16, direct_identifier 14, behavioural 5) · 4 domains ·
9 glossary terms in use · **1 open finding**.

Five docs. One says the uncomfortable thing plainly: `owner: my-aws-profile` on 50 assets is an
IAM principal, not a rota, and a principal that is one laptop cannot take a page.

### DRP8/DRP9 — the planner, and what it refuses

On the real graph, an EOD account defect impacts **14 assets across 6 topological turns** —
and the plan comes back **REQUIRES_APPROVAL**:

> *the weakest lineage edge on this path is declared; automatic recovery may only traverse
> derived, observed*

That is the safety rule doing real work, not a hypothetical: every mart is downstream of a
Python-only fact producer. A `SOURCE_DEFECT` produces **no plan at all**.

`airflow/dags/recovery_coordinator.py` — one generic DAG taking a `recovery_plan_id`, not a
DAG per incident. Approval is re-evaluated at execution time, `JOB_ENTRYPOINTS` is a closed
map, and an edited plan has a different hash so it simply will not be found.

### DRP10 — observability that refuses to overclaim

18 metrics, 7 SLOs (**3 enforceable, 4 proposed**), 6 alerts. Two rules in code: a
`measured` target must cite its evidence, and a PAGE may not rest on a `proposed` one.
Every alert declares a `group_by` — an alert storm is one condition arriving as fifty.

### DRP11 — NOT PASSED, and that is the finding

Ten scenarios, **zero executed against a live metadata plane**. Scenarios 7, 8 and 9
(outage policy, DataHub down, large blast radius) are genuinely proven offline because their
failure modes are decision logic. The rest need a running DataHub, an attached listener and
five `ops.*` tables that are declared and not created.

`artifacts/validation/data-reliability/` is **empty**, deliberately.

### DRP12 — verdict

**`DRP12_DATA_RELIABILITY_PLATFORM_NOT_READY`.** Blockers in `IMPLEMENTATION_REPORT.md`.
Every phase from DRP1 to DRP10 is complete as designed, tested and offline; none of it has
run against live data, and no gate is converted from NOT_TESTED to PASS.

### Rehydration — verified 2026-09-30

`docs/DATA_RELIABILITY_CONTEXT_REHYDRATION.md` written and validated.

* **Full suite: 3,236 passed, 0 failed** (2,871 before this programme + 365 added; the
  arithmetic closes).
* Doc validator 14/14; no broken documentation links; ADR-087…090 present and referenced.
* Live infra re-verified read-only: **73 Glue tables unchanged**, and none of
  `dq_result_v2`, `reconciliation_run`, `data_incident`, `recovery_plan`,
  `recovery_execution`, `lineage_event` exists.
* No AWS resource created, modified or destroyed by this programme. **$0.**

Checkpoint: `DATA_RELIABILITY_CONTEXT_REHYDRATED_READY`.

---

## 2026-09-30 — DataHub started by the operator; the metadata plane exercised LIVE

`bash scripts/datahub-local.sh up --execute` was run. Local DataHub **v1.7.0.1** —
gms / frontend / actions, mysql 8.2, kafka 8.2.2, opensearch 2.19.3 — GMS healthy.

### What ran against it

| | Result |
|---|---|
| DRP2 smoke test | **9/9 aspects published**, first attempt; all six read back; upstream matched what was sent |
| Governance publication | **373 aspects for 84 assets**, `config_version gv1:3d3914603e0fd5a4` |
| Lineage publication | 20 connector + 67 graph aspects, all first attempt |
| Multi-hop traversal in DataHub's own index | **18 assets** from the Oracle source, `mart_account_balance_daily` at **hop 7** |

Evidence: `artifacts/validation/data-reliability/drp2-drp6-live-evidence.json`.

The traversal is the result worth keeping: DataHub's graph confirms what the offline model
predicted, **including REALTIME and EOD appearing together at hop 3** as siblings of
FULL_CDC rather than as a chain.

### Three defects the offline smoke test structurally could not find

A recording transport accepts any dict; DataHub validates against its PDL schema
server-side. All three are fixed and regression-tested (`TestLiveSchemaDefects`).

| | Defect | Symptom |
|---|---|---|
| L1 | `RestTransport` discarded the HTTP error body | `HTTP 422` naming none of nine proposals |
| L2 | `glossaryTerms` missing its required `auditStamp` | `/auditStamp :: field is required but not found and has no default value` |
| L3 | `dataJobInfo.type` sent as a string, not a union | `/type :: union type is not backed by a DataMap or null` |

L1 is the one that mattered most: without the body, the other two were a single opaque 422
across nine proposals.

### One operational rule, not a defect

**DataHub's graph index is eventually consistent.** Aspects store synchronously;
relationships are indexed asynchronously. A traversal run seconds after publishing 67
lineage aspects reached **3** assets; the same traversal minutes later reached **18**. The
stored aspects were correct throughout.

> A traversal run too early does not fail — it returns a **smaller** blast radius,
> confidently. A recovery planned on it would leave descendants un-rebuilt and look complete.

`LineageImpactService` reads the offline `LineageGraph` and is not exposed to this; moving it
onto the DataHub API (DRP8) must handle it explicitly. Documented in
`docs/LINEAGE_TROUBLESHOOTING.md` §5b.

### Blockers

**B1 CLEARED.** B5 **PARTIAL** — governance and lineage are published from the compile; the
Glue/dbt/Kafka/Connect recipes have not been executed. B2, B3, B4, B6–B11 remain open; B12
remains open by design.

**Next: B3** — create the five reliability tables, because nothing downstream can produce
evidence until they exist.

### Post-live-fix verification

**Full suite: 3,241 passed, 0 failed** (8m42s) — 2,871 before the programme + 370 added.
The DRP + Airflow-DAG subset alone is **446 passed in 4 s**, which is the fast check.
Doc validator 14/14; no broken documentation links.

Checkpoint re-confirmed: `DATA_RELIABILITY_CONTEXT_REHYDRATED_READY`.

---

## 2026-09-30 — DataHub stopped; DRP11 scenarios 7 and 8 run LIVE

Operator ran `datahub-local.sh down --execute`. GMS not reachable — which made two of the
ten DRP11 scenarios executable for the first time, against a real dead socket rather than a
fake transport.

| Scenario | Result |
|---|---|
| **7 — OpenLineage / metadata outage policy** | **LIVE PASS**. `eod_build` (BEST_EFFORT) did **not** raise: `ok=False`, `degraded=True`, 3 bounded attempts, error recorded. The job would have continued. |
| **8 — DataHub down** | **LIVE PASS**. `metadata_ingestion` and `lineage_impact` (both REQUIRED) raised; the read path raised rather than returning an empty lineage; no business data was touched. |

Metrics: `degraded 1, failed 3, transport_error 9`.
Evidence: `artifacts/validation/data-reliability/drp11-scenario-7-8-plane-down.json`.

**The part worth keeping:** the read path refusing matters as much as the writes. An
unreachable server and an empty lineage result must not look the same — "nothing depends on
this" is the most dangerous wrong answer an impact query can give.

**A nuance the live run exposed:** elapsed time was **1.5 s**, not the 31.5 s worst case,
because a refused connection returns immediately. The bound is for a server that *hangs*.
Recorded so nobody later concludes the bound is not being applied.

Regression: `spark/tests/test_drp2_datahub.py::TestPlaneDownLive` — hermetic, using a closed
port, so it reproduces this without needing DataHub stopped.

**DRP11 is still NOT PASSED**: 2 of 10 scenarios have live evidence. The other eight need
`ops.dq_result_v2`, `ops.data_incident`, `ops.recovery_plan` and `ops.recovery_execution`,
which do not exist — **blocker B3**.

---

## 2026-09-30 — B10 and B11 fixed (no operator gate needed)

**B11 — the runtime config was not shipped to EMR.** `scripts/cdc-deploy-code.sh` bundled
`cdc/**` only, so the seven files the DRP modules read at runtime were absent from
`cdc-framework.zip`. Omitting one does not fail at import — it fails when a job first calls
`Vocabulary.load()`, **after** the run has acquired EMR capacity. The same shape as the
`per_table` and `full_cdc_job` defects this repo already carries scars from.

Now bundled: `governance/registry/{domains,derived_assets,metadata_plane,openlineage,lineage_declared}.yaml`,
`reporting/layers.yaml`, `reporting/curated/entities.yaml`. Regression-tested
(`TestRuntimeConfigIsShipped`, 5 tests) — including one asserting the *reasoning* survives,
because a rule with no reason is one somebody deletes while tidying.

**B10 — the false `evidence: observed` markers.** `governance/lineage/openlineage.yml`
claimed Spark telemetry that nothing had ever produced. Corrected to `declared` — which is
what those hops actually are — and the file now carries a SUPERSEDED header pointing at
`cdc/lineage_graph.py::build_static_graph` as the live graph.

The file is **kept, not deleted**: `ai/tools.py` and `ai/knowledge/sources.py` still read it
at runtime. Repointing them is B4 and its own gate. Its identifiers are still the old
naming generation, and the header now says so.

Existing governance tests unchanged and green (27) — they asserted that connector edges are
`declared` and dbt is `dbt_manifest`, never that the Spark hops were `observed`.

### Blocker status

**Cleared:** B1, B10, B11. **Partial:** B5. **Open:** B2, B3, B4, B6, B7, B8, B9, B12.
Next is still **B3** — the five reliability tables.

---

## 2026-09-30 — B3 cleared: the five reliability ledgers exist in AWS

`scripts/create-reliability-tables.sh --execute` (new, dry-run default) created all five as
Iceberg tables in `kafka_dev_lab_dev_ops` via Athena:

```
PRESENT  dq_result_v2        (21 columns)
PRESENT  reconciliation_run  (19 columns)
PRESENT  data_incident       (22 columns)
PRESENT  recovery_plan       (21 columns)
PRESENT  recovery_execution  (11 columns)
```

**One engine difference worth recording:** Athena rejects `format-version` outright
(`Unsupported table property key`) and writes Iceberg v2 by default, while the Spark DDL in
`spark/ops/ddl/reliability_tables.sql` needs it. The two engines are not interchangeable
here, which is why a dedicated script exists rather than piping that file into Athena.

### Round-trip proof — the contract and the storage agree

Records built from `cdc/quality.py` and `cdc/incidents.py`, written via Athena, read back:

| Ledger | Row | Value |
|---|---|---|
| `dq_result_v2` | 1 | `status = FAIL`, severity `BLOCKER`, `blocks_watermark = True` |
| `reconciliation_run` | 1 | `status = FAIL`, `difference = -1.0` computed by the model |
| `data_incident` | 1 | `status = OPEN`, blast radius 14, `automatic_recovery_allowed = false` |

That is the first real evidence for **B6** (a DQ verdict persisted) and **B7** (an incident
exists). Neither is fully cleared: the verdict came from a round-trip, not from a DQ engine
run against a live table.

### Blockers

**Cleared:** B1, B3, B10, B11. **Partial:** B5, B6, B7. **Open:** B2, B4, B8, B9, B12.

Next is **B4** — repoint `dq_engine` at the derived catalogue — because a real DQ verdict
cannot be produced until the engine reads rules that describe live tables.

### New deliverables

* `docs/LINEAGE_AND_RECOVERY_TEST_GUIDE.md` — hands-on: test the lineage in AWS, then rerun
  only the affected table, date, key or column. Every command real, every expected output
  produced on this account.
* `README.md` — rewritten as a full project introduction: architecture, all 15 components,
  AWS services, what is and is not proven, quickstart, and a capture shot-list.
* `scripts/capture-evidence.sh` — read-only, repeatable portfolio evidence into
  `artifacts/evidence/<timestamp>/`. **It cannot record video or screenshots** — no display;
  the shot list says which shots need a human.

---

## 2026-09-30 — B4, B6 and B12: DQ rules derived, real verdicts written, mart recovery unblocked

### B4 — the DQ engine no longer reads a drifted file

`cdc/dq_rules.py` generates the rule set `dq_engine.run_suite` consumes, from
`cdc/registry/sources.yaml`. The engine is **untouched**; only its input changed.
`--rules <file>` still works, for pinning a suite to reproduce an old verdict.

| | hand-written file | generated |
|---|---|---|
| datasets | 7 (**1 live**) | **35, all live** |
| checks | ~25 | **153** |
| layers | 4 named, mostly phantom | FULL_CDC 10 · EOD 10 · REALTIME 7 · CURATED 8 |

**The correctness point a generated set gets right.** The registry declares
`dq.not_null: [ACCOUNT_ID, CUSTOMER_ID]` — *source* column names. At FULL_CDC and EOD those
are **not columns at all**; they live inside `payload_after`. A completeness check on them
there can only return NOT_EVALUATED, which blocks the publish for a reason that has nothing
to do with the data. They become real columns one layer later, at CURATED, under the names
`entities.yaml` maps them to — so that is where the not-null rules are emitted, and the CDC
layers get checks their own schema can answer.

It also surfaced a genuine gap, reported on every run rather than dropped:
`loan` and `payment_method` declare `not_null` columns with **no CURATED mapping**, so those
columns were declared-guarded and unguarded.

### B12 — a mart recovery no longer needs an operator

`reporting/curated/facts.yaml` declares each Kimball fact's entities, dimensions and grain,
and `curated_build.assert_fact_inputs()` **checks the job's actual inputs against it at
runtime**. An undeclared input is now a build failure, not a lineage defect found months
later.

That is the whole difference between `declared` and `derived`: a declaration the code checks
is evidence; one it does not is a human's reading of a function.

| | before | after |
|---|---|---|
| `eod → mart` weakest evidence | `declared` | **`derived`** (5 hops) |
| plan approval | `REQUIRES_APPROVAL` | **`AUTOMATIC`** |

The `src → topic` Debezium hop is **still `declared`, permanently** — a test asserts it, so
fixing the fact contract did not make the graph claim telemetry that does not exist.

### B6 — real DQ verdicts, in the live ledger

`scripts/run-dq-athena.py` executes the **same generated rule set** through Athena — not a
second engine, and it writes the same `DqResult` contract to the same ledger. Two executors,
one rule set, one result shape.

First run over the EOD layer: **50 results written to `ops.dq_result_v2`**,
`BLOCKS_PUBLISH True`.

That run exposed two defects **in my own runner**, both the failure class this platform keeps
producing:

1. `${business_date}` was substituted in the outer predicate but **not in the reconciliation
   sub-predicates** — Athena rejected the literal, recorded as `ERROR` ("the check is
   broken"), which was exactly right.
2. **Athena omits NULL values from the result array**, so `SELECT count(*), sum(x)` over an
   empty table returns a row of length 1. Indexing it raised `IndexError` → `ERROR`, when the
   truth was "the table is empty" → `NOT_EVALUATED`. Two very different answers.

Both fixed; `_one()` now pads to the expected width.

### Blockers

**Cleared:** B1, B3, B4, B10, B11, B12. **Partial:** B5, B6, B7.
**Open:** B2 (needs an EMR submission), B8 (DRP11 scenarios), B9 (k3s sizing, unmeasurable
from here).

### B6 — corrected live DQ run (2026-09-30)

After fixing the two runner defects, over the EOD layer for COB 2026-09-28:

| | first run | corrected |
|---|---|---|
| PASS | 20 | **25** |
| ERROR (the *check* was broken) | **20** | **0** |
| NOT_EVALUATED | 10 | 25 |
| `BLOCKS_PUBLISH` | True | **True** |

**100 real verdicts now in `ops.dq_result_v2`** across the two runs, every one produced by
the generated rule set against live Glue tables.

The 25 `NOT_EVALUATED` are tables with no rows for that COB, and the suite still blocks —
which is the whole design on live data: *"we looked and there was nothing"* is not a pass,
and an unevaluated BLOCKER check stops the publish exactly as a failure would.

---

## 2026-09-30 — B2: the OpenLineage listener has now actually run

A local Spark **3.5.0** job, with the listener attached via the conf **derived by
`cdc/lineage_runtime.py`** — not hand-typed — emitted **12 real OpenLineage events**.

| | |
|---|---|
| artifact | `io.openlineage:openlineage-spark_2.12:1.53.0` |
| producer string | `…/OpenLineage/tree/**1.53.0**/integration/spark` — the exact pin |
| events | START 4 · RUNNING 5 · COMPLETE 3 |
| datasets | real inputs and outputs captured on read → aggregate → write |
| run facets | `parent`, `processing_engine`, `spark_applicationDetails`, `spark_properties` |

Evidence: `artifacts/validation/data-reliability/drp3-b2-openlineage-live.json`.

**What this proves:** the pin is correct for Spark 3.5 / Scala 2.12, the artifact resolves
and loads, the derived conf genuinely attaches the listener, the lifecycle is emitted, and
input/output datasets are captured.

**What it does not prove, and the evidence file says so:** emission from EMR Serverless
against Iceberg/Glue tables, and delivery to the DataHub OpenLineage endpoint. The listener
names local paths here; ADR-090's carried `datahub_urn` identity is applied by the
platform's own emitter, not by the listener.

So B2 moves from **open** to **substantially cleared** — the risky unknown (does the pinned
artifact load and attach?) is answered; what remains is running it on EMR, which is a
45–60 minute scheduling exercise rather than a question.

### Blockers

**Cleared:** B1, B3, B4, B10, B11, B12.
**Substantially cleared:** B2, B6.
**Partial:** B5, B7.
**Open:** B8 (DRP11 scenarios), B9 (k3s sizing — not measurable from here).

### Final verification — 2026-09-30

**Full suite: 3,273 passed, 0 failed** (9m13s). Doc validator 14/14.

Six blockers cleared, two substantially cleared, two partial, two open. The two that remain
open need time or hardware, not decisions:

| Open | Needs |
|---|---|
| B8 — DRP11 scenarios 1–6, 9, 10 | one EMR submission, then the scenario runs |
| B9 — production DataHub sizing | a memory measurement on the k3s node |

Checkpoint: `DATA_RELIABILITY_CONTEXT_REHYDRATED_READY`.

---

## 2026-09-30 — B7 and four more DRP11 scenarios, live

| Scenario | Result |
|---|---|
| **3 — EOD DQ failure** | **LIVE PASS to the plan stage.** BLOCKER FAIL → incident OPEN → plan `rp1:45b21f325ca10ec5` (radius 14, turns `[1,1,2,2,5,3]`, `AUTOMATIC`) → execution PENDING → incident **PLANNED** |
| **4 — unrelated branch excluded** | **LIVE PASS.** 14 impacted, **0** digital-branch assets included |
| **9 — large blast radius** | **LIVE PASS.** radius 14 vs limit 2 → `REQUIRES_APPROVAL` |
| **10 — column lineage** | **LIVE PASS to CURATED.** `BALANCE → balance`, `derived` |

Live ledger state:

```
dq_result_v2 151 · reconciliation_run 1 · data_incident 1 · recovery_plan 1 · recovery_execution 1
incident inc-b3-proof: OPEN -> PLANNED, plan attached, automatic_recovery_allowed = true
```

**The first time the reliability loop has moved a real record through more than one state on
live infrastructure.** The execution row is `PENDING` because the repair is an EMR
submission — a row claiming otherwise would be the fabrication this whole programme is built
to avoid.

**B7 substantially cleared.** DRP11 is now **6 of 10 scenarios live** (3 to the plan stage,
4, 7, 8, 9, 10).

### Blockers

**Cleared:** B1, B3, B4, B10, B11, B12.
**Substantially cleared:** B2, B6, B7.
**Partial:** B5.
**Open:** B8 (the EMR half — scenarios 1, 2, 5, 6 and the repair of 3), B9 (k3s sizing).

---

## 2026-09-30 — DRP12 performed. Verdict: NOT_READY

**19 PASS · 6 FAIL · 1 N/A** of 26 acceptance gates.
`docs/validation/DRP12_FINAL_REVIEW.md` · `artifacts/validation/data-reliability/drp12-acceptance.json`

The six failures have **two root causes**, both operator actions:

| Root cause | Gates | Clears with |
|---|---|---|
| **RC1** — the Airflow OpenLineage provider is not installed in the chart | 1 | a Helm upgrade |
| **RC2** — no recovery has been EXECUTED | 5 | one EMR submission + the coordinator run |

RC2's five gates (bounded auto recovery, root validates before descendants, descendants
rerun topologically, incident closes on re-certification, E2E evidence) all describe **one
run that has not happened**. The plan for it exists, is `AUTOMATIC`, and is persisted as
`rp1:45b21f325ca10ec5`.

**No P0 findings.** Two P1s (both = RC1/RC2), three P2s, three P3s — all listed in the review.

### Across the programme

| | DRP0 | now |
|---|---|---|
| assets with governance | 1 | **84** |
| DQ rules resolving to live tables | 1 of 7 | **35 of 35** |
| DQ verdicts ever produced | 0 | **151** |
| lineage | a drifted YAML with false `observed` markers | **81 nodes, 0 cycles, 0 findings** |
| tests | 2,871 | **3,278** |
| AWS spend | — | **$0** |

Checkpoint: `DATA_RELIABILITY_CONTEXT_REHYDRATED_READY`.

---

## 2026-09-30 — the recovery sequence no longer needs a scheduler

The `airflow dags trigger recovery_coordinator` command in the DRP12 review **could not
work** and I should have verified it before publishing it. Two reasons, both checkable in
seconds:

* local Airflow is **2.9.3** with `dags_folder = ~/airflow/dags` — not this repository
* the deployed Airflow 3.2.2 runs on k3s and there is no `kubectl` here

`DagNotFound` was the correct answer to a wrong command.

### The coupling underneath, now fixed

The gate, the turn ordering and the job resolution have **nothing to do with scheduling**,
yet they were only reachable through a registered DAG — which made the safety logic
untestable in every environment except the one cluster.

`scripts/run-recovery.py` now carries those steps and the DAG calls them. Airflow
orchestrates; it does not own. The runner imports no Airflow at all (asserted by a test).

Run against the real plan:

```
plan     rp1:45b21f325ca10ec5   root eod:oracle_coredb_corebank_account
radius   14   weakest=derived   cost=medium   approval=AUTOMATIC
turn 0: curated:banking_account
turn 1: curated:dim_account
turn 2: curated:fact_account_daily_snapshot, curated:fact_transaction
turn 3: mart:stg_fact_account_daily_snapshot, mart:stg_fact_transaction
turn 4: 5 marts
turn 5: mart_account_balance_monthly, mart_account_risk_daily, mart_customer_360_daily
all 14 target(s) resolve to registered entrypoints
```

Execution `exec-21ed05e0d7` recorded as **`BLOCKED_REQUIRES_EMR`** — the gate, the ordering
and the resolution all passed; no turn was run, and the row says so. A coordinator
reporting SUCCEEDED having rebuilt nothing is the failure this platform keeps finding.

### A second bug of mine, found by running it

The execution INSERT used a subquery inside `VALUES`; Athena rejects that outright
(`Unexpected subquery expression in logical plan`). Fixed by resolving the partition value
first. Also verified: **local Airflow 2.9.3 parses all 25 DAGs**, including
`recovery_coordinator` — so the correct local command is simply

```bash
AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow/dags airflow dags list
```

Six new tests (`TestRecoveryRunnableWithoutAScheduler`).

### Documentation refresh — 2026-09-30

The GitHub-facing docs had gone stale behind the work, which is the exact drift this
programme exists to end. Caught and corrected:

* `README.md` — test count, DRP11 at **6 of 10**, the DRP12 score (**19 of 26 gates**), the
  proven/not-proven tables rewritten against the current ledger, and a new **Operator
  tooling** section covering the five scripts (three of which it did not mention at all).
* `docs/LINEAGE_AND_RECOVERY_TEST_GUIDE.md` — `run-dq-athena.py` in §3 and `run-recovery.py`
  in §5, so the guide's own steps are executable rather than described; plus the
  `NOT_EVALUATED` reading note and the correct Airflow invocation.
* `docs/DATA_RELIABILITY_CONTEXT_REHYDRATION.md` — test inventory, file list, tooling.
* `docs/validation/DRP12_FINAL_REVIEW.md` — the corrected RC2 command.

**Full suite: 3,284 passed, 0 failed** (2,871 + 413 added). Doc validator 14/14.

---

## 2026-09-30 — the recovery loop EXECUTES. DRP12: 19/26 → 23/26

`scripts/recovery-drill.py` ran the complete sequence on real Iceberg/Glue/Athena in
**96 seconds**:

```
plant defect (k=2 duplicated)  -> drill_mart sum = 80.0   WRONG
DQ                              -> FAIL, dupes=1, BLOCKER
incident                        -> OPEN
plan                            -> 2 turns, AUTOMATIC
repair root (turn 0)            -> drill_mid rebuilt from drill_src
validate root                   -> PASS   <- descendants gated until here
rerun descendants (turn 1)      -> drill_mart rebuilt
validate downstream             -> PASS, sum 80.0 -> 60.0   CORRECTED
reconciliation                  -> PASS
execution                       -> SUCCEEDED, 2 turns
incident                        -> RESOLVED
```

**`sum 80.0 → 60.0` is the point.** The data was genuinely wrong and is genuinely fixed —
not a verdict asserted about a table nobody touched.

### Why a drill rather than the production marts

Rebuilding `mart_account_balance_daily` needs an EMR submission (45–60 min, one at a time)
and a write to a certified table from a laptop. **The loop needs neither to be exercised.**
The drill uses a clearly prefixed sandbox chain (`drill_src → drill_mid → drill_mart`) and
the evidence file states what it does not prove. A drill presented as a production run
would be the fabrication this whole programme keeps catching.

### DRP12 rescored

**23 PASS · 2 FAIL · 1 N/A.** Four gates closed by the drill: bounded auto recovery
executes · the root is validated before any descendant · descendants rerun in turn order ·
DQ + reconciliation + certification close the incident.

Remaining:

| Gate | Needs |
|---|---|
| Airflow runtime lineage works | the provider installed in the chart (Helm upgrade) |
| E2E evidence exists | scenarios 1, 2, 5, 6 — one ordinary pipeline run |

DRP11 is now **7 of 10** scenarios live. Verdict stands:
**`DRP12_DATA_RELIABILITY_PLATFORM_NOT_READY`**.

---

## Session 39 — DRP11 closed, DRP12 PRODUCTION_READY (2026-09-30)

**25 PASS · 0 FAIL · 1 N/A** of 26 gates. Both failing gates closed on their merits.

### Airflow runtime lineage — and the config defect behind it

`apache-airflow-providers-openlineage==2.20.2` was installed into a pod running the deployed
`apache/airflow:3.2.2` image (reached over SSM; the resident scheduler untouched, all eight
pods stayed `Running`). First run: the provider registered, the DAG went green, **zero
events**.

`AIRFLOW__OPENLINEAGE__TRANSPORT` was the bare string `console`. The provider parses that
key as JSON, so it raised `AirflowConfigException` **during plugin import** — Airflow
skipped the plugin, `REGISTERED_LISTENERS = []`, and every task succeeded. The existing test
compared `values.yaml` with `cdc/lineage_runtime.py::airflow_env()` and passed, because both
carried the bare string.

Fixed in both places; three tests added that parse the value the way the provider does; an
`http` transport is now **refused** rather than half-rendered, because the only working
literal would put the DataHub token into `values.yaml`. ADR-091.

After the fix: **7 events**, namespace `cdc-lakehouse-dev`, `parent` and `root` run facets
present, `processing_engine` reporting Airflow 3.2.2 / adapter 2.20.2 / client 1.53.0.

### DRP11 — 7 of 10 → 10 of 10

| Scenario | Result |
|---|---|
| 2 — bad CDC → quarantine | `ops.dq_quarantine` **created** (it had never existed); poison `op='X'` caught by a BLOCKER check, quarantined **by reference**, 2 valid rows retained, gate `INVALID`, nothing certified |
| 1 — normal path | DQ → reconciliation → certify → watermark, live. Gate `CERTIFIED`, watermark written. **Segmented**: the data-plane and DataHub hops are evidenced separately, not in one correlated run |
| 5 — late CDC → AUTO_CORRECT | the `PROVISIONAL_NRT` key lifted to `PROVISIONAL_CORRECTED`; the `CERTIFIED` key **refused** by the anti-downgrade rule; the unaffected key untouched |
| 6 — FULL_FILL | unresolved SKs 3 → 1 (the member with no dimension row keeps `UNKNOWN_SK -1`, never NULL); `processing_status` untouched, all rows still `CERTIFIED`; `FlowContext(FULL_FILL).status` raises, as it must |

### Ledger state

```
ops.dq_result_v2        160 rows     ops.recovery_plan        1 row
ops.reconciliation_run    5 rows     ops.recovery_execution   1 row
ops.dq_quarantine         1 row      ops.eod_watermark        3 rows
```

### Still open, none of them a gate

1. The provider is **not in the Airflow image**; chart 1.22.0 removed `extraPipPackages`, so
   the resident scheduler emits only after a custom image is built.
2. Scenario 1 is three segments, not one run — one EMR submission joins them.
3. Ingestion recipes still never executed (B5).

**Tests: 3,301 passed, 0 failed.** Doc validator 14/14. **AWS spend: $0.**

---

## Session 40 — the three follow-ups resolved as far as the platform allows (2026-09-30)

### 1. Airflow OpenLineage image — BUILT

Chart `airflow-1.22.0` removed `extraPipPackages`, and the k3s node has **no docker,
buildah, podman or nerdctl** — only `ctr`, which imports but cannot build. Built with
**kaniko in a pod** and imported straight into containerd:

```
docker.io/library/airflow-openlineage:3.2.2-ol2.20.2   sha256:cc189fb871dd...  (635 MB)
```

No registry, no ECR, no IAM change. The Helm upgrade that puts it on the resident scheduler
restarts a live cluster and is **left to an operator** — the values patch is written and
reuses `helm get values` so the UI password is never re-derived or printed.

### 2. Scenario 1 — now ONE correlated EMR run

`eod-scenario1-cob20` / `00g95hkdnkfkl827`: **320 rows, `dq=PASS recon=PASS
status=CERTIFIED`**, source snapshot `80230069689276128` → target `3683870259747946533`,
all under `eod-2026-09-20-032143Z`.

Two earlier submissions **failed correctly** and were not patched around:
`EOD_WAITING_SOURCE` (watermark 481 min behind the cutoff) and `EMPTY_WINDOW` (a query
confirmed the table has events only on 09-20 and 09-29). The COB was moved to a date with
data.

### 3. Ingestion recipes (B5) — EXECUTED

Glue **493 events**, dbt **248 events**, into live DataHub; **147 datasets**. They had
never run. `kafka` and `kafka-connect` need an in-VPC host — MSK resolves to
`10.42.12.173` / `10.42.11.19` and is unreachable from the workstation, **verified by TCP
connect**. That is security invariant 3 working, not a defect.

### Seven defects found by running things

| # | Defect | Fix |
|---|---|---|
| 1 | `spark.jars.packages` cannot resolve — no NAT; the job dies *in resolution having done no work* | pinned jar staged in S3, sha1 verified twice |
| 2 | the derived conf would have **silently replaced** the Iceberg jar | `emr-submit.sh` merges |
| 3 | `job.name` was `unknown` on EMR | `spark.openlineage.appName` derived and passed |
| 4 | Spark emits **no dataset events** on EMR — split classloaders; the cure is *rejected* by the platform | needs a custom EMR image; `jar_runtime_path` held empty by test |
| 5 | the dbt recipe **cannot run to a file sink** — `PATCH` requires a live graph | documented |
| 6 | `convert_urns_to_lowercase` was **defaulted** on dbt | pinned `False` |
| 7 | `GlueSourceConfig` **rejects** that key | removed; proves Glue never lowercases |

Also: the system `acryl-datahub` was unusable — an incompatible `sqlglot` broke **every**
ingestion source import. Fixed with a pinned `.venv-datahub`, gitignored and excluded from
the doc validator (which had started reporting botocore's own example keys as credentials).

**DRP12 stands at 25 PASS · 0 FAIL · 1 N/A — `PRODUCTION_READY`.** Doc validator 14/14.

---

## Session 41 — resident Airflow lineage proven; AIGR0 audited (2026-09-30)

### The last DRP12 caveat is closed

`scripts/airflow-enable-lineage.sh --execute` rolled **Helm revision 2**. The resident
deployment now runs `airflow-openlineage:3.2.2-ol2.20.2` — provider **2.20.2**,
`openlineage-python` **1.53.0**, which is the same client major as the pinned Spark jar.

A real KubernetesExecutor run emitted:

```
OpenLineageClient will use `console` transport
Successfully emitted OpenLineage `START` event of id 01a0f0af-1080-7d67-8e13-a535f44e4b9e
Successfully emitted OpenLineage `FAIL`  event of id 01a0f0af-1080-7d67-8e13-a535f44e4b9e
```

carrying the DAG's real owner (`data-platform`) and its real docstring. Evidence:
`artifacts/validation/data-reliability/drp3-airflow-resident-emission.json`.

**A correction recorded earlier stands:** the provider was never missing — the stock image
ships 2.17.0. What the deployed release lacked was any `AIRFLOW__OPENLINEAGE__*` config.

### Four attempts that failed first, and what each taught

| Attempt | Result |
|---|---|
| write a probe DAG onto the dags PVC | `Permission denied` — the PVC is not writable from the pod |
| `dags test` in the **scheduler** | `Dag could not be found` — in Airflow 3 only the dag-processor mounts the dags volume |
| `dags test` in the **dag-processor** | `exit 137` — OOM at a **512Mi** limit. The listener *was* active; it logged its emission policy for the task before the kill |
| `tasks test` (one `EmptyOperator`) | `exit 137` too — the limit binds on import, not on the task |

The route that worked was a real triggered run, scraping worker-pod logs **while the pods
were alive**: there is no remote logging, and `delete_worker_pods` defaults to true.

### Security finding — rotate the Airflow UI password

The chart's `NOTES.txt` renders `createUserJob.defaultUser.password` in the clear, so the
upgrade printed the admin credential into the operator's terminal. The script now suppresses
NOTES and masks credential-shaped lines. **The exposed password should be rotated:**

```bash
terraform -chdir=terraform/envs/dev apply -replace='module.airflow_k3s.random_password.admin_ui[0]'
# then re-run: scripts/airflow-enable-lineage.sh --execute
```

### AIGR0 — audited

New: `docs/AI_RECOVERY_CURRENT_STATE_AUDIT.md`, `docs/AI_DATA_RELIABILITY_COPILOT_TARGET.md`.

Key findings:

- **Most of AIGR already exists.** `RecoveryPlan` (immutable, hash-keyed), `decide_approval`,
  `LineageImpactService` with exclusions and turns, column lineage, URN minting, DataHub
  client, DQ/certification — all built and live-tested during DRP.
- **The blocking decision is ADR-057.** `ToolSpec.__post_init__` refuses any tool with
  `read_only=False`, in code. AIGR1 cannot proceed without an ADR superseding it.
- **Two root causes cannot be expressed today** — `TRANSFORM_LOGIC_DEFECT` and
  `DQ_RULE_DEFECT` — and they are exactly the two where a rerun is actively harmful. Both
  currently classify as something `REPAIRABLE_BY_RERUN` treats as fixable.
- **`BUSINESS_AI_PRODUCTION_READY` does not exist** under that name; the prompt pack assumes it.
- **AI-P13 is blocked and mutates AWS.** AIGR must not depend on it.
- **AgentCore stays deferred** (ADR-056); its revisit trigger is not met.

`AIGR0_CURRENT_AI_RELIABILITY_CONTEXT_AUDITED`

---

> **Numbering.** The sessions below belong to the AI / data-reliability programme and are
> numbered independently of the Phase A–I platform sessions listed at the top of this file.
> Both series reach the low 50s, so "Session 50" appears twice meaning different work; the
> heading text disambiguates. Cross-references in `DECISION_LOG.md` and
> `IMPLEMENTATION_REPORT.md` use the headings, not the numbers.

## Session 42 — AIGR0 → AIGR12 (2026-09-30)

**`AIGR12_AI_DATA_RELIABILITY_COPILOT_NOT_READY` — 19 pass · 9 not proven live.**
**`AI_DATA_RELIABILITY_CONTEXT_REHYDRATED_READY`.**

### What was built

`ai/reliability/` — `tools.py` (27 tools: 18 read, 7 plan, 2 mutating), `model.py`
(14 root-cause categories with dispositions, typed `RecoveryScope`, `RecoveryCapability`),
`planner.py`, `control.py`, `policy.py`, `copilot.py` (bounded LangGraph).

**ADR-092 supersedes ADR-057.** `ToolSpec` used to refuse every mutation in code. It now
allows exactly one class, `recovery_submit`, whose membership is a **closed frozenset of two
names**. A surface whose *behaviour* is reviewed grows one reasonable tool at a time
(`retry_job`, `clear_task`, `refresh_partition`) until it is no longer a surface; a surface
whose *membership* is fixed cannot.

### The two categories that justify the finer taxonomy

`TRANSFORM_LOGIC_DEFECT` and `DQ_RULE_DEFECT` were previously inexpressible — both mapped to
classes `REPAIRABLE_BY_RERUN` treats as fixable. They are exactly the two causes where a
rerun is actively harmful: one reproduces the wrong answer at cost, the other repairs data
that may be correct.

### Two defects my own code had, found by its own tests

1. **The planner silently dropped impacted assets** that never appeared in the topological
   order — the precise "omitted descendant" failure the design exists to prevent. Now every
   impacted asset is either planned or excluded with a reason, asserted directly.
2. **The budget killed the evidence pack.** A run that exceeded its ceiling produced no
   account of why. `build_evidence` and `final_answer` are now unbudgeted — the accounting
   must survive the thing it accounts for.

### Verdict, and why it is not pessimism

95 AIGR tests, 52 safety-matrix rows, **zero reachable unauthorized mutations**. The copilot
cannot be made to do the wrong thing. It has also never done the right thing on a real
request: **no Bedrock invocation has been verified and no AI-driven recovery has executed.**
DRP12 earned READY only on live evidence after returning NOT_READY twice; applying a weaker
standard here would make the two verdicts incomparable.

**Suite: 3,394 passed, 0 failed.** Doc validator 14/14. **$0.**

---

## Session 43 — AIGR12 PRODUCTION_READY (2026-09-30)

**`AIGR12_AI_DATA_RELIABILITY_COPILOT_PRODUCTION_READY` — 27 pass · 0 fail · 1 blocked externally.**

### The live AI-driven recovery

A natural-language request ran end to end against real infrastructure:

```
"The BALANCE column in EOD ACCOUNT for COB 2026-09-28 is wrong for A001, A002, A003.
 Investigate it and repair only the affected downstream data."

→ resolved against the real 84-asset inventory: eod:oracle_coredb_corebank_account
→ real lineage graph: 16 impacted, 16 executable
→ column lineage DERIVED → column-narrowed scope REFUSED → downgraded to table level
→ real BLOCKER DQ verdict in ops.dq_result_v2
→ plan aigr1:91ec6c50c0cc88c0 — eod_build widened to COB_DATE, reporting kept BUSINESS_KEY_SET
→ approval apr:4165331ce5ae, execution exe:60f058da20d0
→ mart 1600.0 → 1000.0 · violations 3 → 0 · DQ PASS
```

Evidence: `artifacts/validation/data-reliability/aigr10-live-e2e.json`.

### What was wired to reach it

`ai/reliability/context.py` (real inventory + lineage), `registry.py` (per-job
`RecoveryCapability` and the Athena executor). Bedrock invocation was **verified twice**
before the account began requiring an Anthropic use-case form.

### Two defects the live run found — neither in the safety layer

1. **The executor template ignored its scope.** A value heuristic left the mart *differently
   wrong* (900) while plan, policy, approval and execution all reported success. Now a
   template with no scope placeholder is refused, and the EOD repair rebuilds from FULL_CDC.
2. **The verification predicate was wrong** — it flagged a correct row. That is a
   `DQ_RULE_DEFECT`, met by accident inside the code meant to demonstrate the category. The
   check now compares EOD against FULL_CDC, which is the contract.

### Standing limits, stated

- Bedrock gated at the account level (submit the use-case form).
- Column lineage is `DERIVED`; column-narrowed recovery is refused until the mappings are
  **validated against real schemas** — not relabelled.
- 8 of 19 use cases modelled, 4 not built, each labelled.

**Suite: 3,407 passed, 0 failed.** Validator 14/14. **$0.**

> Two ADR-057 tests failed when ADR-092 landed — they asserted that *no* tool may mutate, which the platform deliberately changed. Updated to the new rule while keeping what they protected: a read class still may not mutate, and a tool this repository has not named still cannot mutate whatever class it claims.

---

## Session 44 — the blocked items closed, and a guide to verifying all of it (2026-09-30)

### Column lineage raised from DERIVED to VALIDATED — by checking, not relabelling

`cdc/column_validation.py` confirms both endpoints of every column edge against reality.
The wrinkle that made it interesting: **CDC layers store the source row as JSON**, so
`payload_after` is a `string` and `BALANCE` is not a Glue column at all — it is a key inside
a blob, present in all 200 rows. Glue schema lookup alone confirmed **3 of 67** edges;
reading the JSON keys from real data confirmed **28**, including all six on
`eod_oracle_coredb_corebank_account`.

The 39 that fail are recorded, not hidden: dbt `int_*`/`stg_*` models are ephemeral, and
some EOD tables are empty. They stay `DERIVED` and **cannot narrow a recovery**.

**The live AI recovery now takes the column-narrowed path:**

```
[4] column lineage VALIDATED -> scope narrowed to COLUMN_LINEAGE
[9] mart 1600.0 -> 1000.0 · violations 3 -> 0 · AIGR10 LIVE PASS
```

### The kafka recipe could never have authenticated

`sasl.mechanism: AWS_MSK_IAM` had been in the generated recipe for months. That is the
**Java** client's mechanism name; DataHub's Kafka source uses librdkafka, which answers
`Unsupported SASL mechanism: AWS_MSK_IAM` — verified directly, not inferred. Fixed to
`OAUTHBEARER`, with the recipe now stating that MSK IAM additionally needs an `oauth_cb`
callable that YAML cannot carry.

### A dry run that destroyed real evidence

`ai-recovery-drill.py` in dry-run mode returned dummy values, then **wrote the evidence
file** — replacing a real capture with zeros that still looked like evidence, and printing
`FAIL` for numbers never measured. A dry run now reports `DRY_RUN` and writes nothing.

### Shipped

- `scripts/ai-recovery-drill.py` — the whole copilot loop, dry-run by default
- `cdc/column_validation.py` + 14 tests
- **`docs/HOW_TO_USE_AND_VERIFY.md`** — every capability, the command, and the expected output

**Suite: see the session summary.** Validator 14/14. **$0.**

---

## Session 46 — re-verification after the copilot became usable (2026-09-30)

The first `AIGR12 PRODUCTION_READY` score was taken before the copilot had a CLI, a UI or
quality gates. Using it found **six more defects**. None was in the safety boundary.

| # | Defect |
|---|---|
| 1 | a lineage question was diagnosed — `Root cause UNKNOWN` to a question that asked for lineage |
| 2 | root cause came from keywords in the question, so "plan a recovery" returned UNKNOWN |
| 3 | a DQ check that examined **0 rows** outranked one failing 1 of 670 |
| 4 | a word in the asset's name became a column (`EOD ACCOUNT` → `column ACCOUNT`) |
| 5 | plans had **no** `DQ_RECHECK`/`RECONCILE`/`CERTIFY` — the barrier lived in prose |
| 6 | the executor keyed templates by job, so a `DQ_RECHECK` ran the **rebuild** SQL |

Defect 6 is the instructive one: a verification turn silently re-executing a
`DELETE + INSERT`, with a green result that proved nothing. Found by reading
"7 statements executed" against a plan whose verification actions should have read, not
written. Templates are now keyed by `(job, action)`, and a missing template is refused
rather than falling back.

**Evidence regenerated** against current code — the old capture recorded a 2-turn
rebuild-only plan and was being cited for a barrier that did not exist in it:

```
aigr10-live-e2e.json   4 turns, 7 actions
  EOD_REBUILD 1 · MART_RERUN 1 · DQ_RECHECK 2 · RECONCILE 2 · CERTIFY 1
  mart 1600.0 -> 1000.0 · violations 3 -> 0
source-update scenario  1000.0 -> 1125.0 · violations 2 -> 0
```

**Suite: 3,460 passed, 0 failed.** AIGR alone: **155**. Validator 14/14. **$0.**

`AIGR12_AI_DATA_RELIABILITY_COPILOT_PRODUCTION_READY` — re-verified, 27 · 0 · 1.

---

## Session 47 — portfolio documentation set (2026-09-30)

Three documents now sit in front of the README for a reviewer:

| | |
|---|---|
| `docs/PORTFOLIO_OVERVIEW.md` | the whole project in five minutes — architecture, measured scale, what is proven and how, and an explicit "what is not done" |
| `docs/USE_CASES.md` | nine scenarios end to end with real output: green-pipeline-wrong-number, source restatement, duplicate CDC, impact analysis, the four refusals, realtime/EOD as siblings, prompt injection, lineage validation, cost control |
| `docs/ENGINEERING_JOURNAL.md` | every defect found, reproduced and fixed, grouped by what each one taught |

**Verified for this set:** all 275 markdown files link cleanly (one genuine break fixed — a
`../docs/` path written from inside `docs/claude/`); every referenced script exists; the test
count is taken from a measured run, not arithmetic.

**Measured scale, for the record:** 423 Python files (`cdc/` 46, `ai/` 84, `spark/` 94),
114 test files, 14 Terraform modules, 17 DAGs, 78 scripts, 154 docs, 82 ADRs, 91 ADR rows,
93 decision-log entries, 84 governed assets, 81-node lineage graph, 27 AI tools, 14 root
causes, 15 evidence files.

**Suite: 3,529 passed, 0 failed.** Validator 14/14. **$0.**

---

## Session 48 — Copilot transcripts, and the defect writing them found

**Asked for:** the sample questions and answers from both AI copilots, written up as project
documentation for GitHub.

**Delivered:** `docs/AI_COPILOT_TRANSCRIPTS.md` — real captured sessions from the Reliability
& Governance Copilot (`127.0.0.1:8899`) and the Data Platform Copilot (`127.0.0.1:8501`),
annotated with what each demonstrates: governance resolution, column lineage with confidence,
evidence read from four ledgers, a 22-action plan with quality barriers, blast-radius
narrowing, and four refusals. Linked from the README routing table, the documentation index,
`PORTFOLIO_OVERVIEW.md`, `USE_CASES.md`, `AI_COPILOT_SAMPLE_QUESTIONS.md` and
`AI_COPILOT_USER_GUIDE.md`.

**Found while writing it.** Re-running the questions rather than transcribing the screenshots
exposed a live defect: *"What does the BALANCE column in EOD ACCOUNT feed?"* answered
`column in`. Two causes compounded — an extractor that reads the word after the keyword, and
a substring test in which `"in"` matches clos**in**g_balance. Because the fragment returned
`DERIVED` rather than `ABSENT`, the resolver accepted it and never tried `BALANCE`.

The real cost was silent. `BALANCE` is `VALIDATED`, so the flagship *"repair only the
affected downstream data"* scenario had the evidence to narrow to one column and three keys
and planned whole-date rebuilds instead. After the fix, descendant actions plan at
`BUSINESS_KEY_SET`.

Fixed in `ai/reliability/context.py` (name-or-whole-segment matching) and
`ai/reliability/ask.py` (filler dropped before the lineage lookup; `field` accepted in both
word orders). **ADR-093.** 9 regression tests added — one of which caught a second defect the
first fix had introduced.

**Suite: 3,497 passed, 0 failed** (3,488 → 3,497). **$0** — no AWS call, no model call.

**Still open:** P1 rotate the Airflow UI admin password exposed by Helm NOTES; the Anthropic
use-case form for Bedrock; kafka recipe `oauth_cb` from Python; a custom EMR Serverless image
carrying `openlineage-spark_2.12-1.53.0.jar`; 39 of 67 column edges remain `DERIVED`.

---

## Session 49 — Enough data to demo, and four agent defects it exposed

**Asked for:** make the data and the AI agent good enough to verify and demo from GitHub.

**Measured first.** The live mart holds **1 business date** (2026-09-23, 320 accounts, 320
rows). Six of the eight questions the UI suggests need history. Demo mode holds 30 dates.

**Fixed — four defects, none of which looked like a failure:**

1. **A comparison that was never made, reported as a number.** With no prior date,
   `compare_metric_periods` returns `comparison: None` and the summary fell through to the
   bare value — `VERIFIED`, `CERTIFIED`, `limitations: []`, for a question that asked for a
   change. It now names the missing window and records the limitation.
   (`ai/business_agent/answer.py::_baseline_missing`)
2. **Subjectless analytical questions answered from the knowledge corpus.** *"Is it
   abnormal?"* and *"Which account contributed most to the change?"* are UI chips; neither
   names a metric, and the subjectless allowance covered only `PREDICTION, DIAGNOSIS,
   GOVERNANCE`. Extended to `ANOMALY, DRIVER, FORECAST, TREND`, with the existing guard
   intact — *"Is gross margin abnormal?"* is still refused rather than answered about the
   closing balance. (`ai/business_agent/graph.py`)
3. **`predict_business_risk` was dead in both modes.** `mart_rows` is an optional injection
   and nothing passed it; `scripts/ai-ui.py` computed a source and never used it. Now fetched
   lazily, only for that intent. (`_fetch_mart_rows`)
4. **…and once wired it scored zero rows.** `materialize()` builds features *for* `as_of`,
   while the UI asks as-of the day after the last business date. It now anchors on the newest
   date with data and names it.

**Added:** a data-coverage banner in the UI header (`date_coverage` / `coverage_banner`) —
coverage is a property of the data and belongs where the user starts, not inside the fourth
error.

**Rewrote `docs/DEMO.md`.** It was dated Session 18 and told readers there was no MSK
cluster, no connector and no EMR application. Verified live: MSK `kafka-dev-lab-dev`
**ACTIVE**, EMR Serverless `kafka-dev-lab-dev-spark` idle, four EC2 running. Now three tiers,
with Tier 0 offline at $0.

**Verified in demo mode:** all 8 Ask chips, plus Diagnose (`DATA_INCOMPLETE` on the short
day), Forecast (6 methods backtested, `seasonal_naive` at 4.85% MAPE over 22 points),
Governance and Metrics.

**Suite: 3,514 passed, 0 failed** (3,497 → 3,514; 17 new in
`spark/tests/test_business_thin_data.py`). Cost: one `COUNT(DISTINCT business_date)` Athena
scan; everything else $0.

**Still open:** the live mart needs more COB dates for live comparison/forecast/anomaly —
that is a pipeline run, not a config change. P1 Airflow UI admin password rotation; Bedrock
use-case form; kafka recipe `oauth_cb`; custom EMR image for Spark dataset lineage; 39 of 67
column edges `DERIVED`.

---

## Session 50 — A capture list covering every capability

**Asked for:** questions that cover all the functionality, to screenshot for GitHub.

**Delivered:** `docs/DEMO_CAPTURE_GUIDE.md` — 24 numbered captures across both copilots,
each with the question, the output it actually produced on 2026-09-30, and what it proves.
All $0 in demo mode. Linked from the README routing table, the documentation index,
`DEMO.md` and `AI_COPILOT_SAMPLE_QUESTIONS.md`.

**Found while building it.** `"What is FULL_CDC?"` — a sample the CLI has advertised since
AIGR2 — was refused. A layer name scores equally against every table in that layer, so the
resolver saw a six-way tie and refused: correct machinery, wrong kind of question. It asks
about the architecture, not a table.

A concept glossary (`ai/reliability/context.CONCEPTS`, `concept_for`) now answers
definitional questions before asset resolution, sourced from `docs/L3_SNAPSHOT.md`. It fires
only on definitional phrasing, so `rebuild FULL_CDC for 2026-09-28` still plans a recovery.
Tests assert the wording describes REALTIME and EOD as **siblings** of FULL_CDC, and that
`kafka_offset` is never called a global clock.

The first version returned a partial dict and broke the CLI three times with `KeyError`; it
now returns the same keys as every other answer.

**Also verified for the guide:** the Governance tab on 2026-08-21 returns `INVESTIGATE` /
`CRITICAL completeness` with a proposed fix it will not run; the write attempt returns
`NOT VERIFIED` / `N/A` rather than dressing a refusal as a verified answer.

**Suite: 3,525 passed, 0 failed** (3,514 → 3,525). Validator 14/14. **$0.**

---

## Session 51 — Live mode: what it answers, and the backfill path

**Asked for:** how to run `scripts/ai-ui.py` for real rather than `--demo`.

**Measured the lake instead of guessing.** The whole change history of the platform is
**30 updates on one day**:

| layer | rows | dates | range |
|---|---|---|---|
| `mart.mart_account_balance_daily` | 320 | 1 | 2026-09-23 |
| `snapshot.eod_oracle_coredb_corebank_account` | 320 | 1 | 2026-09-20 |
| `full_cdc.cdc_oracle_coredb_corebank_account` | 670 | 2 commit days | 2026-09-20 (`r` 320 + `u` 30), 2026-09-29 (`r` 320) |

So live mode answers value, definition, certification, lineage, governance and every refusal
today; comparison needs 2 dates and forecast/anomaly need 14, and **no backfill can invent
history the source never had**. Writing synthetic rows into the mart was rejected: it would
make `processing_status = CERTIFIED` a lie about data no EOD run produced.

**Added `scripts/backfill-business-dates.sh`** — dry-run by default, identity guard first.
One `eod_engine --fulfill --skip-readiness --cob-date <d>` per date (a designed path for
historical rebuilds), then **one** FULFILL run covering the whole span for the marts. EOD
before marts, because a mart built on a missing EOD partition writes a zero that looks like a
business collapse.

Backfilling the existing range reaches 10 dates — enough for day-on-day, flat between the two
event days, and short of the 14 a forecast needs. Real variation requires
`scripts/source-lab.sh workload <1-6> --execute` per business date, which is operator-gated.

**The first draft of the script would have failed at execute time, twice** — after the EOD
jobs had run and been paid for. `--flow-mode` is restricted to the `FlowMode` names, so
lowercase `fulfill` is rejected by argparse; and FULFILL takes a **span**
(`--from-date`/`--to-date`), not one call per date. Both found by reading the consumer rather
than the dry-run output, which printed happily.

**Added `docs/LIVE_MODE_SETUP.md`** with the capability table, the measured coverage, the
backfill path and the cost/stop notes. Linked from README, `DEMO.md`,
`AI_COPILOT_USER_GUIDE.md`.

Validator 14/14. Cost: five small Athena metadata queries.

---

## Session 52 — 90 seeded business dates, and a backwards certification ladder

**Asked for:** enough data in the mart to demo and test the AI.

**Delivered:** `scripts/seed-demo-mart.py` — 10,720 rows, **90 business dates**
(2026-06-25 → 2026-09-22), 120 accounts, deterministic. Written to
`kafka_dev_lab_dev_mart.mart_account_balance_daily_demo`, **never** the certified mart, and
read via `AI_MART_RELATION`. Every row carries `source_flow_mode = 'SEED_DEMO'`. The UI shows
a standing SEEDED DATA banner naming the relation.

The fixture is shaped so the engines have something true to find: a weekly cycle and drift
for the forecast, a **short partition on 2026-09-13** (40 of 120 accounts) so Diagnose reports
a pipeline fault rather than a business drop, a **+18% spike on 2026-09-18** for the anomaly
detector, and real `txn_count`/`debit`/`credit` because the live mart is `0.00` in all three.

**Verified on live Athena:** every Ask chip answers; Diagnose returns `DATA_INCOMPLETE`
(−65.9%, 40 rows vs typical 120) on the short day and `MOVED` (+32.1%) on the spike; Forecast
backtests 6 methods over 22 points and picks `ses` at 12.76% MAPE; Governance is `HEALTHY`.

**Found while doing it — the certification ladder was ordered alphabetically.**
`MIN(processing_status) AS weakest_status` exists so one provisional partition cannot hide
inside a certified answer; MIN on a varchar sorts `CERTIFIED` first, so the **strongest** tier
won the column named *weakest*. `ai/insights/pipeline.py` had the same defect independently.
Unreachable today because each date's rows share a tier — seeding mixed tiers would have made
it live. The SQL now emits a rank-prefixed value built from the same `STATUS_ORDER` tuple the
Python ladder uses, portable enough for the SQLite test harness.

**The test asserted the literal `MIN(processing_status) AS weakest_status`** — it pinned the
defect and passed for as long as the defect existed. It now asserts the ordering.

**Also fixed:** the two registries disagreed. `AI_MART_RELATION` reached the business metric
layer but not `ai/analytics/registry.py`, so the same page showed a 90-date series in Ask and
"no rows for this date" in Diagnose; the analytics SQL builders now go through
`spec.qualified()`. A `BREAKDOWN` with no queryable dimension called **no tool at all** and
returned "no tool was called; there is no evidence to answer from" — it now answers the total
and says what could not be broken down, which is how *"What is the average balance per
account?"* (6,349,626 VND) started working.

**Added `docs/SEEDED_DEMO_DATA.md`**; linked from README, `DEMO.md`, `LIVE_MODE_SETUP.md`.

Cost: ~90 small Athena INSERTs plus 10,720 Parquet rows under `warehouse/demo/`. The
certified mart is untouched; `DROP TABLE ..._demo` removes all of it.

---

## Session 53 — `--seeded` as a flag, not an exported variable

The 90 seeded business dates were reachable only through
`AI_MART_RELATION=…_demo python3 scripts/ai-ui.py`. Forgetting the variable is invisible:
the page reads the certified mart, reports "1 business date", and looks exactly as if the
seeding never happened — which is what a screenshot showed.

`scripts/ai-ui.py` now takes `--seeded`. It sets the relation itself, prints it on startup
beside the mode, refuses to combine with `--demo`, and **preflights the table**: if the
relation cannot be read it exits naming the command that builds it, rather than starting a
page whose every question fails inside Athena with "Table not found".

Three run modes, stated together in `LIVE_MODE_SETUP.md` and `DEMO.md`:

```bash
python3 scripts/ai-ui.py            # the real certified mart — 1 business date
python3 scripts/ai-ui.py --seeded   # live Athena, 90 SEEDED business dates
python3 scripts/ai-ui.py --demo     # in-memory fixture, 30 dates, no AWS, $0
```

Verified on `--seeded`: banner reads "Mart holds 90 business dates (2026-06-25 to
2026-09-22) — enough for every panel"; forecast returns 766,879,683 VND by `ses` at 12.2%
backtested error; day-on-day comparison and the anomaly score both answer.

Docs updated: `SEEDED_DEMO_DATA.md`, `LIVE_MODE_SETUP.md`, `DEMO.md`, and the seeder's own
closing hint. Validator 14/14, 351 links clean, 137 tests over the touched files.

---

## Session 54 — Screenshots in the repository

Four captures added under `docs/images/` and embedded in the README above the "Start here"
table, as a 2×2 with a caption each saying what the panel proves rather than what it shows:

| File | Proves |
|---|---|
| `copilot-ask-day-on-day.png` | a day-on-day change in 109 ms, badged `VERIFIED`/`CERTIFIED`, with re-runnable Athena query ids |
| `copilot-forecast-backtest.png` | six methods backtested over 21 walk-forward points; `ses` chosen at 13.32% MAPE; interval is ± the backtested error, not a fitted distribution |
| `copilot-refuses-missing-comparison.png` | on the real one-date mart, the value **plus** `comparison unavailable — the day before has no data; this is a single value, not a change` |
| `copilot-metrics-registry.png` | declared metrics with measure, owner, additivity, and the dimensions they may **not** be cut by |

The first two read the 90-date seeded relation (banner visible); the last two read the real
certified mart. A note under the table says so, because two screenshots of the same UI
showing different data is otherwise a thing a reviewer has to work out.

The refusal capture is deliberately included. It is the answer that was wrong until Session
49 — a bare certified number for a question that asked for a change — and showing the fixed
version is worth more than another green panel.

Also embedded in `DEMO_CAPTURE_GUIDE.md` (mapped to their capture numbers) and
`SEEDED_DEMO_DATA.md`. All four have alt text; 358 links and image paths resolve; the four
`<img src>` paths were checked separately because the markdown link checker does not see HTML
tags. Validator 14/14.

---

## Session 55 — Reliability screenshots, the rerun guide, and a gate that measured nothing

**Asked for:** the `reliability-ui.py` screenshots in the GitHub docs, plus an explanation of
the "green pipeline, wrong number" rerun — stressing that only the affected tables run.

**Delivered:** `docs/RERUN_ONLY_WHAT_BROKE.md` (230 lines) — the four
`reliability-recover.py` commands with real output, and the three independent limits that
decide "only what broke": lineage picks **which** assets (6 of 16), column confidence decides
**whether** the scope may narrow at all (`VALIDATED` only), and `RecoveryCapability` decides
**how finely a job can rebuild** (turn 0 stays `COB_DATE` while turn 2 drops to
`BUSINESS_KEY_SET`). Plus the six root causes that refuse a plan, the turn-order barrier, and
the policy gate. Eight screenshots added under `docs/images/`; two promoted to the README
hero block.

**Found while writing it — the RAG quality gate was measuring a retriever nobody calls.**
`ai/retriever.py` serves `ai/assistant.py`; the copilot and the gate both use
`ai/retrieval/service.py::Bm25Backend`. I fixed the wrong module first, and the tell was that
the gate passed with `MIN_QUERY_COVERAGE = 0.99` — a value that should have starved retrieval
entirely. Same shape as ADR-091: verified against a sibling, not the consumer.

The real defect: a definition question cited `RUNBOOK#iceberg-commit-conflicts` because
"deposits" appears in **0 of 708 chunks** and BM25 scores an unseen word like a ubiquitous one
(`idf.get(w, 0.0)`). Unknown terms now count at the corpus maximum, plus plural folding so
`conflict` and `conflicts` are one term. Measured: recall@5 unchanged at 0.6842, groundedness
**0.5614 → 0.5789**, negative_correct **0.25 → 0.75**. A tighter threshold that would have
removed the last bad citation cost recall@5 0.68 → 0.42 and was refused on the measurement.

**Also fixed:** the reliability UI hardcoded *"Five root causes cannot produce a plan"* and
listed five, omitting `SCHEMA_DRIFT` — the model says six. Now derived at render time.

Validator 14/14; 376 links and images resolve.
