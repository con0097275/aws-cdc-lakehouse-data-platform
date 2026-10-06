# Phase 18 — final production review

Audit date 2026-08-22. Account 111122223333 / ap-southeast-1.
**Audit only — no code was modified during the review.**

Reviewed as: Senior Data Architect · Data Engineer · Analytics Engineer · Airflow Engineer ·
Spark Engineer · SRE · Security · Cost.

## Verdict

**REPORTING_PLATFORM_NOT_READY** — 1 × P0, 4 × P1 open. Details in §2.

The data platform itself is in good shape: the layer semantics, the accuracy ladder, the
idempotency guarantees and the failure behaviour are all proven against live infrastructure
and pinned by tests. What is missing is everything that makes it *operate unattended* —
scheduling and alerting — plus one security defect.

## 1. What was audited, and what passed

| Area | Verdict | Evidence |
|---|---|---|
| Architecture adherence | **PASS** | Fan-out model documented and implemented; `docs/TARGET_ARCHITECTURE.md` §3 |
| 3-layer semantics | **PASS** | FULL_CDC canonical; REALTIME/EOD siblings; raw landing deprecated |
| Datamart source selection | **PASS** | `SourceLayerPolicy` closed enum; `resolve()` refuses `FULL_CDC_RAW` |
| Metadata correctness | **PASS** | 4 DynamoDB tables; every watermark names its execution |
| Config compiler | **PASS** | JSON-schema + semantic validation; invalid config never compiles |
| Dependency graph | **PASS** | Direct edges only; closure computed; cycle fails at compile |
| Turn calculation | **PASS** | Topological waves; retry keeps its turn (`test_zy_*`) |
| Airflow DAG shape | **PASS (code)** | 4 generic DAGs, `.expand()` per turn, `max_active_runs=1`, pools |
| Dynamic task mapping | **PASS (code)** | `reporting_common.py:148-152` |
| dbt manifest dependency sync | **PASS** | `make reporting-compile-dbt` derives `ref()` edges |
| dbt build | **PASS** | Real `dbt=1.9.4` on EMR: `PASS=3 WARN=0 ERROR=0` |
| Spark execution | **PASS** | 15+ EMR jobs, all SUCCESS |
| EMR Serverless | **PASS** | ARM64, auto-stop, no pre-init capacity |
| Iceberg MERGE | **PASS** | Guarded ladder; atomic per model |
| Idempotency | **PASS** | Re-run left FULL_CDC unchanged (live) |
| Watermarks | **PASS** | Advances only behind completed+validated runs (4 attack paths tested) |
| Late data | **PASS** | +999.99 traversed source→mart (live) |
| EOD | **PASS** | Gate needs ledger row AND Iceberg tag |
| AUTO_CORRECT | **PASS** | Refuses certified dates before submitting |
| FULFILL | **PASS** | Stamps RECONCILED; per-date outcomes resumable |
| STREAM_BATCH | **PASS** | Frozen upper bound committed |
| STREAMING_RT | **PARTIAL** | Lifecycle proven; **0 events processed** → P1-3 |
| Checkpointing | **PASS** | Read from S3; no `delete()` exists |
| Runtime state / DynamoDB | **PASS** | 41 executions; illegal transitions raise |
| Execution history | **PASS** | Terminal rows appended; skips carry a reason |
| Observability | **FAIL** | 6 alarms, all MSK. **Zero** on reporting → P1-2 |
| Schema evolution | **PASS** | Iceberg add-column by name; Apicurio BACKWARD |
| IAM | **PASS** | No wildcards outside KMS key policies; no IAM users |
| S3 policies | **PASS** | BPA, KMS, TLS-only, SSE-required |
| Terraform | **PASS** | `validate` clean; 0 NAT gateways |
| Cost | **PASS w/ risk** | $22.63 of $30; guardrails live; see P2-1 |
| Documentation | **PASS** | `make validate-docs` 14/14 |
| Tests | **PASS (partial)** | 698 non-Spark pass; Spark suite unexercised → P2-2 |
| Recovery | **PASS** | 20 injected failures, all correct |

## 2. Findings

### P0-1 — Nothing is scheduled. The orchestration layer has never run.

**Evidence.** `enable_airflow=false` (OPEN-28). The four flow DAGs and the lifecycle DAG
exist and are correctly shaped, but no Airflow deployment has ever executed them. Every
live result in Phase 14 was produced by `scripts/reporting-live-run.py`, which the file
itself describes as replacing *the scheduler*, not the flows.

**Impact.** A reporting platform that only runs when a human types a command is not in
production. Unproven in particular: retry **backoff**, pool exhaustion, scheduler-level
concurrency, catchup behaviour, and the dynamic task mapping actually expanding at runtime.

**Fix.** Deploy Airflow on k3s (`enable_airflow=true`, prompt 12), run one EOD cycle end to
end on the scheduler, and confirm turn 2 waits on turn 1 in a real DAG run.

**Cost to fix.** k3s node ~$0.04/hr plus the EMR jobs it triggers.

---

### P1-1 — Plaintext DB passwords in the connector config, the Kafka topic, and REST

**Evidence.** `artifacts/validation/session-32/connector-secret-handling.txt`.
`docker/cdc-runtime/connect-worker.properties:102-104` has the SSM config provider
**commented out**, so `register-connectors.sh` substitutes the literal secret into the JSON
before POSTing. The password is therefore in `GET /connectors/<name>/config`, in the
`connect-configs` topic (replicated, at rest), and in operator scrollback. The template's
own comment claims the opposite. All four SSM parameters are **Version 1** — never rotated
since the exposure.

**Impact.** Violates CLAUDE.md §3.1. Anyone with Connect REST access or topic read access
recovers the CDC credentials. Mitigating: lab-generated secrets, private-IP databases.

**Fix.** (1) stage `kafka-config-provider-aws` in `versions.env` with a real SHA256 and
mount it like the MSK IAM jar; (2) uncomment `config.providers`; (3) delete `resolve()`'s
`${ssm:...}` substitution so the placeholder survives into the POST; (4) **rotate all four
SSM parameters** and re-register.

---

### P1-2 — No alerting on the reporting framework

**Evidence.** 6 `aws_cloudwatch_metric_alarm` resources, all MSK broker health
(`offline_partitions`, `active_controller_*`, `under_replicated_partitions`, `broker_disk`,
`broker_cpu`). `terraform/modules/reporting_ops/main.tf` defines **no alarms**.

**Impact.** A failed EOD, a stale watermark, or an execution stuck non-terminal is
**silent** until a human queries DynamoDB. The framework's guards prevent *corruption*, not
*unnoticed stoppage* — and a certified mart that quietly stopped updating is worse than one
that visibly failed, because downstream consumers keep trusting it.

**Fix.** Alarms on: executions in a terminal-FAILED state in the last hour; any execution
non-terminal for longer than its `execution_timeout_seconds`; watermark age exceeding the
flow's schedule interval. All three read DynamoDB, so a metric filter or a small Lambda.

---

### P1-3 — The REALTIME layer is not materialised

**Evidence.** `reporting/layers.yaml` binds `REALTIME` to `kafka_dev_lab_dev_stream`, but
no job builds it from FULL_CDC. STREAM_BATCH runs against a bounded projection and records
`source_layer_substituted` on the execution.

**Impact.** Correct results, wrong cost profile: every micro-batch scans the full history
with a predicate instead of a small rolling table. At `*/10 * * * *` that is the dominant
cost driver of the whole platform. The substitution is honestly recorded, so this is a
performance and cost defect rather than a correctness one.

**Fix.** A job materialising `T-N → T` from FULL_CDC, then flip the binding to ACTIVE and
drop `substituted_for`.

---

### P1-4 — STREAMING_RT has processed zero events

**Evidence.** `is_enabled: false`; checkpoint EMPTY read from S3; no application submitted.

**Impact.** The lifecycle (register, observe checkpoint, restart, stop) is proven. Throughput,
backpressure, schema drift mid-stream, and the soft-delete sink are **not**. The mode
cannot be claimed as working.

**Fix.** Enable in a bounded window, drive real CDC through it, and reconcile its output
against FULL_CDC for the same window.

---

### P2-1 — `auto_destroy_after` is 6 days in the past

**Evidence.** `terraform/envs/dev/terraform.tfvars:30` = `2026-08-16T18:00:00Z`; today is
2026-08-22. Every resource carries a tag asserting it should already have been destroyed.

**Impact.** The tag is the automation contract for cleanup. A janitor honouring it would
delete live infrastructure; a human reading it cannot tell what is intentional.
**Do not "just fix" it**: the value feeds provider `default_tags`, which makes the AZ data
source unknown at plan time and forces replacement of all three subnets, MSK, EMR and all
three EC2 instances (`artifacts/validation/session-32/pre-phase14-blockers.txt`).

**Fix.** Change it only as part of a planned rebuild window, with the plan read first.

---

### P2-2 — The Spark test suite is not exercised in the fast loop

**Evidence.** 9 test files start a real `SparkSession` and exceed a 25s per-file budget;
they are excluded from every run in this session. 698 non-Spark tests pass.

**Impact.** The Spark-side contracts — L1/L2 envelope, ordering, quarantine, SCD2, DQ,
schema evolution — are only covered by tests nobody runs routinely. That is where a
regression hides longest.

**Fix.** Mark them `@pytest.mark.spark`, run them in a separate nightly target with a
session-scoped SparkSession fixture.

---

### P3-1 — 8,932 FULL_CDC rows carry NULL dual identity

**Evidence.** 20,390 total rows, 11,458 with `dv_event_id`.

**Impact.** None functionally: they predate the columns, are not in Kafka to re-derive, and
EOD is date-scoped so they cannot contaminate a result. Cosmetic and forensic only.

**Fix.** Leave them, or delete by `event_date` if a clean table is wanted.

### P3-2 — `terraform.tfvars` is not `fmt`-clean

Trivial. `terraform fmt terraform/envs/dev/terraform.tfvars`.

## 3. Checklists

### ARCHITECTURE CHECKLIST

| Item | Status |
|---|---|
| FULL_CDC canonical, written directly from Kafka | ✅ |
| REALTIME and EOD are siblings, not a chain | ✅ |
| No reporting job reads the raw landing | ✅ enforced by `resolve()` |
| Five execution modes, each with a fixed tier | ✅ |
| Accuracy ladder enforced in flow AND in SQL | ✅ both levels |
| EOD is a deterministic function of FULL_CDC + cutoff | ✅ |
| Adding a mart adds zero DAG files | ✅ asserted by test |
| Dependency closure computed, never authored | ✅ |
| REALTIME materialised as its own table | ❌ **P1-3** |

### SECURITY CHECKLIST

| Item | Status |
|---|---|
| No static credentials in Git | ✅ |
| No IAM users or access keys | ✅ |
| No inbound `0.0.0.0/0` | ✅ egress/routes only |
| SSM Session Manager only; no SSH, no key pairs | ✅ |
| S3 Block Public Access + KMS + TLS-only + SSE-required | ✅ |
| IAM least privilege per workload | ✅ no wildcards outside KMS key policies |
| Terraform state encrypted, locked, IAM-restricted | ✅ |
| Versions pinned; no `latest` | ✅ |
| Secrets resolved at runtime, never materialised | ❌ **P1-1** |
| Exposed credentials rotated | ❌ **P1-1** — all params still Version 1 |

### COST CHECKLIST

| Item | Status |
|---|---|
| NAT gateways | ✅ 0 |
| EMR auto-stop, max capacity, ARM64, no pre-init | ✅ |
| Athena bytes-scanned cutoff | ✅ 10 GB |
| Redshift / Trino behind flags, off | ✅ |
| Budgets configured | ✅ 3 |
| Every resource tagged | ✅ |
| Stop/destroy scripts with verification | ✅ |
| Spend within budget | ⚠️ **$22.63 of $30**, ~$5 left, ~$1.11/hr burning now |
| `auto_destroy_after` accurate | ❌ **P2-1** 6 days stale |
| STREAM_BATCH reads a small rolling table | ❌ **P1-3** scans full history every 10 min |

### DATA CORRECTNESS CHECKLIST

| Item | Status |
|---|---|
| Topic key = canonical PK, same PK → same partition | ✅ |
| FULL_CDC keeps every I/U/D/R | ✅ |
| Re-run is idempotent | ✅ proven live |
| `event_order` from source SCN/LSN, never Kafka offset | ✅ |
| EOD cutoff explicit; deletes handled | ✅ |
| MERGE key is business columns | ✅ |
| Lower tier cannot overwrite higher | ✅ proven live |
| Late data reaches the mart | ✅ +999.99 traversed end to end |
| Dual identity survives a cluster rebuild | ✅ |
| Streaming output reconciled against FULL_CDC | ❌ **P1-4** never ran |

### FAILURE-RECOVERY CHECKLIST

| Item | Status |
|---|---|
| Watermark advances only behind completed+validated | ✅ 4 attack paths |
| Failed run leaves no watermark | ✅ |
| Unconfirmed run finalised FAILED, never SUCCEEDED | ✅ codified |
| Illegal status transitions raise | ✅ |
| Concurrency guard independent of the scheduler | ✅ fired for real |
| Checkpoint cannot be deleted by any start path | ✅ no `delete()` |
| Dependency cycle fails at compile | ✅ |
| Unknown dbt vars rejected before dbt starts | ✅ |
| Iceberg commit atomic; failed commit leaves prior snapshot | ✅ |
| Operator runbook per failure class | ✅ 12 runbooks |
| **Alerting when any of this fires** | ❌ **P1-2** |

### TEST SUMMARY

| Suite | Count | Result |
|---|---|---|
| Non-Spark unit/integration | **698** | ✅ all pass |
| Phase 15 recovery (20 injected failures) | 24 | ✅ |
| Phase 16 datamart template | 8 | ✅ |
| Documentation validation | 14 | ✅ |
| Spark-session suites (9 files) | — | ⚠️ **not run** — P2-2 |
| Live E2E (Phase 14, 8 scenarios) | 8 | ✅ on real AWS |

### INFRA SUMMARY

| Resource | State |
|---|---|
| MSK `kafka-dev-lab-dev` | ACTIVE, 3 × kafka.m7g.large, IAM SASL |
| EC2 | source-lab t3a.xlarge, cdc-runtime t3.large, toolbox t3.small |
| EMR Serverless `00g84ii82onjs425` | ARM64, auto-stop 15 min |
| Glue | 7 databases |
| DynamoDB | 4 tables, PAY_PER_REQUEST |
| S3 | lake + state backend, KMS CMK, BPA |
| VPC endpoints | S3 + DynamoDB gateway (free), Glue interface |
| NAT gateways | **0** |
| Airflow | **not deployed** |

### KNOWN LIMITATIONS

1. **Nothing is scheduled** (P0-1) — every run to date was manual.
2. **No alerting** on reporting state (P1-2).
3. **REALTIME is a projection**, not a table (P1-3) — correct results, wrong cost.
4. **STREAMING_RT has processed no events** (P1-4).
5. **Spark tests unexercised** in the routine loop (P2-2).
6. **Single environment.** No staging; `dev` is the only environment ever applied.
7. **Single-region, single-AZ EMR endpoint.** No DR story.
8. **One pilot mart** carries the framework. `mart_channel_engagement_daily` compiles but
   has never been built against data.

### OPERATIONS RUNBOOK INDEX

`docs/runbooks/` — new-mart · manual-rerun · eod-retry · auto-correct-repair ·
fulfill-backfill · stream-batch-recovery · streaming-rt-restart · checkpoint-recovery ·
watermark-investigation · dependency-investigation · runtime-state-investigation ·
iceberg-rollback

## 4. Mandatory gates

| Gate | Required | Actual | Pass |
|---|---|---|---|
| Data correctness proven on live data | yes | yes | ✅ |
| Failure recovery proven | yes | yes | ✅ |
| No credential materialised at rest | yes | **no** | ❌ P1-1 |
| Scheduled execution proven | yes | **no** | ❌ P0-1 |
| Alerting on failure | yes | **no** | ❌ P1-2 |
| Cost guardrails | yes | yes | ✅ |
| Documentation + runbooks | yes | yes | ✅ |

**Three mandatory gates fail. The platform is NOT production ready.**

---

# 5. Remediation — 2026-08-22

All five blockers approved. Three fixed and verified; two remain.

## ✅ P1-1 — Credentials no longer materialised. FIXED and VERIFIED.

The original plan needed `SsmParamStoreConfigProvider`, which is **not published to Maven
Central** — every version 404s. That is what F7 recorded as "unobtainable", and the fallback
(substituting the secret at registration time) *was* the defect.

`FileConfigProvider` ships **inside kafka-clients** — verified
`org/apache/kafka/common/config/provider/FileConfigProvider.class` in
`kafka-clients-7.7.1-ccs.jar`. Nothing to download, nothing to pin, no supply chain.

| Change | File |
|---|---|
| Provider enabled | `connect-worker.properties` |
| `${ssm:...}` → `${file:...}` | both connector templates |
| Secret substitution **removed** | `register-connectors.sh::resolve()` |
| Secrets file written from SSM at 0640 | `cdc_runtime_ec2/templates/user-data.sh.tftpl` |
| File mounted read-only | `docker-compose.yml` |
| **Both CDC passwords rotated** | SSM Version 1 → **2** |

**Evidence:**
```
render output                     : 0 secrets, 2 placeholders
GET /connectors/<n>/config        : ${file:/opt/cdc-runtime/secrets/source-lab.properties:oracle_cdc_password}
connector status                  : RUNNING ['RUNNING']  (both)
secrets file                      : 640 root:ec2-user, NOT world-readable
```

Both connectors RUNNING proves the provider resolved the secret at task start **and** the
databases accepted the rotated credential. One wrinkle worth recording: the file was first
written `0600 root` and the container runs as uid 1000, so `FileConfigProvider` failed with
`Could not read properties from file` and the POST returned 500. Fixed by
`root:<container-gid> 0640` rather than by making it world-readable.

## ✅ P1-2 — Alerting. FIXED (code + alarms defined).

`spark/reporting/metrics.py` publishes from the framework itself rather than from a scanner
Lambda: a scanner that dies stops alerting, and nothing alerts on the scanner. Wired into
`ops_client.finish_successfully` and `fail`, published **after** every durable write and
best-effort — a CloudWatch outage must not turn a committed run into a failed one.

`terraform/modules/reporting_ops/alarms.tf` — three alarms:

| Alarm | Catches | `treat_missing_data` |
|---|---|---|
| `reporting-execution-failed` | the loud failure | `notBreaching` |
| `reporting-no-success` | **the quiet failure** — platform stopped entirely | **`breaching`** |
| `reporting-watermark-lag` | runs succeed but certify nothing new | `missing` |

The middle one is the point. A count-based alarm cannot express "nobody published anything",
and a certified mart that quietly stopped updating is worse than one that visibly failed.
4 tests. **Not yet applied** — needs `terraform apply`.

## ✅ P1-3 — REALTIME materialised. FIXED and VERIFIED.

`spark/jobs/realtime/job.py`; job `00g86arat9tmo827` SUCCESS.

```
REALTIME_WINDOW hours=72 cutoff=2026-08-19 08:26:22
REALTIME_SOURCE_ROWS full_cdc=20390 in_window=20390
REALTIME_ROWS 20390   REALTIME_SNAPSHOT 4892274060029516593
```

Full replace, not incremental: a rolling window shrinks from the back as well as growing at
the front, and a MERGE cannot express "these rows aged out" — it would accumulate forever
and quietly stop being a window. It **refuses to publish an empty window**, because an empty
REALTIME table is indistinguishable to STREAM_BATCH from "nothing changed", and would
advance a watermark past rows never read.

## ❌ P0-1 — Airflow. NOT FIXED.

Requires deploying k3s and proving one scheduled EOD cycle. Not attempted.

## ❌ P1-4 — STREAMING_RT. NOT FIXED.

Requires a bounded window with a live application and reconciliation against FULL_CDC.
Not attempted.

## Regression after fixes

```
176 reporting tests    PASS
14  documentation      PASS
terraform validate     Success
```

## Revised gate status

| Gate | Before | After |
|---|---|---|
| No credential materialised at rest | ❌ | ✅ |
| Alerting on failure | ❌ | ⚠️ code done, **not applied** |
| Scheduled execution proven | ❌ | ❌ |

**Still NOT production ready.** P0-1 is untouched, and P1-2's alarms exist only in code.

## Pre-existing defect found during remediation

`spark/tests/test_reporting_eod_flow.py` defines
`test_an_engine_still_running_never_advances_the_watermark` **twice** (lines 497 and 819).
Python keeps the last, so the first is dead code that has never run. The surviving one hangs
past 90s. Not caused by this work — recorded as **P2-3**.

## P1-4 — STREAMING_RT: ATTEMPTED, NOT COMPLETED. Two defects found.

Enabled, compiled, and submitted twice to EMR Serverless. Both runs failed *before*
processing an event, each on a different real defect:

**Defect A — `--plan` could never read S3.** `streaming_rt_app.py` advertised
"local or s3://" and called plain `open()`. On EMR the plan is ALWAYS in S3, so the
documented form was the only one that mattered and the only one that did not work:

```
FileNotFoundError: [Errno 2] No such file or directory: 's3://.../plan.json'
```

**FIXED** — `_load_plan()` now handles both. This defect alone means STREAMING_RT could
never have run on EMR as written, which is consistent with it never having processed an
event.

**Defect B — `jsonschema` absent from the EMR runtime.** `config_loader` imports it at
module level to validate config, even though the app reads an ALREADY-COMPILED plan and has
nothing to validate:

```
ModuleNotFoundError: No module named 'jsonschema'
```

**NOT FIXED.** Two honest options, neither a five-minute change:
1. make the import lazy so a compiled-plan reader never needs it — the cleaner fix, since
   validation belongs to compile time, not to a streaming worker;
2. bundle the wheels — awkward because `rpds-py` is a compiled extension and does not
   import reliably from a `--py-files` zip.

`is_enabled` returned to `false`, so the config states what is true: this mode does not run.

**P1-4 remains OPEN.** What the attempt did establish is *why* it never worked — two
concrete blockers rather than an untested mode.

## P1-4 — STREAMING_RT: SUBSTANTIAL PROGRESS. Five defects found, four fixed. Still OPEN.

Seven submissions. Each failed on a *different* real defect, and the chain is the finding:
**this app had never been executed**, so every one of these had sat in the code unnoticed.

| # | Defect | Status |
|---|---|---|
| 1 | `--plan` advertised `s3://` but called plain `open()` | **FIXED** — `_load_plan()` |
| 2 | `jsonschema` imported at module scope; absent from EMR runtime | **FIXED** — lazy import |
| 3 | `yaml` same | **FIXED** — lazy import |
| 4 | `networkx` transitively required | **FIXED** — vendored (pure Python, 0 `.so`) |
| 5 | `NoRegionError` — boto3 had no region | **FIXED** — driverEnv/executorEnv |
| 6 | `DynamoDbRuntimeStateRepository(resource)` — takes 4 keyword TABLES | **FIXED** |
| 7 | `ops.streaming_batch_ledger` does not exist | **OPEN** |

**The stream RAN.** Evidence — Structured Streaming wrote a real checkpoint to S3:

```
checkpoints/reporting/dev/mart_account_balance_daily/STREAMING_RT/offsets/0        708 B
checkpoints/reporting/dev/mart_account_balance_daily/STREAMING_RT/commits_$folder$
checkpoints/reporting/dev/mart_account_balance_daily/STREAMING_RT/sources/0/offsets/0
```

`offsets/0` means it resolved its source, opened the stream against FULL_CDC and committed
batch 0's offsets. This is no longer "a mode that has never started". What remains is one
missing table, not an unknown.

Defects 2–4 are one finding wearing three hats: **the streaming worker transitively imports
the whole compile-time stack.** Lazy imports fix the symptom; the structural fix is that a
runtime worker reading a COMPILED plan should not import the compiler at all.

`is_enabled` returned to `false`. **P1-4 remains OPEN** — one `CREATE TABLE` and a rerun.

---

# 6. Session close — P1-4 root cause found; P0-1 not attempted

## P1-4 — nine defects, eight fixed. The ninth is ARCHITECTURAL.

| # | Defect | Status |
|---|---|---|
| 1 | `--plan` advertised `s3://`, called plain `open()` | FIXED |
| 2 | `jsonschema` imported at module scope, absent from EMR | FIXED (lazy) |
| 3 | `yaml` same | FIXED (lazy) |
| 4 | `networkx` transitively required | FIXED (vendored, pure Python) |
| 5 | boto3 `NoRegionError` | FIXED (driverEnv) |
| 6 | `DynamoDbRuntimeStateRepository(resource)` — takes 4 keyword tables | FIXED |
| 7 | `ops.streaming_batch_ledger` missing | **FIXED** — created, Iceberg v2 |
| 8 | `UPDATE SET *, t._is_deleted = ...` is invalid Spark SQL | FIXED (flag projected into source) |
| 9 | **Sink schema does not match the target mart** | **OPEN — needs a design decision** |

### Defect 9, precisely

`IcebergMergeSink` builds its ON clause from the job's `primary_key`
(`account_sk, business_date`) but its source view carries a GENERIC CDC shape:

```
business_key STRING, business_date DATE, event_order BIGINT,
event_time TIMESTAMP, operation STRING
```

so the MERGE resolves `s.account_sk` against a view that has no such column:

```
UNRESOLVED_COLUMN.WITH_SUGGESTION: `s`.`account_sk` cannot be resolved.
Did you mean [`x`.`account_sk`, `x`.`customer_sk`, `x`.`txn_count` ...]
```

The streaming path was written against an assumed generic target that does not exist. The
mart has business columns (`closing_balance`, `customer_sk`, `txn_count`); the sink emits
CDC envelope columns. **They were never reconciled because the app had never run.**

Two honest options, both design decisions rather than patches:

1. **Map in the sink** — the sink projects CDC rows into the target's business schema. Needs
   a per-job column mapping, which is config the framework does not yet have.
2. **Give STREAMING_RT its own target** — a streaming table in the CDC shape, reconciled
   into the mart by a separate batch step. Keeps the sink generic and matches ADR-041's
   position that the stream's output must be reconcilable against FULL_CDC.

Option 2 is more consistent with the existing architecture. Either way it is an ADR, not a
fix, and should not be decided at the end of a session.

**What the attempt DID establish:** the stream starts, resolves its source, opens against
FULL_CDC, commits checkpoint offsets (`offsets/0` in S3), reaches its sink, and fails at a
named, understood mismatch. That is a very different state from "has never run".

## P0-1 — Airflow: NOT ATTEMPTED

No k3s deployment was made. Scheduled execution remains unproven.

## Cascading IAM destruction — recorded for the third time

The targeted MSK destroy again stripped `lake_iam` policy attachments:
`kafka-dev-lab-dev-spark-eod` and `-spark-stream` finished with **zero** attached policies.
Jobs using them fail with `kms:GenerateDataKey ... no identity-based policy allows`. The
`reporting` role survives because it lives in `reporting_ops`, not `lake_iam`.

**Anyone re-running after an MSK destroy must re-apply `lake_iam` before submitting Spark
jobs**, or use the `reporting` role. This is now a predictable consequence, not a surprise.
