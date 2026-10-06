# Session Handoff

> ## 2026-09-17 — PHASE B. `FULL_CDC_STREAMING_PRODUCTION_READY` (STATIC_PASS + LOCAL_PASS)
>
> Kafka → FULL_CDC streaming is hardened in code, config, tests and docs (ADR-074). **It has
> not run on EMR.** Do not read this checkpoint as live-tested.
>
> **The platform is LIVE and billing** (MSK ACTIVE, 4 EC2 running, EMR STARTED; read-only
> check 16:3xZ). Stop it or set an alarm before anything else.
>
> ### What changed
> * The ingest writes `ops.streaming_app_state` (was: 0 rows, no writer), one throttled row per app.
> * The checkpoint is derived from the plan (was: dead code behind `required=True`).
> * A fresh checkpoint starts `earliest` (was `latest`, which would drop ~1,600 LOAN events).
> * `full_cdc_streaming_{start,monitor,stop}` DAGs: paused, flag-gated, EMR STREAMING runs.
> * `scripts/cdc-stream.py` (read-only) and `streaming-reset.sh --app-id`.
>
> ### Verify
> ```bash
> make check-all                       # exit 0, 2,464 passed
> python3 scripts/cdc-stream.py apps   # no AWS
> python3 scripts/cdc-stream.py health # Athena; today ABSENT/exit 1, which is correct
> ```
>
> ### Next — Phase B live acceptance, then Phase C
> Run `docs/FULL_CDC_STREAMING.md` §7 in a CDC window: two `available_now` drains on
> `full-cdc-oracle`, which must RESUME (restart_count 1→2, offsets continue, rows ==
> distinct `dv_event_id`). Then commit: nothing since 2026-09-03 is committed.
> **Phase C (REALTIME config-driven cadence) is not started, by instruction.**

---

> ## 2026-09-10 — PHASE 8 COMPLETE. `CONFIG_DRIVEN_CDC_TABLE_PLATFORM_PRODUCTION_READY`
>
> The config-driven CDC table platform is accepted against **two genuinely new tables** on
> **two engines** and **two payload modes**, onboarded by registry entry alone. See
> `docs/CDC_TABLE_PLATFORM_ACCEPTANCE.md` (18-point checklist) and ADR-069.
>
> ### THE LIVE CAPTURE LEG IS CLOSED
>
> `register-connectors.sh update --execute` was run by a human, and the full path now works
> end to end for both new tables: source → Debezium → Kafka → FULL_CDC → REALTIME.
> `loan` holds 4 rows (c/u/d), `payment_method` 5. `dual_write` put the same events in the
> legacy monolith (20,390 → 20,400) and the counts match exactly.
>
> Getting there found **four more silent-failure defects**, all from derived artifacts kept
> in sync by hand — see ADR-070:
>
> 1. **no Kafka topic** — `auto.create.topics.enable=false`, so Debezium cannot create one;
>    both connectors reported RUNNING and produced nothing, every check green.
>    Fixed by `scripts/cdc-topics.py` (registry-derived, exits non-zero).
> 2. **stale `schemas.json`** — the ingest decoded with a fallback and died with
>    `FIELD_NOT_FOUND: no such struct field 'op'`, which names a field and not the cause.
>    It genuinely cannot be exported before capture starts.
> 3. **`per_table` missing from the deploy zip** — `ModuleNotFoundError` after acquiring
>    capacity, because EMR downloads only the entrypoint.
> 4. **Kafka jars absent for the `stream` role** — `Failed to find data source: kafka`.
>
> …and two in the **acceptance gate itself**, the last thing standing between a table and
> being trusted downstream:
>
> 5. **the judge PASSed anything truthy** — `"FAILED"` for a connector state, `"0"` for a
>    row count, `"probably"` for a boolean. Evidence is now type-checked per check.
> 6. **a genuinely capturing table could never reach ACTIVE** — the walk rightly refuses to
>    claim `CAPTURING` from a template, and the confirmation gate forces the connector update
>    to happen out of band, so the ledger was stuck at `PROVISIONED`. Smoke now advances it
>    on observed records + FULL_CDC rows, and only on those.
>
> ### Live state right now
>
> * Both connectors **RUNNING**, now capturing 10 tables (the two new ones included).
> * 10 registered tables, **0 governance gaps**. `loan` and `payment_method` are **ACTIVE**;
>   the other 8 are DRAFT in the ledger (they predate it — they were never walked).
> * `eod_oracle_coredb_corebank_account` holds COB 2026-08-21: **320 rows / 320 keys**,
>   `CERTIFIED` in `ops.eod_run`, matching an independent Athena count.
> * The three shared OPS tables (`cdc_event_index`, `realtime_run`, `eod_run`) now exist —
>   they never had, through six phases.
> * Legacy monolith at **20,400** rows (was 20,390). The +10 is the two new tables' events
>   arriving via `dual_write`, which is what that mode is for -- not drift. Still the
>   reconciliation baseline, and still never dropped.
>
> ### Three defects this phase found, all previously green in tests
>
> 1. `write_ledger` was best-effort → a close reported `CERTIFIED` with no evidence and
>    exit 0. Now `STATUS_UNVERIFIED`, non-zero exit, data retained.
> 2. `--table` provisioning silently skips the shared OPS targets → they had never been
>    created. The skip stays; it now says so.
> 3. `maintenance.actions` serialised "unset" as `[]` → maintenance was a **platform-wide
>    no-op** that looked like a platform with nothing to do.
>
> ### Verify before you touch anything
>
> ```bash
> make check-all      # static gates + the full suite. THIS is the command.
> ```
>
> `make check` and `make test` are separate and neither implies the other. A full pass found
> the committed plan artifact two table onboardings stale — green under `pytest` the whole
> time, because `cdc-verify` was missing from `make check`. Both holes are closed
> (`cdc-verify` is in `check`; `test_the_committed_plan_artifact_is_not_stale` is in the
> suite and recomputes the hash from the file's own content rather than trusting the number
> the file states about itself).
>
> ### Next
>
> Decide whether to cut `loan` and `payment_method` over from `dual_write` to `PER_TABLE`
> (`scripts/cdc-cutover.py gate --table <id>`). Open issues are at the end of
> `IMPLEMENTATION_REPORT.md`; **#7 — no Airflow DAG schedules the per-table platform** — is
> the largest: every run this session was a manual `emr-submit.sh`.

---

> ## 2026-09-04 — POST-LIVE DEFECT CLOSURE. Platform DOWN, $0/hr. Ready to resume.
>
> Nothing here ran against a live platform: MSK, EC2, EMR, NAT, RDS, endpoints and EIPs are
> all **zero**. The only spend is S3 (300 MB in the lake) and **9 customer KMS keys at
> $1/key/month ≈ $0.30/day**, four of which are already scheduled for deletion.
>
> ### Data safety, checked this session
>
> Every one of the 1,122 objects under `warehouse/ artifacts/ checkpoints/ bootstrap/
> models/ ops/` is on an **Enabled** CMK. Nothing expires overnight. `2fd510a7`, which
> holds 1,818 objects, is Enabled — the earlier `cancel-key-deletion` took effect.
>
> One real loss, already done and not recoverable: **161 objects are encrypted with
> `d1f568fd…`, a CMK that no longer exists**. They are all under `logs/` (57) and
> `query-results/` (104) — the two prefixes the re-encryption script deliberately excludes.
> EMR driver logs from a previous lab generation and Athena result sets. No warehouse data.
>
> ### What changed in code
>
> Seven scorecard criteria moved from FAIL/BLOCKED to **`FIXED, AWAITING LIVE RE-TEST`**.
> That is not a PASS and the PASS count is unchanged at 28. See
> `artifacts/validation/final-e2e/FINAL_TEST_SUMMARY.md` § *What to run tomorrow*.
>
> | Finding | Fix |
> |---|---|
> | G-P1-1 | `full_cdc/job.py` decodes PERMISSIVE, quarantines the row **and writes the payload** it had always only referenced |
> | E2 | writer schema chosen per record by `apicurio.value.globalId`; an unexported id is quarantined, never guessed |
> | item 41 | `scripts/ai-feature-run.py` — feature materialisation + anomaly scoring from the live mart |
> | test harness | one session-scoped `SparkSession`; the full suite runs **1693 passed / 0 errors** (was 1616 + 61 errors) |
>
> ### Artifacts restaged to S3 (backed up first, under `.superseded-2026-09-03/`)
>
> ```
> artifacts/code/full_cdc_job.py       the G-P1-1 + E2 fixes
> artifacts/code/eod_job.py            } re-staged so the lake matches the repo
> artifacts/code/realtime_job.py       }
> artifacts/realtime/*.py, ddl.sql     STREAMING_RT (rtlib.zip was already current)
> artifacts/dbt/framework-flat.zip     8 members had drifted from the repo (finding E3)
> ```
>
> **Read this before the first reporting run tomorrow.** The refreshed `framework-flat.zip`
> replaces 7 flow modules and 1 job YAML that had drifted; the *old* zip is what the
> 2026-09-03 live window proved. The repo is the source of truth (E3 exists because staged
> code drifted), but treat the first AUTO_CORRECT / EOD run as a verification, and roll back
> with `aws s3 cp s3://<lake>/artifacts/dbt/.superseded-2026-09-03/framework-flat.zip
> s3://<lake>/artifacts/dbt/framework-flat.zip` if it misbehaves.
>
> **`schemas.json` in the lake is still the LEGACY topic-keyed export.** The job handles it
> — it falls back by topic and prints `LEGACY EXPORT … run cdc-runtime.sh export-schemas` —
> but the schema-evolution test (item 38) needs the refreshed export first.

> ## 2026-09-03 (late) — STREAMING_RT BUILT AND RUN LIVE. Platform still up.
>
> A four-app realtime serving layer, modelled on the production system at
> `plstream_merged/.../v9` and run end to end on the live platform. Design + evidence:
> **`docs/REALTIME_STREAMING_RT.md`**, artifacts in
> `artifacts/validation/final-e2e/realtime/`.
>
> ```
> rt_eod_base.py     BASE 321 rows, 0 incomplete, watermark 2026-09-03 10:16:40
> rt_stream_app.py   LONG-RUNNING: 4 micro-batches / 240s, 332 facts, 1 flagged,
>                    43 resolver cycles
> rt_autocorrect.py  worklist 1 -> repaired 1 -> still_incomplete 0, BASE 322
> rt_datamart_app.py LONG-RUNNING: 9 cycles / 180s, 17.8s -> 3.1s (source cache)
> ```
>
> The demonstration is a **late dimension**: an account published with NULL dims and
> flagged into a pointer table, then repaired by the correction pass from a fresher source
> and the pointer resolved. Reconciles exactly — view total 2,031,894,187.94 vs the
> session's ground truth 2,031,880,937.77, difference 13,250.17 fully explained.
>
> **Two defects found by running it, both fixed and re-proven:** PySpark `DecimalType`
> rejecting a Python float at the write boundary, and the pending resolver only running
> inside a micro-batch (so a flagged row starved when the source went quiet — fixed with a
> dedicated 5s resolver thread).
>
> **Known gap, documented not hidden (RT-1):** AUTOCORRECT repairs BASE but does not
> advance the watermark, so the view serves the stale STREAM row until EOD. Identical to
> the reference implementation, which names the same gap.
>
> Also fixed this session: the AI copilot UI (`scripts/ai-ui.py`) — three separate bugs,
> see the git log. Tests: **1,595 pass**, validate-docs **14/14**.
>
> **THE PLATFORM IS STILL RUNNING AND BILLING (~$1.23/hr).** Teardown:
> `reencrypt-lake-cmk.sh reencrypt --execute` FIRST, then `tf.sh destroy --execute`.


> ## 2026-09-03T09:06Z — PLATFORM IS LIVE AND BILLING. 292 resources. ~$1.2340/hr.
>
> The operator applied the reviewed plan in an interactive terminal. Terraform state went
> **3 → 292**; git HEAD `1d3cf5b`. Drift re-check: **0 add, 1 change, 0 destroy** — the one
> change is the known non-converging `logs/airflow/` object (G-P3-3), benign.
>
> **Verified running:** MSK ACTIVE (3 × m7g.large, 3.9.x.kraft) · source-lab Oracle
> ARCHIVELOG + SQL Server, 4 tables tracked · cdc-connect + Apicurio healthy · **both
> Debezium connectors RUNNING, task 0 RUNNING** · 17 topics incl. all 8 per-table CDC
> topics · EMR Serverless CREATED · Airflow all pods Running, UI 200 · DynamoDB 4/4 ACTIVE
> · Glue 7 DBs · Athena wg ENABLED, 10 GiB cutoff enforced · NAT 0 · AI runtime $0/hr.
> CDC password chain hash-verified across SSM → `.env` → Connect secrets.
>
> ### TWO BLOCKERS BEFORE ANY SPARK JOB
>
> 1. **Lake CMK divergence.** The apply minted a new CMK `2fd510a7` and granted the
>    Spark/reporting roles **only** that key, but the pre-existing lake objects sit on
>    `383f6d53` (865), `44bf584e` (258), `8d20ebe1` (254), `e66f4dfa` (40) and 161 on a
>    **deleted** key. Any Spark read of existing data fails `kms:Decrypt AccessDenied`,
>    which looks like a KMS fault rather than an S3 one. Fix, ~2 min, copy-in-place:
>    `bash scripts/reencrypt-lake-cmk.sh reencrypt --execute` (phrase `REENCRYPT LAKE`).
> 2. **Glue catalog is empty — 0 tables in all 7 databases.** The apply recreated the
>    catalog, so every Iceberg metadata pointer is gone. The S3 data survived but is
>    orphaned from the catalog; `CREATE TABLE IF NOT EXISTS` will make a new empty table at
>    the same path. Fine for a fresh E2E run, but **"lake preserved" ≠ "data queryable"**,
>    and it is why `athena-smoke.sh` returns `BLOCKED_BY_DESIGN` on all six queries.
>
> ### Standing risk
>
> `auto_destroy_after` is `2026-08-25T00:00:00Z` — **expired**. Every resource is tagged
> already-expired, so ADR-027 provides no protection. **A wall-clock alarm is the only
> thing that will bring this lab down.** True window start `09:06:04Z` (the gate printed a
> stale `2026-08-16` stamp); true baseline `$1.2340/hr` (the gate's `1.1244` constant
> predates the Glue endpoint and the airflow/reporting resources).
>
> Five operator-tooling defects found by running it: see
> `artifacts/validation/final-e2e/infra/50-infra-running-verified.txt` (D1–D5).
>
> ---
>
> ## 2026-09-03 (earlier) — bring-up requested and blocked. Nothing applied. $0.00.
>
> The operator approved the live E2E window and asked for the minimum bring-up. The gate
> was re-verified and **passed on every point**: account `111122223333`, region
> `ap-southeast-1`, workspace `default`, backend profile pinned, provider `6.56.0` exact,
> state unchanged at 3 resources, and the **saved plan is still valid and fresh** —
> `248 create / 0 change / 0 destroy / 0 replace`, sha256 `c70ba8a7…`, with no plan input
> modified since it was generated. Burn is **$0.0000/hr**.
>
> **Apply did not run.** Three blockers, in `artifacts/validation/final-e2e/infra/`:
>
> 1. **`auto_destroy_after` is `2026-08-25T00:00:00Z` — 9 days expired.** Its `validation`
>    block checks ISO-8601 *format*, not recency, so it passes silently and would stamp all
>    248 resources already-expired, defeating ADR-027. **Fix it now:** with 3 resources in
>    state nothing is replaced. Once MSK exists, changing this value replaces all 6 subnets
>    and cascades into MSK, EMR and every EC2 instance (OPEN-34).
> 2. **`monthly_budget_usd = 100`** fails the `<= 50` check assertion (`main.tf:95`).
> 3. **The approval gate cannot be satisfied by an agent, by design.** `tf.sh apply
>    --execute` → `confirm_destructive()` → `lib.sh:161` dies when stdin is not a TTY.
>    Captured in `infra/01-apply-refused.txt`. `-auto-approve` was not used and bare
>    `terraform apply` was not run — that is what this control exists to prevent.
>
> **To resume — three commands in a real terminal:**
>
> ```bash
> # 1. edit terraform/envs/dev/terraform.tfvars:
> #      auto_destroy_after = "<window end + 2h, UTC>"
> #      monthly_budget_usd = 50
> bash scripts/tf.sh plan                    # expect 0 to change, 0 to destroy
> bash scripts/tf.sh apply --execute         # type: APPLY THE SAVED PLAN
> bash scripts/cdc-window-start.sh           # exit 0 REQUIRED before any test
> ```
>
> Then follow the verification runbook in `artifacts/validation/final-e2e/infra/README.md`
> and the tests in `docs/E2E_LIVE_TEST_PLAN.md` §8. Cost: **$1.2340/hr** baseline, hard cap
> **$8.00 (2h) / $15.00 (4h)**. MSK is 62% of burn and cannot be stopped, only destroyed.

- **Session completed:** **20 — cheap-tier foundation APPLIED and VALIDATED**
- **Date:** 2026-08-15
- **Git branch:** `session-02-prerequisites` (tree clean)
- **Terminator:** **FOUNDATION_READY_PENDING_F1_PROPAGATION**
- **Previous:** 19 — final acceptance

**Foundation live: 32 resources, drift 0, idle ~$1.01/month (the lake KMS CMK).
MSK never created — `enable_kafka_platform=false`. Nothing bills by the hour.**

## What is live now

| | |
|---|---|
| S3 lake | `kafka-dev-lab-dev-lake-111122223333` — SSE-KMS, versioned, TLS-only, anon GET 403, 13 prefixes, 6 lifecycle rules |
| Glue | 7 databases, **0 tables** (Spark creates Iceberg tables in the CDC window) |
| Athena | `kafka-dev-lab-dev-wg` — 10 GiB cutoff **enforced**, results KMS-encrypted |
| KMS | 1 customer key, rotation enabled, alias resolves |
| Budget | tag-filtered $30, 3 active notifications |
| Drift | `detailed-exitcode=0`, all 32 `no-op`, 0 replace, 0 destroy |

## Two defects found and fixed

**F1 — the $30 budget could never have fired.** Every cost-allocation tag in the account
was `Inactive`, so the tag-filtered budget would have read $0.00 forever. The Terraform
filter string was correct; activation is an account-level Billing setting Terraform does
not manage, which is why no code review caught it. Six tags activated 2026-08-15T12:21Z.
**Still BLOCKED** — AWS does not backfill and takes ~24h, so verify after
**2026-08-16T12:21Z** (19:21 Asia/Ho_Chi_Minh):

```bash
aws budgets describe-budget --account-id 111122223333 \
  --budget-name kafka-dev-lab-dev-monthly --query 'Budget.CalculatedSpend'
```

**F2 — the Athena smoke queries could never have passed anywhere.** They hardcode
unprefixed schemas (`stream`, `mart`) while the deployed databases are
`kafka_dev_lab_dev_*`. Proven by contrast: unprefixed → `SCHEMA_NOT_FOUND`, prefixed →
`TABLE_NOT_FOUND`. Fixed with `scripts/athena-smoke.sh`, which substitutes the prefix
from `terraform output` so the SQL stays environment-agnostic.

## Next session — the CDC window

Six typed phrases remain, all needing a real interactive terminal:
`ENABLE CDC`, `SEED SOURCE LAB`, `RECORD CHECKSUMS`, `CREATE TOPICS`,
`REGISTER CONNECTORS`, `RUN WORKLOAD n` — plus `APPLY THE SAVED PLAN` for the MSK tier.

**Before starting it:** MSK is ~$0.7749/hr and **cannot be stopped, only destroyed**.
Full stack ~$1.53/hr; a 6–8 hour window is ~$9–12. Set a wall-clock alarm — budget
alerts arrive hours late, and F1 means the tag-filtered budget may still be reporting
$0.00 when you start.

## Superseded — the earlier part of this session

## What was asked, and what actually happened

The operator approved final deployment and the full end-to-end run. Deployment was
attempted. `scripts/bootstrap-state-backend.sh --execute` printed its identity guard,
confirmed account `111122223333`, and then refused:

```
REFUSING: refusing to run non-interactively; a human must type the confirmation phrase
```

This is not a bug and it was not worked around. `confirm_destructive()` at `lib.sh:161`
refuses when stdin is not a TTY, and its own comment says why: *"Refuses outright when
stdin is not a terminal, so no automation can satisfy it."* `scripts/validate-docs.py:515`
asserts that refusal still exists, so removing it would fail `make check`.

Faking a TTY would have defeated the one control this project is built around. The
approval to spend is real; the mechanism that requires a human hand at the moment money
starts being spent is separate, deliberate, and was left intact.

**It is not one gate.** Eight typed phrases stand between here and a completed
end-to-end run:

| # | Phrase | Script | Guards |
|---|---|---|---|
| 1 | `BOOTSTRAP STATE BACKEND` | `bootstrap-state-backend.sh` | everything — gates all 14 modules |
| 2 | `APPLY THE SAVED PLAN` | `tf.sh` | the Terraform apply itself |
| 3 | `ENABLE CDC` | `source-lab.sh` | restarts Oracle |
| 4 | `SEED SOURCE LAB` | `source-lab.sh` | loads seed data |
| 5 | `RECORD CHECKSUMS` | `cdc-runtime.sh` | downloads connector artifacts |
| 6 | `CREATE TOPICS` | `cdc-runtime.sh` | creates Kafka topics |
| 7 | `REGISTER CONNECTORS` | `register-connectors.sh` | starts capture |
| 8 | `RUN WORKLOAD n` | `source-lab.sh` | mutates source data |

## What was completed instead — all of it real

**Sessions 03–19 committed.** The critical finding from the readiness review (M1): 331
untracked files, including all 48 `.tf` files and the entire `ai/` and `spark/` trees,
with no git remote. The last commit predated 90% of the code. Now `60de297`.
**A remote still does not exist** — that needs your credentials.

**OPEN-22 CLOSED, and it was a real risk.** Built an Airflow 3.2.2 venv and parsed the
DAGs. All 8 import cleanly. One test failed: `DAG.schedule_interval` was removed in
Airflow 3, so `test_recovery_dags_are_manual_only` raised `AttributeError` instead of
evaluating — an error, not a pass, but it had silently stopped checking that recovery
DAGs are manual-only. Now reads `DAG.schedule`, passes on 2.9.3 **and** 3.2.2, and is
mutation-checked on both (schedule a recovery DAG → both fail).

**OPEN-06 CLOSED.** shellcheck installed and enforced at `severity=warning`. It paid for
itself in the first run.

**Two latent defects found in `scripts/tf.sh`, both in code that had never executed:**

1. The cost-preview Python was inlined in a bash single-quoted string, so every
   apostrophe terminated it — `d['resource_changes']` reached Python as
   `d[resource_changes]`, a `NameError`.
2. `f"{t.get(\"aws_kms_key\",0)}"` is a `SyntaxError` before Python 3.12. Installed
   interpreter is 3.10.

Either one aborted the block **before** the NAT-gateway / Secrets-Manager invariant check
at the end — the part that actually guards spend. That control had never worked. Moved to
`scripts/plan_cost.py` (a real file, 12 tests), and a violation now exits non-zero rather
than printing a line that scrolls past.

**The state bucket would have been created untagged.** Six tag variables declared,
none applied — shellcheck surfaced them as unused. `budget_guardrails` filters on
`user:Project$kafka-dev-lab`, so the bucket would have been invisible to the project
budget. Fixed before the bucket exists.

**A real plan, produced and costed.** Cheap tier (`enable_kafka_platform=false`), local
state, not applied. The fixed cost preview's first genuine execution:

```
32 to add, 0 to change, 0 to destroy
hourly while running : $0.0000/hr
KMS CMKs (floor)     : $1.00/month
NAT gateways         : 0   Secrets Manager : 0
```

## Verified numbers

417 Python tests (405 + 12 new), 47 dbt tests, `make check` 14/14, shellcheck clean at
warning severity, `terraform validate` and `fmt` clean. Every number re-run for this
handoff, not quoted.

## Next session — what unblocks it

Run these yourself, in order. In Claude Code, prefix with `!` so the output lands in the
conversation. Each prompts for the phrase in the table above.

```bash
export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1
aws sts get-caller-identity                                 # expect 111122223333

# Gate 1 — one S3 bucket, ~$0.01/month
bash scripts/bootstrap-state-backend.sh --execute

# Then the cheap tier: no hourly billing, ~$1/month floor
cd terraform/envs/dev && terraform init -backend-config=backend.hcl && cd -
bash scripts/tf.sh plan
bash scripts/tf.sh apply --execute
```

**Do not run a bare `terraform apply`.** `enable_kafka_platform` defaults to `true`;
`terraform/envs/dev/terraform.tfvars` has been set to `false` for the cheap tier.

The metered CDC window (MSK on, ~$1.53/hr, ~$9–12 for 6–8 hours) is a separate decision.
MSK cannot be stopped — only destroyed. Set a wall-clock alarm before starting it; budget
alerts arrive hours late.

## Known limitations — unchanged

Still never deployed. No throughput, latency or scale figure exists. 6 of 10 failure
drills `NOT_RUN`. Local tests use a Hadoop catalog, not Glue. Registry-side schema
rejection untested. **No git remote.**

---

## 2026-09-30 — DRP0 handoff (Data Reliability Platform)

> **Everything above this line dates from Session 02 and is stale.** "Still never
> deployed" was true then and is false now: the platform was rebuilt and re-proven live on
> 2026-09-29/30 (73 Glue tables, EOD `closed=10 certified=5`, 2,871 tests). Read
> `PROJECT_STATE.md` from the bottom, not the top. The one line above that is still
> true is **no git remote**.

**Phase completed:** DRP0 — audit only. No AWS mutation, no install, no pipeline change,
no version pinned. Read-only AWS calls only (`sts get-caller-identity`, `glue get-*`).

**Checkpoint reached:** `DRP0_RELIABILITY_CONTEXT_AUDITED`.

**What the next session must know before touching anything**

1. The governance plane describes a platform that no longer exists — `domains.yml` names
   12 datasets, **1** of which is live, and 72 of 73 live tables are ungoverned. Do not
   "fix" this by rewriting the YAML by hand; DRP1's decision (ADR-087, D-DRP0-2) is to
   **derive** it from `cdc/registry/sources.yaml`.
2. `governance/lineage/openlineage.yml` claims `evidence: observed` for hops nothing
   emits. **Left in place deliberately** — DRP3 owns that correction, once a listener
   makes the marker true.
3. `spark/ops/dq_engine.py` reads the drifted rules file at runtime and would block a
   publish today. Changing its rule source is a DRP5 action behind its own gate, not a
   drive-by edit.
4. Airflow deployed is **3.2.2**; this workstation has **2.9.3**. Any OpenLineage provider
   validated here is validated against the wrong major. Same for dbt (1.9.11 pinned vs
   1.9.4 local).

**Next phase:** DRP1 — governance metadata foundation. It changes config and tests only;
it deploys nothing and spends nothing. The success measure is a single number: the live
Glue tables carrying governance metadata going from **1/73** to **73/73**.

**Read first:** `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md`, then
`docs/LINEAGE_SOURCE_MATRIX.md`, then `docs/DATA_RELIABILITY_PLATFORM_TARGET.md`, then
ADR-087.

---

## 2026-09-30 — DRP1 handoff

**Checkpoint reached:** `DRP1_GOVERNANCE_METADATA_FOUNDATION_READY`.
Offline implementation — no DataHub, no deployment, no live AWS, no new dependency.

**The state in one command**

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.governance_plan import compile_inventory; i=compile_inventory(); \
  print(i.config_version(), i.coverage()); [print(' ',f) for f in i.findings]"
```

84 assets, `owned 84/84`, one open finding (`curated:dim_customer_bi` — declared only in
Python, governed but reported).

**What the next session must know**

1. **Do not "tidy up" `governance/catalog/domains.yml` or `governance/dq/rules.yml`.** They
   are still drifted *and still read at runtime* — `ai/guards.py:266` and
   `spark/ops/dq_engine.py:317`. Repointing them is DRP5's gate; `dq_engine` currently blocks
   publishes, so a drive-by edit changes publishing behaviour.
2. **The ladder is now canonical in `cdc/certification.py`.** `spark/common/flows.py` imports
   it and `dbt/macros/status_priority.sql` mirrors it. Rank 0 is reserved for an unrecognised
   status — never number a tier 0.
3. **`governance/registry/*.yaml` is not in `cdc-framework.zip`.** The first Spark job that
   reads it must add it to `scripts/cdc-deploy-code.sh`, or the run dies with a missing file
   *after* acquiring EMR capacity — the `per_table` / `full_cdc_job` shape.
4. **A fact has one home.** `classification` is refused inside a `governance:` block, and a
   dimension's PII may not be restated in the overlay. If a new phase needs a fact a second
   time, give it a different job and check consistency.

**Next phase:** DRP2 — DataHub platform. It is the first phase in this programme that can
spend money, and `enable_datahub` defaults false. Answer DRP0 open question 3 first (can the
k3s node host it under `lab_low_cost`?) before any apply.

**Read first:** `docs/GOVERNANCE_METADATA_CONTRACT.md`, then ADR-088, then ADR-087.

---

## 2026-09-30 — DRP2 → DRP12 handoff (Data Reliability Platform)

**Reached:** DRP2 … DRP10 complete offline. **DRP11 NOT PASSED. DRP12 NOT READY** with
twelve named blockers in `IMPLEMENTATION_REPORT.md`.

**The one-line state:** the reliability plane is complete as code and has never run.

**Before touching anything**

1. `metadata_plane.mode` is `disabled` and that is the **correct** default. A publish there
   is `skipped`, not failed. Do not "fix" it.
2. `governance/catalog/domains.yml` and `governance/dq/rules.yml` are **still drifted and
   still read at runtime** by `ai/guards.py:266` and `spark/ops/dq_engine.py:317`.
   Repointing them is blocker B4 and its own gate — `dq_engine` blocks publishes.
3. `governance/lineage/openlineage.yml` still claims `evidence: observed` for hops nothing
   emits. Left deliberately; it is replaced when the listener first runs (B10).
4. Rank **0** in the certification ladder is reserved for an unrecognised status. Never
   number a tier 0.
5. `governance/registry/*.yaml` is **not** in `cdc-framework.zip` (B11). The first Spark job
   that reads it dies *after* acquiring EMR capacity — the `per_table` shape.

**The three operator-gated actions that unblock the most**

```bash
bash scripts/datahub-local.sh preflight          # safe, always
bash scripts/datahub-local.sh up --execute       # B1
# then the live smoke test: docs/DATAHUB_OPERATIONS_RUNBOOK.md §4
```

**Read first:** `docs/DATA_RELIABILITY_OVERVIEW.md`, then
`docs/LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md`, then
`docs/validation/DATA_RELIABILITY_E2E.md` §5 for the shortest path to a real PASS.

---

## 2026-09-30 — DataHub is UP; B1 cleared

Local DataHub **v1.7.0.1** is running (operator ran `datahub-local.sh up --execute`).
GMS `http://localhost:8080`, frontend `http://localhost:9002`, both on `127.0.0.1` only.

**Exercised live:** smoke test 9/9 · governance 373 aspects / 84 assets · lineage 87 aspects
· multi-hop traversal **18 assets, mart at hop 7**.
Evidence: `artifacts/validation/data-reliability/drp2-drp6-live-evidence.json`.

**Three defects the offline tests structurally could not find** — `glossaryTerms` needs an
`auditStamp`, `dataJobInfo.type` is a union, and the transport was discarding the server's
error body. All fixed, all regression-tested. The lesson is recorded as D-LIVE-1: an offline
test of a wire format proves internal consistency and nothing about the far end.

**One rule to remember before planning any recovery:** DataHub's graph index is eventually
consistent. A traversal seconds after a publish reached 3 assets; minutes later, 18. It does
not fail — it silently under-reports the blast radius. See `LINEAGE_TROUBLESHOOTING.md` §5b.

**Next blocker is B3**, not B2: create the five reliability tables from
`spark/ops/ddl/reliability_tables.sql`. Nothing downstream can produce evidence until
`ops.dq_result_v2`, `ops.data_incident`, `ops.recovery_plan` and `ops.recovery_execution`
exist.

**To stop DataHub:** `bash scripts/datahub-local.sh down --execute` keeps the store;
`nuke --execute` removes it. Neither touches AWS. Still **$0** in AWS for the whole programme.

---

## Handoff after Session 39 (2026-09-30) — DRP12 is PRODUCTION_READY

**25 PASS · 0 FAIL · 1 N/A** of 26 gates. `docs/validation/DRP12_FINAL_REVIEW.md`.

**The one thing to read first.** A green Airflow DAG had been emitting **zero** lineage
events. `AIRFLOW__OPENLINEAGE__TRANSPORT` was the bare string `console`; the provider parses
that key as JSON, so it raised **during plugin import** — Airflow skipped the plugin,
`REGISTERED_LISTENERS = []`, and every task succeeded. The test covering this value compared
`airflow/helm/values.yaml` against `cdc/lineage_runtime.py::airflow_env()`; both carried the
bare string, so it passed. Recorded as **ADR-091**: assert the *consumer's* parse, not two of
your own files agreeing. After the fix: **7 events**, `parent`/`root` facets present.

Note the Spark listener really does take a flat
`spark.openlineage.transport.type=console` string — two integrations, two encodings, one
field. That is why the bare form looked right.

**DRP11 is 10 of 10.** Scenario 2 **created `ops.dq_quarantine`** — it had never existed, so
the quarantine path had never run once. Scenario 5 proved the anti-downgrade rule refuses a
late `AUTO_CORRECT` against a `CERTIFIED` day; scenario 6 proved `FULL_FILL` patches
unresolved surrogate keys without touching the tier.

**Scenario 1 is a segmented pass and must stay labelled as one.** Its hops are evidenced in
three separate captures, not one correlated run. Do not quietly promote it.

**What is left, none of it a gate:**

1. The provider is **not in the Airflow image**. Chart `airflow-1.22.0` removed
   `extraPipPackages`, so there is no values-only install — it needs a custom image
   (`FROM apache/airflow:3.2.2` + the pinned provider), pushed to ECR, then
   `--set images.airflow.repository/tag` and `AIRFLOW__OPENLINEAGE__DISABLED=false`.
2. One EMR submission would join scenario 1 into a single run.
3. Ingestion recipes still never executed (B5).

**How the Airflow check was run without risking the cluster:** an isolated pod from the
*same* image with an ephemeral sqlite metadata DB, over SSM. All eight production pods
stayed `Running`. The reproducible command is in
`docs/LINEAGE_AND_RECOVERY_TEST_GUIDE.md` §1f. Delete the pod afterwards.

Still **$0** in AWS for the whole programme.

---

## Handoff after Session 40 (2026-09-30) — the three follow-ups

**DRP12 stays `PRODUCTION_READY` (25 · 0 · 1).** What changed is that the three
non-gate follow-ups were pushed as far as the platform allows, and doing so found seven
more defects — every one of them invisible to a test.

**One thing needs you.** The Airflow image is **built and imported**
(`airflow-openlineage:3.2.2-ol2.20.2`, in containerd on the k3s node). The Helm upgrade
that rolls it out restarts a live cluster, so it was left for an operator. The patch
reuses `helm get values` so the UI password is never re-derived or printed; roll back with
`helm rollback airflow -n airflow`.

**Two things the platform refuses, and they are worth knowing before you retry them:**

1. `spark.driver.extraClassPath` — EMR Serverless answers
   `ValidationException: Option ... is not supported`, **at submit time**. That is why
   `jar_runtime_path` in `governance/registry/openlineage.yaml` is empty: a value there
   stops every `OPENLINEAGE=1` job from starting. A test holds it empty. Dataset-level
   Spark lineage on EMR needs a **custom EMR Serverless image**.
2. MSK from the workstation — brokers resolve to `10.42.x.x` and TCP connect times out.
   Correct: security invariant 3. The `kafka` / `kafka-connect` recipes need an in-VPC host.

**The sentence worth carrying forward.** Three separate failures this session were not
errors: `spark.jars.packages` died *in resolution having done no work*; the OL listener
attached, emitted the APPLICATION event and **silently omitted every dataset**; and the
Airflow plugin failed during import leaving `REGISTERED_LISTENERS = []` while every task
went green. None of them raised anything a dashboard would show. The recurring shape is a
component that **succeeds at everything except the thing it was for**.

**Do not "fix" these — they are the gates working:** `EOD_WAITING_SOURCE` (source behind
the cutoff) and `EMPTY_WINDOW` (no events in the window). Both refused to certify. The
answer was to pick a COB that has data, not to loosen the gate.

**Local state:** DataHub is **running** (`scripts/datahub-local.sh down --execute` stops
it, keeping the store). `.venv-datahub` holds the pinned ingestion CLI — the system
`acryl-datahub` is unusable because an incompatible `sqlglot` breaks every source import.

Still **$0** in AWS for the metadata programme; this session added four short EMR
Serverless submissions.

---

## Correction filed 2026-09-30 — read this before touching Airflow lineage

**The OpenLineage provider was never missing.** Earlier handoff text said it was not in the
Airflow image. A read-only check against the running scheduler says otherwise:

```
== image in use                apache/airflow:3.2.2
== openlineage env             (none set)
== is the listener registered? OpenLineageProviderPlugin | ...-openlineage==2.17.0
```

The stock image ships the provider and the plugin is registered. What the deployed release
lacks is **any** `AIRFLOW__OPENLINEAGE__*` configuration. The wrong belief survived because
an earlier probe ran `pip install ...==2.20.2` and its success was read as "it was absent" —
`pip install` upgrades as quietly as it installs, and nothing had asked the cluster.

**One command does the rollout**, dry-run by default:

```bash
scripts/airflow-enable-lineage.sh --verify              # read-only
scripts/airflow-enable-lineage.sh                       # dry run
scripts/airflow-enable-lineage.sh --execute             # config + pinned image
scripts/airflow-enable-lineage.sh --rollback --execute
```

Do **not** install helm locally. There is no public k3s API endpoint; helm and kubectl are
already on the node and the script reaches them over SSM.

**Why still deploy the image,** given the provider is present: stock pairs it with
`openlineage-python 1.47.1` while the pinned Spark listener is **1.53.0**. Two integrations
writing one graph on different client majors drift in producer strings and facet schema
versions — invisibly, until two halves of one lineage path disagree.

---

## Handoff after Session 41 (2026-09-30)

### DRP12 is now unconditional

The resident Airflow deployment **emits OpenLineage**. Helm revision 2,
`airflow-openlineage:3.2.2-ol2.20.2`, provider 2.20.2, `openlineage-python` 1.53.0 (same
client major as the Spark jar). A real KubernetesExecutor run emitted `START` and `FAIL`
events carrying the DAG's real owner and docstring.

**`DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY` — 25 PASS · 0 FAIL · 1 N/A.**

### Do this first

**Rotate the Airflow UI admin password.** The chart's NOTES rendered it in the clear during
the upgrade. The script now suppresses NOTES, but that protects the *next* run:

```bash
terraform -chdir=terraform/envs/dev apply -replace='module.airflow_k3s.random_password.admin_ui[0]'
scripts/airflow-enable-lineage.sh --execute
```

### Two operational gaps worth knowing

1. **No remote logging.** Worker-pod logs vanish when the pod is deleted
   (`delete_worker_pods` defaults to true), so lineage in task logs must be scraped while
   the pod lives. Enabling remote logging to `s3://…/logs/airflow/` would fix this properly.
2. **The dag-processor is capped at 512Mi**, which is not enough to run `airflow dags test`
   or even `airflow tasks test` in-process — both are OOM-killed. Use a real triggered run.

### AIGR0 is done; AIGR1 is the next action, and it starts with an ADR

`docs/AI_RECOVERY_CURRENT_STATE_AUDIT.md` and `docs/AI_DATA_RELIABILITY_COPILOT_TARGET.md`.

**Read the audit before writing any AIGR code.** Three things will otherwise be got wrong:

1. **Most of the copilot already exists.** `RecoveryPlan`, `decide_approval`,
   `LineageImpactService`, column lineage, URNs, DataHub client, DQ/certification — all from
   DRP, all live-tested. AIGR is integration, not green-field.
2. **`ToolSpec` refuses mutation in code** (`ADR-057`). Nothing past AIGR1 can be built
   without an ADR superseding it. Do not just delete the check.
3. **`TRANSFORM_LOGIC_DEFECT` and `DQ_RULE_DEFECT` cannot be expressed** by today's
   `FailureClass`, and they are precisely the two causes where a rerun makes things worse.
   Today they classify as `DQ_VIOLATION`/`JOB_FAILURE`, which `REPAIRABLE_BY_RERUN` treats
   as fixable.

Also: the prompt pack assumes a checkpoint `BUSINESS_AI_PRODUCTION_READY` that does not
exist under that name, and AI-P13 (the only AWS-mutating AI phase) is blocked — AIGR must
not depend on it. AgentCore stays deferred per ADR-056.

---

## Handoff after Session 42 — the copilot exists; it has never run

`AI_DATA_RELIABILITY_CONTEXT_REHYDRATED_READY` · **read
`docs/AI_DATA_RELIABILITY_CONTEXT_REHYDRATION.md` first** — it is written to be believed
without re-deriving.

**One sentence:** the copilot's safety boundary is complete and adversarially tested (95
tests, 52 attack rows, zero reachable unauthorized mutations), and it has never been given a
live model, a live catalogue or an executor — so it has never recovered anything.

**`AIGR12_AI_DATA_RELIABILITY_COPILOT_NOT_READY`**, 19 pass · 9 not proven live. That is the
project's own standard, the one DRP12 was held to. Do not clear it by lowering the bar.

**Next action, in order:**

```
1. verify bedrock:InvokeModel with the pinned model
2. wire resolve_asset + get_column_lineage to cdc/datahub_client.py
3. populate RecoveryCapability per registered job
4. inject an executor into RecoveryControlService
5. plant a defect, ask in English, approve, let it run
```

Step 5 is the AI-driven equivalent of the 96-second recovery drill the deterministic
platform already executed.

**Also still open from the data plane:** rotate the Airflow UI admin password exposed by the
chart's Helm NOTES during the revision-2 upgrade.

**Three things not to undo:**

1. `ToolSpec` refusing a mutation is a feature (ADR-092). There is one way through.
2. `jar_runtime_path` is empty on purpose — EMR Serverless rejects `extraClassPath` at
   submit time and a value there stops every lineage job starting.
3. `TRANSFORM_LOGIC_DEFECT` and `DQ_RULE_DEFECT` must never reach a rerun.

---

## Handoff after Session 43 — both checkpoints reached

```
DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY        25 · 0 · 1
AIGR12_AI_DATA_RELIABILITY_COPILOT_PRODUCTION_READY     27 · 0 · 1 blocked externally
```

**An AI-driven recovery has run for real.** A natural-language request resolved a real
asset, read real lineage, refused to narrow on `DERIVED` lineage, wrote a real DQ verdict,
built a capability-aware plan, took a real approval, executed a real Athena repair, and
verified the mart at **1000.0** — `aigr10-live-e2e.json`.

**Three things to do next, none of them blocking:**

1. **Rotate the Airflow UI admin password** exposed by Helm NOTES (P1).
2. Submit the **Anthropic use-case form** in the Bedrock console — the account started
   refusing invocations mid-session after two had succeeded. Only this unblocks model
   accuracy evals; nothing in the recovery loop depends on it.
3. Validate the column mappings against real schemas so column lineage can rise from
   `DERIVED` to `VALIDATED` — **by validating them, never by relabelling them**. Until then
   column-narrowed recovery is refused, live, at scope construction.

**The lesson worth carrying:** the two defects the live run found were not in the safety
layer. A template that ignored its scope left the mart *differently wrong* while every gate
reported success, and a verification predicate flagged a correct row. Both were invisible to
95 passing tests. Running the thing remains the only way to find that class of defect.

---

## Handoff after Session 44

**Start here to use or check anything: [`docs/HOW_TO_USE_AND_VERIFY.md`](docs/HOW_TO_USE_AND_VERIFY.md)** —
every capability, the command that exercises it, and the output that proves it worked.

```
DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY        25 · 0 · 1
AIGR12_AI_DATA_RELIABILITY_COPILOT_PRODUCTION_READY     27 · 0 · 1 blocked externally
3,424 tests · validator 14/14 · $0
```

**The recovery loop now narrows to a column**, because the lineage it narrows on was
*confirmed against real data* rather than relabelled: 28 of 67 edges, including every edge
on the account table. `python3 scripts/ai-recovery-drill.py --execute` reproduces it.

**Three things left, none blocking the loop:**

1. Rotate the Airflow UI admin password (P1).
2. Submit the Anthropic use-case form in the Bedrock console.
3. The `kafka` recipe needs a Python `oauth_cb` — YAML cannot carry a callable, so
   `datahub ingest -c kafka.yaml` alone will not authenticate. The mechanism name is now
   correct (`OAUTHBEARER`, not the Java client's `AWS_MSK_IAM`).

**The pattern that held all session:** every defect was found by running something, and none
was in the safety layer. A recipe that generated cleanly could never authenticate. A dry run
overwrote the evidence of a real one. A validation that looked impossible only needed to
look in the right place — inside the JSON, not at the schema.
