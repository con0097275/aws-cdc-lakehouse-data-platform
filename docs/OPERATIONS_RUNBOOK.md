# Operations Runbook

Operational index. Deep procedures live in `docs/RUNBOOK.md` and `docs/runbooks/`; this file
records what the 2026-09-03 live window proved about the *order* things must happen in.

## Bring-up

```bash
# 0. set auto_destroy_after (future!) and monthly_budget_usd in terraform/envs/dev/terraform.tfvars
bash scripts/tf.sh plan                     # expect 0 change, 0 destroy
bash scripts/tf.sh apply --execute          # phrase: APPLY THE SAVED PLAN — billing starts
bash scripts/cdc-window-start.sh            # exit 0 REQUIRED
```

**Wait for cloud-init before touching a node.** `cdc-window-start.sh` reports "SSM Online"
as soon as the agent registers, which is *not* readiness — it passed an operator through to
a node whose user-data was still staging `/opt/source-lab`. Check
`cloud-init status` = `done` on source-lab and cdc-runtime first.

## CDC enablement

```bash
bash scripts/source-lab.sh enable-cdc --execute    # phrase: ENABLE CDC — Oracle RESTARTS
bash scripts/source-lab.sh verify-cdc              # MUST pass before connectors
bash scripts/source-lab.sh seed --execute          # phrase: SEED SOURCE LAB
bash scripts/cdc-runtime.sh create-topics --execute
bash scripts/register-connectors.sh register --execute
```

**If `verify-cdc` reports `oracle c##dbzuser CANNOT authenticate`:** the reconcile step ran
while Oracle was restarting and its failure was swallowed. Re-run the reconcile alone —
it does not restart anything:

```bash
# on the source-lab node, over SSM
bash /opt/source-lab/healthcheck.sh --init
```

Root cause: `ORACLE_CDC_PASSWORD` never enters the Oracle container, so the container's
startup hook creates `C##DBZUSER` with an unbound `&1` and this reconcile is the **only**
thing that ever sets the real password.

## Reporting

Working configuration, discovered the hard way:

```
REPORTING_JOB_ROLE_ARN       .../kafka-dev-lab-dev-spark-eod    # NOT ...-reporting (cannot emit logs)
REPORTING_EMR_LOG_URI        s3://<lake>/logs/emr/              # required — CloudWatch is unreachable
REPORTING_FRAMEWORK_ZIP      s3://<lake>/artifacts/dbt/framework-flat.zip   # flat, not reporting-framework.zip
REPORTING_DBT_BOOTSTRAP_ARGS JSON ARRAY, not a shell string
DBT_PROFILES_DIR             $HOME/.dbt
```

EMR submission: the Kafka/Iceberg/Avro jars ship on the image but are **not** on the default
classpath. Full recipe in `artifacts/validation/final-e2e/cdc/02-lakehouse-layers.txt`.

## Teardown — order matters

```bash
# 1. capture evidence FIRST
aws dynamodb scan --table-name kafka-dev-lab-dev-job-execution > evidence.json
# 2. converge the lake onto the current CMK BEFORE destroying
bash scripts/reencrypt-lake-cmk.sh audit
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute      # phrase: REENCRYPT LAKE
# 3. destroy
bash scripts/tf.sh destroy --execute                        # phrase: DESTROY THE DEV LAB
bash scripts/verify-destroy.sh --destroyed
```

Two traps, both hit before: **re-encrypt before destroy** (a destroy schedules the lake CMK
for deletion and the next apply mints a new one, orphaning older objects behind
`kms:Decrypt` denied), and **do not destroy the DynamoDB tables** — they hold the execution
and watermark evidence.

## Cost control

Baseline **$1.2340/hr**; MSK is 62% and **cannot be stopped, only destroyed**.
`scripts/cdc-window-start.sh` prints a stale window-start stamp and an out-of-date
`$1.1244/hr` constant — trust the instance launch time and the figure above.
**`auto_destroy_after` provides no protection once expired: a wall-clock alarm is the only
teardown trigger.**

## Diagnosing

| Symptom | Most likely cause |
|---|---|
| Connector HTTP 400 "Failed to resolve Oracle database version" | CDC password never reconciled — run `healthcheck.sh --init` |
| EMR job fails "Unable to push logs" | no CloudWatch reachability; set `REPORTING_EMR_LOG_URI` and use a role with `s3:PutObject` on `logs/` |
| Spark `kms:Decrypt` AccessDenied | lake CMK divergence — run `reencrypt-lake-cmk.sh` |
| `ModuleNotFoundError: run_dbt_job` | wrong framework zip — use `framework-flat.zip` |
| Athena `SCHEMA_NOT_FOUND` | unprefixed schema name; databases are `kafka_dev_lab_dev_*` |
| Alarm stuck in ALARM | see findings G1/G2 — three alarms have no live signal |

---

# Developer how-to index

**To verify the platform rather than change it**, use
[`TEST_EVERY_FEATURE.md`](TEST_EVERY_FEATURE.md): every feature, the command that
exercises it, the output to expect, and an independent check in S3/Glue/Athena.

Every entry is a *where*, not a *retelling*. The deep version lives in the linked runbook.

## How to add a CDC table
One entry in `cdc/registry/sources.yaml` — `table:` + `primary_key:` is the minimum.
`make check`, `cdc-deploy-code.sh`, provision, register the connector.
**No Spark job, no DAG edit, no Iceberg DDL.** Full walkthrough with verification queries:
[`ADD_A_CDC_TABLE.md`](ADD_A_CDC_TABLE.md).

## How to change the realtime window
`realtime:` on the table (or in `defaults:`):

```yaml
realtime: {window_hours: 72, late_grace_hours: 24, retention_hours: 168}
```

`retention_hours >= window_hours + late_grace_hours` is **enforced at compile time** — a
window that outlives its retention drops rows it is still responsible for. Recompile and
deploy; the REALTIME DAG picks up the new cadence without a Python edit.
[`REALTIME_WINDOW_RUNBOOK.md`](REALTIME_WINDOW_RUNBOOK.md)

## How to change the EOD schedule
`eod: {schedule: "30 2 * * *"}` on the table. Tables sharing a schedule string are grouped
into one DAG (`cdc/scheduling.py::schedule_groups`), so a new cadence adds a task group, not
a DAG. [`REPORTING_ORCHESTRATION.md`](REPORTING_ORCHESTRATION.md)

## How EOD T-1 is closed
`business_date_lag_days: 1` → COB = today − 1. The cutoff is
`COB + 1 day at 00:00` in `business_timezone` (UTC, ADR-024), applied to
`source_commit_ts` — business time, never ingest time. Events strictly **before** the cutoff;
dedup per PK, last event wins by commit SCN/LSN. Then readiness, DQ, reconciliation, and only
then the certification marker. [`EOD_SNAPSHOT_RUNBOOK.md`](EOD_SNAPSHOT_RUNBOOK.md),
[`EOD_CONTROL_PLANE.md`](EOD_CONTROL_PLANE.md)

## How to add a datamart
A YAML in `reporting/jobs/` (target, PK, partition, merge strategy, one block per flow mode)
and a dbt model in `dbt/models/marts/`. The model uses
`reporting_merge_config()`, `incremental_filter()`, `processing_status_for_mode()` and
`merge_guard()` so one SQL file serves every mode.
**Declare the grain and mean it** — the uniqueness test is what caught
`mart_channel_engagement_daily` selecting a finer-grained fact without aggregating.

## How to declare external EOD dependencies
```yaml
required_datasets:
  - {layer: EOD, table_id: oracle.coredb.corebank.account, certification: CERTIFIED}
dependencies:
  - {dependency_type: EOD_TABLE, upstream_dataset: kafka_dev_lab_dev_curated.fact_account_daily_snapshot,
     flow_mode: EOD, required: true, kickoff_condition: WATERMARK_PAST, lag_days: 0}
```
The gate resolves each against `ops.eod_info` **for the same COB** and names the one that is
missing. [`REPORTING_ORCHESTRATION.md`](REPORTING_ORCHESTRATION.md) §5

## How auto-correct works
Reads FULL_CDC as a change feed, finds the affected dates and keys, repairs only those, and
**refuses to overwrite a CERTIFIED date** — it escalates instead.
[`AUTO_CORRECT_RUNBOOK.md`](AUTO_CORRECT_RUNBOOK.md) §3

## How stream-batch works
Reads the REALTIME layer every 10 minutes with a 2-minute safety rewind, stamps
`PROVISIONAL_NRT`, advances a watermark only on success.
[`AUTO_CORRECT_RUNBOOK.md`](AUTO_CORRECT_RUNBOOK.md) §2

## How streaming-RT works
A resident Structured Streaming app writing the mart from FULL_CDC. Gated off
(`is_enabled: false` + `ENABLE_STREAMING_RT`) because it is the one hourly cost.
[`AUTO_CORRECT_RUNBOOK.md`](AUTO_CORRECT_RUNBOOK.md) §5

## How maintenance is triggered
**Metric-driven, never blind.** `cdc-maintenance.py measure` first; the planner refuses to
compact without `--metrics`, because a nightly rewrite with no predicate touches every event
ever captured to fix one day of small files. Scope is per layer; orphan removal must outlive
the longest possible writer.
[`ICEBERG_MAINTENANCE_RUNBOOK.md`](ICEBERG_MAINTENANCE_RUNBOOK.md)

## How to rerun / rebuild safely
1. Find what is wrong (`eod_run_hist`, `job_execution`).
2. Rebuild the layer that is wrong, **not** the one downstream of it.
3. Historical re-close: `--skip-readiness` (the gate has nothing left to protect). Never for
   the current day — the open-day guard refuses to certify it anyway.
4. dbt `--full-refresh` **only** when a model's grain changed; the merge key cannot remove
   rows written under the old grain.
5. Never delete a streaming checkpoint to force a rerun.

## How to diagnose WAITING_SOURCE
The message names the gap in minutes. Then, in order: is the day actually over? is the ingest
running (`cdc-stream.py status`)? are the connectors RUNNING? is the table just quiet — in
which case the **platform** watermark should still pass the cutoff.
[`EOD_CONTROL_PLANE.md`](EOD_CONTROL_PLANE.md) §5

## How to diagnose WAITING_DEPENDENCY
The task log names the dataset. Read its `ops.eod_info` row; if it is not CERTIFIED, the
upstream close is the thing to fix, not the mart.

## How to recover a streaming app without deleting the checkpoint
The checkpoint is derived from the **app id**, so re-submitting the same app resumes from
committed offsets. `cdc-stream.py status` → read the driver log → re-submit → verify the row
count did not move. Deleting a checkpoint replays the entire subscription and is a deliberate
backfill, not a restart. [`FULL_CDC_STREAMING_RUNBOOK.md`](FULL_CDC_STREAMING_RUNBOOK.md) §4

---

# Rebuilding after a destroy

`terraform destroy` removes the compute and the catalog. It does **not** empty the lake
bucket, and that asymmetry is the whole difficulty: S3 objects survive under a CMK that is
scheduled for deletion, while the new stack grants its roles only the new key.

Learned 2026-09-23, after three failed ingests. Follow the order.

## 0. Before the apply

```hcl
# terraform/envs/dev/terraform.tfvars — a past date makes the apply refuse
auto_destroy_after = "<a future instant>"
```

## 1. Rescue the old CMK FIRST

Do this **before** anything else. A key in `PendingDeletion` blocks decryption immediately —
you do not wait for the deletion date — and that blocks even the cleanup tools, because
`aws s3 mv` copies, and copying decrypts.

```bash
aws kms describe-key --key-id <old-lake-key> --query 'KeyMetadata.KeyState'
aws kms cancel-key-deletion --key-id <old-lake-key>
aws kms enable-key         --key-id <old-lake-key>
```

## 2. Apply, then wait for cloud-init

```bash
bash scripts/tf.sh plan && bash scripts/tf.sh apply --execute
```

**Do not touch a node before `cloud-init status` says `done`.** SSM reporting "Online" is
the agent registering, not the node being ready — `cdc-runtime` shows no containers and
Connect REST is unreachable until staging finishes.

## 3. Converge the lake onto the current key

```bash
bash scripts/reencrypt-lake-cmk.sh audit                  # read-only
bash scripts/reencrypt-lake-cmk.sh reencrypt --execute    # operator-gated
```

Skipping this produces `kms:Decrypt` denied **one object at a time** — the Kafka jars first,
then the next stale object, then the next. It reads like an S3 problem and is a key problem.

If you only need to unblock the jobs, the `artifacts/` prefix is what they read at startup:

```bash
aws s3api list-objects-v2 --bucket $LAKE --prefix artifacts/ --query 'Contents[].Key' \
  --output text | tr '\t' '\n' | while read k; do
    aws s3 cp "s3://$LAKE/$k" "s3://$LAKE/$k" --sse aws:kms \
      --sse-kms-key-id alias/<lake-alias> --metadata-directive REPLACE --only-show-errors
  done
```

## 4. Reset the streaming checkpoints — REQUIRED, not optional

The checkpoints outlive the stack, and a rebuilt MSK cluster starts its offsets at 0 again.
A surviving checkpoint claims positions from a cluster that no longer exists, so resuming
**permanently skips** every event before those offsets — silently, because the topics still
hold them.

```bash
export LAKE_BUCKET=<lake>
bash scripts/streaming-reset.sh --app-id full-cdc-oracle    --execute
bash scripts/streaming-reset.sh --app-id full-cdc-sqlserver --execute
```

It moves rather than deletes, and needs a typed phrase — deliberately. See
`FULL_CDC_STREAMING_RUNBOOK.md` §4.

## 5. Source, topics, connectors

```bash
bash scripts/source-lab.sh enable-cdc --execute     # SQL Server CDC does NOT survive
bash scripts/source-lab.sh verify-cdc               # must say ALL CHECKS PASSED
bash scripts/cdc-runtime.sh create-topics --execute
bash scripts/register-connectors.sh register --execute        # `register` is a SUBCOMMAND
bash scripts/cdc-runtime.sh export-schemas --execute $LAKE    # bucket is POSITIONAL
```

`schemas.json` must be re-exported: the new Apicurio issues new `globalId`s, and the ingest
selects writer schemas by that id.

## 6. Rebuild the catalog and run the chain

```bash
bash scripts/cdc-deploy-code.sh
bash scripts/emr-submit.sh provision s3://$LAKE/artifacts/code/provision_cdc_tables.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse --execute
```

**`--execute` is required.** Without it the job prints `CDC_PROVISION_DRIFT` per table,
creates nothing, and still reports SUCCESS.

Then ingest → EOD → `curated_build` → `dbt build`, as in
[`ADD_A_CDC_TABLE.md`](ADD_A_CDC_TABLE.md) §5.

## 7. What you do NOT run

`register_tables.py` re-attaches **surviving** Iceberg tables to a rebuilt catalog. After a
destroy the old tables are under a retired key and the job role cannot read them, so it fails
twice over. Rebuild from source instead — it is faster than recovering, and the result is on
one key.
