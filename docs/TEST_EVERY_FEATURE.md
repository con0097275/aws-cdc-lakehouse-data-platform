# Testing every feature, end to end

A hands-on verification guide. For each feature: **what to run**, **what you should see**,
and **how to check it independently** — in S3, Glue, Athena or the control tables.

Every expected value below is from the live run of **2026-09-23** on the rebuilt stack. Your
numbers will differ if you reseed; the *shapes* (equalities, zeros, statuses) should not.

Run ids for every claim: [`validation/FULL_CDC_REPORTING_E2E.md`](validation/FULL_CDC_REPORTING_E2E.md) §9.

---

## 0. Setup — do this once per shell

```bash
cd ~/terraform-kafka-kraft/aws-cdc-lakehouse
export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1
export LAKE=kafka-dev-lab-dev-lake-111122223333
export APP=$(aws emr-serverless list-applications \
  --query 'applications[?state!=`TERMINATED`]|[0].id' --output text)
echo "lake=$LAKE app=$APP"
```

**Forgetting `AWS_PROFILE` is the single most common failure.** Without it the CLI silently
uses a different account and you get `KMS.NotFoundException` or an empty resource list.

A small Athena helper, used throughout:

```bash
cat > /tmp/aq.py <<'PY'
import sys, time, boto3
a = boto3.Session(profile_name="my-aws-profile", region_name="ap-southeast-1").client("athena")
q = a.start_query_execution(QueryString=sys.argv[1], WorkGroup="kafka-dev-lab-dev-wg")["QueryExecutionId"]
while True:
    st = a.get_query_execution(QueryExecutionId=q)["QueryExecution"]["Status"]
    if st["State"] in ("SUCCEEDED","FAILED","CANCELLED"): break
    time.sleep(2)
if st["State"] != "SUCCEEDED": print("FAILED:", st.get("StateChangeReason")); sys.exit(1)
r = a.get_query_results(QueryExecutionId=q)
for row in r["ResultSet"]["Rows"]:
    print("\t".join(c.get("VarCharValue","NULL") for c in row["Data"]))
PY
aq() { python3 /tmp/aq.py "$1"; }
```

---

## 0b. The one command that checks everything

```bash
make cdc-e2e-verify                                   # every layer, read-only
make cdc-e2e-verify ARGS="--cob 2026-09-23"           # pin the business date
python3 scripts/cdc-e2e-verify.py --layer full_cdc    # one layer only
```

It derives every table name from the **compiled plan**, so it cannot drift from what the
platform actually built. Output is one line per check:

```
FULL_CDC -- append-only canonical layer
  PASS  oracle.coredb.corebank.account       320 events
  PASS  oracle.coredb.corebank.transaction   2,000 events
  ....  FULL_CDC total                       5,828 events

CURATED -- conformed entities, Kimball dimensions and facts
  PASS  dim_account = banking_account + unknown member   321 rows
  PASS  referential integrity (fact -> dim_account)      0 orphans
  ....  unresolved (watch, not fail)  0 unknown account_sk, 0 null amount_base

MART -- the declared grain must actually hold
  PASS  mart_account_balance_daily (account_sk, business_date)  320 rows == grain
  ....  mart_channel_engagement_daily  0 rows -- check the INPUT for this COB

S3 -- where each layer physically lives
  ....  FULL_CDC  s3://<lake>/warehouse/full_cdc/   740 objects, 18.2 MB
  ....  EOD       s3://<lake>/warehouse/snapshot/   294 objects,  3.4 MB
==============================================================================
PASS -- 39 checks held, 0 skipped
```

**Every check is an equality or a zero**, never a threshold — a threshold needs a judgement
call about how wrong is acceptable, and these do not. Two things are deliberately reported
rather than failed: an unresolved surrogate key and a NULL `amount_base` are legitimate when
a dimension member has not arrived, so the *number* is what you watch.

The rest of this document is what each of those checks means and how to reproduce it by
hand.

---

## 1. Is the platform even up?

```bash
aws kafka list-clusters-v2 --query 'ClusterInfoList[].[ClusterName,State]' --output text
aws ec2 describe-instances --filters "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceType]' --output text
aws emr-serverless list-applications --query 'applications[].[name,state]' --output text
```

**Expect:** MSK `ACTIVE`; four instances (`toolbox`, `airflow`, `source-lab`, `cdc-runtime`);
EMR Serverless `STARTED` or `STOPPED` — **both are $0 when idle**, it has no pre-initialized
capacity.

```bash
ID=$(aws ec2 describe-instances --filters "Name=tag:Name,Values=kafka-dev-lab-dev-cdc-runtime" \
  "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
aws ssm send-command --instance-ids $ID --document-name AWS-RunShellScript \
  --parameters 'commands=["cloud-init status"]' --query 'Command.CommandId' --output text
```

**`cloud-init status` must say `done`.** SSM reporting "Online" only means the agent
registered — the node stages `/opt/cdc-runtime` afterwards, and touching it early gives you
"Connect REST unreachable" with no containers running.

---

## 2. Source databases and CDC enablement

```bash
bash scripts/source-lab.sh status
```

**Expect, all PASS:**

```
[PASS] source-oracle healthy          [PASS] source-sqlserver healthy
[PASS] log_mode=ARCHIVELOG            [PASS] SQL Server Agent Running
[PASS] supplemental logging MIN=YES   [PASS] database CDC enabled
                                      [PASS] 5 declared tables tracked by CDC
ALL CHECKS PASSED - safe to deploy connectors
```

**If SQL Server says `database CDC=` and `tables NOT tracked`:** CDC does not survive a
container rebuild. Re-enable it — Oracle **restarts**, so do it before anything else:

```bash
bash scripts/source-lab.sh enable-cdc --execute    # phrase: ENABLE CDC
bash scripts/source-lab.sh verify-cdc              # must pass BEFORE connectors (risk R15)
```

Credentials live in SSM SecureString, never in Git:

```bash
for p in oracle-password oracle-cdc-password sqlserver-sa-password sqlserver-cdc-password; do
  aws ssm get-parameter --name "/kafka-dev-lab/dev/source-lab/$p" --with-decryption \
    --query 'Parameter.Value' --output text >/dev/null && echo "OK $p"
done
```

---

## 3. Kafka: topics, connectors, schema registry

```bash
bash scripts/cdc-runtime.sh status      # containers + connector state
bash scripts/cdc-runtime.sh topics      # via MSK IAM
```

**Expect:** `cdc-connect` and `cdc-apicurio` healthy; both connectors `RUNNING`; topics
`cdc.oracle.COREBANK.*` (5) and `cdc.sqlserver.digital.dbo.*` (5), plus heartbeats, DLQs and
schema-history.

Registering them, if missing — note `register` is a **subcommand**, `--execute` alone prints
usage:

```bash
bash scripts/cdc-runtime.sh create-topics --execute
bash scripts/register-connectors.sh register --execute     # phrase: REGISTER CONNECTORS
bash scripts/register-connectors.sh status
```

### Schema evolution — the bit people assume works

```bash
bash scripts/cdc-runtime.sh export-schemas --execute $LAKE   # bucket is POSITIONAL
aws s3 cp s3://$LAKE/artifacts/code/schemas.json - | python3 -c "
import json,sys; d=json.load(sys.stdin)
print('by_global_id:', len(d['by_global_id']), ' by_topic:', len(d['by_topic']))"
```

**Expect `by_global_id` to be about 2x `by_topic`** — 2026-09-23 showed 28 and 14.

**That 2x is NOT evidence of schema evolution**, and reading it that way is a mistake I made
and had to correct. Apicurio registers a **Key** and an **Envelope (value)** artifact per
topic, so 14 topics x 2 = 28 with exactly **one version each**. Confirm it yourself:

```bash
aws s3 cp s3://$LAKE/artifacts/code/schemas.json - | python3 -c "
import json,sys
from collections import Counter
d=json.load(sys.stdin); c=Counter()
for gid,s in d['by_global_id'].items():
    sch=json.loads(s) if isinstance(s,str) else s
    c[f\"{sch.get('namespace','')}.{sch.get('name')}\"]+=1
for k,v in sorted(c.items()): print(v,k)"
```

Every line should read `1 <topic>.Key` / `1 <topic>.Envelope`. A **2** against one of those
names is a topic that genuinely carries two writer versions — which is what §4b scenario 5
produces, and the only real proof that per-record selection matters.

The ingest picks the schema **per record** by `apicurio.value.globalId`. If an id is missing
from the export, the record is **quarantined, not decoded with the topic's schema** — because
that would be a known-different writer version, and `from_avro` does not raise on a
compatible-looking mismatch, it returns plausible wrong values.

**Re-export after every rebuild.** A new Apicurio issues new ids; a stale export
mis-decodes silently.

---

## 4. Adding a table — one YAML entry, nothing else

Edit `cdc/registry/sources.yaml`:

```yaml
      - table: statement
        primary_key: [STATEMENT_ID]
        dq: {not_null: [STATEMENT_ID, ACCOUNT_ID]}
```

```bash
make check                                    # compiles, refuses on drift
bash scripts/cdc-deploy-code.sh               # framework + entrypoints + compiled plan
bash scripts/emr-submit.sh provision \
  s3://$LAKE/artifacts/code/provision_cdc_tables.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse --execute
```

**`--execute` is required.** Without it the job prints `CDC_PROVISION_DRIFT` for every table,
creates nothing, and **still reports SUCCESS**.

**Verify:**

```bash
for d in full_cdc snapshot stream ops; do
  echo -n "kafka_dev_lab_dev_$d: "
  aws glue get-tables --database-name kafka_dev_lab_dev_$d --query 'length(TableList)' --output text
done
```

**Expect 10 / 10 / 7 / 6** for the shipped registry (3 tables are `realtime: {enabled: false}`,
so REALTIME has 7). A new table adds one to each layer it participates in.

You did **not** write a Spark job, edit a DAG, or write Iceberg DDL. Full walkthrough:
[`ADD_A_CDC_TABLE.md`](ADD_A_CDC_TABLE.md).

---

## 4b. Generating sample data — and proving it arrived

The generators are `docker/source-lab/generators/oracle-workload.sql` and
`sqlserver-workload.sql`. They use **fixed id ranges**, so re-running produces the same
logical outcome: a generator with random ids makes reconciliation impossible, which defeats
the point of a lab.

```bash
bash scripts/source-lab.sh workload 1 --execute     # phrase: RUN WORKLOAD 1
```

Six scenarios, and **the exact number of CDC events each produces** — this is what makes the
end-to-end check an equality rather than a guess:

| # | what it does | Oracle events | SQL Server events | total |
|---|---|---|---|---|
| **1** | steady I/U/D — the baseline mix | 50 txn inserts + 30 account updates + 10 customer updates = **90** | 50 inserts + 40 app_user updates = **90** | **180** |
| **2** | burst — many rows in ONE transaction | 500 inserts, one commit SCN | 500 inserts, one commit LSN | **1,000** |
| **3** | late-arriving reference | 20 orphan txns, parents 5s later | 20 orphan events, parents 5s later | 40 + parents |
| **4** | deletes | 25 deletes + 11 soft closes + 6 hard deletes = **42** | 25 + 11 + 6 = **42** | **84** |
| **5** | COMPATIBLE DDL | adds `account.risk_score`, 50 rows updated | adds `app_user.loyalty_tier`, new capture instance | — |
| **6** | INCOMPATIBLE DDL | the registry **should reject it** | same | — |

Scenario 2 is the one that tests ordering: 500 rows sharing a single commit SCN/LSN means
`kafka_offset` is the only tie-breaker left, and it is only valid *within* a partition.

Scenario 6 is a **negative** test. Success is the schema registry refusing the change; if
those events reach FULL_CDC decoded, that is the finding.

### The full loop, with the numbers checked

```bash
# 1. baseline BEFORE generating anything
python3 scripts/cdc-e2e-verify.py --layer full_cdc --save /tmp/before.json

# 2. generate exactly 180 events
bash scripts/source-lab.sh workload 1 --execute

# 3. let Debezium publish, then drain Kafka into FULL_CDC
python3 scripts/cdc-stream.py submit --app-id full-cdc-oracle       # prints the command
python3 scripts/cdc-stream.py submit --app-id full-cdc-sqlserver

# 4. assert the delta is EXACTLY what the generator produced
python3 scripts/cdc-e2e-verify.py --layer full_cdc \
        --since /tmp/before.json --expect-delta 180
```

Expected tail:

```
DELTA since before.json: FULL_CDC +180 events
    full_cdc:TOTAL                                   +180
    full_cdc:oracle.coredb.corebank.account           +30
    full_cdc:oracle.coredb.corebank.customer          +10
    full_cdc:oracle.coredb.corebank.transaction       +50
    full_cdc:sqlserver.digital.dbo.app_user           +40
    full_cdc:sqlserver.digital.dbo.digital_event      +50
  PASS  FULL_CDC growth matches the generator    180 events
```

**`--expect-delta` is an exact match, not a floor.** "At least 180" would pass while
something else was also writing rows, which is precisely the condition you want to catch.

### If the delta is short

| delta | look at |
|---|---|
| **0** | the ingest never ran, or the connector is not publishing. `cdc-runtime.sh status`, then `ops.streaming_app_state` — **no row for an app means it has never been run** |
| **less than expected** | events still in Kafka: re-run the ingest. The checkpoint resumes, so this is safe and adds only what is new |
| **more than expected** | something else wrote rows, or a checkpoint was reset and the topic replayed — check `restart_count` |
| **counts unequal** (`events != ids`) | a genuine duplicate. This is the one that should never happen; capture the run id and the driver log |

Then carry the same rows forward through the layers:

```bash
# EOD for the business date those events belong to
bash scripts/emr-submit.sh eod-all s3://$LAKE/artifacts/code/eod_engine.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse \
  --table ALL --cob-date <COB>

# then CURATED, then dbt, then re-verify everything
python3 scripts/cdc-e2e-verify.py --cob <COB> --bucket $LAKE
```

**FULL_CDC grows by 180; EOD does not.** EOD is one row per PK per business date, so 30
updates to 30 existing accounts move the snapshot's *contents* without changing its row
count. That difference is the clearest demonstration of what the two layers are for — and if
EOD *did* grow by 30, the dedup is broken.

---

## 5. Kafka → FULL_CDC (layer 1 of 3)

```bash
python3 scripts/cdc-stream.py apps      # app ids and their derived checkpoints
python3 scripts/cdc-stream.py submit --app-id full-cdc-oracle   # PRINTS the command
```

`submit` prints rather than runs — starting an EMR job is a gate. Copy, read, run. Or
directly:

```bash
BOOTSTRAP=$(terraform -chdir=terraform/envs/dev output -json | python3 -c \
  "import json,sys;print(json.load(sys.stdin)['kafka_platform']['value']['msk_bootstrap_brokers_sasl_iam'])")
export EXECUTOR_INSTANCES=1 EXECUTOR_CORES=2 EXECUTOR_MEMORY=4g DRIVER_CORES=2 DRIVER_MEMORY=4g

bash scripts/emr-submit.sh full-cdc-oracle \
  s3://$LAKE/artifacts/code/full_cdc_stream_job.py stream \
  --bootstrap "$BOOTSTRAP" \
  --topics cdc.oracle.COREBANK.ACCOUNT,cdc.oracle.COREBANK.BRANCH,cdc.oracle.COREBANK.CUSTOMER,cdc.oracle.COREBANK.LOAN,cdc.oracle.COREBANK.TRANSACTION \
  --schemas-uri s3://$LAKE/artifacts/code/schemas.json \
  --warehouse s3://$LAKE/warehouse \
  --table glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events \
  --table-plan s3://$LAKE/artifacts/cdc/table-plan.json \
  --app-id full-cdc-oracle \
  --state-table glue_catalog.kafka_dev_lab_dev_ops.streaming_app_state \
  --migration-mode dual_write --event-index
```

### Check it — exactly-once is the property that matters

```bash
aq "SELECT count(*) events, count(DISTINCT dv_event_id) ids
    FROM kafka_dev_lab_dev_full_cdc.cdc_oracle_coredb_corebank_account"
```

**Expect `events = ids`.** 2026-09-23: 320 / 320. Across all ten tables: **5,828 / 5,828**.

**Then run the same job again** and re-check: the count must **not move**. That is the
idempotency proof — the checkpoint resumes from committed offsets and nothing is re-read.

### In S3

```bash
aws s3 ls s3://$LAKE/warehouse/full_cdc/cdc_oracle_coredb_corebank_account/ --recursive | head
aws s3 ls s3://$LAKE/checkpoints/full_cdc/full-cdc-oracle/ --recursive | head
```

`data/` and `metadata/` under the table; the checkpoint under a **separate prefix** — a
streaming checkpoint and an Iceberg warehouse must never share one.

### Streaming state

```bash
aq "SELECT app_id, mode, profile, status, restart_count, rows_total
    FROM kafka_dev_lab_dev_ops.streaming_app_state"
```

`mode=available_now, profile=lab_low_cost` is the lab. `continuous_microbatch / production`
is the resident form (§10).

**No row for an app = it has never been run.** That is the tell when Kafka has events and
FULL_CDC is empty.

---

## 6. REALTIME (the STREAM layer) — bounded rolling window

```bash
bash scripts/emr-submit.sh realtime-all s3://$LAKE/artifacts/code/realtime_engine.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse --table ALL
```

```bash
aq "SELECT count(*) rows, count(DISTINCT dv_pk_hash) keys
    FROM kafka_dev_lab_dev_stream.rt_oracle_coredb_corebank_account"

aq "SELECT table_id, refresh_mode, logical_lower_bound, grace_lower_bound,
           upper_bound, retention_bound, row_count, status
    FROM kafka_dev_lab_dev_ops.realtime_run ORDER BY started_at DESC LIMIT 3"
```

**Expect `rows = distinct dv_event_id`.** REALTIME is a bounded **event window**, not a
state snapshot: `window_rows` filters FULL_CDC on `source_commit_ts` and `materialise`
overwrites the target with the result. There is **no ranking and no collapse per key**, so a
row updated three times inside the window appears three times.

`rows = distinct dv_pk_hash` is NOT an invariant here. It happened to hold on 2026-09-23
(320 / 320) only because that data had exactly one event per key. Generate a workload with
updates and the two numbers separate — correctly.

The collapse to one row per key happens **downstream**, in `transform.collapse_to_grain`,
when a flow reads the layer. That is why `flow_runner` refuses a source without a total
order: the ranking is the consumer's job, and REALTIME preserves the material it needs.

**Expect the window bounds to be FROZEN at run start**, not computed per row:
`upper_bound` = the run's start instant, `logical_lower_bound` = minus 72h,
`grace_lower_bound` = minus a further 24h, `retention_bound` = minus 168h. A window that
moved while the job ran would include events it had already decided to exclude.

Change the window in the registry, nothing else:

```yaml
realtime: {window_hours: 72, late_grace_hours: 24, retention_hours: 168}
```

`retention_hours >= window_hours + late_grace_hours` is **enforced at compile time**.

### Testing `latest_state` (the hot-path shape)

```bash
# the pairing is enforced; this FAILS, and the message says why
python3 -c "
import yaml,sys,pathlib
r=yaml.safe_load(pathlib.Path('cdc/registry/sources.yaml').read_text())
r['sources'][0]['tables'][0]['realtime']={'refresh_mode':'latest_state'}
pathlib.Path('/tmp/bad.yaml').write_text(yaml.safe_dump(r))"
python3 -c "
import sys; sys.path.insert(0,'.')
from cdc.config_loader import load_config
try: load_config('/tmp/bad.yaml')
except Exception as e: print('REFUSED:', e)"
```

Expect `REFUSED: ... refresh_mode 'latest_state' needs delete_policy 'soft_flag'`.

With the correct pairing, the layer's invariant changes:

```bash
# full_refresh: events, several per key possible
aq "SELECT count(*) rows, count(DISTINCT dv_pk_hash) keys
    FROM kafka_dev_lab_dev_stream.rt_oracle_coredb_corebank_account"

# latest_state: one row per key, and tombstones are VISIBLE between rebuilds
aq "SELECT count(*) rows, count(DISTINCT dv_pk_hash) keys,
           sum(CASE WHEN is_deleted THEN 1 ELSE 0 END) tombstones
    FROM kafka_dev_lab_dev_stream.rt_oracle_coredb_corebank_account"
```

Under `latest_state` expect `rows = keys`, and `tombstones` to be non-zero only between the
delete and the next day-boundary rebuild. **A consumer that does not filter `is_deleted`
would carry those into a mart** — which is why the delete policy is enforced.

**REALTIME is not STREAM_BATCH.** This is a materialised *layer*; STREAM_BATCH (§8) is a
*flow mode* that reads it every ten minutes.

---

## 7. EOD — the certified snapshot layer

```bash
bash scripts/emr-submit.sh eod-all s3://$LAKE/artifacts/code/eod_engine.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse \
  --table ALL --cob-date <YYYY-MM-DD>
```

Read the driver log — the four lines that matter:

```
EOD_CUTOFF   ... cutoff_utc=<COB+1>T00:00:00+00:00 tz=UTC delete=exclude_from_snapshot
EOD_CLOSED   ... rows=320 deletes=0 dq=PASS recon=PASS status=CERTIFIED
EOD_SUMMARY  closed=10 certified=10 rows=5828
```

### The two control tables, and why there are two

```bash
# CURRENT state: one row per (table, COB), MERGEd in place
aq "SELECT table_id, cob_date, certification_status, row_count,
           prev_watermark_ts, watermark_ts
    FROM kafka_dev_lab_dev_ops.eod_info WHERE cob_date = DATE '<COB>'"

# EVERY attempt, forever, including the ones that failed
aq "SELECT table_id, attempt, status, dq_status, reconciliation_status, err_msg
    FROM kafka_dev_lab_dev_ops.eod_run_hist WHERE cob_date = DATE '<COB>' ORDER BY attempt"
```

`eod_info` answers *"is this date final?"* directly. `eod_run_hist` answers *"what happened?"*
— asking an append log the first question means "the latest row that happens to be
CERTIFIED", a convention no column enforces.

### Things you should try to break

**Close a day that has not ended:**

```
EOD_DAY_OPEN ...: cutoff <T>T00:00:00+00:00 has not passed; built, NOT certified
EOD_OPEN_DAY_BUILD built and deliberately NOT certified. Nothing is wrong.
```

The snapshot is still written — only the marker is withheld. `--skip-readiness` waives the
*source* gate, never the *clock*.

**Close before the source has caught up:**

```
EOD_WAITING_SOURCE ...: source watermark <T> is 842 min behind the cutoff <T>
```

Nothing is built, the attempt is still recorded. See
[`EOD_CONTROL_PLANE.md`](EOD_CONTROL_PLANE.md) §5 for the diagnosis order.

**Verify a failed run does not advance the watermark:**

```
EOD_WATERMARK_HELD ... status=BUILT_NOT_CERTIFIED -- eod_info keeps <previous>
```

### Grain

```bash
aq "SELECT count(*) rows, count(DISTINCT (dv_pk_hash, business_date)) grain
    FROM kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account"
```

**Expect equal** — one row per PK per business date.

---

## 8. CURATED and the data marts

```bash
bash scripts/emr-submit.sh curated-build s3://$LAKE/artifacts/code/curated_build.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json \
  --layers s3://$LAKE/artifacts/reporting/layers.json \
  --entities s3://$LAKE/artifacts/reporting/curated-entities.json \
  --warehouse s3://$LAKE/warehouse --cob-date <COB>
```

Log output names every entity and its row count, including the empty ones:

```
CURATED_ENTITY banking_account rows=320 from=...eod_oracle_coredb_corebank_account
CURATED_DIMENSIONS {"dim_account": 321, "dim_customer": 201, "dim_channel": 5, ...}
CURATED_FACTS {"fact_transaction": 2000, "fact_account_daily_snapshot": 320, ...}
CURATED_PROVISIONAL status=PROVISIONAL_NRT: not certified for <COB> -> ...
```

**Dimensions are source rows + 1** — the extra is the unknown member (`-1`), inserted, never
implied. An empty source still builds its dimension with only that member, so facts resolve
to `UNKNOWN_SK` rather than hitting a missing table at the end of a long job.

Then the marts:

```bash
VARS='{"flow_mode":"EOD","cob_date":"<COB>","execution_id":"check-001",
       "config_version":"<from the plan>","source_layers":{"EOD":"kafka_dev_lab_dev_curated"},
       "date_of_data":"<COB>"}'

bash scripts/emr-submit.sh dbt-build s3://$LAKE/artifacts/code/emr_dbt_bootstrap.py eod \
  --wheelhouse-uri s3://$LAKE/artifacts/dbt/wheelhouse.zip \
  --project-uri s3://$LAKE/artifacts/dbt/dbt-project.zip \
  --mart-schema kafka_dev_lab_dev_mart --curated-schema kafka_dev_lab_dev_curated \
  --select "*" --command build --vars-json "$VARS"
```

**Expect `Done. PASS=56 WARN=0 ERROR=0 SKIP=0`.**

Add `--full-refresh` **whenever a model's grain changed** — the merge key decides which rows
to *update* and cannot remove rows written under the old grain. Symptom: you fix the SQL,
rebuild cleanly, and the uniqueness test still fails against yesterday's rows.

### Input → output, checked independently of dbt

```bash
# PK holds
aq "SELECT count(*) rows, count(DISTINCT transaction_id) pk
    FROM kafka_dev_lab_dev_curated.fact_transaction"

# declared grain holds
aq "SELECT count(*) rows, count(DISTINCT (account_sk, business_date)) grain
    FROM kafka_dev_lab_dev_mart.mart_account_balance_daily"

# nothing silently unresolved
aq "SELECT sum(CASE WHEN account_sk = -1 THEN 1 ELSE 0 END) unknown_account,
           sum(CASE WHEN amount_base IS NULL THEN 1 ELSE 0 END) unresolved_fx
    FROM kafka_dev_lab_dev_curated.fact_transaction"

# no orphan FK
aq "SELECT count(*) orphans FROM kafka_dev_lab_dev_curated.fact_transaction f
    LEFT JOIN kafka_dev_lab_dev_curated.dim_account d USING (account_sk)
    WHERE d.account_sk IS NULL"
```

2026-09-23: 2,000 / 2,000 PK · 320 / 320 grain · **0** unknown · **0** unresolved FX ·
**0** orphans.

**A mart returning 0 is not automatically a bug.** Check whether any row falls on the COB
first — the seeded transactions are dated 2026-01-01, so the channel marts are legitimately
empty for a COB of today. An empty table violates no uniqueness or not-null test, which is
exactly why you check the input as well as the output.

---

## 9. The four flow modes

```bash
source scripts/reporting-env.sh     # DERIVES everything from terraform output

python3 scripts/reporting-live-run.py --flow-mode EOD          --business-date <COB> --execute
python3 scripts/reporting-live-run.py --flow-mode AUTO_CORRECT --business-date <COB> --execute
python3 scripts/reporting-live-run.py --flow-mode FULFILL      --business-date <COB> \
        --from-date <D1> --to-date <D2> --execute
python3 scripts/reporting-live-run.py --flow-mode STREAM_BATCH --business-date <COB> --execute
```

Drop `--execute` for a dry run that plans and submits nothing.

**What each should say:**

| mode | expected `detail` |
|---|---|
| EOD | `null`, `status: SUCCEEDED`, `watermark_after` populated |
| AUTO_CORRECT | `N date(s), M key(s), path=...; 1 date(s) **ESCALATED** (already certified…)` |
| FULFILL | one result per date in the range, all SUCCEEDED |
| STREAM_BATCH | `committed to <timestamp>` |

**`ESCALATED` is the one to look for.** A certified date is final; AUTO_CORRECT refuses it
and escalates rather than silently rewriting a published number.

The accuracy ladder: `PROVISIONAL_NRT` → `PROVISIONAL_CORRECTED` → `CERTIFIED`. A row moves
up, never down — rows that would downgrade an existing one are dropped **before** the merge:

```bash
aq "SELECT processing_status, count(*) FROM kafka_dev_lab_dev_mart.mart_account_balance_daily
    GROUP BY 1"
```

---

## 10. STREAMING_RT — the resident app

**Off by default**, and deliberately: it is the one hourly cost in the reporting layer.

```bash
grep -A3 'STREAMING_RT:' reporting/jobs/mart_account_balance_daily.yaml   # is_enabled: false
```

To exercise it without AWS or cost:

```bash
make reporting-streaming-rt-demo     # lifecycle, watermark, restart — against fakes, $0
python3 -m pytest spark/tests/test_reporting_streaming_rt.py -q
```

To run it for real you must set `is_enabled: true`, set `ENABLE_STREAMING_RT=true` on the
Airflow node, and unpause `streaming_rt_start`. **It holds capacity until stopped.**

The same resident shape is available for the *ingest*, and this one has been exercised live:

```bash
JOB_RUN_MODE=STREAMING bash scripts/emr-submit.sh full-cdc-oracle-prod \
  s3://$LAKE/artifacts/code/full_cdc_stream_job.py stream ... \
  --table-plan s3://$LAKE/artifacts/cdc/<a production-profile plan>.json

aws emr-serverless cancel-job-run --application-id $APP --job-run-id <run>   # STOP IT
```

`JOB_RUN_MODE=STREAMING` omits the execution timeout entirely — EMR rejects a timeout of
`0` as well as a finite one for that mode. The script prints the cancel command; a resident
job never reaches SUCCESS, so it does not wait.

---

## 11. Airflow — the orchestrator

```bash
bash scripts/airflow-node.sh status
bash scripts/airflow-node.sh credentials
bash scripts/airflow-node.sh ui          # SSM port-forward -> http://localhost:8080
```

**There is no ingress rule for 8080 and no load balancer. The port-forward is the only path.**

On the node:

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl -n airflow get pods
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- airflow dags list
```

**Expect** `api-server`, `scheduler`, `dag-processor`, `triggerer`, `postgresql` all Running,
and ~21 DAGs — **one per cadence, not per table**:

| DAG | schedule |
|---|---|
| `cdc_eod`, `cdc_realtime`, `cdc_maintenance` | per-table policy, grouped by schedule |
| `datamart_eod` | `30 2 * * *` |
| `datamart_auto_correct` | `*/30 * * * *` |
| `datamart_stream_batch` | `*/10 * * * *` |
| `datamart_fulfill` | `None` — manual only, on purpose |

Ten tables produce three CDC DAGs, not thirty. A test asserts the count does not grow with
the table count.

### Prove the scheduler actually runs things

```bash
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow dags unpause datamart_stream_batch
# wait for the next 10-minute tick, then:
aws emr-serverless list-job-runs --application-id $APP --max-results 5 \
  --query 'jobRuns[].[name,state,createdAt]' --output text
```

**Expect a `streambatch-...` job created AFTER your unpause, reaching SUCCESS.** Compare
`createdAt` against the moment you unpaused — otherwise you are looking at your own manual
run. 2026-09-23: unpaused 14:08Z, scheduler submitted at 14:12Z, SUCCESS.

**Pause it when you are done.** Every tick is a real EMR job.

```bash
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow dags pause datamart_stream_batch
```

---

## 12. Iceberg maintenance

```bash
python3 scripts/cdc-maintenance.py measure --out /tmp/metrics.json
python3 scripts/cdc-maintenance.py plan --metrics /tmp/metrics.json
```

**Expect a per-table verdict and, on a freshly written lake, `(nothing)`.** The planner is
metric-driven and **refuses to run without `--metrics`** — a nightly compaction with no
predicate rewrites every event ever captured to fix one day of small files.

`orphan_file_count = 0` from `measure` means **"not measured"**, not "none found"; counting
orphans means listing the whole location and diffing against the manifests, which the job
does under its own gate.

```bash
bash scripts/emr-submit.sh maintenance s3://$LAKE/artifacts/code/cdc_maintenance_job.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse \
  --metrics s3://$LAKE/artifacts/cdc/metrics.json
```

**Verify it changed nothing it should not:** row counts before and after must be identical.
Compaction rewrites files, never rows.

---

## 13. Offline checks — no AWS, no cost

```bash
make check                    # registry, plan determinism, docs consistency  (14 checks)
make test                     # 2,670 tests against real Spark and real Iceberg
make lint-shell               # shellcheck every operator script
make cdc-provision-plan       # the Iceberg DDL for every target, creates nothing
make cdc-governance           # governance record and gaps per table
make reporting-stream-batch-demo   # STREAM_BATCH ladder against fakes
```

`make check` also **fails if the committed plan is stale** against the registry — the guard
against a derived artifact drifting from its source.

---

## 14. When something fails

| symptom | cause |
|---|---|
| `KMS.NotFoundException` on any command | `AWS_PROFILE` not set — you are in the wrong account |
| `kms:Decrypt` denied, one object at a time | objects on a retired CMK. `reencrypt-lake-cmk.sh audit` |
| `KMSInvalidStateException: pending deletion` | a key is scheduled for deletion; decryption stops **immediately**, not on the deletion date |
| job SUCCESS but nothing created | the job is dry-run by default — add `--execute` |
| `ModuleNotFoundError: No module named 'x'` | `SHARED_MODULES` in `cdc-deploy-code.sh` — the zip name is the *import* name |
| `ApplicationMaxCapacityExceededException [disk: 100 GB]` with nothing running | EMR dynamic allocation ignoring `spark.executor.instances`; `DYNAMIC_ALLOCATION=false` is the default now |
| ingest fails right after a rebuild | a checkpoint outlived the stack. [`FULL_CDC_STREAMING_RUNBOOK.md`](FULL_CDC_STREAMING_RUNBOOK.md) §4 |
| `Connect REST unreachable`, no containers | `cloud-init` still running on `cdc-runtime` |
| mart is empty, all tests green | check the *input* for that COB — an empty table violates no test |

Driver logs for any run:

```bash
aws s3 cp s3://$LAKE/logs/emr/applications/$APP/jobs/<run-id>/SPARK_DRIVER/stdout.gz - | gunzip
```

---

## 15. Cost, before you walk away

```bash
aws emr-serverless list-job-runs --application-id $APP --states RUNNING PENDING \
  --query 'length(jobRuns)' --output text                        # expect 0
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow dags pause datamart_stream_batch
bash scripts/airflow-node.sh stop --execute
```

Stack up ≈ **$27/day**; down ≈ **$0.27/day**. **Stopping EC2 is cheaper, not free** — EBS
bills regardless; only destroy removes it, and **MSK cannot be stopped at all**. Full
picture: [`COST.md`](COST.md).
