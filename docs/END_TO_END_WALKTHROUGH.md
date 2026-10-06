# End-to-End Walkthrough — Kafka CDC → S3 → Data Model → AI

A guided tour of this platform that you can **run**, not just read. Every command below was
executed against account `111122223333` in `ap-southeast-1` on 2026-08-27, and every
"expect" block is real output, not an illustration.

Budget: the whole walkthrough is **under $0.50** if you follow it once. The AI section is
**$0.00** — it makes no model calls.

---

## 0. Before you start

```bash
cd /path/to/aws-cdc-lakehouse
export AWS_PROFILE=my-aws-profile AWS_DEFAULT_REGION=ap-southeast-1
make identity
```

**If you skip the `export`, roughly half of this guide fails with confusing errors** —
`ParameterNotFound` on secrets, empty resource listings, `NoSuchEntity` on roles. The CLI
silently falls back to different credentials. When something looks impossibly broken, check
`aws configure list` shows `profile my-aws-profile` before debugging anything else.

---

## 1. What this platform is

Change Data Capture from two source databases, landed in Kafka, written to an Iceberg
lakehouse on S3, modelled into a mart, and queryable — with an AI layer bolted **alongside**,
never inside, the data path.

```
Oracle 23ai ──┐
              ├── Debezium 2.7.3 ──> MSK (Kafka) ──> Spark ──> Iceberg on S3 ──> Glue ──> Athena
SQL Server ───┘                                                    │
                                                                   └──> AI layer (read-only)
```

The layer model is a **fan-out**, not a chain — this trips people up constantly:

| Layer | Meaning | Grain |
|---|---|---|
| **L1 FULL_CDC** | canonical truth, every I/U/D, append-only, written straight from Kafka | one row per change event |
| **L2 STREAM** | derived, low-latency view | one row per change event |
| **L3 SNAPSHOT** | as-of state at a cutoff, deduped by PK | one row per key per day |
| **MART** | business-facing model | one row per business grain |

`STREAM` is **derived from** FULL_CDC, not a landing zone before it. FULL_CDC is the only
thing entitled to be called canonical.

### The one rule the AI layer obeys

The AI layer is **downstream and adjacent**. It reads the mart and the docs; it writes
nothing, owns no truth, and no pipeline code imports it:

```bash
grep -rn 'import ai\.\|from ai\.' spark/jobs spark/reporting airflow/dags | wc -l
```

```text
0
```

That `0` is enforced by a test. If the AI layer vanished, every number this platform
produces would be unchanged.

---

## 2. Is it running?

```bash
aws ec2 describe-instances --filters "Name=tag:Project,Values=kafka-dev-lab" \
  "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].{Name:Tags[?Key==`Name`]|[0].Value,Id:InstanceId}' --output table
aws kafka list-clusters-v2 --query 'ClusterInfoList[].[ClusterName,State]' --output text
```

Expect four instances (`source-lab`, `cdc-runtime`, `airflow`, `toolbox`) and
`kafka-dev-lab-dev ACTIVE`.

**Cost note:** MSK cannot be stopped, only destroyed. While this is up you are paying for it.
`bash scripts/show-cost-resources.sh` lists everything billable.

---

## 3. Test the source → CDC path

### 3.1 Sources are healthy and CDC-enabled

```bash
bash scripts/source-lab.sh status
```

```text
=== containers ===
  [PASS] source-oracle healthy
  [PASS] source-sqlserver healthy
=== Oracle CDC preconditions ===
  [PASS] log_mode=ARCHIVELOG
  [PASS] supplemental logging MIN=YES
=== SQL Server CDC preconditions ===
  [PASS] SQL Server Agent Running
  [PASS] database CDC enabled
  [PASS] 4 tables tracked by CDC
=== result ===
  ALL CHECKS PASSED - safe to deploy connectors
```

**Do not skip this.** The script blocks connector deployment on these preconditions
deliberately. Debezium against a database without supplemental logging gives you silently
incomplete UPDATE before-images — data that looks fine and is wrong.

If Oracle shows `missing`, it is usually still pulling its 1.7 GB image on a fresh instance.
Wait, don't debug.

### 3.2 Connectors are running

```bash
bash scripts/cdc-runtime.sh status
```

```text
cdc-connect     Up (healthy)
cdc-apicurio    Up (healthy)
sqlserver-digital-source        RUNNING
oracle-corebank-source          RUNNING
```

### 3.3 Topics exist

```bash
bash scripts/cdc-runtime.sh topics
```

```text
cdc.oracle.COREBANK.ACCOUNT
cdc.oracle.COREBANK.BRANCH
cdc.oracle.COREBANK.CUSTOMER
cdc.oracle.COREBANK.TRANSACTION
cdc.sqlserver.digital.dbo.app_user
cdc.sqlserver.digital.dbo.channel
cdc.sqlserver.digital.dbo.digital_event
cdc.sqlserver.digital.dbo.merchant
cdc.dlq.oracle   cdc.dlq.sqlserver
__debezium-heartbeat.cdc.oracle  ...
```

Two things worth noticing: **per-table topics** (the Kafka key is the canonical PK, so one PK
always lands in one partition), and **DLQ topics** — poison records are quarantined with
their error class and source offset, not dropped.

### 3.4 Generate change traffic

```bash
bash scripts/source-lab.sh workload 1 --execute     # type: RUN WORKLOAD 1
```

Scenarios 1–4 are safe to repeat. **5 and 6 are not routine**: 5 creates a second SQL Server
capture instance (the maximum is two) and 6 is *designed* to be rejected by the schema
registry. Run 1–4 first and confirm events land before going near them.

### 3.5 Prove events reached Kafka correctly

```bash
bash scripts/cdc-runtime.sh correctness all
```

Six assertions that matter: I/U/D reach the right topics as Avro; same-PK ordering holds
within a partition; Oracle SCN / SQL Server LSN are present and monotonic; a connector
restart does **not** re-snapshot; compatible schema evolution is accepted; **incompatible
schema change is rejected**.

---

## 4. Test the S3 / data-model path

### 4.1 The lake

```bash
aws s3 ls s3://kafka-dev-lab-dev-lake-111122223333/warehouse/
```

```text
PRE curated/   PRE full_cdc/   PRE mart/   PRE ops/
PRE quarantine/  PRE snapshot/  PRE stream/
```

Checkpoints live under `checkpoints/`, deliberately **not** under `warehouse/` — a streaming
checkpoint sharing a prefix with an Iceberg warehouse is a corruption waiting to happen.

### 4.2 The catalog

```bash
for db in mart curated full_cdc stream ops; do
  echo "$db: $(aws glue get-tables --database-name kafka_dev_lab_dev_$db --query 'TableList[].Name' --output text)"
done
```

```text
mart: mart_account_balance_daily
curated: fact_account_daily_snapshot
full_cdc: cdc_events
stream: cdc_events  cdc_events_realtime
ops: emr_smoke  eod_watermark  streaming_batch_ledger
```

**If these come back empty but S3 still has data, nothing is lost.** `terraform destroy`
removes Glue *databases*; the Parquet and every Iceberg `metadata.json` survive. You have a
lake holding all its data and none of its pointers. Recovery is one catalog write per table:

```bash
python3 spark/ops/register_tables.py --bucket kafka-dev-lab-dev-lake-111122223333 --dry-run
```

then submit `register_tables.py` to EMR Serverless (it needs Iceberg + Glue, so it cannot run
on your laptop). See `docs/runbooks/rebuild-from-scratch.md` §5.

### 4.3 Query the mart

```bash
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) AS n FROM kafka_dev_lab_dev_mart.mart_account_balance_daily" \
  --work-group kafka-dev-lab-dev-wg --query QueryExecutionId --output text
```

```text
1280        # bytes scanned: 0  — Iceberg answers COUNT(*) from metadata
```

The workgroup enforces a **10 GiB bytes-scanned cutoff** server-side, so a client cannot
override it. That is the cost guardrail, not a convention.

### 4.4 Cross-check before trusting a number

```bash
make reporting-verify
```

A job can report `SUCCEEDED` and produce nothing. The watermark is a **claim**; the
cross-check in `docs/VERIFY_END_TO_END.md` §3 is what turns it into evidence.

---

## 5. Airflow

```bash
bash scripts/airflow-node.sh ui            # terminal 1 — wait for "READY"
bash scripts/airflow-node.sh credentials   # terminal 2
```

Open **http://localhost:8080**, user `admin`.

`ui` waits until the tunnel genuinely serves HTTP 200 before saying READY — an SSM
port-forward takes about **15 seconds** to carry traffic after it prints
`Waiting for connections...`, and it will reconnect if the session drops. There is no ingress
rule for 8080 and no load balancer; SSM is the only path in.

---

## 6. The AI layer

Everything here is **local, read-only, and $0.00**. No model is called.

### 6.1 What it can do

```bash
make ai-tools
```

Eight read-only tools, and — more informative — the three deliberately **absent**:

```text
DELIBERATELY NOT IMPLEMENTED
  get_feature_value          feature_offline is not materialised yet
  get_reconciliation_status  ops.metric_variance / data_certification are empty
  run_model_inference        the only model has a synthetic label, no predictive meaning

WRITE_TOOLS: {}   (asserted empty by test — ADR-057)
```

A tool that answers from nothing is worse than no tool.

### 6.2 Ask it something

```bash
make ai-retrieve Q="how does the L3 snapshot handle deletes"
```

```text
8.9232  docs/L3_SNAPSHOT.md § L3 EOD (SNAPSHOT) — as-of T-1 @baa348ea
8.1390  docs/L3_SNAPSHOT.md § ... > 1. Three ways to get this wrong @baa348ea
7.9384  docs/L1_FULL_CDC.md § L1 FULL_CDC — canonical CDC truth @baa348ea
```

Every hit carries a **content hash** (`@baa348ea`). Citations point at a specific version of a
specific file, not a vague title.

### 6.3 Prove it is grounded, not fluent

```bash
python3 ai/eval/e2e_p14.py
```

```text
S1  PASS rag-architecture   3 citations resolved to real files with matching text
S3  PASS structured         query_athena: {'rows': [{'row_count': '1280'}]}
S9  PASS unsafe             intent=UNSAFE refused=True guard_blocked=True tools_called=none
10 PASS / 0 FAIL / 0 BLOCKED  of 10
```

The citation check does not verify a citation was *formatted*. It **opens the cited file and
locates the cited text inside it**. A retriever inventing a plausible path fails.

And S3's `1280` is the same number the direct Athena query returned in §4.3 — the agent is
reading the governed table, not guessing.

### 6.4 Try to break it

```bash
python3 ai/eval/drills_p15.py
```

```text
20/20 PASS   P0 violations: 0
```

Twenty failure and security drills: corpus loss, backend outage, throttling, timeouts,
malformed input, **prompt injection in the user request, in a retrieved document, and in a
tool result**, SQL mutation, shell/Terraform/Kafka commands, budget exhaustion.

The reason injection fails is worth stating plainly: not the wording of the refusal, but that
**`WRITE_TOOLS` is empty**. There is no write tool to hijack. Refusal is also enforced twice —
the router refuses, and `assert_read_only_sql` independently raises — so a routing miss alone
cannot execute a mutation.

Try it yourself:

```bash
python3 - <<'PY'
import sys; sys.path.insert(0, "ai")
from guards import assert_read_only_sql, GuardViolation
for sql in ["DROP TABLE mart.x", "SELECT 1; DELETE FROM mart.x",
            "/* SELECT */ UPDATE mart.x SET a=1", "MSCK REPAIR TABLE mart.x"]:
    try:
        assert_read_only_sql(sql); print("NOT BLOCKED (bad):", sql)
    except GuardViolation:
        print("blocked  :", sql)
PY
```

All four must print `blocked`, including the comment-obfuscated and multi-statement forms.

### 6.5 Retrieval quality, honestly

```bash
make ai-eval-rag
```

```text
recall@5  0.6842   MRR 0.486
repo-vocabulary only   0.7551
paraphrased only       0.2500     <-- the weakness
```

BM25 degrades badly on paraphrase. That is the measurement ADR-049 asks for, and it argues
*for* embeddings — which are not enabled because Bedrock is not invokable on this account.
The number is published rather than hidden because a retrieval score you cannot see is a
retrieval score you cannot trust.

---

## 7. Shut it down

```bash
bash scripts/stop-ephemeral.sh        # stops EC2; S3, Glue and state survive
bash scripts/show-cost-resources.sh   # what is still billable
```

**STOP is cheaper, not free.** A stopped instance bills no compute but its EBS volume bills
per provisioned GiB-month. Only `destroy` removes the volume. MSK cannot be stopped at all.

Before any `terraform destroy`, read the CMK warning in §8.

---

## 8. The three traps

**1. `terraform destroy` schedules the lake CMK for deletion.** It has done this twice. New
objects then get a *new* key while old objects keep the old one; roles granted only the new
key cannot read the old data, and it surfaces as `kms:Decrypt` denied — which sends you
debugging IAM or S3 instead of KMS. Check with:

```bash
bash scripts/reencrypt-lake-cmk.sh audit
```

Anything other than one key (plus regenerable `logs/` and `query-results/`) means run
`reencrypt --execute`.

**2. Updating an SSM parameter is not a rotation.** Both nodes read SSM only at boot. The old
secret stays live until user-data re-runs.

**3. A watermark is a claim, not a fact.** Always cross-check that the target table exists and
has rows.

---

## 9. Where to go next

| Document | For |
|---|---|
| `docs/VERIFY_END_TO_END.md` | the deeper verification, start at §3 |
| `docs/runbooks/rebuild-from-scratch.md` | rebuilding, and the benign errors not to chase |
| `docs/ARCHITECTURE_AND_TEST_GUIDE.md` | why the layer model is a fan-out |
| `docs/AI_E2E_VALIDATION.md` | the 10 AI scenarios and their evidence |
| `docs/AI_FAILURE_MATRIX.md` | the 20 failure drills |
| `docs/runbooks/ai-platform-operations.md` | AI failure modes and what to check first |
| `DECISION_LOG.md` / `docs/adr/` | why any of this is the way it is |
