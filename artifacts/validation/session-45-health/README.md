# Platform health check — 2026-09-20 (read-only)

Account 111122223333 · profile `my-aws-profile` · `ap-southeast-1` · env `dev`.
**Read-only**: `sts`/`s3 ls`/`head-object`/`describe-*`/`list-*`, Athena none, and SSM
RunShellScript commands that only read (docker ps, logs, SELECTs). No apply, no connector,
no topic, no checkpoint, no AWS mutation.

The stack was **destroyed and re-applied today**: MSK created 08:17Z, EC2 launched 08:44Z,
new EMR application id `00g8tlee3db1gu25`, new lake CMK `0b5b383b` (08:16Z).

| file | what it establishes |
|---|---|
| `00-identity-budget.txt` | account guard; budget actual $51.69 / $100, forecast $95.58 |
| `01-compute.txt` | MSK ACTIVE 3×m7g.large KRaft 3.9.x; 4 EC2 running; EMR `CREATED` |
| `02-s3.txt` | lake bucket 4,385 objects / 302 MB; warehouse, checkpoints, artifacts intact |
| `03-catalog-secrets.txt` | **every `kafka_dev_lab_dev_*` Glue database has 0 tables**; 8 SSM SecureStrings present |
| `04-ssm-msk-kms.txt` | 4 nodes SSM Online; IAM bootstrap brokers; 2 CMK aliases; Athena WG ENABLED |
| `05-cdc-runtime-status.txt` | Connect + Apicurio healthy; **no connectors deployed** |
| `06-kafka-topics.txt` | all 10 CDC topics + DLQ + heartbeat + schema-history exist |
| `07/13-source-lab-status*.txt` | first read caught Oracle mid-init; re-read: Oracle healthy, ARCHIVELOG + supplemental logging PASS; SQL Server CDC not enabled |
| `08..12-oracle-*` | Oracle container created 09:00:15, healthy on re-check; volumes intact |
| `14-airflow-node.txt` | k3s active; all Airflow pods Running; 4 pools created |
| `15-airflow-dags.txt` | **21 DAGs loaded, 0 import errors**, incl. `full_cdc_streaming_{start,monitor,stop}` |
| `16-emr-bedrock.txt` | EMR autoStart on, autoStop 15 min; Bedrock has Claude + `cohere.embed-v4` |
| `17-warehouse-cmk.txt` | all 11 FULL_CDC + EOD + realtime + mart + ops prefixes still hold data |
| `18-cmk-readability.txt` | 2026-09-10 CMK still Enabled and its objects decrypt |
| `19/20-kms-*.txt` | **1,428 objects are encrypted with CMK `28b39c6a`, PendingDeletion 2026-09-24** |
| `22-verify-three.txt` | those objects already fail with `KMS.KMSInvalidStateException: pending deletion` |
| `23..26-*` | SQL Server `sa` login OK but **no `digital` database**; Oracle seeded (320/200/50/2000/4); registry 0 artifacts; Connect REST 200 |
| `27-tfstate-kms-ids.txt` | 299 Terraform resources; `28b39c6a` = the previous **data lake** CMK |

## The one thing that is time-critical

`28b39c6a` ("kafka-dev-lab-dev data lake encryption (S01-11)") is the lake CMK used by every
object written on 2026-09-17. It is in `PendingDeletion` with a hard delete on **2026-09-24**,
and a key in that state **already refuses decryption**. Affected: 393 warehouse data/metadata
files, `artifacts/code/cdc-framework.zip` and every job entrypoint, `artifacts/cdc/table-plan.json`,
plus 1,025 disposable query-results/log objects.

`aws kms cancel-key-deletion` followed by `aws kms enable-key` restores access immediately and
is reversible. After 2026-09-24 nothing restores it.

## Addendum — the `tables tracked=5 (expected 4)` FAIL (2026-09-20, after `enable-cdc`)

**The platform was right and the gate was wrong.** Verified on the host
(`28-sqlserver-cdc-truth.txt`): exactly the five registered SQL Server tables are tracked,
each with its own capture instance —

    app_user  channel  digital_event  merchant  payment_method
    dbo_app_user  dbo_channel  dbo_digital_event  dbo_merchant  dbo_payment_method

`docker/source-lab/healthcheck.sh:217` asserted `[ "$NTAB" = "4" ]` while
`sqlserver/02-enable-cdc.sql` enables five and `cdc/registry/sources.yaml` declares five.
Phase 8 onboarded `payment_method` into the registry and the enable script and left the gate
behind, so a correctly enabled lab reported FAIL and told the operator not to deploy
connectors.

Fixed by DERIVING the expectation from the file that does the enabling — both are staged to
the same host from the same commit — and by comparing NAMES, not a count:

* a table that is declared but not tracked is named in the failure
* a table tracked but not declared is a WARN (it costs retention, not correctness)
* an unreadable expectation is a FAIL, never a silent pass

`register-connectors.sh` does not consult this gate, so nothing was blocked by it; what was
damaged is trust in the gate, which is what protects against risk R15.

Also fixed: `cd "$LAB_DIR"` had no `|| exit`, and `make lint-shell` covered `scripts/*.sh`
only — so the scripts that actually run on the lab hosts were never linted. Both closed;
all four `docker/**/*.sh` are shellcheck-clean at warning severity.

Covered by `spark/tests/test_source_lab_cdc_gate.py` (15 tests), which binds registry ↔
enable SQL ↔ gate for BOTH engines and was mutation-checked by restoring the hardcoded count
(4 tests fail).

**Not yet deployed to the running host**: Terraform uploads `docker/source-lab/**` to
`s3://<lake>/bootstrap/source-lab/` and the instance pulls it at boot, so the fix reaches the
lab on the next apply + bootstrap, or by copying the file over SSM.

**Separate, still open**: the five SQL Server tables hold **0 rows** —
`source-lab.sh seed --execute` has not run. Oracle is seeded (320/200/50/2000/4).

## Addendum 2 — after `enable-cdc` + `seed` (2026-09-20 09:4xZ)

**CDC is live and capturing.** `source-lab.sh verify-cdc` now exits **0**:

* Oracle: ARCHIVELOG, MIN supplemental logging, ALL-COLUMN logging on all **5** tables,
  rows 4 / 200 / 320 / 50 / 2000 (branch, customer, account, loan, transaction).
* SQL Server: Agent Running, database CDC on, **5** capture instances, change rows
  app_user 150 · digital_event 3000 · channel 4 · merchant 50 · **payment_method 50** —
  the seed inserts were captured, which is the end-to-end proof that capture works.

**The lake CMK blocker is CLOSED** (`34-kms-after.txt`): `28b39c6a` is `Enabled` with no
deletion date, and `artifacts/cdc/table-plan.json`, `artifacts/code/cdc-framework.zip` and
the 2026-09-17 warehouse files all read again. `cd7c3d82` remains PendingDeletion on
2026-09-24 — that is the **old MSK** key; the live cluster uses `3cdedee2`, so letting it go
costs nothing.

### Three more instances of the same Phase 8 drift, found by running the thing

| file | defect | fix |
|---|---|---|
| `sqlserver/99-verify-cdc.sql` §7/§8 | hand-written UNION over **4** tables, omitting `payment_method` | built from `cdc.change_tables` / `sys.tables`, so a captured table cannot be missing |
| `sqlserver/99-verify-cdc.sql` §4 | `SELECT capture_instance, source_schema, source_table` — **those columns do not exist**; every run printed `Invalid column name` and continued | `OBJECT_SCHEMA_NAME/OBJECT_NAME(source_object_id)` |
| `oracle/99-verify-cdc.sql` §6 | row counts omitted `loan` | all five listed |
| both §3 headers | `(4 rows, ...)` stated as fact | "one row per registered table" |

A table missing from a verification REPORT reads as a table that is fine — worse than the
gate failing loudly.

### Synced to the running host

`healthcheck.sh`, `sqlserver/99-verify-cdc.sql`, `oracle/99-verify-cdc.sql` were uploaded to
`s3://<lake>/bootstrap/source-lab/` (the key Terraform manages, same content ⇒ no plan diff)
and pulled onto `i-0c04da52f6408a8f8`; md5s match the repo. The previous `healthcheck.sh` is
kept on the host as `healthcheck.sh.bak-20260920T093604Z`.

Tests: `spark/tests/test_source_lab_cdc_gate.py` **20 passed**, covering registry ↔ enable ↔
gate ↔ verify for both engines; mutation-checked.

## Addendum 3 — post-connector readiness (2026-09-20 10:0xZ)

**Capture verified end to end.** Both connectors RUNNING (task 0 RUNNING), snapshot drained,
and every topic matches its source exactly — 2,574 Oracle + 3,254 SQL Server = **5,828
events**, Apicurio holding 28 artifacts. Not the R15 failure mode: RUNNING *and* producing,
checked independently on both sides (`35-connector-status.txt`, `36-capture-flowing.txt`).

### CRITICAL: the deployed `schemas.json` is from 2026-09-17 and its ids no longer mean the same thing

`s3://<lake>/artifacts/code/schemas.json` (17 Sep, 83 KB) carries `by_global_id` 1..32.
Today's Apicurio is a **fresh KafkaSQL instance**, so it reassigned ids in registration order
(`39-globalid-compare.txt`, `40-globalid-mapping.txt`):

| globalId | deployed file | live registry | |
|---|---|---|---|
| 4 | `ACCOUNT.Envelope` | `ACCOUNT-value` | agrees, by luck |
| 14 | `TRANSACTION.Envelope` | `LOAN-value` | **disagrees** |
| 26 | `digital_event.Envelope` | `payment_method-value` | **disagrees** |

An ingest run against this file decodes LOAN with TRANSACTION's writer schema and
payment_method with digital_event's. Some topics still decode cleanly, which is what makes it
dangerous — a partially correct decode reads as a working pipeline. This is ADR-070 defect 2
(`FIELD_NOT_FOUND: no such struct field 'op'`) in its quieter form.

**Must run before any ingest:**
`bash scripts/cdc-runtime.sh export-schemas --execute kafka-dev-lab-dev-lake-111122223333`

### Already deployed today
`cdc-deploy-code.sh` ran at 10:03Z: `cdc-framework.zip`, `full_cdc_stream_job.py`,
`per_table.py`, `provision_cdc_tables.py` are current, and the S3 plan is
`cdccfg-1c0796dd6d3b65b0` **schema 8 with `ingestion.apps`** — byte-identical to the repo.

### Still blocking the lake
All eight Glue databases hold **0 tables**. Spark/Athena/EOD/realtime/maintenance/AI all
depend on the catalog; the S3 data is intact and readable.

### Repo
`make check` exit 0 (14/14 static, shellcheck clean, plan not stale). **186 uncommitted
paths**, HEAD still `1d3cf5b` (2026-09-03), no git remote.
