# Runbook — rebuild the lab from scratch, without the seven traps

The order below is the one that works. It was derived on **2026-08-26** by rebuilding after
a `terraform destroy`, hitting seven defects in sequence — each one hidden behind the
previous — and fixing all seven in this repository.

**If you follow this order and something still fails, it is a new defect.** Every failure
listed here is now fixed in code; the "if it comes back" rows tell you what a regression
looks like.

```bash
cd /path/to/aws-cdc-lakehouse
export AWS_PROFILE=my-aws-profile
export AWS_DEFAULT_REGION=ap-southeast-1
```

---

## The whole thing, in order — copy-paste

Every defect below the fold is fixed in this repository, so this sequence is expected to run
clean. Nine of them were found by running it on 2026-08-26; each is listed later with a
regression signature in case one comes back.

```bash
cd /path/to/aws-cdc-lakehouse
export AWS_PROFILE=my-aws-profile
export AWS_DEFAULT_REGION=ap-southeast-1

# 0. does a destroy have the lake CMK on a deletion fuse?  (see section 0)
for k in $(aws kms list-keys --query 'Keys[].KeyId' --output text); do
  aws kms describe-key --key-id "$k" \
    --query 'KeyMetadata.[KeyId,KeyState,Description]' --output text
done | grep -i pendingdeletion || echo "no key pending deletion - good"

# 1. build it
bash scripts/tf.sh plan                              # expect 0 destroy, 0 replace
bash scripts/tf.sh apply --execute                   # phrase: APPLY THE SAVED PLAN   (~30 min, MSK)

# 2. wait until it is genuinely healthy
scripts/cdc-window-start.sh

# 3. source databases BEFORE connectors
bash scripts/source-lab.sh enable-cdc --execute      # phrase: ENABLE CDC             (Oracle restarts)
bash scripts/source-lab.sh verify-cdc                # must end ALL CHECKS PASSED

# 4. connectors
bash scripts/register-connectors.sh register --execute   # phrase: REGISTER CONNECTORS
bash scripts/cdc-runtime.sh status                       # both RUNNING

# 5. prove data is actually moving  -- do NOT skip; RUNNING does not mean flowing
bash scripts/cdc-runtime.sh topics

# 6. shut it down as soon as the run is proven
scripts/stop-ephemeral.sh --execute
```

### If step 5 shows zero messages

The connector is RUNNING and producing nothing — risk R15. Stored offsets from an earlier
attempt make Debezium think the snapshot is done:

```bash
bash scripts/register-connectors.sh reset --execute   # phrase: RESET CONNECTOR OFFSETS
```

`reset` now stops each connector, deletes its offsets and resumes it. Before 2026-08-26 it
deleted without stopping, which Connect 3.6+ rejects with
`400 Connectors must be in the STOPPED state` — and the reset silently did nothing.

### Counting messages correctly

`kafka.tools.GetOffsetShell` **moved package in Kafka 3.x**. Calling the old class returns
`ClassNotFoundException`, and a shell pipeline that sums its output reports **0** — a
working pipeline that looks dead. Use `kafka-get-offsets`:

```bash
kafka-get-offsets --bootstrap-server "$BOOTSTRAP" \
  --command-config /opt/cdc-runtime/client.properties | grep '^cdc\.'
```

Healthy first run, from the seeded lab:

```
cdc.oracle.COREBANK.CUSTOMER                 400
cdc.oracle.COREBANK.ACCOUNT                  640
cdc.oracle.COREBANK.TRANSACTION            4,000
cdc.sqlserver.digital.dbo.app_user           300
cdc.sqlserver.digital.dbo.digital_event    6,000
```

> Set those two variables **first, in every shell**. Half the "AccessDenied" reports in this
> project are a missing `AWS_PROFILE` falling back to default credentials.

---

## 0. Before you apply — check for a scheduled CMK deletion

`terraform destroy` schedules the lake CMK for deletion on a 7-day fuse. **Every byte in
the lake is encrypted with it**, and the deletion is irreversible once it fires.

```bash
for k in $(aws kms list-keys --query 'Keys[].KeyId' --output text); do
  aws kms describe-key --key-id "$k" \
    --query 'KeyMetadata.[KeyId,KeyState,DeletionDate,Description]' --output text
done | grep -i pendingdeletion
```

If the **data lake** key is listed, and you want the existing data:

```bash
aws kms cancel-key-deletion --key-id <lake-key-id>
aws kms enable-key          --key-id <lake-key-id>
```

Leave the **MSK** keys to delete — MSK is recreated with a fresh key. This has now bitten
this project twice.

---

## 1. Apply

```bash
bash scripts/tf.sh plan            # review: expect 0 destroy, 0 replace
bash scripts/tf.sh apply --execute # phrase: APPLY THE SAVED PLAN
```

MSK creation dominates: **~30 minutes**.

### If apply ends with `AuthFailure` on an EC2 instance

A long apply can outlive a credential refresh. The instance is usually **created and in
state**; Terraform just could not confirm it, so it marks it **tainted** and the next plan
wants to destroy and rebuild it.

**Check before you let it:**

```bash
aws ec2 describe-instances --instance-ids <id> --query 'Reservations[].Instances[].State.Name'
aws ssm describe-instance-information --filters Key=InstanceIds,Values=<id> \
  --query 'InstanceInformationList[].PingStatus'
```

For the Airflow node, confirm it really came up:

```bash
aws ssm start-session --target <id>
sudo k3s kubectl get pods -n airflow      # want api-server, scheduler, triggerer, postgresql Running
```

If it is healthy, do not rebuild a working node:

```bash
cd terraform/envs/dev && terraform untaint 'module.airflow_k3s[0].aws_instance.this[0]'
```

---

## 2. Readiness gate

```bash
scripts/cdc-window-start.sh        # blocks until every component is genuinely healthy
```

Do not start testing on a non-zero exit. The window bills from the moment MSK is ACTIVE, so
the expensive mistake is debugging a "failure" that was really "not booted yet".

---

## 3. Enable CDC on the source databases — **before** the connectors

```bash
bash scripts/source-lab.sh enable-cdc --execute    # phrase: ENABLE CDC
bash scripts/source-lab.sh verify-cdc              # must end ALL CHECKS PASSED
```

Oracle restarts for ARCHIVELOG. Several minutes. Expected.

**Benign errors in this output — do not chase them:**

| Error | Meaning |
|---|---|
| `ORA-32588: supplemental logging attribute all column exists` | already enabled; idempotency guard |
| `ORA-01920: user name 'C##DBZUSER' conflicts` | user already exists; the `ALTER` after it sets the password |
| `SQLServerAgent Error: ... job is already running` | capture job already started |

`verify-cdc` must end with **`ALL CHECKS PASSED - safe to deploy connectors`**. If it says
`FAILURES ABOVE`, stop — deploying connectors onto a half-configured source is risk R15
(connector reports healthy, produces nothing).

---

## 4. Register the connectors

```bash
bash scripts/register-connectors.sh register --execute   # phrase: REGISTER CONNECTORS
bash scripts/cdc-runtime.sh status                       # both connectors RUNNING
```

Note the subcommand: `register --execute`, not `--execute` alone.

Expect **`HTTP 201`** per connector. Then the initial snapshot: ~2,000 Oracle rows,
~3,204 SQL Server rows.

---

## 5. Recover the Glue catalog (only if you kept the S3 data)

`terraform destroy` removes the Glue **databases**; the S3 data and every Iceberg
`metadata.json` survive. That leaves a lake holding all its data and none of its pointers —
Athena says `Table not found`, which reads like a data problem and is not.

```bash
python3 spark/ops/register_tables.py --bucket <lake-bucket> --dry-run   # discovery only, free
```

Actual registration needs a Spark session with Iceberg + Glue, so it must be submitted to
EMR Serverless — it cannot run on your laptop.

---

## 6. Verify end to end

`docs/VERIFY_END_TO_END.md`. Start at **§3** — the watermark/Glue/S3 cross-check that
catches a job reporting SUCCEEDED while producing no table.

---

## The seven defects, and what a regression looks like

All seven are fixed in this repository. Each was invisible until the one before it was
fixed.

| # | Symptom you would see | Root cause | Fixed in |
|---|---|---|---|
| 1 | `register` prints "registering …" then **nothing**; `GET /connectors` returns `[]` | JSON passed to `curl -d '...'`; apostrophes in the templates (`connector's`, `SQL Server's`, `'d' envelope`) closed the quote early | `scripts/register-connectors.sh` — payload is base64, decoded to a file on the node |
| 2 | `HTTP 500 Could not read properties from file .../source-lab.properties` | secrets written `0600 root`, but cp-kafka-connect runs as `uid=1000(appuser)` | `terraform/modules/cdc_runtime_ec2/templates/user-data.sh.tftpl` — `chown 1000:1000` (**not** `chmod 0644`, which would expose the CDC passwords to every local user) |
| 3 | `cdc-runtime.sh status` → `Error parsing parameter '--parameters': Expected: ','` | `--parameters "commands=[\"$*\"]"` string-interpolated; the jq expression's quotes broke the CLI parser | `scripts/cdc-runtime.sh` — `json.dumps`, the fix `register-connectors.sh` already had |
| 4 | The Oracle CDC password **printed in plaintext** during `enable-cdc` | SQL\*Plus echoes substitution-variable expansion by default | `docker/source-lab/oracle/02-enable-cdc.sql` — `SET VERIFY OFF` / `SET ECHO OFF` |
| 5 | `HTTP 400 Login failed for user 'dbzuser'` | server login existed with **no database user** in `digital` → "Cannot open database", which surfaces as a login failure | `docker/source-lab/healthcheck.sh` — creates login, user and role explicitly instead of re-running the init script and discarding its errors |
| 6 | `HTTP 400 User dbzuser does not have access to CDC schema` | Debezium calls `sys.sp_cdc_help_change_data_capture`, which needs the capture instances' gating role | same file — grants **`cdc_reader`**, not `db_owner`; verified sufficient by dropping `db_owner` and re-testing |
| 8 | Connector **RUNNING**, every task `FAILED` with `NoClassDefFoundError: io/apicurio/registry/serde/AbstractKafkaSerializer` | only the Apicurio *converter* and *avro-serde* were fetched; the chain `converter → avro-serde → serde-common → schema-resolver → client → rest-client-jdk` was incomplete | `docker/cdc-runtime/versions.env` + `verify-artifacts.sh` — the full chain pinned with SHA256 |
| 9 | `reset` returns `400 Connectors must be in the STOPPED state`, then `register` returns `409 already exists` — old offsets survive and the connector reports RUNNING while producing **nothing** | Kafka Connect 3.6+ refuses to modify offsets on a running connector; the script deleted without stopping | `scripts/register-connectors.sh` — `stop` → wait for `STOPPED` → delete offsets → `resume` |
| 7 | `HTTP 400 Failed to resolve Oracle database version`, server logs **`ORA-01005`** | **two** Oracle JDBC drivers on the plugin path — `ojdbc11.jar` and an `ojdbc8` bundled inside the Debezium archive. Classloader picked arbitrarily; `ojdbc8`'s O5LOGON against Oracle 23ai fails as a *null password* | `docker/cdc-runtime/verify-artifacts.sh` — deletes bundled `ojdbc8`, then **hard-fails** unless exactly one `ojdbc*.jar` remains |

### Why #7 was the hardest

Every symptom said "wrong password". The password was provably identical across SSM, the
source-lab `.env` and the cdc-runtime secrets file; it authenticated fine in sqlplus; and
passing it *inline* instead of through `${file:...}` failed the same way. Oracle's own
`unified_audit_trail` settled it:

```
C##DBZUSER  return_code 1005  CLIENT ADDRESS=(HOST=10.42.0.181)
```

Oracle received the username and **no password** — from the JDBC client, not from the
config. That is a driver fault, not a credential fault.

**Diagnostic worth reusing:**

```sql
SELECT TO_CHAR(event_timestamp,'HH24:MI:SS'), dbusername, return_code, authentication_type
FROM unified_audit_trail
WHERE return_code <> 0 AND event_timestamp > SYSTIMESTAMP - INTERVAL '40' MINUTE
ORDER BY event_timestamp DESC FETCH FIRST 8 ROWS ONLY;
```

---

## Rotating a source-lab password

**Updating SSM alone does nothing.** Both nodes read SSM only at boot:

```
SSM (rotated)        b75739ad2667
source-lab .env      535384758f67   <- still old
cdc-runtime secrets  535384758f67   <- still old
```

To actually rotate, update SSM **and** refresh the node files, then reconcile:

```bash
aws ssm put-parameter --name /kafka-dev-lab/dev/source-lab/oracle-cdc-password \
  --type SecureString --overwrite --value "<new>"

# re-run user-data so both nodes re-read SSM (taint forces a clean bootstrap)
cd terraform/envs/dev
terraform taint 'module.cdc_runtime[0].aws_instance.cdc_runtime'
cd ../../.. && bash scripts/tf.sh apply --execute

bash scripts/source-lab.sh enable-cdc --execute   # ALTERs the DB to the new value
```

Verify all three agree before trusting a rotation:

```bash
aws ssm get-parameter --name /kafka-dev-lab/dev/source-lab/oracle-cdc-password \
  --with-decryption --query Parameter.Value --output text | md5sum | cut -c1-12
```

---

## Cost

Full stack is **~$1.23/hr**; MSK is ~66% of it and **cannot be stopped, only destroyed**.
Against the **$30/month** budget of record (ADR-030) that is roughly a month of budget per
day. Stop as soon as the run is proven: `docs/runbooks/stop-and-resume.md`.
