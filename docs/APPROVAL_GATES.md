# APPROVAL GATES

- Session: 01 (addendum)
- Status: **NORMATIVE** — these gates bind every later session
- Related: `CLAUDE.md` §2, §9; `CLAUDE_SKILLS_FULL_PLAYBOOK.md`; ADR-027

Five classes of action require explicit human approval before execution. Session 02 is
the first session that can spend money or mutate anything, so these gates exist now
rather than at Session 17.

**A gate is not a formality.** Each one below exists because a specific,
already-identified failure would otherwise be discovered by its invoice or by silent
data loss.

---

## 0. Universal preconditions

Apply to **all five** gates. Any single failure blocks the action.

| # | Precondition | Command | Blocks if |
|---|---|---|---|
| 0.1 | Identity printed and verified | `aws sts get-caller-identity --profile my-aws-profile --region ap-southeast-1` | account ≠ `111122223333` |
| 0.2 | Profile is correct | — | profile is `default`, `prod`, or anything but `my-aws-profile` (`CLAUDE.md` §2) |
| 0.3 | Region is correct | — | region ≠ `ap-southeast-1` |
| 0.4 | Workspace/environment is correct | `terraform workspace show` | not `dev` |
| 0.5 | Working tree is committed | `git status --porcelain` | uncommitted changes — the diff under review must be the diff that runs |
| 0.6 | Static checks pass | `make validate-docs`, `terraform fmt -check`, `validate`, `tflint`, `checkov` | any failure |
| 0.7 | Budget notification is live | `aws budgets describe-budgets --account-id 111122223333` | subscriber list is empty |

> **0.7 PASSES — corrected 2026-08-12 (Session 02 Stage A).**
>
> Session 01 recorded 0.7 as failing because repo A sets `budget_email = ""`. That
> inspected the Terraform variable, not the account, and it was wrong. A read-only
> check (`artifacts/validation/session-02/aws/gate-0.7-budgets.txt`) found **two
> account-wide budgets already in place**:
>
> | Budget | Limit | Filter | Notifications | Subscriber |
> |---|---:|---|---|---|
> | `My Monthly Cost Budget` | $30.00 | none — account-wide | ACTUAL >85 %, >100 %; FORECASTED >100 % | **owner@example.com** |
> | `My Zero-Spend Budget` | $1.00 | none — account-wide | ACTUAL > $0.01 | — |
>
> Being unfiltered, they cover this project automatically — and they cannot miss an
> untagged resource, which a `Project`-tag-filtered budget can. Gate 1 is **not** blocked.
>
> The lesson generalises: **0.7 is a check against the account, not against the code.**
> Run the command in the table; never infer the answer from a `.tfvars` file.
>
> Carry forward as **OPEN-09**: the live budget is **$30** while `docs/COST.md` sizes
> the envelope against **$80**. At $30 the affordable window count is ~2.6, not ~7.9.
> Reconcile before Stage B applies anything hourly.

---

## Gate 1 — `terraform apply`

The gate that spends money.

### Must be shown before approval

1. **Saved plan file**, not a re-planned apply:
   ```bash
   terraform plan -out=tfplan
   terraform show -no-color tfplan > artifacts/validation/session-NN/plan.txt
   ```
   Approval is granted against `tfplan` and `apply` runs `terraform apply tfplan`. A
   plan generated at apply time is a different plan.
2. **Resource action counts** — `to add / change / destroy`, with every `destroy` and
   every `replace` named individually and justified. A surprise replacement of MSK or a
   KMS key is the expensive failure this catches.
3. **Cost delta**, per `docs/COST.md`: marginal $/hr, expected window duration, total
   window cost, and the running month-to-date against the **$30** budget (ADR-030).
4. **Security assertions**, mechanically checked against the plan:
   - zero ingress rules with `cidr_blocks = ["0.0.0.0/0"]` on SSH, database, Kafka,
     Airflow, Trino, Redshift, Grafana, Prometheus, Registry or Connect REST ports
   - no `key_name` on any instance
   - no plaintext secret in any resource attribute or output
   - all six required tags present on every taggable resource
   - `describe-nat-gateways` will remain empty
5. **Destroy plan for the same change**, proving reversibility before creation:
   ```bash
   terraform plan -destroy -out=tfplan-destroy
   ```
6. **`auto_destroy_after`** set to a real ISO-8601 timestamp for this window — not
   `"manual"`.

### Approval

Human operator, explicitly, per apply. Approval of one apply never carries to the next.

### Blocks

- Any universal precondition failing, **including 0.7**
- Any unexplained `destroy` or `replace`
- Cost delta pushing month-to-date over $80 without a recorded decision
- `enable_redshift_serverless` **and** `enable_trino` both true without the ADR-026
  override plus a `DECISION_LOG.md` entry
- MSK `broker_ebs_gib` > 20 or `enable_storage_autoscaling = true` without a recorded
  decision — MSK storage **cannot be reduced after creation** (risk R12)

### Evidence retained

`artifacts/validation/session-NN/`: `plan.txt`, `plan-destroy.txt`, apply output,
`aws sts get-caller-identity` output, post-apply resource inventory.

### Post-apply, mandatory

Record every created billable resource in `PROJECT_STATE.md` § *Live resources / cost
drivers*, each with its ARN/ID, start time, stop/destroy command and owner. An
un-recorded live resource is an unbounded cost.

---

## Gate 2 — Helm / Kubernetes changes

Sessions 12, 13C, 15.

### Must be shown

1. **Rendered diff**, never a blind upgrade:
   ```bash
   helm template <release> <chart> --version <pinned> -f values.yaml   # first install
   helm diff upgrade <release> <chart> --version <pinned> -f values.yaml   # subsequent
   ```
2. **Pinned chart version and image tags.** No `latest`, no floating tag
   (`CLAUDE.md` §3.9). Each image digest recorded in `docs/VERSIONS.md`.
3. **Executor assertion** for Airflow: the value is `KubernetesExecutor`.
   `KubernetesCeleryExecutor` does not exist and `CeleryKubernetesExecutor` is legacy
   (`CLAUDE.md` §7, ADR-010).
4. **Resource requests and limits** on every pod. An unbounded pod on a single-node
   `t3.large` k3s cluster evicts the scheduler.
5. **No Service of type LoadBalancer or NodePort** — private-only access via SSM port
   forwarding. A LoadBalancer creates a billable, publicly-reachable ELB.
6. **PVC plan** for the Airflow metadata database, with the backup path
   (`CLAUDE.md` §7 requires backup/retention).
7. **Rollback command** stated before the change:
   `helm rollback <release> <revision>`.

### Approval

Human operator per release change. Config-only value changes still require a rendered
diff — a values change can delete a PVC.

### Blocks

- Unpinned chart or image
- Any `LoadBalancer` / `NodePort` service
- Missing resource limits
- A diff showing PVC deletion or replacement without an accepted backup
- `helm upgrade --force` or `--recreate-pods` without a stated reason

### Evidence retained

Rendered template or diff, `helm history`, `kubectl get pods -o wide` before and after.

---

## Gate 3 — Kafka connector registration

Sessions 04, 05. **The gate with the largest blast radius on the source databases.**

### Must be shown

1. **Connector JSON with secrets externalized.** Every credential is a
   `${secretsManager:...}` reference through Connect's `config.providers`, never a
   literal. Connect's REST API echoes its configuration back — a literal password is
   published to anyone who can reach the endpoint.
2. **Pre-registration MSK IAM proof**, run from the Connect host with the worker's exact
   client configuration:
   ```bash
   kafka-topics.sh --bootstrap-server <brokers> --command-config client.properties --list
   ```
   This is the risks R2/R3 mitigation. Without it, the connector fails with
   `SaslAuthenticationException` and the cause is indistinguishable from a dozen others.
3. **Topics pre-created explicitly**, with partition count and replication factor.
   `auto.create.topics.enable=false` (Gap 12), so a connector expecting
   auto-creation produces nothing and reports healthy.
4. **`kafkasql-journal` exists** before Apicurio starts (risk R3). The registry cannot
   create its own backing topic and fails by returning 5xx on write while looking up.
5. **Key strategy assertion**: the record key is the canonical PK with stable field
   order, so the same PK always lands in the same partition (`CLAUDE.md` §5.1, §5.4).
6. **Snapshot mode and its cost.** An unintended `initial` snapshot on a large table
   reads the whole table and can exceed Kafka's 24-hour retention while consumers are
   still catching up (risk R6).
7. **DLQ configured**: `errors.tolerance`, `errors.deadletterqueue.topic.name`,
   `errors.deadletterqueue.context.headers.enable=true`.
8. **Schema compatibility level** confirmed `BACKWARD` on the value subject, and the
   subject naming strategy `TopicNameStrategy` (`docs/DATA_CONTRACTS.md` §10). Changing
   the naming strategy later orphans every registered schema.

### Approval

Human operator per connector, and again for any configuration change. A
`PUT /connectors/<name>/config` is as consequential as the original registration.

### Blocks

- Any literal credential in the connector JSON
- MSK IAM proof not performed
- Target topics absent
- No DLQ configured
- Snapshot mode not stated
- Connector REST endpoint reachable from outside the VPC

### Evidence retained

Connector JSON with secrets redacted, `kafka-topics --list` output,
`GET /connectors/<name>/status`, topic descriptions, first N records' keys proving
partition assignment.

---

## Gate 4 — Database CDC changes

Session 03, and any later schema change. **These are mutations to a database, and some
are not cleanly reversible.**

### Must be shown

1. **Exact DDL**, reviewed statement by statement. Oracle
   `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`, SQL Server `sys.sp_cdc_enable_db` and
   `sp_cdc_enable_table` — with the table list explicit, never a wildcard.
2. **Impact statement.** Supplemental logging increases redo volume; SQL Server CDC
   creates change tables and capture jobs that grow. On a lab instance with bounded
   disk, both can fill the volume.
3. **Retention setting**, with its relationship to Kafka's 24-hour retention and the
   24-hour RPO (risk R6, OPEN-02). Source-side CDC retention shorter than the outage
   window means a re-snapshot is the only recovery.
4. **Reversal DDL**, written before the change:
   `sys.sp_cdc_disable_table`, `ALTER DATABASE DROP SUPPLEMENTAL LOG DATA`.
5. **Confirmation the data is synthetic.** ADR-005 permits these mutations *because* the
   lab holds only generated data. The same DDL against real data is a different
   decision requiring a different approval.
6. **Verification queries** to be run after, proving the change took effect:
   ```sql
   -- Oracle
   SELECT log_mode, supplemental_log_data_min FROM v$database;
   -- SQL Server
   SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'OPSDB';
   SELECT name, is_tracked_by_cdc FROM sys.tables;
   ```

### Approval

Human operator per database, per change set.

### Blocks

- Reversal DDL not written
- Wildcard table enablement
- Any statement touching a non-lab database
- Any `DROP`, `TRUNCATE` or `DELETE` against source data — outside the scope of a CDC
  enablement change entirely
- Verification queries not asserted **before** the connector is deployed. A connector
  against a CDC-disabled table starts healthy and silently produces nothing (risk R15).

### Evidence retained

DDL executed, verification query output, reversal DDL, disk usage before and after.

---

## Gate 5 — Destructive cleanup

Sessions 17 and every window close. **The most-used gate in this project**, because
ADR-027 makes destroy the routine end of every window rather than an exception.

### Must be shown

1. **Destroy plan**, reviewed:
   ```bash
   terraform plan -destroy -out=tfplan-destroy
   terraform show -no-color tfplan-destroy > artifacts/validation/session-NN/plan-destroy.txt
   ```
2. **Explicit list of what is intentionally retained** and why: state bucket, S3 lake,
   CMKs, secrets, budget. A destroy plan that includes the state bucket is a hard stop
   (ADR-021).
3. **Data-durability assertion.** Anything needed by the next window is already in S3:
   `ops.layer_watermark`, L1/L2 Iceberg tables, Airflow metadata `pg_dump`, DAGs in Git.
   Destroying MSK loses unread topic data — that is accepted and bounded by the 24-hour
   RPO, but it must be a decision each time, not a discovery.
4. **Iceberg maintenance is not mid-flight.** `remove_orphan_files` and
   `rewrite_data_files` must not be running. Orphan cleanup with a retention below
   72 hours can delete files an in-flight commit is about to reference (risk R14).
5. **Two-key confirmation for irreversible deletions**: KMS `schedule-key-deletion`,
   S3 bucket emptying, Redshift namespace deletion. Each is stated separately and
   approved separately from the destroy itself.

### Approval

Human operator per destroy. **Routine does not mean automatic.**

### Blocks

- State bucket in the destroy plan
- S3 lake bucket deletion without an explicit, separate approval
- Any Iceberg maintenance job running
- `terraform destroy` without `-target` when only a subset was intended
- **Any destroy against a workspace or account other than `dev` / `111122223333`**

### Mandatory post-destroy verification

```bash
aws kafka list-clusters-v2      --profile my-aws-profile --region ap-southeast-1 --query 'length(ClusterInfoList)'          # 0
aws ec2 describe-instances      --profile my-aws-profile --region ap-southeast-1 \
  --query 'Reservations[].Instances[?State.Name!=`terminated`].InstanceId'                                              # []
aws ec2 describe-volumes        --profile my-aws-profile --region ap-southeast-1 --query 'Volumes[].VolumeId'               # []  <- the forgotten one
aws ec2 describe-vpc-endpoints  --profile my-aws-profile --region ap-southeast-1 --query 'VpcEndpoints[].VpcEndpointId'     # []
aws ec2 describe-nat-gateways   --profile my-aws-profile --region ap-southeast-1 --query 'NatGateways[?State!=`deleted`]'   # []
aws emr-serverless list-applications    --profile my-aws-profile --region ap-southeast-1                                    # []
aws redshift-serverless list-workgroups --profile my-aws-profile --region ap-southeast-1                                    # []
aws redshift-serverless list-namespaces --profile my-aws-profile --region ap-southeast-1                                    # []  <- storage bills without a workgroup
```

`describe-volumes` and `list-namespaces` are the two that catch real leaks: EBS bills
while a volume exists even with no instance, and a Redshift namespace keeps managed
storage after its workgroup is gone. A destroy verified only by
`list-clusters-v2` is not verified.

Record the result and the post-destroy Cost Explorer figure in
`IMPLEMENTATION_REPORT.md`. The floor should be **~$2.28/month** (ADR-030); anything higher means
something survived.

---

## Gate summary

| Gate | Trigger | Approver | Hard stop |
|---|---|---|---|
| 0 | any of gates 1–5 | — | wrong account, profile, region or workspace; budget notifies nobody |
| 1 | `terraform apply` | operator, per apply | unexplained destroy/replace; over budget; MSK storage growth |
| 2 | `helm install/upgrade`, `kubectl` mutation | operator, per release | unpinned version; LoadBalancer; PVC deletion |
| 3 | connector `POST`/`PUT` | operator, per connector | literal credential; no MSK IAM proof; no DLQ |
| 4 | source database DDL | operator, per change set | no reversal DDL; non-lab database; wildcard tables |
| 5 | `terraform destroy`, key/bucket deletion | operator, per destroy | state bucket in plan; wrong account; maintenance in flight |

**No gate may be self-approved by an agent.** `CLAUDE.md` §9.7 requires distinguishing
`static-validated`, `planned`, `deployed` and `live-tested`; an agent may prepare and
present a gate package and may state that it is complete, but the transition from
`planned` to `deployed` is a human action.
