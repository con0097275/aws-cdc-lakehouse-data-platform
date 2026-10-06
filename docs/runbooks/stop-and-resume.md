# Runbook — stop for the night, resume correctly

The lab is designed to be brought up, used, and taken down again. This is the procedure for
doing that without losing the deployment, and without paying for it while you are not using
it.

Written after Session 36, where resuming exposed four things that had only ever worked
because they were fixed by hand on a running node. They are all in Terraform now — see
[What is automatic now](#what-is-automatic-now) before you assume a manual step is needed.

---

## Decide which of the two you are doing

| | STOP | DESTROY |
|---|---|---|
| Compute billing | ends | ends |
| EBS / S3 / KMS | **keeps billing** | S3 + KMS keep billing |
| MSK | **cannot be stopped** — see below | deleted |
| Airflow metadata DB | survives | lost (local-path PVC on the node) |
| Time to resume | ~5 min | ~30 min (MSK creation dominates) |
| Command | `scripts/stop-ephemeral.sh --execute` | `scripts/tf.sh destroy --execute` |

**MSK is the decision.** It is ~66% of the burn and it has no stop. The only way to stop
paying for it is to delete it, which is a destroy, not a shutdown.

| Component | Rate | Source |
|---|---|---|
| MSK brokers, 3 × `kafka.m7g.large` | 0.7650 USD/hr | `docs/COST.md` §116-121 |
| MSK broker storage, 300 GiB | 0.0493 USD/hr | `docs/COST.md` |
| source lab `t3a.xlarge` | 0.1888 USD/hr | `module.source_lab` output |
| Airflow / CDC runtime `t3.large` | 0.1056 USD/hr each | ap-southeast-1 on-demand |
| Whole stack as last planned | **0.9161 USD/hr** | `scripts/tf.sh plan` cost block |

So: keeping MSK overnight costs about **$19.50/day** for nothing. Stopping only the EC2
instances saves roughly $0.42/hr and leaves the expensive thing running.

---

## A. Stop for the night, keep the deployment

```bash
scripts/stop-ephemeral.sh              # dry run first, always
scripts/stop-ephemeral.sh --execute
```

Stops the four EC2 instances. **Does not touch MSK** — deliberately, because stopping a
managed MSK cluster is not a thing that exists.

If you are not coming back tomorrow, do a destroy instead. Paying $19.50/day to keep a
Kafka cluster warm for a lab is the single most expensive habit available here.

### Resume from a stop

```bash
scripts/tf.sh plan                     # expect: 0 to add, 0 to destroy
aws ec2 start-instances --region ap-southeast-1 --profile my-aws-profile \
  --instance-ids $(aws ec2 describe-instances --region ap-southeast-1 --profile my-aws-profile \
    --filters "Name=tag:Project,Values=kafka-dev-lab" Name=instance-state-name,Values=stopped \
    --query 'Reservations[].Instances[].InstanceId' --output text)

scripts/cdc-window-start.sh            # blocks until everything is genuinely healthy
```

The Airflow node keeps its metadata DB, its pools and its DAG history across a stop. k3s
and the Helm release come back on their own. Nothing below needs re-running.

Then follow `docs/runbooks/airflow-access.md` for the UI tunnel.

---

## B. Destroy, and rebuild later

```bash
scripts/tf.sh destroy --execute
scripts/verify-destroy.sh              # confirms nothing billable survived
```

### Rebuild

```bash
scripts/tf.sh plan                     # REVIEW the cost block before applying
scripts/tf.sh apply --execute          # ~25 min, MSK dominates
scripts/cdc-window-start.sh
```

`apply` alone is now enough to get a **working, wired** Airflow. That was not true before
Session 36.

---

## What is automatic now

Everything in this list was, at some point, a manual fix on a live node. Each is now in
Terraform or in `stage2.sh`, so a rebuild reproduces it. Do not re-do these by hand.

| Thing | Where it lives now | What it was |
|---|---|---|
| Helm chart installs at all | `airflow/helm/values.yaml` — `useHelmHooks: false` on both jobs | migrations ran as a post-install hook while `--wait` waited for pods that were blocked on migrations. Deadlock. |
| Airflow pools exist | `stage2.sh` — creates `reporting_jobs`, `spark_jobs`, `streaming_apps`, `maintenance` | no pools existed, so every task pinned to one sat unschedulable with **no state at all** and only a scheduler warning |
| EMR app id, job role, workgroup, KMS, region | `templatefile()` in `modules/airflow_k3s/main.tf` | looked up on the node, which is denied `iam:GetRole` and `emr-serverless:ListApplications`; placeholders survived into the running deployment |
| `.airflowignore` survives | `stage2.sh` — excluded from the DAG sync's `--delete` | deleted every minute, so Airflow periodically parsed the vendored reporting package as DAGs |
| Airflow may submit to EMR | `modules/lake_iam` — `reporting_submit` policy + `athena_query` attachment | role had `lake_read` + `secrets_read` only; the Athena gate failed on `s3:PutObject` before EMR was ever reached |

`stage2.sh` now **refuses to install** if any `REPLACED_AT_BOOTSTRAP_` token survived the
render, rather than baking a placeholder into a running deployment.

### Re-running the bootstrap by hand

It is idempotent, and this is the supported repair path if a node comes up wrong:

```bash
aws ssm send-command --region ap-southeast-1 --profile my-aws-profile \
  --instance-ids "$AF" --document-name AWS-RunShellScript \
  --parameters 'commands=["/opt/airflow-stage2.sh"]'
```

It needs six variables exported by stage 1 (`LAKE_BUCKET`, `AWS_REGION`, `CHART_VERSION`,
`APP_VERSION`, `PROJECT_NAME`, `ENVIRONMENT`). On a node built by Terraform they are already
in `/opt/run-stage2.sh`; run that instead if it exists.

---

## Verify the resume actually worked

Do not trust "the DAGs are visible". They were visible for a whole session while being
unable to run anything.

```bash
# 1. all four flow DAGs present, zero import errors
kubectl -n airflow exec deploy/airflow-dag-processor -c dag-processor -- \
  airflow dags list | grep datamart
kubectl -n airflow exec deploy/airflow-dag-processor -c dag-processor -- \
  airflow dags list-import-errors        # expect "No data found"

# 2. pools exist -- without these nothing schedules and nothing says why
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- airflow pools list

# 3. the runtime wiring reached the pods
kubectl -n airflow exec deploy/airflow-dag-processor -c dag-processor -- \
  printenv | grep -E 'REPORTING_|AWS_REGION' | sort
#    REPORTING_EMR_APPLICATION_ID must NOT be empty or a REPLACED_ token

# 4. the role can actually submit
aws iam list-attached-role-policies --role-name kafka-dev-lab-dev-airflow \
  --region ap-southeast-1 --profile my-aws-profile \
  --query 'AttachedPolicies[].PolicyName'
#    expect: lake-read, secrets-read, athena-query, reporting-submit, SSMManagedInstanceCore
```

Then trigger one wave and confirm the mapping expands **per job**, not per plan key:

```bash
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow dags trigger datamart_eod --logical-date 2026-08-22T00:00:00+00:00 --run-id resume_check

kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- \
  airflow tasks states-for-dag-run datamart_eod resume_check
```

`turn_1.gate_and_run` should show one `map_index` **per job in turn 1** (two, for the
current plan). Three map indices for every wave regardless of the plan is the old
`.map()`-over-a-dict defect and means an old `reporting_common.py` is deployed.

---

## Cost hygiene

- `auto_destroy_after` in `terraform/envs/dev/terraform.tfvars` is a **tag, not a timer**.
  Nothing enforces it. It is there so a sweep can find forgotten resources; it will not
  turn anything off for you.
- The budget alarm is `kafka-dev-lab-dev-monthly`. Check actual spend with
  `aws budgets describe-budgets --account-id <id>` — as of 2026-08-24 the account was
  already **over** its $30 budget at $35.36.
- EMR Serverless bills only while a job runs, with a 15-minute auto-stop. It is not part of
  the hourly figure above and does not need stopping.
