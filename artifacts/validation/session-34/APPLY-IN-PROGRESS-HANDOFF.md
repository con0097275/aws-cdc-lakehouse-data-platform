# Handoff — Airflow APPLY SUCCEEDED, 2026-08-23

## Watch it

```bash
tail -f /tmp/claude-1000/-home-operator-terraform-kafka-kraft-aws-cdc-lakehouse/04073156-49a6-4d01-aa98-e8adf3e76646/scratchpad/apply2.log
```

Ctrl-C stops the tail, not the apply. Expect **25-40 min** (MSK dominates).

## What happened, in order

1. Budget raised to **$50**; the `<= 30` guard in `terraform/envs/dev/main.tf` amended to
   `<= 50` with a comment. **ADR-030 still says $30 and must be updated.**
2. `enable_airflow = true`.
3. First apply ran 20 min and **FAILED**:
   `DependencyViolation: subnet-087e0a98f3d567dfd has dependencies`.
4. Root cause: the **Glue interface endpoint** `vpce-0ab68b05be981f1ca` held an ENI in that
   subnet. Terraform's replacement graph did not include the `vpc_endpoints` module, so the
   endpoint outlived the subnet it lives in. **This is a module ordering defect** — the
   endpoint must be destroyed before, and recreated after, its subnet.
5. Endpoint deleted by hand; ENI released after ~130 s; re-planned clean (23 create,
   1 replace) and re-applied.

At the point of failure EVERYTHING was destroyed and NOTHING recreated: no MSK, no EC2.
That is why the second apply creates 23 resources.

## Why the subnets were being replaced at all

`availability_zone -> (known after apply) # forces replacement` — the
`aws_availability_zones` data source is unresolvable at plan time. **Pre-existing**: proved
by reverting every edit and re-planning (still 18 destroy). Tracked as OPEN-34, but the
stale `auto_destroy_after` is NOT the whole story — reverting it did not clear the
replacement. Root cause still unknown and worth a session of its own.

## When the apply finishes

```bash
AWS_PROFILE=my-aws-profile aws ec2 describe-instances --region ap-southeast-1 \
  --filters Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].[InstanceId,InstanceType,Tags[?Key==`Component`].Value|[0]]' --output text
```

Expect **4**: source-lab, cdc-runtime, toolbox, **airflow**.

### Then, for P0-1 (the actual goal)

1. **DAGs import** — on the airflow node, `airflow dags list`. Expect exactly the four
   flow DAGs plus the streaming lifecycle. Any import error shows here first.
2. **Turn 1 parallelism** — `datamart_eod` expands over the plan; both marts are in turn 1
   and must run CONCURRENTLY, not in sequence.
3. **Turn ordering** — to prove turn 2 WAITS, the pilot graph needs a two-turn shape. Both
   current marts are leaves. Add a fixture job with a `JOB` dependency on one of them; do
   not reshape the real marts to make a test pass.
4. **Gates** — each mart declares an `EOD_TABLE` dependency. `dataset_gates.CompositeGate`
   now enforces those (it did not before today). Pass one into `Coordinator(...)` to see
   WAITING_DEPENDENCY when the table is not closed.

## State after the rebuild

- **Lake data SURVIVES**: S3, DynamoDB (4 tables), Glue, every Iceberg table —
  FULL_CDC 20,390 rows, curated, mart, REALTIME, streaming ledger. Airflow can be proven
  against this WITHOUT re-seeding.
- **source-lab is NEW and EMPTY** — `healthcheck.sh --init` then `--seed` only if CDC is
  wanted again.
- **cdc-runtime is NEW** — check `/opt/cdc-runtime/secrets/source-lab.properties` exists
  (user-data writes it) and is `root:<container-gid> 0640`, not `0600 root`, or
  FileConfigProvider cannot read it and connector registration returns 500.
- **lake_iam** is restored by this apply; verify `spark-eod` / `spark-stream` have policies
  before submitting Spark, or jobs fail on `kms:GenerateDataKey`.

## Cost

$32.64 spent of the new $50. Full stack ~$1.11/hr. Airflow adds ~$0.04/hr.


---

# RESULT — apply completed successfully

```
MSK  kafka-dev-lab-dev   ACTIVE
EC2  i-0cce8e354416f0404  t3a.xlarge  source-lab    (NEW, empty)
     i-07c7985dcf0f496da  t3.large    cdc-runtime   (NEW)
     i-021bb7bb9e20b8323  t3.large    AIRFLOW       (NEW - P0-1)
     i-0ea555aee2d230404  t3.small    toolbox       (NEW)
```

**The Airflow node exists. Airflow itself was NOT yet verified** — the instance had only
just launched and its SSM agent had not registered, so k3s and the chart had not finished
bootstrapping. That is expected at this point, not a failure.

## Next session starts HERE

```bash
export AWS_PROFILE=my-aws-profile
AF=i-021bb7bb9e20b8323

# 1. did the node finish bootstrapping?
aws ssm send-command --instance-ids $AF --document-name AWS-RunShellScript \
  --region ap-southeast-1 --parameters 'commands=[
    "cloud-init status","systemctl is-active k3s",
    "k3s kubectl get pods -A","tail -20 /var/log/cloud-init-output.log"]'

# 2. DAGs import, and ONLY the four flow DAGs exist
#    (expect datamart_eod / _auto_correct / _fulfill / _stream_batch + streaming lifecycle)
#    airflow dags list   inside the scheduler pod

# 3. turn 1 parallelism: datamart_eod expands over the plan; BOTH marts are turn 1
#    and must run CONCURRENTLY

# 4. turn ORDERING: both current marts are LEAVES, so they prove parallelism only.
#    Add a fixture job with a JOB dependency to prove turn 2 waits.
#    Do NOT reshape the real marts to make the test pass.
```

## Verify before trusting anything Spark-side

```bash
for R in spark-eod spark-stream connect source-lab; do
  aws iam list-attached-role-policies --role-name kafka-dev-lab-dev-$R \
    --query 'length(AttachedPolicies)' --output text
done   # all must be > 0
```

## cdc-runtime is NEW

Check `/opt/cdc-runtime/secrets/source-lab.properties` exists and is
`root:<container-gid> 0640`. If it is `0600 root`, FileConfigProvider cannot read it and
connector registration returns HTTP 500 (this happened twice).

## Lake data is INTACT

FULL_CDC 20,390 rows, curated, mart, REALTIME, streaming ledger, 4 DynamoDB tables.
Airflow can be proven against this with NO re-seeding. source-lab is empty but is only
needed if you want fresh CDC.
