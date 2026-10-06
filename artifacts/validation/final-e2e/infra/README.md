# artifacts/validation/final-e2e/infra

Evidence for the **minimum live resource bring-up**, requested 2026-09-03.

## Status: NOT BROUGHT UP. Nothing was applied. $0.00 spent.

| File | What |
|---|---|
| `00-pre-apply-baseline.txt` | Full gate re-verification, plan freshness, live inventory, blockers |
| `01-apply-refused.txt` | `scripts/tf.sh apply --execute` output, ending in the gate refusal |

Re-verification passed on every point that was mine to check — identity `111122223333`,
region `ap-southeast-1`, workspace `default`, backend profile pinned, provider `6.56.0`
exact, state unchanged at 3 resources, **saved plan still valid** (`248 create / 0 change /
0 destroy / 0 replace`, sha256 `c70ba8a7…`), and **$0.0000/hr** burning.

Nothing has materially changed since the plan. The bring-up did not proceed for three
reasons, none of which is a fault in the plan:

- **B1 — `auto_destroy_after` is still `2026-08-25T00:00:00Z`, 9 days expired.** Condition
  C1 of `docs/E2E_LIVE_TEST_PLAN.md` §3, not met. Applying now stamps all 248 resources with
  an already-expired `AutoDestroyAfter` tag, which is the ADR-027 control that says when the
  lab must come down. It is the cheapest possible moment to fix — with 3 resources in state
  nothing is replaced; once MSK exists, changing this value replaces all 6 subnets and
  cascades into MSK, EMR and every EC2 instance (OPEN-34).
- **B2 — `monthly_budget_usd` is still `100`**, against the `<= 50` check assertion
  (`main.tf:95`, ADR-030 as amended). Condition C2, not met.
- **B3 — the approval gate cannot be satisfied by an agent, by design.**
  `scripts/tf.sh apply --execute` calls `confirm_destructive()`, which at
  `scripts/lib.sh:161-163` does `[[ ! -t 0 ]] && die`. Its own comment: *"Refuses outright
  when stdin is not a terminal, so no automation can satisfy it."*
  `scripts/validate-docs.py` asserts that refusal still exists, so removing it fails
  `make check`.

Both `tf.sh` branches were traced before running anything: without `--execute` the dispatch
`exit 0`s before `cmd_apply`; with `--execute` it sets `DRY_RUN=0` and dies at the TTY check
before `terraform apply` is reached. There is no non-interactive path that applies. Verified
after the refusal: state still 3 resources, 0 EC2, 0 MSK.

`-auto-approve` was not used, and bare `terraform apply` was not run — that is precisely
what this control exists to prevent.

---

## Verification runbook — run after a successful apply

Each row uses the **approved repository script** where one exists. Save each output into
this directory under the filename given. Nothing here starts a resource that Terraform
already created; `airflow-node.sh start` is the one legitimate start, for a node that is
created stopped.

### Phase 1 — readiness gate (blocks everything else)

```bash
bash scripts/cdc-window-start.sh | tee artifacts/validation/final-e2e/infra/10-window-start.txt
```
Blocks until every component is genuinely healthy and prints the burn rate. **Exit 0 is
required before any test.** Billing starts at MSK ACTIVE, so the expensive mistake is
testing against a half-booted stack.

### Phase 2 — per-resource verification

| # | Resource | Command | Pass criterion | Evidence file |
|---|---|---|---|---|
| 1 | Network | `aws ec2 describe-vpcs --filters Name=tag:Project,Values=kafka-dev-lab` | **1** VPC, 6 subnets, **0** NAT | `11-network.txt` |
| 2 | MSK | `aws kafka list-clusters-v2` | 1 cluster, `ACTIVE`, 3 brokers, KRaft | `12-msk.txt` |
| 3 | Source lab | `bash scripts/source-lab.sh status` | both containers `healthy` | `13-source-lab.txt` |
| 4 | CDC runtime | `bash scripts/cdc-runtime.sh status` | `cdc-connect` + `apicurio` up | `14-cdc-runtime.txt` |
| 5 | Apicurio | `bash scripts/cdc-runtime.sh registry-ui` (SSM forward) | `/apis` responds; global rule `BACKWARD` | `15-registry.txt` |
| 6 | EMR Serverless | `aws emr-serverless list-applications` | 1 app, ARM64, `emr-7.2.0`, auto-stop 15 min | `16-emr.txt` |
| 7 | Airflow | `bash scripts/airflow-node.sh status` then `start --execute` | 7 pods Running; UI 200 via SSM | `17-airflow.txt` |
| 8 | Reporting state | `aws dynamodb list-tables` | **4** tables, all `PAY_PER_REQUEST` | `18-dynamodb.txt` |
| 9 | S3 + roles | `aws s3 ls s3://kafka-dev-lab-dev-lake-111122223333/` | 8 prefixes; lake objects intact | `19-s3.txt` |
| 10 | Glue | `aws glue get-databases` | **7** project databases | `20-glue.txt` |
| 11 | Athena | `bash scripts/athena-smoke.sh --execute` | prefixed schemas resolve; 10 GiB cutoff enforced | `21-athena.txt` |
| 12 | CloudWatch | `aws cloudwatch describe-alarms --alarm-name-prefix kafka-dev-lab-dev` | **9** alarms, none in `ALARM` | `22-cloudwatch.txt` |
| 13 | AI runtime | `make ai-tools` and `make ai-governance` | 8 tools resolve; IAM role present; **$0.00/hr** | `23-ai.txt` |

### Phase 3 — connectivity and health smoke tests

```bash
bash scripts/source-lab.sh verify-cdc      # ARCHIVELOG + supplemental logging + sp_cdc_enable_*
bash scripts/cdc-runtime.sh smoke          # includes the NEGATIVE auth test
bash scripts/cdc-runtime.sh topics         # via MSK IAM, not plaintext
bash scripts/register-connectors.sh status # both connectors RUNNING, task 0 RUNNING
bash scripts/show-cost-resources.sh --compute
```

`verify-cdc` is not optional: without ARCHIVELOG and supplemental logging the Debezium
connector runs **HEALTHY and produces nothing** (risk R15), which costs a whole window to
diagnose.

### Ordering traps

- Run `cdc-window-start.sh` **first**, and do not start testing on a non-zero exit.
- `source-lab.sh enable-cdc` **before** `register-connectors.sh register`.
- `cdc-runtime.sh create-topics` **before** registering — topics are pre-created with fixed
  partition counts and the connectors must not create them.
- Note the wall-clock apply time. The tag-filtered budget reports hours late and cannot stop
  anything; MSK is 62% of burn and cannot be stopped, only destroyed.
