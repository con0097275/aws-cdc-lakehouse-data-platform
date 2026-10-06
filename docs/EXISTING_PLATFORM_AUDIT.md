<!-- PROVENANCE: copied verbatim from
     /path/to/aws-cdc-lakehouse-claude-guide-v2/docs/EXISTING_PLATFORM_AUDIT.md
     (Session 00 output, guide package v2). Copied 2026-08-12 by Session 01 under ADR-025.
     The original is read-only and checksummed by the guide package MANIFEST.json/SHA256SUMS.
     Corrections must be made HERE, not upstream. -->

# EXISTING PLATFORM AUDIT

- Session: 00 — Audit existing repository
- Date: 2026-08-07
- Subject: `~/terraform-kafka-kraft/kafka-kraft-aws/kafka-aws-production-lab` (authoritative — see `docs/SOURCE_REPOSITORY_DECISION.md`)
- AWS account: `111122223333` / region `ap-southeast-1` / profile `my-aws-profile`
- Method: read-only file inspection + read-only AWS API calls. No Terraform state operation, no AWS write.

> **Headline: the platform code is sound, but the platform is not running.** Everything below §7 describes what the code *would* build. As of 2026-08-07 the account contains none of it.

---

## 1. Runtime status — DESTROYED

Live inventory in `ap-southeast-1` (all read-only queries, 2026-08-06/07):

| Check | Command | Result |
|---|---|---|
| MSK clusters | `aws kafka list-clusters-v2` | **0** |
| MSK in other regions | same, for `us-east-1`, `ap-southeast-2`, `ap-northeast-1`, `eu-west-1` | **0** in each |
| Non-default VPCs | `aws ec2 describe-vpcs` | **0** — only default `172.31.0.0/16`. `vpc-0345fd1e6238b81ee` (`10.42.0.0/16`) is gone |
| EC2 instances (pending/running/stopping/stopped) | `aws ec2 describe-instances` | **0** — `i-03e1daf813522ba7b` is gone |
| KMS aliases `kafka*`/`cdc*`/`lake*` | `aws kms list-aliases` | **0** — `alias/kafka-prod-lab-lab` is gone |
| NAT gateways | `aws ec2 describe-nat-gateways` | **0** |
| Glue databases | `aws glue get-databases` | only `vannk-dev-oracle-db` (unrelated prior work) |
| Athena workgroups | `aws athena list-work-groups` | only `primary` |
| EMR Serverless / Redshift Serverless | `list-applications` / `list-namespaces` | **0** / **0** |

**Lifecycle evidence:**

- `terraform.tfstate.backup` — lineage `bd2a46c8-9ae6-abd3-d27b-1c18076b9d75`, serial **52**, **43 resources**, 10 outputs, Terraform 1.15.8.
- `terraform.tfstate` — **same lineage**, serial **106**, **0 resources**, 0 outputs.
- CloudTrail `DeleteCluster` by `my-aws-profile` at **2026-07-26T19:09:46Z**.

**Correction of upstream documents:** `kafka-aws-production-lab/IMPLEMENTATION_REPORT.md` and `VALIDATION_REPORT.md` claim no `terraform apply` was ever run. They predate the 2026-07-26 apply and are stale. State files and AWS APIs are ground truth.

---

## 2. Repository layout

Flat single root module. **There is no `modules/` directory** — this matters for `reference/TERRAFORM_MODULE_MAP.md`, which assumes a modular layout the platform repo does not have.

```
kafka-aws-production-lab/
├── CLAUDE.md  PLAN.md  PROMPT_CLAUDE.md  README.md
├── IMPLEMENTATION_REPORT.md  VALIDATION_REPORT.md   # stale — see §1
├── Makefile  .tflint.hcl  pyproject.toml  .gitignore
├── .github/workflows/ci.yml
├── app/        producer.py consumer.py create_topic.py load_test.py
│               smoke_checks.py requirements.txt
├── docs/       ARCHITECTURE.md COST.md KAFKA_PRODUCTION_GUIDE.md
│               OPERATIONS.md SECURITY.md VERSIONS.md
├── scripts/    preflight.sh deploy.sh destroy.sh verify-destroy.sh
│               smoke-test.sh ssm-shell.sh show-cost-resources.sh
│               connect-grafana.sh connect-prometheus.sh lib.sh
└── terraform/                       # root module, 14 .tf files
    ├── versions.tf providers.tf locals.tf variables.tf outputs.tf
    ├── network.tf msk.tf kms.tf iam.tf iam_samples.tf
    ├── toolbox.tf monitoring.tf budget.tf
    ├── templates/    user-data.sh.tftpl prometheus.yml.tftpl
    │                 alertmanager.yml alerts.yml
    │                 grafana-datasource.yml grafana-dashboard.json
    │                 grafana-dashboard-provider.yml
    ├── .terraform/            (providers only)
    ├── .terraform.lock.hcl
    ├── backend.hcl.example
    ├── terraform.tfvars  terraform.tfvars.example
    ├── terraform.tfstate  terraform.tfstate.backup  tfplan
```

The `scripts/` directory already implements the operational pattern this project needs — `preflight.sh`, `deploy.sh`, `destroy.sh`, `verify-destroy.sh`, `show-cost-resources.sh` — and maps closely onto the Make targets requested by `reference/COMMAND_CHEATSHEET.md`. Reusable as a pattern.

---

## 3. Backend — LOCAL state (integration blocker)

There is **no `backend` block anywhere** in `terraform/*.tf`. State is local files in `terraform/`.

An S3 backend exists only as an opt-in template, `terraform/backend.hcl.example`:

```hcl
# Create the backend separately. Do not let the disposable lab delete its own state backend.
bucket       = "replace-with-existing-terraform-state-bucket"
key          = "kafka-prod-lab/lab/terraform.tfstate"
region       = "ap-southeast-1"
encrypt      = true
use_lockfile = true      # S3-native locking; no DynamoDB table
```

The bucket is an unresolved placeholder, and `.terraform/` contains only `providers/` — no backend-config marker — confirming the last `init` used the local backend.

**Consequences:**

1. `terraform_remote_state` consumption is **not possible today**. Some change is required before any downstream module can read Repo A's outputs.
2. `CLAUDE.md:40` requires Terraform state to be encrypted, locked and IAM-restricted. A local state file on a workstation satisfies none of these. The state backup also contains the MSK ARN, VPC/subnet IDs and the Grafana SSM parameter name — it is sensitive.
3. The comment in `backend.hcl.example` line 1 is good guidance and should be preserved: the state backend must not be destroyable by the stack it stores.

---

## 4. Version pins

`terraform/versions.tf`:

| Item | Pin | Note |
|---|---|---|
| `required_version` | `~> 1.15.0` | state written by **1.15.8** |
| `hashicorp/aws` | **`6.56.0`** | exact, no `~>` |
| `hashicorp/random` | **`3.9.0`** | exact |

`.terraform.lock.hcl` is committed with hashes for `linux_amd64`, `darwin_amd64`, `darwin_arm64`, `windows_amd64`. This satisfies `CLAUDE.md:41`. Carry these pins forward into `docs/VERSIONS.md`.

---

## 5. Resource inventory (from state serial 52 — what a re-apply recreates)

43 managed resources + data sources.

**Network (`network.tf`)**

| Resource | Detail |
|---|---|
| `aws_vpc.this` | `10.42.0.0/16` |
| `aws_subnet.public` ×3 | `/24` at index 0–2, `map_public_ip_on_launch = true` (line 29) |
| `aws_subnet.private` ×3 | `/24` at index 10–12, `map_public_ip_on_launch = false` (line 43) |
| `aws_internet_gateway.this` | attached |
| `aws_route_table.public` | `0.0.0.0/0` → IGW (lines 54-55) |
| `aws_route_table.private` ×3 | **zero `route` blocks** (line 71 onward) — deliberate no-NAT design |
| `aws_route_table_association` ×6 | |
| `aws_security_group.toolbox` | **zero inbound rules**; egress all (lines 88-101) |
| `aws_security_group.msk` | self-all; 9098 / 11001 / 11002 sourced **from the toolbox SG only** (lines 128-148); egress all |

**MSK (`msk.tf`)** — `aws_msk_cluster.this`, `aws_msk_configuration.this`, `aws_cloudwatch_log_group.msk` (`/aws/msk/kafka-prod-lab-lab/brokers`, KMS-encrypted, 3-day retention), `aws_appautoscaling_target.msk_storage` + `aws_appautoscaling_policy.msk_storage` (`KafkaBrokerStorageUtilization`, target 70%, max 200 GiB). A `check "kraft_metadata_mode"` block asserts the version string ends in `.kraft`.

**KMS (`kms.tf`)** — `aws_kms_key.this` (rotation enabled, 7-day deletion window, policy allows account root + `logs.<region>.amazonaws.com` scoped by encryption context), `aws_kms_alias.this` (`alias/kafka-prod-lab-lab`), `random_password.grafana_admin`, `aws_ssm_parameter.grafana_admin_password` (SecureString under the CMK).

**IAM (`iam.tf`, `iam_samples.tf`)** — `aws_iam_role.toolbox` (EC2 assume) + `AmazonSSMManagedInstanceCore` + inline policy scoped to `kafka-cluster:*` on `lab.*` topics / `lab-*` groups / `lab-*` transactional IDs, plus `kafka:GetBootstrapBrokers`, `kafka:DescribeClusterV2`, `ssm:GetParameter`, `kms:Decrypt`; `aws_iam_instance_profile.toolbox`; three **unattached** sample policies (producer / consumer / admin) for security demonstration.

**Compute (`toolbox.tf`)** — `aws_instance.toolbox`: AL2023 resolved from the SSM public AMI parameter, `t3.small`, **public subnet[0]**, IMDSv2 required with hop limit 1, encrypted gp3 root (30 GiB) on the CMK, gzipped user-data installing Prometheus/Grafana/Alertmanager and the Python client apps.

**Monitoring (`monitoring.tf`)** — 6 CloudWatch metric alarms: offline-partitions, active-controller-missing, multiple-active-controllers, under-replicated-partitions, broker-disk-high (75%), broker-cpu-high (60%).

**Budget (`budget.tf`)** — `aws_budgets_budget`, created only when `budget_email != ""`. Currently `""` in tfvars, so **no budget is created**. Cost filter keys on `user:Project$kafka-prod-lab`.

---

## 6. MSK shape

| Aspect | Value | Source |
|---|---|---|
| Type | **Provisioned** (`aws_msk_cluster`), not Serverless | `msk.tf:35` |
| Metadata mode | **KRaft**, `kafka_version = "3.9.x.kraft"`, enforced by a `check` block | `terraform.tfvars:10` |
| Brokers | **3 × `kafka.m7g.large`**, one per AZ | `terraform.tfvars:12-13` |
| Storage | 100 GiB EBS, autoscaling to 200 GiB at 70% utilisation | `terraform.tfvars:14-19` |
| Client auth | **IAM SASL only** — `sasl.iam = true`, `unauthenticated = false`. **TLS mutual auth and SCRAM are NOT enabled.** Port **9098** only | `msk.tf:59-63` |
| Encryption | at rest with the customer CMK; in transit `client_broker = TLS`, `in_cluster = true` | `msk.tf:66-69` |
| Public access | `DISABLED` | `msk.tf:47-48` |
| Placement | brokers in the 3 **private** subnets | `msk.tf` |
| Egress path | **none** — no NAT, no VPC endpoints | `network.tf:71` |
| Broker config | `auto.create.topics.enable=false`, `default.replication.factor=3`, `min.insync.replicas=2`, `num.partitions=6`, `unclean.leader.election.enable=false`, `log.retention.hours=24`, `compression.type=producer` | `msk.tf:7-33` |
| Open Monitoring | JMX exporter + node exporter enabled (ports 11001 / 11002) | `msk.tf` |
| Broker logs | CloudWatch, 3-day retention, KMS-encrypted | `msk.tf:1-5` |

**Implications for CDC.** `auto.create.topics.enable=false` means every Debezium topic must be created explicitly — good practice, but it makes topic provisioning a required step in Session 05, not an implicit one. `num.partitions=6` is the cluster default; `CLAUDE.md:46` requires that the same PK always lands in the same partition, which the Kafka default partitioner guarantees **only if the partition count for a topic never changes**. Repartitioning a CDC topic breaks ordering permanently. Record this as a Session 05 constraint.

`log.retention.hours=24` is a real risk for L1 ingestion: if a Spark streaming job is down for more than 24 hours, unread CDC events are deleted from Kafka and the only recovery is a Debezium re-snapshot. Session 06 must either raise retention for CDC topics or document the RPO explicitly.

---

## 7. Monitoring stack — not a reusable managed service

Prometheus **3.13.1**, Grafana **13.1.1**, Alertmanager **0.33.1** run as **Docker containers on the toolbox EC2 instance**, installed by `templates/user-data.sh.tftpl`, bound to `127.0.0.1`, reached by SSM port-forwarding (`grafana_port_forward_command`, `prometheus_port_forward_command`).

This is **not** Amazon Managed Prometheus or Managed Grafana. Two consequences:

1. Because the toolbox instance is terminated, the monitoring stack does not exist and holds no historical data.
2. It cannot be "reused" the way a managed service can. Re-applying recreates it from user-data as a fresh, empty install. Any dashboard or alert this project needs for Connect, Spark, Airflow or freshness must be added to the `templates/` files — i.e. it is a code change to Repo A, or a re-implementation in the target repo.

The Grafana admin password is generated by `random_password` and stored at SSM SecureString `/kafka-prod-lab-lab/grafana/admin-password` under the CMK. This satisfies `CLAUDE.md:38` (secrets at runtime, not in outputs) and is a good pattern to copy.

---

## 8. Outputs — 10 defined

`terraform/outputs.tf`:

| Output | Description present? | Useful downstream? |
|---|---|---|
| `aws_account_id` | ✅ | ✅ guard |
| `aws_region` | ❌ | ✅ guard |
| `msk_cluster_arn` | ❌ | ✅ IAM scoping |
| `bootstrap_brokers_sasl_iam` | ✅ | ✅ **the critical one** |
| `toolbox_instance_id` | ❌ | ✅ SSM ops |
| `grafana_password_parameter_name` | ❌ | ○ ops only |
| `ssm_shell_command` | ❌ | ○ convenience |
| `grafana_port_forward_command` | ❌ | ○ convenience |
| `prometheus_port_forward_command` | ❌ | ○ convenience |
| `sample_iam_policy_arns` | ✅ | ○ demo only |

**Missing and required by this project:** `vpc_id`, `private_subnet_ids`, `public_subnet_ids`, `msk_security_group_id`, `toolbox_security_group_id`, `kms_key_arn`, `msk_cluster_name`, `vpc_cidr`, `availability_zones`, `toolbox_role_arn`. Full contract in `docs/GAP_ANALYSIS.md` §4.

Also note `CLAUDE.md:39` requires no plaintext secrets in outputs — Repo A complies (it publishes the SSM *parameter name*, never the value).

---

## 9. Tagging

Applied to every resource through provider `default_tags` (`providers.tf`, values from `locals.tf`):

```
Project          = kafka-prod-lab
Environment      = lab
ManagedBy        = Terraform
Owner            = ryan
CostCenter       = learning
AutoDestroyAfter = manual
```

All six keys required by `CLAUDE.md:76` are present. Per-resource `Name` tags follow `<project>-<env>[-<role>][-<az>]`; subnets additionally carry `Tier = public|private`.

**Load-bearing:** the AWS Budget cost filter keys on `user:Project$kafka-prod-lab`. Changing `project_name` silently breaks cost attribution. Also note `AutoDestroyAfter = "manual"` — for this project's ephemeral cost model, that value should become a real timestamp so an automated sweeper can act on it.

---

## 10. Security posture summary

| `CLAUDE.md` invariant | Status | Note |
|---|---|---|
| §3.1 no static credentials in Git/tfvars/user-data | ✅ | Grafana password generated at apply time into SSM SecureString |
| §3.2 no IAM user/access key created | ✅ | Only roles + instance profile. (The **operator** identity is an IAM user — pre-existing, see §11) |
| §3.3 no inbound `0.0.0.0/0` | ✅ | Toolbox SG has zero inbound; MSK ingress from toolbox SG only |
| §3.4 no SSH/key pair | ✅ | SSM Session Manager; no `key_name` |
| §3.5 S3 BPA, KMS, TLS-only bucket policy | **N/A** | Repo A creates no S3 bucket. This project must implement it in Session 02 |
| §3.6 secrets from Secrets Manager/SSM at runtime | ✅ | |
| §3.7 IAM per workload | ⚠️ **partial** | Only a `toolbox` role exists. The eight workload roles (connect, spark, airflow, athena, redshift-serverless, trino, governance, source-lab) must all be built |
| §3.8 state encrypted, locked, IAM-restricted | ❌ | **Local state.** See §3 |
| §3.9 pin versions, no `latest` | ✅ | |
| §3.10 pin third-party GH Actions to SHA | ⚠️ **unverified** | `.github/workflows/ci.yml` not audited this session — out of scope, but flag for Session 15 |

---

## 11. Operator identity note

`aws sts get-caller-identity` returns `arn:aws:iam::111122223333:user/my-aws-profile` — an **IAM user**, not an assumed role.

`CLAUDE.md:32` forbids *creating* IAM users and access keys. It does not forbid a pre-existing operator identity, and this identity predates the project. **Recorded as accepted-and-noted, not a violation.** Recommendation for Session 01: consider whether the operator should assume a scoped deployment role for `terraform apply` so that the audit trail distinguishes human actions from deployment actions. Low priority for a single-operator lab.

---

## 12. What is genuinely reusable

Given the runtime is absent, "reuse" means reuse of **code and patterns**, not of live resources:

| Asset | Reusable? | Notes |
|---|---|---|
| Network topology (3-AZ, public/private, no NAT) | ✅ pattern | But needs VPC endpoints added — see `docs/GAP_ANALYSIS.md` Gap 8 |
| MSK configuration + KRaft + IAM auth | ✅ directly | Sound and invariant-compliant |
| KMS key + policy | ✅ directly | Decide whether the lake gets its own key |
| Toolbox pattern (SSM-only, IMDSv2, no inbound SG) | ✅ directly | Template for the source-lab and CDC-runtime instances |
| `scripts/*.sh` operational suite | ✅ pattern | Maps onto `reference/COMMAND_CHEATSHEET.md` targets |
| Tagging via `default_tags` | ✅ directly | Already satisfies the six-key requirement |
| CloudWatch alarms | ✅ pattern | Extend for Connect/Spark/freshness |
| Prometheus/Grafana user-data | ○ partial | Fresh install on every apply; dashboards need extending |
| Version pins | ✅ directly | Carried into `docs/VERSIONS.md` |
| Outputs | ❌ insufficient | 4 of ~12 needed |
| Backend | ❌ unusable | Local state |
| Live AWS resources | ❌ **none exist** | |
