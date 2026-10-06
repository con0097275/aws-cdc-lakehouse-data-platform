<!-- PROVENANCE: copied verbatim from
     /path/to/aws-cdc-lakehouse-claude-guide-v2/docs/GAP_ANALYSIS.md
     (Session 00 output, guide package v2). Copied 2026-08-12 by Session 01 under ADR-025.
     The original is read-only and checksummed by the guide package MANIFEST.json/SHA256SUMS.
     Corrections must be made HERE, not upstream. -->

# GAP ANALYSIS

- Session: 00 — Audit existing repository
- Date: 2026-08-07
- Baseline: `docs/EXISTING_PLATFORM_AUDIT.md`
- Target: `ARCHITECTURE.md`, `MASTER_PLAN.md`, `reference/TERRAFORM_MODULE_MAP.md`, `reference/ACCEPTANCE_CRITERIA.md`

---

## 1. Summary

The guide is written as if a Kafka platform already exists and only the lakehouse needs building. **It does not exist.** Repo A's infrastructure was destroyed on 2026-07-26 and the account is empty. So the gap is not "lakehouse on top of Kafka" — it is "Kafka platform *and* lakehouse", with the Kafka platform's code already written and its integration surface incomplete.

Two gaps are architecture-changing and must be resolved in Session 01 before any Terraform is written: **Gap 1** (no consumable state) and **Gap 8** (private subnets have no egress path to AWS APIs).

---

## 2. Gap matrix

Priority: **MUST** = blocks core release (`MASTER_PLAN.md:47-51`). **SHOULD** = required for acceptance but not for the pipeline to run. **COULD** = optional feature flags.

| # | Gap | Priority | Blocks | Owning session |
|---|---|---|---|---|
| **0** | **Kafka platform not deployed.** No MSK, VPC, subnets, SG, CMK, toolbox. Nothing to reuse at runtime. | **MUST** | 04, 05, 06, 12, 15 | Gate 0 (pre-02) |
| **1** | **Repo A backend is local state.** No `backend` block; `backend.hcl.example` bucket is a placeholder. `terraform_remote_state` impossible. Also violates `CLAUDE.md:40`. | **MUST** | integration contract, all TF sessions | 01 (decide), 02 (implement) |
| **2** | **Output surface incomplete. 5 of 16 required keys exported** — `outputs.tf` declares 10 outputs, but only 5 are contract keys; the other 5 are operator conveniences. No `vpc_id`, `private_subnet_ids`, `msk_security_group_id`, `kms_key_arn`, `msk_cluster_name`. | **MUST** | 02, 03, 04, 06, 12 | 01 (decide), 02 |
| **3** | **No data lake.** No S3 bucket, no lake KMS key, no Glue databases (`stream`, `full_cdc`, `snapshot`, `curated`, `mart`, `ops`, `quarantine`), no Athena workgroup, no lake IAM roles. | **MUST** | 06–13 | 02 |
| **4** | **No CDC runtime.** No Kafka Connect, no Schema Registry (Apicurio KafkaSQL), no connector config, no DLQ topics. | **MUST** | 05, 06 | 04 |
| **5** | **No source lab.** No Oracle Free / SQL Server Developer containers, no supplemental logging, no CDC enablement. | **MUST** | 04, 05 | 03 |
| **6** | **No Spark runtime.** No EMR Serverless application, no job role, no logging config, no auto-stop / max-capacity / timeout guardrails. | **MUST** | 06–11 | 06 |
| **7** | **No orchestration.** No k3s, no Airflow 3, no `KubernetesExecutor`, no metadata DB, no DAGs. | **MUST** | 12 | 12 |
| **8** | **No egress path from private subnets to AWS APIs.** `aws_route_table.private` has zero routes; no NAT (by policy) and no VPC endpoints. Connect, EMR Serverless, Airflow and Spark all need S3, Glue, STS, Secrets Manager, ECR, CloudWatch, Kafka control-plane. | **MUST** | 04, 06, 12 | **01 (design + cost), 02 (implement)** |
| 9 | **Eight workload IAM roles missing.** Only `toolbox` exists. `CLAUDE.md:39` requires connect, spark, airflow, athena, redshift-serverless, trino, governance, source-lab. | MUST | 02–13 | 02 (+ per session) |
| 10 | **No governance.** No DQ rules, no lineage (OpenLineage/Marquez), no catalog metadata, no reconciliation ledger (`ops.reconciliation_run`), no PII masking. | SHOULD | acceptance | 14 |
| 11 | **Observability does not cover the lakehouse.** 6 alarms are Kafka-broker-only. No Connect, Spark, freshness, DQ, Iceberg-health, or Airflow signals. Prometheus/Grafana are per-instance and reset on every apply. | SHOULD | acceptance, SLO | 15 |
| 12 | **No Kafka topic provisioning.** `auto.create.topics.enable=false`, so every `cdc.*` topic must be created explicitly with a fixed partition count. | MUST | 05 | 05 |
| 13 | **Kafka retention is 24 h.** `log.retention.hours=24`. A streaming outage > 24 h loses unread CDC events; recovery requires a Debezium re-snapshot. | MUST | 06 | 05/06 (set RPO) |
| 14 | **No backup/DR for Airflow metadata DB.** `CLAUDE.md:82` requires backup/retention. | SHOULD | 12, 15 | 12 |
| 15 | Redshift Serverless, Trino, Marquez, Bedrock, Lake Formation, Glue DQ absent. | COULD | optional gates | 13B, 13C, 14, 16 |
| 16 | **No CI/CD for this project.** Repo A has `.github/workflows/ci.yml`; the target repo has nothing. `CLAUDE.md:42` requires third-party actions pinned to full commit SHA. | SHOULD | acceptance | 15/18 |
| 17 | **Target repo is not a Git repository.** `git rev-parse` fails. `SESSION_WORKFLOW.md:30` requires a commit/branch per handoff — impossible today. | SHOULD | every handoff | 01 |

---

## 3. Gap 8 in detail — the architecture-breaking one

`terraform/network.tf:71` declares three private route tables with **no `route` blocks at all**. Brokers therefore have no path off the subnet. That was a correct, deliberate, cost-saving choice for Repo A, because the only client was the toolbox — which sits in a **public** subnet with a public IP (`network.tf:29`).

This project cannot repeat that. Every new compute workload must run in the private subnets and every one of them needs AWS API access:

| Workload | Needs |
|---|---|
| Kafka Connect + Debezium | Secrets Manager, STS, ECR (or S3 for plugins), CloudWatch Logs, MSK control plane |
| EMR Serverless (Spark) | S3 (lake + checkpoints + logs), Glue Data Catalog, STS, CloudWatch Logs, KMS |
| Airflow on k3s | S3 (logs, DAGs), STS, ECR, CloudWatch Logs, EMR Serverless API |
| Source lab (Oracle / SQL Server) | ECR or S3 for container images, SSM, CloudWatch Logs |

`CLAUDE.md:66` forbids NAT Gateway by default. The remaining option is **VPC endpoints**:

- **Gateway endpoints — free**: `s3`, `dynamodb`. Take these unconditionally.
- **Interface endpoints — billed per endpoint, per AZ, per hour, plus data processed**: `glue`, `sts`, `secretsmanager`, `kms`, `logs`, `monitoring`, `ecr.api`, `ecr.dkr`, `ssm`, `ssmmessages`, `ec2messages`, `elasticmapreduce`, `emr-serverless`.

A naive full set across 3 AZs is on the order of 30+ endpoint-AZ-hours billed continuously — **a new always-on cost that no guide document currently budgets for**, and one that persists even when MSK is destroyed, because it lives with the VPC.

**Design directions for Session 01** (decide, do not implement here):

1. **Reduce AZ count for endpoints.** Endpoints need not be in all 3 AZs for a lab. One or two AZs cuts the bill proportionally, at the cost of AZ-failure resilience — acceptable for `lab_low_cost`.
2. **Reduce the endpoint set.** Put ECR image pulls behind S3 where possible; some workloads can be moved to the public subnet with no public IP + a time-boxed NAT only during metered windows.
3. **Make endpoints ephemeral too.** Bundle them into the same `enable_*` flag as the workloads that need them, so a destroyed lab costs nothing.
4. **Reconsider "no NAT" for metered windows only.** A NAT Gateway during a 6-hour window costs less than a month of interface endpoints. This is a genuine trade-off against `CLAUDE.md:66`, and if chosen, it must be an explicit ADR with the invariant amended — not a silent deviation.

Recommendation to carry into Session 01: option 3 combined with option 1, and cost both against option 4 before deciding.

---

## 4. Integration contract

What the lakehouse must receive from the Kafka platform. **Availability is against Repo A's current `outputs.tf`.**

| Contract key | Type | Consumers | Available today? |
|---|---|---|---|
| `aws_account_id` | string | identity guard | ✅ |
| `aws_region` | string | identity guard | ✅ |
| `msk_cluster_arn` | string | IAM policy scoping for connect/spark roles | ✅ |
| `msk_bootstrap_brokers_sasl_iam` | string (CSV) | Connect worker, Spark `kafka.bootstrap.servers` | ✅ as `bootstrap_brokers_sasl_iam` |
| `toolbox_instance_id` | string | SSM shell / port-forward | ✅ |
| `msk_cluster_name` | string | CloudWatch metric dimensions, topic naming, IAM resource ARNs | ❌ |
| `vpc_id` | string | every new security group | ❌ |
| `vpc_cidr` | string | endpoint SG rules, subnet math | ❌ |
| `private_subnet_ids` | list(string) | Connect EC2, EMR Serverless network config, k3s node, source lab | ❌ |
| `public_subnet_ids` | list(string) | toolbox / egress-constrained placement | ❌ |
| `availability_zones` | list(string) | endpoint placement, AZ-count decisions | ❌ |
| `msk_security_group_id` | string | SG-to-SG ingress for Connect and Spark on 9098 | ❌ |
| `toolbox_security_group_id` | string | SSM jump path, Grafana scrape source | ❌ |
| `kms_key_arn` | string | decide: reuse for the lake, or issue a separate lake CMK | ❌ |
| `toolbox_role_arn` | string | trust policy / cross-role operations | ❌ |
| `private_route_table_ids` | list(string) | attaching S3 **gateway** endpoints (Gap 8) | ❌ |

**Score: 5 of 16 available.** Every missing key is needed by Session 02 or Session 04.

### 4.1 Resolution options — OPEN, for Session 01

Not decided here, because each one either modifies Repo A (currently read-only per `LOCAL_PROJECT_CONTEXT.md:15`) or changes repository topology.

| Option | Mechanism | Pros | Cons |
|---|---|---|---|
| **(A) Absorb Repo A** into the target repo as a `kafka_platform` module, with provenance | one repo, one state, one `apply`/`destroy` | No read-only exception needed. Fixes the output gap directly. Single lifecycle matches the ephemeral cost model — one command brings the whole lab up and down. Simplest dependency ordering. | Forks from upstream; upstream fixes must be merged manually |
| **(B) Keep Repo A separate**, add the missing outputs + S3 backend, consume via `terraform_remote_state` | clean separation of platform and lakehouse | Mirrors real org boundaries; good portfolio story | **Requires an explicit written exception** to `LOCAL_PROJECT_CONTEXT.md:15`. Needs an out-of-band state bucket. Two applies to sequence |
| **(C) Leave Repo A untouched**, resolve via `aws_*` data sources filtered on `Project = kafka-prod-lab` | zero change to Repo A | No exception needed | **Fails hard whenever the platform is destroyed — which is its normal state under the ephemeral cost model.** Fragile apply ordering; tag-based lookup is implicit coupling; still cannot get `kms_key_arn` reliably |

**Architect's recommendation: (A).** The decisive argument is the cost model. `docs/COST.md` requires the entire lab to be destroyed between metered windows; under (C), every destroyed window breaks the lakehouse plan/apply, and under (B) it breaks the remote-state read unless outputs are carefully made optional. (A) gives one `terraform destroy` that is also the cost control.

If (B) is preferred for the portfolio narrative, the exception must be granted in writing and Repo A must first gain: the 11 missing outputs, an S3 backend with a separately-managed bucket, and a documented apply order.

---

## 5. Specification defects in the guide package

Found while cross-reading the reference documents. These are **document bugs, not implementation gaps** — but each one will cause rework if it reaches the session that depends on it. Fix before the session named in the last column.

| # | Defect | Evidence | Fix by |
|---|---|---|---|
| D1 | **L1 partition spec references columns L1 does not have.** Partitioning is `days(event_ts)` + `source_system`, but the L1 minimum column list defines neither — it has `source_ts`, `ingest_ts`, `event_date`. `CLAUDE.md:56` uses a third pair (`event_ts` / `source_commit_ts`). | `reference/ICEBERG_LAYER_SPEC.md:9` vs `:19-35` | **06** |
| D2 | **The ordering key has three names.** `event_order` / `event_order_key` / `source_order_1`+`source_order_2`. | `CLAUDE.md:52`, `reference/CDC_EVENT_CONTRACT.md:63`, `reference/ICEBERG_LAYER_SPEC.md:79-80` | **06** |
| D3 | **`source_commit_position` type conflict.** Nested `{type, value, secondary}` in the event contract; flat `string` + separate `source_position_type` in the Iceberg spec. The `secondary` element has no L1 column, so SQL Server's event-serial-number tie-breaker has nowhere to live. | `reference/CDC_EVENT_CONTRACT.md:42-46` vs `reference/ICEBERG_LAYER_SPEC.md:26-27` | **05/06** |
| D4 | **L3 build SQL references undefined columns** — `source_commit_ts`, `source_order_1`, `source_order_2` appear in no layer's column contract. | `reference/ICEBERG_LAYER_SPEC.md:79-84` | **08** |
| D5 | **Envelope fields silently dropped at L1.** `event_version`, `source_system`, `source_database`, `source_schema`, `source_table`, `transaction_id`, `snapshot_flag`, `schema_subject`, `headers` are in the contract but absent from the L1 column list — yet `CLAUDE.md:48` requires L1 to preserve all source and Kafka metadata. | `reference/CDC_EVENT_CONTRACT.md` vs `reference/ICEBERG_LAYER_SPEC.md:19-35` | **06** |
| D6 | **Module map disagrees with target tree.** 14 modules listed vs 5 shown. `lake_iam`, `governance`, `budget_guardrails`, `observability_ext` have no responsibility section at all. | `reference/TERRAFORM_MODULE_MAP.md:14-31` vs `reference/REPO_TREE_TARGET.md:26-31` | **02** |
| D7 | **`ops` database has no S3 prefix.** `ops.reconciliation_run` is specified, and `ops` is in the Glue database list, but `ARCHITECTURE.md:71-82` defines no `warehouse/ops/` path. | `reference/RECONCILIATION_AND_ACCURACY.md`, `ARCHITECTURE.md:71-82` | **02** |
| D8 | **Cutoff timezone never named.** L2/L3 correctness depends on `[T 00:00, T+1 00:00)` "in the locked timezone", but no document states which. | `reference/ICEBERG_LAYER_SPEC.md` | **01** |
| D9 | **"Domain" is undefined.** L3 tables are named `<domain>_<entity>` and S3 paths use `<domain>`, but nothing maps a source/schema/table triple to a domain. | `ARCHITECTURE.md:71-76`, `reference/ICEBERG_LAYER_SPEC.md` | **02** |
| D10 | **Sessions 00/01 cannot reach their own terminator.** `prompts/00_PROMPT.md:65` and `prompts/01_PROMPT.md` require ending with `READY_FOR_APPLY_APPROVAL` and include stages C/D for live deployment — but both sessions are document-only and explicitly forbid apply. | `prompts/00_PROMPT.md:65-79` vs `sessions/00_audit_existing_repo.md` | **00 (now)** |
| D11 | **Session 00 scope narrower than its prompt.** The session file says "the existing repository" (singular) and never mentions `SOURCE_REPOSITORY_DECISION.md`; the prompt requires auditing both repos and producing that document plus an integration contract. | `sessions/00_audit_existing_repo.md` vs `prompts/00_PROMPT.md:33` | **00 (now)** |
| D12 | **Session 01's cost constraint is missing.** Two blank lines sit exactly where Session 00 places its "no apply/destroy" sentence. | `sessions/01_cost_and_target_architecture.md:42-44` | **01** |
| D13 | **`TARGET_PROJECT` is the guide package itself.** Implementation code is directed into the same directory as the read-only guide, which already contains a different `CLAUDE.md` and `README.md` than `REPO_TREE_TARGET.md` expects, and whose 73 files are checksummed by `MANIFEST.json` / `SHA256SUMS`. | `LOCAL_PROJECT_CONTEXT.md:6`, `reference/REPO_TREE_TARGET.md`, `START_HERE_CLAUDE.md:3` | **01** |
| D14 | **Project name mismatch.** `CLAUDE.md:12` says `kafka-dev-lab`; Repo A builds `kafka-prod-lab-lab`. No `kafka-dev-lab` has ever existed. | `CLAUDE.md:12` vs `terraform.tfvars:1-2` | **01** |
| D15 | **`VALIDATION_REPORT.md` is stale.** Claims 43 markdown files; the package has 72. | `VALIDATION_REPORT.md` | **01** |
| D16 | **Version label collision.** `MANIFEST.json` and `README.md` say v4; the directory name, `CHANGELOG_V2.md` and seven reference docs say v2. No changelog describes the v2→v4 delta. | `MANIFEST.json:2`, `README.md:1` | low priority |
| D17 | **Redshift/Trino mutual exclusion stated three ways** with different strengths — only the `allow_multiple_optional_query_engines` form is machine-checkable. | `ARCHITECTURE.md:199`, `reference/SERVING_LAYER_STRATEGY.md:103`, `CLAUDE.md:88` | **13B/13C** |

D1–D5 are the dangerous cluster: they all concern the CDC event → L1 → L2 → L3 column lineage, they are mutually inconsistent, and `CLAUDE.md` §5 makes ordering correctness a hard invariant. **Session 01 should produce a single normative column contract** that supersedes all three documents, rather than letting Session 06 improvise one.

---

## 6. What is NOT a gap

Stated explicitly so later sessions do not redo this work:

- MSK cluster configuration, KRaft mode, IAM SASL auth, TLS, CMK encryption, private placement — all correct and invariant-compliant.
- Network topology (3 AZ, public/private split, no NAT) — correct as a starting point; needs endpoints added, not redesign.
- Tagging — all six required keys already applied via `default_tags`.
- Version pinning discipline — already exact; carry into `docs/VERSIONS.md`.
- SSM-only access pattern, IMDSv2, zero-inbound security groups — directly reusable as the template for source-lab and CDC-runtime instances.
- Operational script suite (`preflight`, `deploy`, `destroy`, `verify-destroy`, `show-cost-resources`) — a working pattern that maps onto `reference/COMMAND_CHEATSHEET.md`.
- Secret handling (generate at apply → SSM SecureString → never in outputs) — correct pattern to copy.
