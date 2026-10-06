# AI Infrastructure Gap Analysis (AI-P12)

**Status:** plan only. **Nothing was applied.**
Account `111122223333` · `ap-southeast-1` · workspace `default` · profile `my-aws-profile`
Re-run 2026-08-27 against the **rebuilt** stack (287 resources). The previous AI-P12 run
analysed a stack that has since been destroyed and rebuilt; its findings are superseded.

---

## 1. Current AI infrastructure

| Component | Classification | Evidence |
|---|---|---|
| RAG storage (`s3://<lake>/ai/knowledge/`) | `EXISTS_AND_VALID` | prefix present, 1 object |
| RAG vector index (S3 Vectors) | `OPTIONAL_DISABLED` | `enable_ai_vector_index = false`; 0 vector buckets |
| Bedrock Knowledge Base | `OPTIONAL_DISABLED` | `list-knowledge-bases` → 0 |
| AI IAM role | `MISSING_REQUIRED` | no `*-ai` role; created only under `enable_ai_rag` |
| Bedrock model access | `EXISTS_NEEDS_CHANGE` | 29 models + 25 inference profiles offered, **none invokable** |
| Agent runtime (Lambda) | `OPTIONAL_DISABLED` | 0 Lambda functions; `enable_ai_agent_runtime = false` |
| AgentCore | `NOT_REQUIRED` | deferred by ADR-056 |
| AI observability | `EXISTS_AND_VALID` | CloudWatch EMF from the runtime; no new resource needed |
| Offline feature store | `OPTIONAL_DISABLED` | `features/offline/` prefix exists; Glue DB not created |
| Online feature store | `NOT_REQUIRED` | ADR-053 — no sub-100 ms consumer exists |
| Model artifacts | `EXISTS_AND_VALID` | `models/artifacts/`, 2 objects |
| Model registry | `NOT_REQUIRED` | ADR-052 — content-addressed versioning in S3 |
| S3 prefixes | `EXISTS_AND_VALID` | `ai/knowledge/`, `features/offline/`, `models/artifacts/`, `ops/` all present |
| KMS | `EXISTS_AND_VALID` | `alias/kafka-dev-lab-dev-lake` → `e66f4dfa` |
| Athena workgroup | `EXISTS_AND_VALID` | `kafka-dev-lab-dev-wg` with bytes-scanned cutoff |
| OPS metadata (ADR-036) | `EXISTS_AND_VALID` | 4 DynamoDB tables |

### Gaps that matter

1. **Bedrock is not invokable.** `INVALID_PAYMENT_INSTRUMENT` on the Marketplace
   subscription. Embeddings and the hosted agent are both blocked by this, and no Terraform
   change can fix it — it is an account/billing action.
2. **No AI IAM role exists.** Created by `enable_ai_rag`; costs nothing.
3. **Two CMKs over one bucket.** `warehouse/` objects written before the rebuild use
   `44bf584e`; the current lake key is `e66f4dfa`. A role granted only the new key cannot
   read pre-rebuild Iceberg data.

---

## 2. Feature flags

All five default `false` and **none** is set in `terraform.tfvars` — "what exists" stays
answerable from tfvars alone.

| Flag | Default | Set? | Recommended now | Why |
|---|---|---|---|---|
| `enable_ai_rag` | `false` | no | **true** | +2 IAM resources, $0, no replacements |
| `enable_ai_vector_index` | `false` | no | **false** | ADR-049 gate unmet *and* embeddings not invokable — the index could never be populated |
| `enable_ai_feature_store` | `false` | no | **false — see §5** | triggers 3 EC2 replacements |
| `enable_ai_online_feature_store` | `false` | no | **false** | ADR-053: no consumer |
| `enable_ai_agent_runtime` | `false` | no | **false** | `copilot.zip` does not exist; apply would fail |

There is no `enable_ai_platform`, `enable_agentcore` or `enable_ai_observability` variable:
observability is CloudWatch EMF emitted by code, and AgentCore is deferred. A flag for a
component that does not exist implies a resource that is not there.

---

## 3. Cost

| Resource | Idle | Per request/job | Storage | Destroy | Retained |
|---|---|---|---|---|---|
| `aws_iam_role.kb` + inline policy | **$0.00** | $0 | — | clean | none |
| `aws_glue_catalog_database` (feature_offline) | **$0.00** | $0 | — | clean | S3 data survives |
| S3 Vectors index | not created | — | — | — | — |
| Bedrock Knowledge Base | not created | — | — | — | — |
| Lambda agent runtime | not created | — | — | — | — |

**Total incremental cost of the recommended plan: $0.00/hr.**

Explicitly checked for and **absent from both plans**: OpenSearch, SageMaker endpoints, NAT
Gateway, EKS, ElastiCache, MSK Serverless. No new persistent EC2 and no long-running AI
runtime. Verified by grep over both plan files, not by assertion.

---

## 4. Terraform gates

| Gate | Result |
|---|---|
| `terraform fmt -recursive -check` | clean |
| `terraform validate` | Success |
| `make lint-shell` | exit 0, no errors or warnings |
| `make validate-docs` | 14 passed, 0 failed |
| AI test suites (`spark/tests/test_ai_*.py`) | **516 passed** |
| `make test` (full) | **1340 passed, 61 errors** — all 61 are `Cannot find catalog plugin class ... SparkCatalog`, a missing local Iceberg runtime JAR. Environmental, unrelated to AI code |

---

## 5. Plan review

### Plan A — current tfvars (all AI flags off)

```
Plan: 0 to add, 4 to change, 0 to destroy.
```

All four are S3 object re-uploads of files edited this session (`docker-compose.yml`, two
workload SQL files, one lake prefix). No replacements, no destroys, **0 AI resources**.

### Plan B — `enable_ai_rag` + `enable_ai_feature_store`

```
Plan: 6 to add, 7 to change, 3 to destroy.   <-- 3 REPLACEMENTS
```

### 🔴 STOP — the feature store flag replaces all three EC2 instances

Isolated per flag:

| Flag | Plan | Replacements |
|---|---|---|
| `enable_ai_rag=true` | 2 add, 4 change, 0 destroy | **0** |
| `enable_ai_feature_store=true` | 4 add, 7 change, 3 destroy | **3** |

The three are `module.airflow_k3s[0].aws_instance.this[0]`,
`module.cdc_runtime[0].aws_instance.cdc_runtime` and
`module.source_lab[0].aws_instance.source_lab`, each with
`~ ami = ... -> (known after apply) # forces replacement`.

**Mechanism.** `enable_ai_feature_store` adds a Glue database, which rewrites
`module.lake_iam` `lake_read` and `lake_write`. All three EC2 modules declare
`depends_on = [module.lake_iam]`. A **module-level `depends_on` defers every data source
inside that module**, so `data.aws_ami` becomes unknown at plan time → `ami` is
`(known after apply)` → forced replacement.

This is the same defect class as the earlier `monthly_budget_usd` cascade: a module-level
`depends_on` turning an unrelated in-place IAM edit into an instance rebuild. Applying it
would destroy the running Oracle and SQL Server containers, the Debezium connectors, and the
Airflow Postgres PVC with all DAG run history.

**Recommendation.** Apply `enable_ai_rag = true` alone — 2 IAM resources, $0, zero
replacements. Leave `enable_ai_feature_store = false` until the ordering is fixed by moving
`depends_on` off the module and onto the specific resources that need it, so data sources
are no longer deferred.

---

## 6. IAM / network / security

- **IAM.** `aws_iam_role.kb` is a dedicated Bedrock KB role with an inline policy — no new
  IAM users, no access keys, no wildcard principal.
- **Network.** No VPC, subnet, security group, NAT or endpoint changes. No `vpc_config` on
  any AI resource. No inbound rule is added.
- **Security.** No plaintext secret in any planned resource; encryption stays on the lake
  CMK. The **two-CMK split** above is the one open security-relevant issue.

---

## 7. Rollback

Set the flag back to `false` and apply: the AI resources are `count`-gated, so they are
removed and nothing else is touched. No AI resource holds state — the S3 prefixes and their
objects are owned by `module.data_lake`, not by the AI modules, so disabling a flag never
deletes corpus or feature data.

---

## 8. Blockers — status after AI-P13

| # | Blocker | Status |
|---|---|---|
| 1 | Bedrock `INVALID_PAYMENT_INSTRUMENT` | **OPEN — operator/billing only.** Re-tested 2026-08-27: `cohere.embed-english-v3` still returns `AccessDeniedException`. No Terraform or code change can clear it. Fix at Billing → Payment preferences, then re-subscribe in Bedrock → Model access. |
| 2 | `module.lake_iam` `depends_on` replaces 3 EC2 | **OPEN by choice.** `enable_ai_feature_store` stays `false`. The fix is to move `depends_on` off the module onto the specific resources, so `data.aws_ami` is no longer deferred. Not attempted during an apply phase. |
| 3 | `ai_runtime/package/copilot.zip` missing | **FIXED.** `make ai-package` / `scripts/build-agent-package.sh`. 656 KB, 16 members, deterministic, no vendored dependencies, zero `__pycache__`. |
| 4 | Multiple CMKs over one lake bucket | **RESOLVED 2026-08-27.** All 567 `warehouse/` objects and every other durable object are on `e66f4dfa`. `logs/` and `query-results/` intentionally left. |

### Blocker 4 in full

| CMK | Objects | State |
|---|---|---|
| `44bf584e` | 856 | Enabled — holds all 567 `warehouse/` Iceberg objects |
| `8d20ebe1` | 266 | Enabled |
| `d1f568fd` | 161 | **PendingDeletion — deletes 2026-09-02** |
| `7f03af9e` | 50 | Enabled (AWS-managed `aws/s3`) |
| `e66f4dfa` | 20 | Enabled — the current lake key, and the bucket default |

`kafka-dev-lab-dev-reporting` and `kafka-dev-lab-dev-spark-eod` are granted **only**
`e66f4dfa`. Every Iceberg data file is on `44bf584e`. Spark therefore cannot read the lake it
owns, and the failure surfaces as `kms:Decrypt` denied rather than as an S3 error, which
points debugging at the wrong service.

The `PendingDeletion` key holds only Athena query results and EMR logs — regenerable, so its
deletion costs nothing. It is a warning about the cycle, not a data-loss event.

**Root cause:** `terraform destroy` schedules the lake CMK for deletion; the next apply
creates a new one and repoints bucket default encryption, but objects already written keep
the key they were written with. Two destroy/apply cycles produced five keys.

**Fix:** `bash scripts/reencrypt-lake-cmk.sh reencrypt --execute` (type `REENCRYPT LAKE`).
660 durable objects are copied in place onto the current key; `logs/` and `query-results/`
are skipped deliberately. Copy-in-place preserves path, content and size, so Iceberg metadata
stays valid. Versioning is Enabled, so the originals are retained and the operation is
reversible.

### Resolved

Run 2026-08-27. Result verified by re-scanning all 1,353 objects:

```text
680 objects  e66f4dfa  [Enabled]   <- current key, was 20
357 logs/            ) deliberately skipped: regenerable
316 query-results/   )
0   durable objects still on a non-current key
567/567 warehouse objects on the current key
```

The `7f03af9e` AWS-managed key is gone from the bucket entirely. `d1f568fd`
(`PendingDeletion`, 2026-09-02) now holds only logs and query results, so its deletion costs
nothing.

**Two bugs in the tool, both in one line, both mine.** `copy-source` must be passed **raw** —
the AWS CLI URL-encodes it itself, so pre-encoding double-encodes:

| Encoding | Effect |
|---|---|
| `quote(safe='')` | `/` → `%2F` — all 660 failed |
| `quote(safe='/')` | `=` → `%3D` → `%253D` — the 94 Hive-partitioned keys failed |
| raw | correct |

Only partitioned paths contain `=` (`business_date=YYYY-MM-DD`), which made the failure look
specific to `fact_account_daily_snapshot` when it was purely about the character. `CopyObject`
validates the source before writing, so every failure was a clean no-op — nothing was
half-written, and the retry was safe.
