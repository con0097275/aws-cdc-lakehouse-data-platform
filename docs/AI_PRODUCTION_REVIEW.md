# AI Platform Final Production Review (AI-P16)

Audit date 2026-08-27 · account `111122223333` · `ap-southeast-1` · workspace `default`
Reviewed as: Principal Data Architect · Principal AI Platform Engineer · Senior GenAI
Engineer · MLOps · Security · SRE · FinOps.

**Audit only.** Two defects were fixed during AI-P15 and are re-verified here; nothing was
refactored opportunistically in this phase.

---

## 1. Findings

### P0 — none open

One P0 was found in AI-P15 and fixed: `assert_no_infrastructure_action` blocked
`terraform destroy` but let `rm -rf /opt/checkpoints` and
`kafka-consumer-groups --reset-offsets --execute` through. Re-verified closed (drill 18 PASS,
7 regression tests).

### P1 — 1 open, does not block any mandatory gate

| | |
|---|---|
| **Finding** | Bedrock is not invokable; answer generation is entirely untested |
| **Evidence** | `cohere.embed-english-v3` → `AccessDeniedException: INVALID_PAYMENT_INSTRUMENT`. Re-tested at audit time. |
| **Impact** | Retrieval, routing, tools and refusal are proven. **Synthesis is not.** Prompt-injection resistance is proven at the *tool* boundary, not in generated text. Token cost is $0 because nothing is called, not because it is optimised. |
| **Fix** | Operator: Billing → Payment preferences, then Bedrock → Model access. Then re-run `ai/eval/e2e_p14.py` and `make ai-eval-agent`. |
| **Owner** | operator / AWS billing — no code or Terraform change can clear it |

This is a **capability gap, not a defect**. The platform is designed to degrade to
retrieval-only and does so cleanly; the risk it creates is that a future reviewer assumes
generation was validated. It was not.

### P2 — 3 open

| # | Finding | Evidence | Impact | Fix | Owner |
|---|---|---|---|---|---|
| P2-1 | Two Terraform resources manage the same S3 key | `module.data_lake…prefix["logs/airflow/"]` and `module.airflow_k3s[0].aws_s3_object.log_prefix[0]`; every plan shows a 1-resource tag diff that never converges | Non-destructive, but a permanently non-empty plan trains operators to ignore plans — which is how a real change gets applied unnoticed | Decide which module owns the object; remove the other | platform / Terraform |
| P2-2 | BM25 collapses on paraphrase | `make ai-eval-rag`: recall@5 **0.7551** repo-vocabulary vs **0.2500** paraphrased | A user who does not use repo vocabulary gets poor retrieval, and the citation still looks authoritative | Enable embeddings once Bedrock works and re-run the ADR-049 gate | AI platform |
| P2-3 | The pilot model is not a model | `model:eb58e352986ba990` reports accuracy/F1/AUC-ROC all `1.0`; artifact carries `synthetic_label` | Proves plumbing only. A perfect AUC is a red flag, and publishing it without the caveat invites misreading | Keep `run_model_inference` unimplemented until a real label exists | MLOps |

### P3 — 1 open

`sync.py` warns the corpus was built from a dirty git tree, so `git_commit` is not a faithful
pointer. Cosmetic today; rebuild the corpus from a clean tree before any external publication.

---

## 2. Checklists

### ARCHITECTURE
- [x] AI is downstream/adjacent — `grep -rn 'import ai\.' spark/jobs spark/reporting airflow/dags` → **0**, enforced by test
- [x] FULL_CDC / REALTIME / EOD semantics untouched — 2 connectors RUNNING, watermarks unchanged through all drills
- [x] No competing source of truth — AI owns no table; corpus is derived, S3 prefixes owned by `module.data_lake`
- [x] `ai/` remains deletable

### RAG QUALITY
- [x] Corpus governance — allow-listed sources, whole-document quarantine on secret detection (never redact-and-publish)
- [x] Citations verified by **opening the file and locating the text**, not by format
- [x] Corpus content-addressed — `corpus:b639b715eabf0e37`, chunking `v2-heading-aware`
- [x] Vector store choice recorded (ADR-049); dense path **conditional** on beating BM25
- [ ] Paraphrase recall acceptable — **0.25, P2-2**

### FEATURE / ML CORRECTNESS
- [x] Point-in-time leakage tests pass (2 passed) — including the horizon test that a naive `<=` comparison does not catch
- [x] Feature event time and lineage present
- [x] Training reproducible — content-addressed `model:` / `dataset:` / `code_version`
- [x] Feature Store ≠ RAG — separate modules, separate flags, no shared path
- [x] Online store correctly **absent** (ADR-053: no sub-100 ms consumer exists)

### AGENT SAFETY
- [x] `WRITE_TOOLS == {}`, asserted by test
- [x] Read-only V1 — `ToolContract` refuses `read_only=False` at construction
- [x] Deterministic router (ADR-050); RAG and structured analytics are separate intents
- [x] Refusal enforced **twice** — router and `assert_read_only_sql`
- [x] Injection tested in user request, retrieved document and tool result — all `UNSAFE`
- [x] Tool calls bounded — `timeout_seconds` now enforced (AI-P15 fix), audited as `TIMEOUT`

### SECURITY
- [x] IAM least privilege — KB role has 4 scoped statements incl. explicit `DenyRawCdc`; `InvokeModel` pinned to one model ARN; KMS scoped to the lake CMK
- [x] Trust policy carries `aws:SourceAccount` **and** `aws:SourceArn`
- [x] No IAM users, no access keys
- [x] Network — no VPC/SG change, no `vpc_config`, no public inbound; SSM is the only UI path
- [x] KMS — lake converged to `e66f4dfa`; **567/567 warehouse objects**, 0 durable objects stale
- [x] Secrets from SSM SecureString at runtime; 9 redaction rules
- [x] Athena adversarial tests pass (112)

### OBSERVABILITY
- [x] Audit is a **control, not telemetry** — written on failure too; a tool whose audit sink raises does not silently succeed
- [x] Audit stores an input **hash**, never the input
- [x] 17 metrics; `emit()` fails open on sink outage, raises on validation error
- [x] Every answer carries request id, agent/prompt/corpus versions

### COST
- [x] Budgets: account `$100`, project `$100`, zero-spend `$1`
- [x] Athena workgroup: **10 GiB** bytes-scanned cutoff, `EnforceWorkGroupConfiguration=true`
- [x] AI incremental cost **$0.00/hr** — IAM role + policy only
- [x] Always-on AI components: **NONE** (12 costed)
- [x] Disabled and justified: Redshift, Trino, EKS, NAT, OpenSearch, SageMaker endpoints, Marquez, Lake Formation, online feature store, vector index, agent runtime

### E2E
- [x] **10 PASS / 0 FAIL / 0 BLOCKED** (`ai/eval/e2e_p14.py`)
- [x] Structured answer cross-checked against a direct governed query — agent `1280`, independent Athena `1280`
- [x] OPS answer cross-checked against DynamoDB ground truth

### FAILURE-RECOVERY
- [x] **20/20 drills PASS, 0 P0 violations**
- [x] Runtime restart, knowledge re-sync, model-config rollback, feature/model rollback all verified
- [x] No checkpoint deleted, no CDC watermark changed

---

## 3. Infra summary

| | |
|---|---|
| Terraform state | 291 resources · plan `0 add / 1 change / 0 destroy / 0 replace` |
| AI resources applied | `kafka-dev-lab-dev-ai-knowledge-base` (role + inline policy) |
| AI flags on | `enable_ai_rag` only |
| Lake CMK | `e66f4dfa` — 567/567 warehouse objects |
| Glue catalog | 8 Iceberg tables across 5 databases |
| Cost of the AI layer | **$0.00/hr** |

---

## 4. Known limitations

1. **Generation is untested** (P1) — Bedrock not invokable.
2. **Paraphrase retrieval is weak** (P2-2) — 0.25 recall@5.
3. **The pilot model has no predictive meaning** (P2-3) — synthetic label.
4. **A permanently non-empty Terraform plan** (P2-1) — one duplicated S3 object.
5. `enable_ai_feature_store` remains **false** by choice: enabling it rewrites
   `module.lake_iam`, and every EC2 module's `depends_on` then defers `data.aws_ami`, forcing
   replacement of all three instances. Fix the ordering before enabling.

---

## 5. Runbook index

| Runbook | Use |
|---|---|
| `docs/END_TO_END_WALKTHROUGH.md` | run the whole platform, CDC → S3 → mart → AI |
| `docs/VERIFY_END_TO_END.md` | deeper verification; start at §3 |
| `docs/runbooks/rebuild-from-scratch.md` | rebuild order and benign errors |
| `docs/runbooks/ai-platform-operations.md` | AI failure modes, drill triage |
| `docs/AI_FAILURE_MATRIX.md` | 20 drills and their evidence |
| `docs/AI_E2E_VALIDATION.md` | 10 E2E scenarios |
| `docs/AI_INFRA_GAP_ANALYSIS.md` | infra gaps and the CMK convergence |
| `scripts/reencrypt-lake-cmk.sh` | CMK convergence after a destroy cycle |

---

## 6. Readiness gates

| Gate | Result | Evidence |
|---|---|---|
| AI downstream/adjacent | ✅ | 0 imports from pipeline code |
| No competing source of truth | ✅ | AI owns no table |
| RAG ≠ structured analytics | ✅ | deterministic router, separate intents |
| Feature Store ≠ RAG | ✅ | separate modules and flags |
| Point-in-time leakage tests | ✅ | 2 passed |
| Agent V1 has no mutation authority | ✅ | `WRITE_TOOLS == {}` |
| Athena safety adversarial tests | ✅ | 112 passed |
| Unsafe action block tests | ✅ | drills 17, 18 PASS |
| RAG eval vs baseline | ✅ | quality gate PASSED |
| Agent eval vs baseline | ✅ | quality gate PASSED, `p0_violations 0` |
| E2E scenarios | ✅ | 10/10 |
| Recovery tests | ✅ | 20/20, 0 P0 |
| IAM least privilege | ✅ | 4 scoped statements, explicit Deny |
| Cost controls exist | ✅ | 3 budgets + enforced workgroup cutoff |
| Expensive optionals justified/disabled | ✅ | 11 flags false |
| No unintended destructive drift | ✅ | 0 destroy, 0 replace |
| Docs and handoff current | ✅ | this review + walkthrough + state |

**17 / 17 mandatory gates pass.**

The four open findings are P1–P3 and none maps to a mandatory gate. They are real and are
recorded above rather than closed by assertion — in particular, **"production ready" here
means the read-only retrieval-and-tools platform is ready; the generation path has never
been exercised.**
