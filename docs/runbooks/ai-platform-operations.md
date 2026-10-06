# Runbook — AI platform operations

Ten failure modes, each with a symptom you can recognise before you know the cause.
Authored by AI-P11, 2026-08-26.

```bash
export AWS_PROFILE=my-aws-profile AWS_DEFAULT_REGION=ap-southeast-1
make ai-governance     # version snapshot + cost classes
make ai-security       # 23 checks against source
```

**The rule that governs all of these:** the copilot is READ-ONLY. No recovery below involves
the agent changing anything — it cannot, and no tool available to it can.

---

## 1. RAG sync failure

**Symptom** `make ai-corpus-build` fails, or `ops.ai_knowledge_sync` shows no row for today.

```bash
make ai-corpus                      # dry run — what WOULD be published
python3 ai/knowledge/sync.py --dry-run
```

| Cause | Fix |
|---|---|
| a document tripped the secret scan | the manifest names the file and the rule. **Fix the document**, do not weaken the scanner |
| `git_dirty: true` in the manifest | expected while working; the corpus is still valid, the commit pointer just is not faithful |
| corpus_version unchanged after an edit | the edit was to an excluded path. Check `sources.py` — exclusion is deliberate |

**Never** publish past a quarantine. The whole document is withheld precisely because
redacting-and-publishing bets the pattern list is complete.

---

## 2. Retrieval outage

**Symptom** answers arrive with no citations, or `retrieve_knowledge` errors.

Retrieval **degrades, it does not fail**: hybrid falls back to BM25, and BM25 is in-process
with no service behind it. If retrieval is genuinely down, the corpus is missing:

```bash
ls ai/knowledge/corpus_*        # empty -> make ai-corpus-build
make ai-ask Q="What is FULL_CDC?"
```

`backend` on every returned chunk says which path served it. A silent switch from `hybrid`
to `bm25` is visible there, not hidden.

---

## 3. Model outage

**Symptom** `generated: false` and `errors` contains `generation unavailable`.

**This is the designed behaviour, not an incident.** Tier 1 answers from tool output at $0.
The answer is less fluent and equally correct.

Currently expected — Bedrock is not invokable from this account:

| Error | Action |
|---|---|
| `Model use case details have not been submitted` | submit the Anthropic use-case form in the Bedrock console |
| `INVALID_PAYMENT_INSTRUMENT` | fix the AWS Marketplace payment method |

```bash
python3 -c "import sys;sys.path.insert(0,'ai');from agent.bedrock import BedrockClient;print(BedrockClient(enabled=True).verify_access())"
```

Do **not** disable the guards or widen a tool to compensate for a missing model.

---

## 4. Athena tool failure

**Symptom** `query_athena` errors, or returns `PermissionDenied`.

| Message | Meaning |
|---|---|
| `database … is not allow-listed` | working as designed — only `_mart/_curated/_ops/_snapshot` |
| `holds raw CDC and is denied` | working as designed, and IAM denies it too |
| `no qualified table reference` | the query had no `db.table`; unqualified names are refused |
| `exceeded …s and was cancelled` | the query was **cancelled**, not abandoned — an abandoned query keeps billing |
| `rows exceeds max_rows` | raise `limit` deliberately, up to 1000 |

Check bytes scanned in the audit row. If it is large, the query lacked a partition predicate.

---

## 5. Agent runtime failure

**Symptom** the hosted function errors or times out.

Nothing is deployed today (`enable_ai_agent_runtime = false`), so this is the LOCAL path:

```bash
python3 -c "import sys;sys.path.insert(0,'ai');from agent.handler import health;print(health())"
```

`status: degraded` means the corpus is missing or the tool catalog is empty. Once deployed,
`reserved_concurrent_executions = 2` caps blast radius; raise it deliberately, never to
"fix" a queue.

---

## 6. Feature lookup failure

**Symptom** `get_feature_definition` errors, or a feature value is missing.

`get_feature_value` is **deliberately not implemented** — `feature_offline` has not been
materialised. That is a recorded gap, not a bug. The online store is `DisabledOnlineStore`
and **raises** rather than returning `None`, so a caller that assumed it exists finds out at
the call site instead of silently scoring against defaults.

---

## 7. Model inference failure

**Symptom** `get_model_status` errors, or inference refuses.

Inference **refuses** on a missing or null feature rather than imputing — an imputed feature
produces a confident score from data that was never supplied.

`run_model_inference` is not exposed as a tool: the only model carries `synthetic_label:
true` and has no predictive meaning. Exposing it would invite an answer built on a
meaningless score.

---

## 8. Cost spike

**Symptom** the budget alarm fires, or daily spend jumps.

```bash
aws ce get-cost-and-usage --time-period Start=$(date -u -d '7 days ago' +%F),End=$(date -u +%F) \
  --granularity DAILY --metrics UnblendedCost \
  --query 'ResultsByTime[].{Day:TimePeriod.Start,USD:Total.UnblendedCost.Amount}' --output table
make ai-governance      # cost class per component
```

**Check the platform before the AI.** The AI plane has no always-on component; the lakehouse
runs at ~$1.23/hr and MSK alone is ~66% of it. An AI cost spike is far more likely to be
MSK left running — see `docs/runbooks/stop-and-resume.md`.

If it genuinely is the AI plane: `AI_ENABLE_GENERATION=false`, and check
`AthenaBytesScanned` and `RoutingDecision` — a free lookup becoming a paid call is a cost
regression no accuracy metric would show.

---

## 9. Unsafe-request investigation

**Symptom** `GuardrailBlocks` rises, or a refusal appears in the logs.

```bash
python3 -c "
import sys;sys.path.insert(0,'ai')
from agent_tools.contract import AUDIT_LOG
print([(a.tool_name,a.status,a.error_class) for a in AUDIT_LOG if a.status in ('DENIED','REJECTED')])"
make ai-eval-agent-gate     # 18 unsafe scenarios must all block
```

A refusal is the system working. What matters is whether any request **reached a tool**:
`p0_violations` must be `0`. If it is not, stop and treat it as a security incident — do not
proceed to the next phase.

Audit rows carry an input **hash**, never the question: a question can contain a customer
name, its hash cannot.

---

## 10. Version rollback

Every version is content-addressed, so rollback is "point at the previous content", never
"decrement a counter".

| Component | Rollback |
|---|---|
| corpus | rebuild at the older commit; the hash re-derives |
| chunking | revert the strategy — every chunk id changes, which is what makes re-embedding decidable |
| prompt | `git revert`; the version is a hash of the text and cannot be stale |
| tool | revert the `ToolSpec`; bump the version on any contract change |
| agent | `git revert`, then **re-run `make ai-eval-agent-gate`** before trusting it |
| feature group | a bump writes NEW rows; history is preserved for point-in-time reconstruction |
| ml model | point inference at the prior artifact — `model_version` is in every scored row |
| runtime | flag off is instant; redeploy the previous package |

```bash
make ai-governance         # what is in force right now
make ai-eval-rag-gate      # retrieval has not regressed
make ai-eval-agent-gate    # safety has not regressed  <- never skip this one
```

---

## Failure drills (AI-P15)

`python3 ai/eval/drills_p15.py` — 20 drills, safe injection, ~30s, $0. Expect **20/20 PASS,
0 P0 violations**. Full matrix: `docs/AI_FAILURE_MATRIX.md`.

Run it after any change to `ai/guards.py`, `ai/agent_tools/contract.py`, the tool catalog, or
the router. Those four files are where a safety property gets removed by accident.

### The two failure shapes worth recognising

**A declared control that is never enforced.** `ToolSpec.timeout_seconds` existed on every
tool, `ToolTimeout` had a handler, and the docstring said calls were timed — but nothing
bounded them. Everything *looked* correct at every call site. If you add a control, add the
test that proves it fires; a control with no failing test is a comment.

**A blocklist that covers the obvious case.** `assert_no_infrastructure_action` stopped
`terraform destroy` and missed `rm -rf /opt/checkpoints`. Whoever wrote it tested the command
they were thinking about. When you extend it, add the category, not the string.

### If a drill fails

| Drill | Meaning | First check |
|---|---|---|
| 1 | corpus missing or unreadable | `make ai-corpus-build`, then `ls ai/knowledge/corpus_*` |
| 2 | hybrid retrieval stopped degrading | it must **never** raise — BM25 is the floor |
| 6 | tool calls are unbounded again | `_call_bounded` in `contract.py`; must not be a `with` block |
| 7/8 | Athena guard weakened | `DENIED_DATABASE_SUFFIXES`, `MAX_LIMIT`, workgroup cutoff |
| 14–16 | injection reached a tool | check `WRITE_TOOLS == {}` first — that is the real control |
| 17/18 | **P0.** A mutation path is open | stop, fix, re-run before anything else |

A drill that fails on `unsafe_fallback` is more serious than one that fails on `detected`:
it means the system answered anyway.
