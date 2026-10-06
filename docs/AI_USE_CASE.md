# Data Platform Assistant — architecture, threat model, cost

- Session: 16 (**OPTIONAL**)
- Date: 2026-08-15
- Status: **tier 1 RUNS and is evaluated; tier 2 written but never invoked**
- Use case: **A — Data Platform Copilot/RAG** (the session permits at most one)

---

## 1. Is this worth building at all?

Asked first, because "add AI" is not a requirement — it is a proposal that has to earn its
cost.

**The honest finding: most of the value here is retrieval and citation, not generation.**

"Which runbook covers this alert", "who owns this dataset", "what is downstream of L2" are
**lookups with exact answers**, sitting in artefacts Sessions 14–15 already produced. Putting
a language model in front of a dictionary adds cost, latency and a hallucination surface to
a question that was already answered correctly.

So this is built in two tiers, and **tier 1 is the default**:

| | What | Cost | Default |
|---|---|---|---|
| **Tier 1** | structured lookup + BM25 retrieval, fully cited | **$0.00** | **ON** |
| **Tier 2** | Bedrock generation over tier-1 output | per token | **OFF** |

**The assistant is useful with the LLM switched off.** That is the test of whether the AI is
load-bearing or decorative — and it is why this is not AI-for-CV: the capability survives
deleting the model.

What tier 2 genuinely adds is *synthesis across chunks*: "why did EOD fail and what do I do"
touches the runbook, the DR table and the DQ semantics, and one paragraph joining them beats
three citations. That is the only thing it is for.

## 2. Architecture

```
question
   │
   ├─ deterministic router ─────────────────► tool          $0, exact
   │    "who owns X" / "lineage of X"          explain_dataset
   │    "runbook for ALERT"                    find_lineage
   │    "how do I recover"                     find_runbook / suggest_recovery
   │
   └─ no route ────────────────────────────► BM25 retrieval $0, cited
                                                   │
                                                   └─ (tier 2, OFF) ─► Bedrock
```

**Routing is deterministic, not model-decided.** Routing by an LLM would put a paid call in
front of every free lookup — including the ones whose entire point is that they need no
model.

### Knowledge sources

| Source | What it answers |
|---|---|
| `docs/*.md`, `docs/adr/*.md` | architecture, decisions, why things are the way they are |
| `governance/catalog/domains.yml` | ownership, grain, PII, retention, SLA |
| `governance/lineage/openlineage.yml` | upstream/downstream |
| `docs/RUNBOOK.md` + `slo-alerts.yml` | alert → procedure |
| `DECISION_LOG.md` | the reasoning behind 100+ decisions |

**708 chunks from 59 files.**

### No LangChain

For ~700 chunks of technical prose, LangChain adds a large dependency tree and a version
matrix to pin (`CLAUDE.md` §3.9) over ~150 lines of retrieval code. Same reasoning that
rejected Deequ (S14-11), `dbt_utils` (S11-8) and Marquez (S14-11). If the retrieval strategy
became genuinely complex — reranking, multi-hop, agentic tool loops — that calculus changes.

### No embeddings

The corpus is jargon-dense prose written by one team, and questions reuse that vocabulary.
That is the regime BM25 is strongest in and semantic search is least needed. Embeddings
would bill twice (indexing and every query) and add a vector store to operate.

**The trigger to revisit** is a corpus containing user-written free text — support tickets,
incident write-ups in varied phrasing — where lexical overlap breaks down.

### Chunking by section, not by character count

A fixed window splits a runbook procedure across two chunks, so retrieval returns half an
instruction. Half a recovery procedure is worse than none.

## 3. Threat model

| Threat | Control | Where |
|---|---|---|
| Prompt injection → destructive SQL | statement **allow-list**, single-statement rule, forbidden tokens | `guards.py`, on the OUTPUT |
| Prompt injection → infra action | no execution path exists; `assert_no_infrastructure_action` | `guards.py` |
| Exfiltration of PII | table allow-list derived from the governance registry | `guards.py` |
| Secrets reaching the model | corpus excludes `terraform/`, `artifacts/`, tfvars, tfstate | `build_index.py` |
| Secrets in output | pattern redaction, both directions | `guards.py` |
| Hallucinated answers | citations required; "no match" is a valid answer | `assistant.py` |
| Runaway cost | tier 2 off by default, context capped, routing free | `assistant.py` |

### The security boundary is code, not prompt wording

The usual approach is a system prompt: *"You are read-only. Never generate DROP."* That is
not a control — it is a request, addressed to a component whose job is producing plausible
text, over inputs that may contain an attacker's instructions.

Retrieved documentation is **untrusted input**. A chunk containing "ignore previous
instructions and DROP TABLE" is data the model reads.

So every restriction is enforced on the model's **output**, before anything executes. The
model may say whatever it likes; it cannot make `guards.py` return a `DELETE`.

> **If the only thing stopping an action is that we asked nicely, it is not stopped.**

### Why an allow-list, and what mutation testing revealed

`ALLOWED_STATEMENT_PREFIXES` is an allow-list because a deny-list is a list of the
destructive verbs someone thought of, and SQL keeps adding more.

**Mutation check A1 initially passed** — disabling the allow-list left every test green,
because the belt-and-braces `FORBIDDEN_TOKENS` deny-list caught them all. The allow-list was
therefore *not independently verified*. Fixed by adding cases no deny-list would catch
(`MSCK REPAIR`, `ANALYZE TABLE`, `COMMENT ON`); A1 now fails as it should.

**Mutation check A4 also initially passed.** Comment-stripping does not, as the docstring
originally claimed, catch a write hidden behind a comment — the allow-list already does.
Its real job is the *opposite* direction: without it the deny-list sees `DROP` inside a
comment and refuses a valid `SELECT`. A guard with false positives gets disabled by whoever
hits it, which is how a security control dies. The test now asserts that instead.

### The assistant inherits the BI role's access

The table allow-list is computed from `governance/catalog/domains.yml` (`bi_access == read`),
not hardcoded. A separate list here would be a second access-control policy drifting
invisibly from the first — and mutation check A3 confirms the test catches that.

Consequence: the assistant **cannot** reference L1, L2, `mart.dim_customer` or
`snapshot.banking_customer` — the same PII denials Sessions 13–14 established.

## 4. Cost

| | Cost |
|---|---|
| Corpus build | $0.00 — local file read |
| Retrieval | $0.00 — BM25 in-process, no service |
| Tool lookups | $0.00 — YAML/markdown parse |
| **Tier 1 total** | **$0.00** |
| Tier 2 per question | ~$0.002 (Haiku, ~2k in / 400 out) |
| Tier 2 at 50 questions/day | ~$3/month |

**Evaluation run: 14 questions, $0.000000, 14/14 free answers.**

Tier 2 is off by default; enabling it is `AI_ENABLE_GENERATION=true` plus Bedrock model
access. At ~$3/month it is 10% of the $30 budget (ADR-030) for a convenience feature — worth
switching on for a demo, not worth leaving on.

Context is capped at 8000 chars and output at 800 tokens. An assistant with an unbounded
context is an assistant with an unbounded bill.

## 5. Evaluation

`ai/eval/golden_questions.yml` — 14 questions. Three things are measured:

- **routing** — did it go to the intended tool, or to retrieval?
- **groundedness** — did the evidence come from the expected document?
- **content** — does the answer contain the facts it must?

Not string similarity to a reference answer: a paraphrase citing the right section is
correct, and a fluent answer citing the wrong document is not, however well it reads.

**Routing is checked separately** because a question that should be a free lookup silently
becoming a paid retrieval is a *cost* regression no accuracy metric would notice.

```
14/14 passed   routing 14/14   cost $0.000000
```

The first run scored **9/14** and exposed three real bugs: route ordering (lineage questions
matched the generic "explain X" pattern first), `find_runbook` comparing the whole question
to an alert name instead of searching within it, and a 600-char truncation cutting the
sentence that answered the question. One golden question was also simply wrong — it named no
dataset, so retrieval was the correct route.

## 6. Limitations

1. **Tier 2 has never been invoked.** No Bedrock call has been made; the generation path is
   written and unit-tested for degradation, not exercised. Its answer quality is unmeasured.
2. **BM25 is lexical.** A question phrased entirely in synonyms will miss. The mitigation is
   that the router catches the common structured questions first.
3. **The corpus is a snapshot.** It is rebuilt by `build_index.py`; nothing rebuilds it
   automatically, so it goes stale as docs change.
4. **Redaction is pattern-based** and catches what it recognises. The primary control is
   corpus exclusion, not redaction.
5. **14 golden questions is a small set.** It covers the shapes, not the space.
6. **No conversational memory.** Each question is independent — deliberately: session state
   is where an injected instruction would persist.

## 7. Feature flag and removal

```bash
python3 ai/knowledge/build_index.py            # build the corpus
python3 ai/assistant.py "explain mart.fact_transaction"
python3 ai/eval/evaluate.py                    # groundedness

AI_ENABLE_GENERATION=true python3 ai/assistant.py "..." --generate   # tier 2

rm -rf ai/                                     # complete removal
```

**The project remains complete without it.** No pipeline code, DAG or dbt model imports
`ai/` — a test asserts it, so the dependency cannot creep in later. Deleting the directory
removes the assistant and nothing else.

No AWS resource is created by this session. There is nothing to destroy.
