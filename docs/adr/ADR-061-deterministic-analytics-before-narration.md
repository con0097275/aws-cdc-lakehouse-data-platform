# ADR-061 — Deterministic analytics before language-model narration

**Status:** Accepted · 2026-08-28 · Supersedes nothing; constrains ADR-055/056/057.

## Context

The requirement is conversational analytics — *"why is the total account balance so small
today"*, *"what will it be tomorrow"*, *"what should I check, the mart looks wrong"* — plus a
UI for data-governance triage.

The obvious implementation is natural-language-to-SQL plus a language model that narrates the
result. Two facts make that the wrong first move here:

1. **Bedrock is not invokable on this account** (`INVALID_PAYMENT_INSTRUMENT`). An
   LLM-first design would ship nothing at all.
2. **None of these questions is actually a generation problem.** "Why did it move" is a
   contribution analysis. "What will it be" is a forecast with a measured error. "What should
   I check" is ranked data-quality triage. Each has a correct answer that arithmetic can
   produce and a reviewer can check.

An LLM asked to do these directly produces a fluent answer whose derivation cannot be
inspected, and which fails silently when the underlying data is incomplete.

## Decision

**Analysis is deterministic and mandatory. Narration by a language model is optional and
sits on top.**

- `ai/analytics/` computes verdicts, contributions, forecasts and findings with no model call.
- Every result carries its evidence: the baseline used, the row counts, the method chosen,
  the backtested error.
- A language model, when available, may only rephrase a result it is given. It never chooses
  the number.

Three rules follow, each of which the tests enforce:

**Completeness is checked before the business is blamed.** A metric that "dropped 60%"
because half the partition is missing is a pipeline incident. `diagnose()` returns
`DATA_INCOMPLETE` and refuses to decompose. Answering it as a business event sends someone to
the wrong team while the real fault keeps running.

**Non-additive measures refuse decomposition.** `AVG` and `COUNT(DISTINCT)` do not sum across
segments. A contribution table whose parts do not sum to the whole is worse than no table,
because it looks like arithmetic. `MetricSpec.additive` is declared per metric and
`NOT_DECOMPOSABLE` is a first-class verdict.

**A forecast without a measured error is an opinion.** Methods are chosen by walk-forward
backtest, the interval comes from that method's own historical error on that series, and
fewer than two seasonal cycles produces a refusal rather than a number.

## Consequences

- The feature works today, at $0 per question, with Bedrock unavailable.
- Answers are reproducible: the same inputs give the same output, which makes regression
  testing possible at all.
- The metric registry becomes the contract. Only declared metrics with declared dimensions
  can be discussed, which prevents "explain the metric with whatever column correlates".
- Governance stays **propose-only**: findings emit a fix command for a human to run. The
  read-only guarantee of ADR-057 is untouched, and the copilot does not become a second
  source of truth about the lakehouse.
- Cost of narration, when it arrives, is bounded and optional rather than load-bearing.

## Options

**NL-to-SQL over the mart.** Free-form generated SQL cannot be reviewed before it runs, gives
no stable answer to the same question, and would have to be re-validated against the Athena
guards on every call. The metric registry fixes the query shape and varies only parameters.

**LLM-first narration with the analysis inline.** Ships nothing while Bedrock is blocked, and
produces answers whose derivation cannot be audited when it is not.

**Option A — deterministic engines, optional narration (CHOSEN).** Works with Bedrock down,
auditable, testable, $0 per question.

**Option B — NL-to-SQL over the mart.** Free-form generated SQL cannot be reviewed before it
runs, gives no stable answer to the same question, and re-opens the Athena guard surface on
every call. Rejected.

**Option C — LLM-first, analysis inline in the prompt.** Ships nothing while Bedrock is
blocked, and when it is not, produces answers whose derivation cannot be audited. Rejected.

## Cost

**$0.00 per question beyond the Athena scan.** No model is invoked. A diagnosis reads one
aggregate and one grouped aggregate over a bounded date range; the workgroup's 10 GiB
bytes-scanned cutoff still applies and is enforced server-side. Demo mode costs nothing at
all — no AWS call is made — which is what makes the feature developable while the platform
is destroyed for cost reasons.

Narration, when Bedrock becomes available, is per-request and optional; it can be disabled
without losing the answer.

## Security

No new write path, no new IAM, no new network exposure.

- The engines reach data only through the existing read-only Athena tool, which keeps
  `assert_read_only_sql`, the denied-database list and the scan cap.
- SQL shape is fixed by `MetricSpec`; only dates and a declared dimension vary, so there is
  no path from user text to arbitrary SQL.
- `governance.py` emits fix commands as **strings**. It imports no subprocess, no boto3
  mutation client, and a test asserts this.
- `WRITE_TOOLS` remains empty; the four new tools are `read_only=True`, enforced at
  `ToolSpec` construction.

## Rollback

Delete `ai/analytics/`, drop the four tools from `CATALOG`, and remove the three intents from
the router. Nothing else depends on them: no table is created, no state is stored, no
Terraform resource exists. `ai/` remains deletable in full, and a test asserts no pipeline
code imports it.

## Validation

`spark/tests/test_ai_analytics.py` — 22 tests covering the three judgements this ADR turns
on: a short partition is reported as a data problem and never decomposed; a non-additive
measure refuses decomposition; a forecast on insufficient history refuses rather than
guessing. Plus routing precedence, exactness of contributions (parts sum to the whole), and
the propose-only property of the governance module.
