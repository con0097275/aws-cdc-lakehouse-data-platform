# ADR-050 — Routing — RAG never answers a number

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

'What was yesterday's balance' has an exact answer in a mart. Retrieval over prose returns documents that discuss balances.

## Options

Let the model decide the route; embed business rows so RAG can answer them.

## Decision

A DETERMINISTIC router runs first. Structured questions go to a tool or to governed Athena over MART/SERVING; only unmatched questions reach retrieval. RAG must never answer a numeric business question.

## Consequences

Routing patterns are code and must be maintained. In exchange, the common questions cost $0 and cannot hallucinate.

## Cost

Routing by an LLM would put a paid call in front of every free lookup — including the ones whose entire point is that they need no model.

## Security

Fewer model invocations means a smaller injection surface. The router cannot be talked into a different route by the question text.

## Rollback

Remove the route; the question falls through to retrieval.

## Validation

Routing is scored SEPARATELY from accuracy in every evaluation, so a free lookup silently becoming a paid call is caught as a cost regression.
