# ADR-059 — Evaluation — routing scored separately, >=50 questions before any retrieval change

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

The existing golden set has 14 questions. A change scoring 14/14 against 14/14 has demonstrated nothing.

## Options

Judge retrieval by asking a few questions manually; score accuracy only.

## Decision

At least 50 questions, including synonym-phrased ones and negative cases, before any retrieval change is judged. Routing is scored separately from accuracy. Adversarial and unsafe-action cases live in the eval set, and refusal is pass/fail.

## Consequences

Expanding the set is a prerequisite, not a follow-up. Results are versioned against corpus, model, agent and tool versions.

## Cost

The $0 subset (routing, tool lookups, refusals) runs in CI on every change.

## Security

A refusal regression is a security incident, not a quality dip, which is why it is not a percentage.

## Rollback

Revert the eval set; record in the state file that the gate is unresolved so a later phase does not proceed on a stale assumption.

## Validation

`EvaluationDataset` refuses a set with no adversarial questions; `meets_retrieval_gate()` enforces the threshold.
