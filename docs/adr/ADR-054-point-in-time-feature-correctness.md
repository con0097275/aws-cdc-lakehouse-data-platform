# ADR-054 — Point-in-time correctness — event time, and the leakage tests

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

A training set joined on processing time leaks the future. The model scores well and is worthless, and nothing looks broken.

## Options

Join on ingestion time and 'be careful'; document the rule without enforcing it.

## Decision

`feature_event_time <= label_event_time`, `<=` not `<`. `created_at` is processing time, recorded for audit and never a join key. One implementation of the rule, used by both training and inference. Three leakage tests are mandatory before any model is trained.

## Consequences

Feature pipelines must carry event time end to end. Backfills must stamp the event time they represent, not the time they ran.

## Cost

$0 — this is a semantic rule.

## Security

Leakage is a correctness and, for a risk model, a governance failure. The forward-horizon test is the one that catches it.

## Rollback

Revert the training set; features are unaffected because training only reads.

## Validation

Nine point-in-time tests, including the label-horizon exclusion that plain `<=` does not catch.
