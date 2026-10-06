# ADR-053 — Online features — DynamoDB, and off by default

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

An online store is only justified by a low-latency consumer. This project has none.

## Options

SageMaker Feature Store online — a new service class with its own cost envelope and destroy path.

## Decision

DynamoDB, following the proven ADR-036 pattern, behind `enable_ai_online_feature_store`, DEFAULT OFF and no table created. Streaming features are not built at all.

## Consequences

When a consumer appears, the adapter exists and the pattern is already operated here. Until then nothing is provisioned.

## Cost

DynamoDB `PAY_PER_REQUEST` is $0 idle. A one-minute streaming cadence is roughly 7x the monthly budget — recorded in `docs/COST.md`.

## Security

Narrow IAM: no `Scan`, no `DeleteItem`, no wildcard — the same policy shape ADR-036 already uses.

## Rollback

Set the flag false; no table exists to destroy.

## Validation

The contract refuses `streaming_enabled=true` and refuses an online store with no offline source.
