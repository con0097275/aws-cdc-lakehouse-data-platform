# ADR-058 — AI observability — reuse CloudWatch; metrics fail open, audit fails closed

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

The AI plane needs telemetry, and the project already has a logging stack.

## Options

A new logging stack; agent traces in Iceberg.

## Decision

Reuse CloudWatch and the existing Prometheus/Grafana. High-frequency traces go to CloudWatch; durable audit rows go to `ops.ai_agent_execution_hist`. Metrics may fail open; the AUDIT record must fail closed.

## Consequences

`routing_decision` and `guard_violation_count` are first-class, because one is the cost signal and the other the security signal.

## Cost

CloudWatch at this volume is cents. Iceberg per model call would be a small-files generator costing more than the telemetry is worth.

## Security

The asymmetry is deliberate: metrics observe the work, so their outage must not break the agent; the audit row is part of the control, so an unauditable action must not proceed.

## Rollback

Stop emitting; nothing else changes.

## Validation

Contract test asserts the agent history table stores a question HASH, not the question text.
