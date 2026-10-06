# ADR-051 — Athena tool — guards, workgroup, and IAM layer separation

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

Today the assistant generates SQL and nothing executes it. Adding an executor is the moment a read-only guarantee stops being theoretical.

## Options

Trust a system prompt; validate with a deny-list of destructive verbs.

## Decision

Execution is wrapped by: an ALLOW-list of statement prefixes, single-statement enforcement, comment stripping, an injected LIMIT re-validated after injection, a dedicated Athena workgroup with `enforce_workgroup_configuration = true`, result caps applied before the model sees output, and an audit row per call. Raw CDC is denied to the AI role in IAM.

## Consequences

Two controls to maintain instead of one. That is the point: the allow-list is the second control, not the only one.

## Cost

Bounded by the workgroup's bytes-scanned cutoff, which a client cannot override.

## Security

A deny-list is a list of the destructive verbs someone thought of, and SQL keeps adding more. IAM-layer separation means a guard bug cannot reach `warehouse/stream/` or `warehouse/full_cdc/`.

## Rollback

Remove the executor; the assistant returns to generate-only SQL, which is the current safe state.

## Validation

Mutation testing: each control is disabled in turn and a test must fail. A control whose absence no test detects is not verified.
