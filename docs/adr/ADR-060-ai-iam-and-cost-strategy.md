# ADR-060 — AI IAM and cost — a ninth role, and nothing always-on

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

Eight workload roles exist. Broadening `spark` or `airflow` to serve the AI plane would widen two well-scoped roles for a third workload.

## Options

Reuse an existing role; grant `bedrock:InvokeModel` on `*`.

## Decision

A dedicated `ai` workload role. `bedrock:InvokeModel` scoped to PINNED model ids. Glue and S3 scoped to MART/SERVING. `warehouse/stream/` and `warehouse/full_cdc/` explicitly DENIED. Every AI component is per-request, per-job or metered-window; none is always-on.

## Consequences

One more role to maintain, and a hard ceiling on what a compromised agent can reach.

## Cost

Forbidden without a written, operator-approved cost review: OpenSearch Serverless, always-on SageMaker endpoint, new EKS, new NAT Gateway, new persistent EC2 AI runtime, large online feature stores.

## Security

Least privilege, and denial at the IAM layer so an application-layer bug cannot reach raw CDC.

## Rollback

Detach the role and delete it; no data is affected.

## Validation

Contract tests reject `CostClass.ALWAYS_ON` on a model definition; AI-P12 asserts the flags-off plan creates nothing.
