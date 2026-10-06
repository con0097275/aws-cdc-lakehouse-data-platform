# ADR-002 — Kafka Connect runtime

> **Superseded in part 2026-08-13 by [ADR-031](ADR-031-secret-store.md):**
> credentials live in **SSM Parameter Store SecureString**, not Secrets Manager.
> The runtime pattern below is unchanged — generated at apply, read at container
> start, never in outputs or user-data — only the store differs.


- Status: **ACCEPTED** (Session 01)
- Related: ADR-003, risks R2, R3, R15

## Context

Debezium needs a Connect runtime reaching MSK over IAM SASL. Options are MSK Connect
(managed) or self-managed Connect on EC2/containers. `CLAUDE.md` §4 forbids
always-on managed components without a flag and a destroy path.

## Options

| Option | Cost | Plugin control | Verdict |
|---|---|---|---|
| **Self-managed on ephemeral EC2** | `t3.large` at $0.1056/hr, destroyed with the window | full — any connector, any version, any SMT | **CHOSEN** |
| MSK Connect | per-MCU-hour, no stop, provisioned per connector | limited to uploaded custom plugins; slower iteration | Rejected |
| Connect on k3s alongside Airflow | marginal | full | Rejected for this session |

## Decision

Self-managed Kafka Connect in **distributed mode**, one worker, on an ephemeral
`t3.large` in a public subnet with a zero-inbound security group (ADR-022).

Distributed mode with a single worker, not standalone: standalone stores offsets in
a local file, which dies with the instance. Distributed mode keeps
`connect-offsets`, `connect-configs` and `connect-status` **in Kafka**, so a
destroyed and recreated worker resumes where it stopped. That property is what makes
the ephemeral model safe for CDC, and it is worth the small extra complexity.

Co-locating Connect with Apicurio Registry on one instance is deliberate: they share
a lifecycle, they are both MSK IAM clients with identical client configuration
(risks R2/R3), and one instance is $0.1056/hr instead of two.

## Consequences

- Internal topics must be **created explicitly** — `auto.create.topics.enable=false`
  (Gap 12) means Connect cannot create them itself. This is a silent failure at
  first boot if forgotten.
- Internal topic replication factor must be ≤ broker count (3) and ≥ 2 to survive a
  broker restart.
- Connect REST is private-only, reached via SSM port forwarding, never exposed.
- Single worker means no task-level HA. A worker loss costs minutes of a window; it
  loses no data because offsets are in Kafka.

## Cost

$0.1056/hr while running, $0 when destroyed, plus ~$4.80/month of gp3 if the volume
is left behind — so destroy, don't stop (`docs/COST.md` §3.2).

## Security

- Instance profile only; no static credentials.
- DB credentials read from Secrets Manager at container start, never in user-data
  or environment plaintext (`CLAUDE.md` §3.1, §3.6).
- Zero inbound SG rules; MSK reached by SG-to-SG reference on 9098.
- Connector configs containing passwords use the Connect
  `config.providers` mechanism so secrets never land in the REST API response.

## Rollback

`enable_cdc_runtime = false` and destroy. Connector state survives in Kafka internal
topics for the cluster's lifetime; if the cluster is also destroyed, recovery is a
Debezium re-snapshot (risk R6).

## Validation

- `kafka-topics --list` succeeds from the Connect host using the worker's exact
  client config, **before** any connector is deployed (mitigates R2).
- Internal topics exist with the intended partition count and RF.
- Connector restart produces no duplicate or lost events beyond documented
  at-least-once semantics.
