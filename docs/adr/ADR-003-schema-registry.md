# ADR-003 — Schema Registry: Apicurio with KafkaSQL

- Status: **ACCEPTED** (Session 01)
- Related: ADR-004 (Glue Schema Registry as alternative), risk R3

## Context

Avro serialization needs a registry for schema IDs and compatibility enforcement.
Candidates: Apicurio Registry with KafkaSQL persistence, Confluent Schema Registry,
or AWS Glue Schema Registry.

## Options

| Option | Persistence | Debezium integration | Cost | Verdict |
|---|---|---|---|---|
| **Apicurio + KafkaSQL** | a Kafka topic — no separate database | first-class converter | $0 beyond the shared EC2 | **CHOSEN** |
| Apicurio + PostgreSQL | RDS or a container | first-class | RDS cost, or another container to operate | Rejected |
| Confluent Schema Registry | a Kafka topic | first-class | licensing questions for non-CE use | Rejected |
| AWS Glue Schema Registry | managed | converter exists but less exercised with Debezium+Iceberg | ~free | **ADR-004: alternative, not default** |

## Decision

Apicurio Registry with **KafkaSQL** persistence, co-located with Kafka Connect
(ADR-002).

KafkaSQL is chosen specifically because it avoids introducing a database. A
PostgreSQL-backed registry would mean either RDS (cost, and `CLAUDE.md` §4.3
discourages it) or another stateful container whose volume must survive the window.
Storing registry state in Kafka means the registry is as ephemeral as everything
else and recovers by replaying its own topic.

## Consequences

- **The registry becomes an MSK IAM Kafka client**, inheriting risk R2 in full.
  Its failure mode is nastier: the REST API starts and returns 5xx on write rather
  than failing at boot, so it looks healthy while rejecting every schema.
- `kafkasql-journal` must be **created explicitly** — `auto.create.topics.enable=false`
  means the registry cannot create it (Gap 12). This is the single most likely
  first-boot failure.
- Compatibility level is `BACKWARD` on value subjects; subject naming strategy is
  `TopicNameStrategy`. Both are fixed in `docs/DATA_CONTRACTS.md` §10 — changing the
  naming strategy later orphans every registered schema.

## Cost

$0 incremental — shares the CDC runtime instance. The `kafkasql-journal` topic is
kilobytes.

## Security

Private-only REST endpoint, SSM port forwarding for inspection. IAM SASL to MSK via
the instance profile. No registry authentication in the lab, which is acceptable
only because the endpoint is unreachable from outside the VPC — recorded so it is
not mistaken for a production-ready posture.

## Rollback

ADR-004's Glue Schema Registry is the documented fallback, and it is reached by
changing the converter configuration plus re-registering schemas. Taking it requires
a successful E2E spike first (`DECISIONS.md` ADR-004), because the Debezium →
Glue → Spark → Iceberg path is less exercised than the Apicurio one.

## Validation

- Register a schema, read it back by ID, and confirm `kafkasql-journal` has records.
- A backward-compatible change (add optional field with default) is **accepted**.
- An incompatible change (drop a required field) is **rejected** — both directions
  must be proven, since a registry that accepts everything is worse than none.
- Registry restart recovers all schemas from the journal topic.
