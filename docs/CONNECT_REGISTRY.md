# Kafka Connect + Apicurio Registry

- Session: 04
- Date: 2026-08-13
- Status: **`static-validated` and `planned`.** Nothing deployed; no connector has ever run.
- Decisions: [ADR-002](adr/ADR-002-connect-runtime.md), [ADR-003](adr/ADR-003-schema-registry.md)
- Risks addressed: **R2** (MSK IAM on self-managed Connect), **R3** (Apicurio inherits R2)

---

## 1. What this reuses, and what it does not create

The instruction was to reuse the existing Kafka infrastructure and not duplicate
monitoring. Asserted against the plan, not claimed:

| | Planned | Meaning |
|---|---:|---|
| MSK clusters | **1** | The existing one. No second cluster |
| VPCs | **1** | The existing one. No new network |
| New Prometheus/AMP stacks | **0** | Metrics go to the Prometheus already on the toolbox |
| CloudWatch alarms | **6** | The inherited broker alarms. **No duplicates added** |
| Ingress rules using a CIDR | **0** | All 5 are SG-to-SG |

### Monitoring is extended, not duplicated

The existing Prometheus scraped `msk-jmx` and `msk-node` from **static targets** rendered
by Terraform. Adding Connect the same way would need the toolbox to know the CDC
runtime's IP — while the CDC runtime already depends on the toolbox's VPC. **That is a
module cycle.**

Two new scrape jobs use **EC2 service discovery** filtered on `Component=cdc-runtime`
instead. This breaks the cycle, and a rebuilt Connect host is picked up without editing
anything. The toolbox role gains `ec2:DescribeInstances` — account-wide because the API
takes no resource ARN, read-only, and returning nothing beyond instance metadata.

> The `Component=cdc-runtime` tag is load-bearing. Change it and the scrape silently
> finds no targets — Prometheus reports no error, the job is simply empty.

## 2. Risk R2 — the single root cause behind two H/H risks

**MSK IAM auth needs `aws-msk-iam-auth` on the classpath AND the SASL callback handler
set on all FOUR client scopes.** Connect does *not* inherit top-level security settings
into its producer, consumer and admin clients — each is configured separately.

```properties
sasl.client.callback.handler.class=...IAMClientCallbackHandler          # worker
producer.sasl.client.callback.handler.class=...IAMClientCallbackHandler # producer
consumer.sasl.client.callback.handler.class=...IAMClientCallbackHandler # consumer
admin.sasl.client.callback.handler.class=...IAMClientCallbackHandler    # admin  <-- most forgotten
```

Miss one and the failure is a `SaslAuthenticationException`, no records on any topic, and
internal topics never created.

**R3 is the same root cause with a nastier symptom.** Apicurio's KafkaSQL persistence
makes the registry an MSK IAM client too — but its REST API **starts successfully and
returns 500s on write**. It looks healthy while rejecting every schema.

**Mitigation, in order:** `client.properties` carries the identical config, and
`smoke-test.sh` step 1 runs `kafka-topics --list` with it **before anything else**. If
that fails, the script stops — every downstream check depends on it and their errors are
much harder to read.

## 3. Explicit topic creation — Gap 12

`auto.create.topics.enable=false`, so **nothing creates topics on demand**:

| Consumer | Without its topic |
|---|---|
| Connect | Fails at worker startup |
| Apicurio | REST starts, 500s on every write (R3) |
| Debezium | Connector runs, produces nothing |

`create-topics.sh` creates all 15 idempotently:

| Topic | Parts | Cleanup | Why |
|---|---:|---|---|
| `connect-configs` | **1** | compact | **Must be 1.** Config ordering is global; more partitions corrupt it |
| `connect-offsets` | 25 | compact | |
| `connect-status` | 5 | compact | |
| `kafkasql-journal` | 1 | **compact** | The registry replays it to rebuild state. Time-based deletion would lose live schemas |
| `cdc.oracle.corebank.*` (4) | 3 | delete, 24 h | |
| `cdc.sqlserver.digital.*` (4) | 3 | delete, 24 h | |
| `cdc.dlq.{oracle,sqlserver}` | 3 | delete, **7 d** | Poison records are evidence for an investigation that starts after someone notices |

All get `min.insync.replicas=2` with RF=3 — survives one broker restart.

> **Partition counts are permanent.** `CLAUDE.md` §5.1 requires the topic key to be the
> canonical PK so the same PK always lands in the same partition. Changing partition count
> later re-hashes keys and breaks that guarantee for every record already written.

## 4. Artifact pinning — and an honest gap

`versions.env` pins six artifacts by exact version and URL: Debezium Oracle/SQL Server
2.7.3.Final, `aws-msk-iam-auth` 2.2.0, Apicurio Avro serde 2.6.2.Final, `ojdbc11`
23.5.0.24.07, and the SSM config provider 0.1.2.

**Oracle JDBC is included deliberately.** Debezium does not bundle it for licensing
reasons, and omitting it is a classic first-boot failure: the connector loads, then dies
on `ClassNotFoundException: oracle.jdbc.OracleDriver`.

**The SHA256 values are recorded as `UNVERIFIED`, not invented.** Nothing here has been
downloaded — that requires a running instance. `verify-artifacts.sh` **fails closed** on
`UNVERIFIED` rather than proceeding, so the first apply cannot silently accept an
unverified jar. The one-time flow:

```bash
bash scripts/cdc-runtime.sh record-checksums --execute   # prints observed SHA256
# paste into docker/cdc-runtime/versions.env, replacing UNVERIFIED
```

Every subsequent boot enforces them. **A fabricated checksum would be worse than an
absent one**, which is why they are marked rather than guessed.

## 5. Security

| Control | How |
|---|---|
| No inbound from the internet | CDC runtime SG has **zero** ingress from any CIDR. Only two SG-to-SG rules, both from the toolbox, for metrics |
| Connect REST not exposed | Binds the **private IP**, reached by SSM port forwarding. `smoke-test.sh` step 5 actively probes the **public** IP and fails if it answers |
| Registry not exposed | Same. Anonymous read disabled, admin password from SSM |
| No static credentials | MSK auth is the instance role. Connector configs use `${ssm:/path}` via the config provider — never a literal password |
| Least privilege | The `connect` role has `msk_produce` (topic-scoped) + `secrets_read` + `bootstrap_read`. Not cluster-wide |
| Negative test | `smoke-test.sh` step 7 attempts a **PLAINTEXT** connection and **fails the suite if it succeeds** — proving IAM is enforced, not assumed |

## 6. The link that opens the source lab

Session 03 shipped the source lab with **zero ingress rules**, so its databases were
unreachable from anything. Session 04 adds the first and only rules that open 1521/1433 —
SG-to-SG, referencing the CDC runtime's security group, not a CIDR range.

Enabling `enable_cdc_runtime` without `enable_source_lab` is fine; the reverse leaves the
databases closed, which is the safe direction.

## 7. Smoke tests

`bash scripts/cdc-runtime.sh smoke` — read-only, exits non-zero on any failure.

1. **MSK IAM client config** — `kafka-topics --list`. Stops the suite if it fails.
2. Required topics exist.
3. Internal topic RF ≥ 2, and `connect-configs` partitions **= 1**.
4. Connect REST up; `OracleConnector` and `SqlServerConnector` present in `/connector-plugins`.
5. **Connect REST is NOT reachable on the public IP.**
6. **Apicurio writes a real schema and reads it back** — health alone is insufficient,
   because R3's whole point is that health lies.
7. **Unauthenticated PLAINTEXT client is rejected.**

## 8. Cost

| | |
|---|---:|
| CDC runtime `t3.large` | **$0.1056/hr** |
| gp3 40 GiB | included in the window |
| Full stack with source lab + CDC runtime | **$1.0996/hr** |
| Destroyed | **$0.00** — `delete_on_termination = true` |

Co-locating Connect and Apicurio is worth $0.1056/hr: two hosts would double it for
services that share a lifecycle and an identical MSK IAM configuration.

The one always-on addition is **$0** — the Apicurio admin password is an SSM
SecureString, and standard-tier parameters are free (ADR-031).

## 9. Rollback

```bash
bash scripts/tf.sh plan     # with enable_cdc_runtime=false
bash scripts/tf.sh apply --execute
```

Connector offsets live in Kafka (`connect-offsets`), so **destroying and recreating the
worker resumes where it stopped.** That property is what makes the ephemeral model safe
for CDC and is the reason ADR-002 chose distributed mode over standalone.

The Kafka topics survive a CDC-runtime destroy. They are recreated idempotently, and
their retained data is bounded by the 24 h / 7 d retention above.
