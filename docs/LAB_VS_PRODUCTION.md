# The cost-optimized lab and the production target

One architecture, deployed two ways. This document says which is which, because the
difference between *"I built a production data platform"* and *"I built a production-shaped
platform and ran it in metered windows"* is the difference between a claim that survives an
interview and one that does not.

**What ran here is the lab.** The production column is a design, validated by
`terraform validate` and by ADRs, and it has never been operated.

---

## 1. The lab, as actually deployed

Applied and destroyed inside metered windows — 2026-08-16, then rebuilt for the live
sessions of September 2026. Never left standing.

```text
                        COST-OPTIMIZED LAB  (~$1.40/day while up, $0 at rest)

  ┌─ EC2 (containers) ──────────┐        Oracle 21c XE + SQL Server 2022
  │  source_lab_ec2             │        no RDS: Oracle/SQL Server licensing is the cost
  └──────────────┬──────────────┘
                 │
  ┌─ EC2 ────────▼──────────────┐        Kafka Connect + Debezium + Apicurio
  │  cdc_runtime_ec2            │        one instance, not a cluster
  └──────────────┬──────────────┘
                 │
  ┌─ MSK Provisioned ───────────┐        Kafka 3.9.x KRaft · IAM + TLS · 3 AZ
  │  kafka_platform             │        private brokers · smallest viable broker type
  └──────────────┬──────────────┘
                 │
  ┌─ EMR Serverless ────────────┐        emr-7.2.0 · auto-stop ON · 100 GB cap
  │  emr_serverless             │        job timeouts · no pre-initialized capacity
  └──────────────┬──────────────┘
                 │
  ┌─ S3 + Glue + Athena ────────┐        Iceberg v2 · KMS CMK · BPA · TLS-only policy
  │  data_lake · glue_catalog   │        78 tables / 8 databases
  │  athena                     │        workgroup with a bytes-scanned cutoff
  └──────────────┬──────────────┘
                 │
  ┌─ k3s on EC2 ────────────────┐        Airflow 3.2.2 · KubernetesExecutor
  │  airflow_k3s                │        Postgres container on a PVC
  └─────────────────────────────┘

  Access: SSM Session Manager only — no SSH, no key pair, no bastion, no NAT gateway.
  Secrets: SSM SecureString / Secrets Manager, read at runtime.
  Guardrails: budget_guardrails module + AutoDestroyAfter tag on every resource.
```

**Deliberately absent, and why:**

| Not in the lab | Reason |
|---|---|
| NAT gateway | ~$32/month for egress a private lab does not need; VPC endpoints instead (`vpc_endpoints`) |
| RDS Oracle / SQL Server | licensing; containers on EC2 give the same CDC behaviour for a fraction of the cost |
| EKS | k3s on one EC2 instance satisfies `KubernetesExecutor`; EKS is a flag with a destroy path |
| Pre-initialized EMR capacity | it bills while idle, which is most of the time in a lab |
| Redshift Serverless / Trino | flags, default off, never both at once, and never without a benchmark |
| Multi-AZ Airflow | one scheduler is enough to prove the DAG contract; HA is a cost, not a capability |

---

## 2. The production target

```text
                        PRODUCTION TARGET  (designed, never operated)

  Managed sources ──► Debezium on a Connect CLUSTER ──► MSK sized to throughput
                        (multi-worker, rebalancing)      tiered storage for replay depth
                                                                  │
                        ┌─────────────────────────────────────────┤
                        ▼                                         ▼
          RESIDENT streaming apps                     Scheduled batch (EMR Serverless)
          (pre-initialized capacity,                  EOD close, CURATED, dbt marts
           the only thing that holds it)
                        │                                         │
                        └──────────────┬──────────────────────────┘
                                       ▼
                        S3 + Glue + Iceberg  ·  per-domain CMKs with rotation
                                       │
                  ┌────────────────────┼────────────────────┐
                  ▼                    ▼                    ▼
              Athena             Redshift / Trino      Power BI
            (ad-hoc)            (behind a benchmark)   (import, or DirectQuery
                                                        after a latency test)

  Airflow: multi-AZ, managed RDS metadata with backups and a retention policy.
  Governance: DataHub deployed privately, ingestion on a schedule.
  Observability: CloudWatch + Prometheus/Grafana, consumer-lag paging.
```

---

## 3. Concern by concern

| Concern | Lab (ran) | Production target (designed) |
|---|---|---|
| **Lifetime** | Applied, exercised, destroyed in a metered window | Long-running; the streaming layer resident |
| **Availability** | Single-node Airflow, single region, no failover | Multi-AZ Airflow, HA metadata, documented RTO/RPO |
| **Compute** | EMR Serverless, auto-stop, 100 GB cap, job timeouts | Same, plus pre-initialized capacity for resident streams only |
| **Kafka** | MSK Provisioned, 3 AZ, KRaft, smallest viable brokers, 24 h retention | Same topology sized to throughput; tiered storage sets replay depth, and replay depth sets RPO |
| **Metadata store** | Postgres container on a PVC | Managed RDS, backed up, retention policy |
| **Networking** | Private subnets, **no NAT**, VPC endpoints, SSM Session Manager | Private subnets with controlled egress; SSM retained in place of SSH |
| **Secrets** | SSM SecureString / Secrets Manager at runtime | Unchanged — this does not scale differently |
| **Encryption** | KMS CMK for lake and MSK, TLS-only bucket policies | Plus rotation and per-domain CMK separation |
| **Scaling** | One broker type, one app, one AZ for compute | Horizontal Connect workers, sized MSK, autoscaled k8s node groups |
| **Serving** | Athena only, bytes-scanned cutoff | Athena ad-hoc; Redshift Serverless or Trino behind a decision gate |
| **Governance plane** | DataHub local, $0 | DataHub private, scheduled ingestion |
| **Observability** | CloudWatch + the OPS ledgers in Iceberg | Plus paging on consumer lag, freshness and DQ breach |
| **Cost** | **~$1.40/day up, $0 at rest** | A budget decision, not an architecture one |

---

## 4. What does not change

This is the part that matters, and the reason the lab is evidence of anything at all:

- **The CDC ordering contract.** Source-native SCN/LSN, offsets comparable only within a
  partition, deletes and tombstones handled explicitly.
- **FULL_CDC as the single canonical history**, with REALTIME and EOD derived from it as
  siblings rather than a chain.
- **The certification gates.** DQ and reconciliation run before a close is certified, and a
  lower tier may never overwrite a higher one.
- **Lineage evidence classes.** An edge that is `DERIVED` rather than `VALIDATED` may not
  narrow a recovery, at any scale.
- **The closed mutation surface** on the AI layer: 2 of 27 tools may write, enforced in the
  tool contract rather than by review.
- **No static credentials, no SSH, no public ingress, KMS throughout.**

A correctness property that only holds at one size was never a correctness property. Those
six hold in both columns, which is why the lab runs are worth quoting.

---

## 5. What the lab therefore cannot tell you

Stated plainly, because an interviewer will ask:

| Unknown | Why it is unknown |
|---|---|
| Throughput, events/second | Never measured. The lab holds one real business date of CDC history |
| End-to-end freshness | The 30 s trigger and 45 s poll are *configured cadences*, not measured latencies |
| Behaviour under sustained load | No soak test; the resident apps have never run a full working-day window |
| Failover time | No failover exists to time |
| Cost at scale | The $1.40/day figure is a lab figure and does not extrapolate |

See [`CAPABILITY_MATRIX.md`](CAPABILITY_MATRIX.md) for the same distinction applied
capability by capability, and [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md) for the list
this project refuses to hide.

Resource-level detail, including the three traps that bite on a rebuild:
[`PLATFORM_RESOURCE_INVENTORY.md`](PLATFORM_RESOURCE_INVENTORY.md). Costs and unit prices:
[`COST.md`](COST.md) · [`PRICE_REFERENCE.md`](PRICE_REFERENCE.md).
