# METADATA COST MODEL

- Phase: DRP10 · **Today's metadata-plane cost is $0.** Nothing is deployed.
- Related: `CLAUDE.md` §4, `DATAHUB_ARCHITECTURE.md` §8, `docs/COST.md`

---

## 1. Where the money would go

| Driver | Shape | Today |
|---|---|---|
| DataHub infrastructure | GMS + frontend + Elasticsearch + metadata store + Kafka, **always on** | **$0** — `mode: disabled` |
| metadata ingestion | short Glue/dbt/Kafka reads on a schedule | $0 — never run |
| OpenLineage overhead | listener CPU on jobs that already run; one jar download per EMR run | $0 — `enabled: false` |
| DQ compute | Spark, on jobs that already run | already in the data-plane budget |
| profiling | **would** read column values at scale | **off everywhere, by policy** |
| recovery compute | a bounded rerun of affected assets only | $0 — nothing has run |

## 2. The volume decision nobody should discover later

**Streaming jobs emit per micro-batch.** `full_cdc_ingestion` at a one-minute trigger is
**1,440 lineage events per day per table**; ten tables is 14,400. Batch jobs emit two events
per run.

That is recorded in ADR-090 and owned by whoever turns on `full_cdc_ingestion` or
`streaming_rt`. It is not a reason to avoid streaming lineage — it is a reason to decide the
sampling or aggregation before switching it on rather than after the first bill.

## 3. Why `local` and `disabled` rather than a deployment

`CLAUDE.md` §4.10 already keeps **Marquez** off as "a 24/7 service; `ops.lineage_event` needs
nothing running". DataHub is strictly heavier. Rejecting Marquez on cost and then accepting
something larger without saying so would be incoherent, so DataHub inherits a **stricter**
version of the same rule:

- default `disabled`
- `local` runs on the workstation, on demand, $0 in AWS, stopped by hand
- `production` is flag-gated, default false, and **blocked on DRP0 open question 3** — whether
  the k3s node can host it under `lab_low_cost` is unmeasured

Measured headroom on this workstation: **19.4 GB RAM, 113 GB free** against a requirement of
≥10 GB / ≥20 GB. `local` is feasible here, which is what makes the $0 path real.

## 4. Offline-first is a cost control

`FileTransport` writes valid metadata change proposals to disk, and every ingestion recipe
defaults to a **file sink**. DRP4–DRP7 were built and tested with nothing running.

Without that, every phase after DRP2 would have required an always-on service to make any
progress — which is how a "cheap lab" acquires its first permanent bill.

## 5. Cost levers, in the order to pull them

1. `metadata_plane.mode: disabled` — the whole plane, off, one line.
2. `governance/registry/openlineage.yaml` → `enabled: false` — no listener attaches.
3. Drop `OPENLINEAGE=1` from the submit — per-job opt-out.
4. Narrow `database_pattern` / `topic_patterns` in the recipes.
5. Ingest on a schedule rather than per run — the catalogue is not a real-time system.
6. `bash scripts/datahub-local.sh down --execute` — local, stopped, store kept.

## 6. What is NOT bounded yet

| | Why it matters |
|---|---|
| DQ / reconciliation / incident history retention | append-only by design; unbounded growth is slow but real (`DATA_RETENTION_POLICY.md` §6) |
| lineage metadata retention | run lineage is the high-volume half (`§2` above) |
| Elasticsearch index sizing in `production` | part of DRP0 open question 3 |
| Athena scan cost of querying `ops.*` from the runbook | the workgroup already has a bytes-scanned cutoff |

## 7. The number to quote

> **$0 today, and every phase from DRP2 to DRP10 was built and tested without spending
> anything.** The first cost is a deliberate `local` start on a workstation; the first AWS
> cost needs a flag flipped, a sizing measurement, and an approval.
