# Capability matrix — what is real, and how real

- Written: Session 18, 2026-08-15
- Partially corrected: **2026-10-06** — rows carrying 2026-09 evidence are current. Anything
  still describing an undeployed, never-run platform predates Sessions 40–52 and is stale;
  `PROJECT_STATE.md` is the authority on what exists in AWS today.

**Read this before any other document in the repository.**

Every capability below carries one of four labels. They are not grades of quality — they
are grades of *evidence*, and the difference matters when someone asks "does it work?"

| Label | Means | Evidence |
|---|---|---|
| **LIVE TESTED** | executed, and the result was observed | test output, command output in `artifacts/validation/` |
| **IMPLEMENTED** | code exists and passes tests, but the **deployed** behaviour is unverified | passing tests against local fixtures |
| **DESIGN ONLY** | written and validated, never executed | `terraform validate`/`plan`, documentation |
| **OPTIONAL** | feature-flagged, default **off** | flag defaults |

## The single most important fact

> **This platform is not production-proven, and the labels below say so consistently.**
> What it *has* done is run: the full platform was applied 2026-08-16 and destroyed the same
> day, and Sessions 40–52 rebuilt it to run real CDC through real EMR Serverless jobs on
> 2026-09-03, 2026-09-06 and 2026-09-23. Current AWS footprint is a question for
> `PROJECT_STATE.md`, not this file.

**Superseded, 2026-10-06.** This section used to read *"This platform has never been
deployed. No AWS resource has ever been created by it… total AWS spend to date $0.00."*
That was true when it was written at Session 18 and false by Session 40. It is corrected
here rather than deleted, because a capability matrix that quietly rewrites its own history
is worth nothing.

Every session still ends at an approval gate rather than self-approving spend — that part
was never the stale half.

---

## Data pipeline

| Capability | Status | Evidence |
|---|---|---|
| CDC event contract (SCN/LSN normalisation, `event_order`) | **LIVE TESTED** | `test_ordering.py` 17, `test_envelope.py` 15 |
| L1 STREAM envelope + quarantine | **LIVE TESTED** | `test_l1_local_spark.py` 7, `test_quarantine.py` 10 — real Spark |
| L1 → L2 window / watermark / late sweep | **LIVE TESTED** | `test_l2_window.py` 26, `test_l2_local_spark.py` 9 |
| L2 → L3 snapshot, dedup, deletes, rebuild determinism | **LIVE TESTED** | `test_l3_snapshot.py` 30 — real Iceberg tables |
| Four processing flows + anti-downgrade MERGE | **LIVE TESTED** | `test_flows.py` 17, `test_four_flows_spark.py` 16 |
| SCD2 + point-in-time joins + deterministic surrogate keys | **LIVE TESTED** | `test_scd2.py` 27 |
| Kimball facts, additivity registry | **LIVE TESTED** | `test_kimball_spark.py` 13 |
| dbt marts, tests, lineage | **LIVE TESTED** | `dbt build` **PASS=56 WARN=0 ERROR=0** on EMR Serverless, 2026-09-23 (`00g906di31908g27`) |
| CURATED layer (conformance + Kimball dims/facts) | **LIVE TESTED** | `curated_build.py` 2026-09-23 — 8 entities, 9 dimensions, 3 facts; SCD2, grain and referential integrity validated before write (ADR-080) |
| Kafka -> FULL_CDC ingest, exactly-once | **LIVE TESTED** | 5,828 events, 5,828 distinct `dv_event_id`, 2026-09-23 |
| Writer-schema selection per record by `globalId` | **IMPLEMENTED** | the mechanism is wired and unit-tested (`TestWriterSchemaIsChosenPerRecord`); an unknown id is quarantined, not decoded by topic. NOT yet exercised against a topic holding TWO versions -- the registry currently holds exactly one version per topic |
| EOD close: cutoff, DQ, reconciliation, watermark hold | **LIVE TESTED** | 10/10 tables closed 2026-09-23, every one `dq=PASS recon=PASS` |
| EOD **certification** (the success path) | **IMPLEMENTED** | refusal path live (`EOD_DAY_OPEN`, `WAITING_SOURCE`, `EOD_DECERTIFIED`); success path by `test_eod_engine_spark.py` only — see P2-9 |
| **Debezium connectors producing real CDC** | **DESIGN ONLY** | connector JSON written; never registered |
| **Kafka topics carrying real events** | **DESIGN ONLY** | no MSK cluster has ever existed |
| **Spark Structured Streaming against Kafka** | **LIVE TESTED** | `rt_stream_app.py` consumed `cdc.oracle.COREBANK.ACCOUNT` from MSK over IAM auth on 2026-09-03 (EMR job `00g8g80h6375mg27`) and again on a rebuilt platform 2026-09-06 — see *Realtime serving layer* below. This row read **DESIGN ONLY** until 2026-10-06; it was written before those runs and never revisited |
| **The pipeline end to end** | **DESIGN ONLY** | no stage has processed a real CDC event |

**The distinction that matters:** the *transformation logic* is genuinely tested — against
real Spark and real Iceberg tables, not mocks. What is untested is everything that requires
a running Kafka, a running connector or a deployed cluster.

## Realtime serving layer (STREAMING_RT)

`spark/realtime/` — four applications, two of them long-running processes rather than jobs.
Design and full evidence: [`REALTIME_STREAMING_RT.md`](REALTIME_STREAMING_RT.md); EMR job
ids: `artifacts/validation/final-e2e/realtime/README.md`.

| Capability | Status | Evidence |
|---|---|---|
| Four-app layer end to end (STREAM, AUTOCORRECT, EOD, DATAMART) | **LIVE TESTED** | 6 EMR Serverless jobs, all SUCCESS, 2026-09-03 15:44–16:20 UTC; **re-proven 2026-09-06** on a rebuilt platform (new MSK cluster, new CMK, fresh seed — nothing carried over) |
| Resident streaming process with its own trigger loop | **LIVE TESTED** | `RT_STREAM_SUMMARY {"batches":4,"facts":332,"full":331,"flagged":1,"resolver_cycles":43}` — 30 s trigger, 5 s resolver, stopped on its own `--run-seconds` budget |
| Late dimension: publish **flagged**, never drop | **LIVE TESTED** | account 990500 (2026-09-03) and 990900 (2026-09-06) published with `segment_code NULL, dim_complete false` while a pointer row recorded `missing_dims=customer` |
| Correction pass repairs from a *fresher* source | **LIVE TESTED** | `WORKLIST 1 → REPAIRED 1 → STILL_INCOMPLETE 0`, pointer closed with `resolved_by=AUTOCORRECT` — both windows |
| Watermarked BASE/STREAM merge view, incl. the RT-1 fix | **LIVE TESTED** | reading the datamart either side of the repair moved PRIORITY from `503,970,320.00 / 80 accounts` to `503,982,665.67 / 81` — exactly one account and exactly its balance (2026-09-06) |
| Reconciliation of the whole view | **LIVE TESTED** | `2,031,894,187.94 − 2,031,880,937.77 = 13,250.17`, decomposed to the cent against the session's ground truth |
| Source cache = one consistent snapshot per cycle | **LIVE TESTED** | 9 cycles / 180 s at 17.8 s → 3.1 s (2026-09-03); 12 cycles at 19.9 s → 3.5 s (2026-09-06) |
| The rules, at unit level | **LIVE TESTED** | `spark/tests/test_realtime_rt.py` — **34 passed**, no Spark/Kafka/AWS: event-time dedup beating arrival order, the watermark in both directions, a repaired BASE row winning, probe-column completeness, queue backpressure, resolver ageing |
| **The reference's 08:00→20:00 resident window** | **DESIGN ONLY** | every run here is bounded by `--run-seconds` (240 s / 180 s). A working-day window has never been run, and no freshness or throughput figure exists |
| **Multi-dimension enrichment** (reference: 6 joins, 12 report sections) | **IMPLEMENTED** | one dimension (CUSTOMER) and 3 sections are wired and proven. The mechanism is per-join; only the count was simplified |

## Orchestration

| Capability | Status | Evidence |
|---|---|---|
| 8 Airflow DAGs, structure and policy | **IMPLEMENTED** | `test_dags.py` 31 |
| Airflow scheduling a real EMR job | **LIVE TESTED** | the `*/10` `datamart_stream_batch` schedule submitted a job at 2026-09-23 14:12Z which SUCCEEDED |
| Retry/timeout/pool/catchup policy | **LIVE TESTED** | asserted + mutation-checked |
| Business-date & idempotency contract | **LIVE TESTED** | tests reject self-computed dates |
| **DAGs importing under Airflow 3.2.2** | **DESIGN ONLY** | tests ran against locally installed **2.9.3** (OPEN-22) |
| **KubernetesExecutor launching a task pod** | **DESIGN ONLY** | no cluster deployed |
| k3s + Helm deployment | **DESIGN ONLY** | `terraform validate` + saved plan, 5 resources |

## Governance, quality, lineage

| Capability | Status | Evidence |
|---|---|---|
| Dataset registry (12 datasets, owner/PII/retention/SLA) | **IMPLEMENTED** | `test_governance.py` 27 |
| DQ engine — 6 check types, 3 verdicts | **LIVE TESTED** | `test_dq_engine.py` 24, real Spark |
| PII tokenisation + BI access denial | **IMPLEMENTED** | policy rendered and asserted |
| **IAM deny producing `AccessDeniedException`** | **DESIGN ONLY** | policy correct as rendered; never exercised |
| Lineage graph, declared vs observed | **IMPLEMENTED** | config validated |
| **A single lineage event emitted** | **DESIGN ONLY** | listener never ran |

## Serving

| Capability | Status | Evidence |
|---|---|---|
| Athena workgroup (cutoff, encryption, lifecycle) | **DESIGN ONLY** | `terraform validate`; workgroup never created |
| Serving views, PII tokenised | **IMPLEMENTED** | SQL written, structure asserted |
| Partition-pruning queries + counter-cases | **IMPLEMENTED** | SQL written; **bytes scanned never measured** |
| **Power BI connection** | **DESIGN ONLY** | steps written, never walked |

## Observability, DR, FinOps

| Capability | Status | Evidence |
|---|---|---|
| 14 SLIs, 13 alerts, runbook mapping | **IMPLEMENTED** | `test_observability.py` 28 |
| **Any SLI measured / alert fired** | **DESIGN ONLY** | no Prometheus deployed (OPEN-24) |
| Recovery: L3 rebuild, late event, replay idempotency, L2 window | **LIVE TESTED** | real Spark/Iceberg |
| 6 of 10 failure drills | **DESIGN ONLY** | need infrastructure; recorded `NOT_RUN` |
| **FinOps account audit** | **LIVE TESTED** | real AWS API calls, Session 17 |
| Cost/stop/verify scripts | **LIVE TESTED** | run against the real account |
| RPO/RTO figures | **DESIGN ONLY** | estimates from architecture, never measured |

## Optional

| Capability | Status |
|---|---|
| AI assistant tier 1 (retrieval + lookup) | **LIVE TESTED** — 14/14 golden, $0.00 |
| AI assistant tier 2 (Bedrock) | **OPTIONAL** + **DESIGN ONLY** — never invoked |
| Redshift Serverless / Trino | **OPTIONAL** — default off, 0 resources |
| Marquez / Lake Formation / Glue DQ | **OPTIONAL** — default off |

---

## Honest summary in three sentences

1. **The data-correctness logic is genuinely tested** — **3,529** Python tests (collected
   2026-10-06) and 56 dbt tests, most against real Spark and real Iceberg tables rather than
   mocks, with every guard mutation-checked.
2. **The infrastructure is designed, validated and has been deployed in metered windows** —
   applied and destroyed 2026-08-16, rebuilt for the live sessions of September 2026. It has
   never been left standing, which is why nothing here is production-proven.
3. **Real CDC events have been processed** — 5,828 through FULL_CDC, and 332 facts through
   the resident streaming layer — but **no throughput, latency or scale claim is made
   anywhere in this repository**, because none was measured. The runs prove the paths work,
   not how fast they are.

## What this is NOT

- Not production-proven. Deployed in metered windows and torn down, never operated.
- Not benchmarked — no TB/day, no events/second, no concurrent-user figure exists. The only
  durations recorded anywhere are wall-clock times of single runs, labelled as such.
- Not enterprise scale. It is a **lab** on a $30/month budget, deliberately sized so a
  single person can run it in a metered window.
- Not HA. Single-node Airflow, single-region, no automated failover — each stated with its
  cost in `docs/DR.md` §5.
