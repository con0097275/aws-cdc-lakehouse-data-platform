# Service level objectives

- Session: 15
- Date: 2026-08-15
- Status: **SLIs defined and rules static-validated; NOT measured against a running pipeline**

---

## 1. The five SLOs

| SLO | Target | SLI | On breach |
|---|---|---|---|
| **NRT freshness** | ≤ **10 min**, 95% of 15-min windows | `sli:nrt_freshness_seconds:max` | **ticket** |
| **EOD completion** | certified by **06:00 UTC**, 99% of days | `sli:eod_completion_age_seconds` | ticket |
| **CDC completeness** | **100%** — zero reconciliation difference | `sli:reconciliation_difference:max` | **page** |
| **Data quality** | ≥ **95%** checks PASS | `sli:dq_pass_ratio` | ticket |
| **Pipeline availability** | ≥ **99%** of scrape targets up | `sli:pipeline_targets_up_ratio` | page |

### Why CDC completeness is 100% and not 99.9%

Every other target is a percentage because latency and availability are continuous. CDC
completeness is not: a reconciliation difference means an event was **lost or duplicated**
(`CLAUDE.md` §5.3). There is no acceptable rate of losing financial events, and "99.9%
complete" is not a service level, it is a defect budget for data loss.

So the target is exact, and it is the only data SLO that **pages**.

### Why NRT freshness only tickets

It is the headline number and it is *provisional*. NRT output is explicitly
`PROVISIONAL_NRT` (S09), certified figures come from EOD, and nothing downstream treats
intraday values as final. Paging a human at 03:00 for a provisional latency target is how a
pager gets ignored — and then the completeness page gets ignored too.

## 2. Page vs ticket

There are exactly two classes, and no third:

| Class | Meaning |
|---|---|
| **page** | data is being lost, or the pipeline is down. Wake someone. |
| **ticket** | recorded; someone looks in the morning. |

A "warning" nobody has decided how to react to is noise wearing a severity label. 5 alerts
page; 8 ticket. **Every one of the 13 carries a `runbook:` annotation** — an alert with no
runbook is a question mark delivered at 03:00.

## 3. The alert that makes the rest trustworthy

```yaml
alert: PipelineMetricsStale
expr:  sli:metrics_age_seconds > 3600
```

Pushed metrics are **sticky**. A job that pushes `freshness=3min` and then never runs again
leaves Prometheus reporting 3 minutes forever: the pipeline is dead and every dashboard is
green.

That is the observability form of the vacuous DQ pass (S14-3) — *absence of signal read as
health*. Every push therefore includes `cdc_lakehouse_pushed_timestamp_seconds`, and this
alert keys off the timestamp rather than any metric value. Without it, "healthy" and "gone"
are indistinguishable.

## 4. Instrumented surfaces

| Surface | How | Metrics |
|---|---|---|
| Oracle / SQL Server | exporter on the source lab, `tag:Component=source-lab` | redo/log retention, connection count |
| Debezium / Connect | JMX, `tag:Component=cdc-runtime` (S04) | connector state, task failures, **CDC lag** |
| Kafka | MSK JMX + node exporter (existing) | broker health, under-replicated partitions |
| **Consumer lag** | `kafka_exporter` on the toolbox | `kafka_consumergroup_lag` |
| Schema Registry | Apicurio `/q/metrics` | registry availability |
| Spark / Iceberg | **Pushgateway** | batch duration, checkpoint age, commit failures |
| Airflow | `/admin/metrics` on the k3s node | task failures, DAG duration |
| Athena | CloudWatch (native) | bytes scanned, query state |
| DQ jobs | **Pushgateway** | checks by verdict, freshness, reconciliation difference |

### Why consumer lag comes from a broker-side exporter

`kafka_consumergroup_lag` is log-end-offset minus committed-offset, computed **by the
broker**. JMX on the client reports only what the client believes about its own progress —
which is exactly wrong when the client is stuck, because a wedged consumer reports a
confident, unchanging position.

### Why batch metrics are pushed, not scraped

Prometheus scrapes long-lived processes. An EOD run lasts twenty minutes and then the
process is gone: a 30-second scrape either misses it or catches it mid-flight. Batch jobs
push.

**Not CloudWatch custom metrics** — billed per metric per month, and this dashboard carries
~20 series per dataset across 12 datasets. Prometheus already exists; the Pushgateway is one
more container on the toolbox's existing `kafka-observability` docker network. No new
instance, no new EBS.

## 5. What is not measured

`sli:pipeline_targets_up_ratio` deliberately measures **scrape targets**, not job success
rates. A pipeline that is not running produces no failures at all, and a "0 failures"
availability metric would read as 100% for a stack that is switched off.

## 6. Status

Every rule here is **static-validated only**. Nothing is deployed, so no SLI has been
measured, no alert has fired, and no burn rate exists. The thresholds are reasoned from the
architecture (24h Kafka retention, the EOD schedule, the 10-minute NRT target) and are
expected to need adjustment against real traffic — the first week of real data is when an
SLO stops being a guess.
