# ADR-010 — Airflow 3 executor: `KubernetesExecutor`

- Status: **ACCEPTED** (Session 01)
- Related: ADR-011
- Addresses: `sessions/01_cost_and_target_architecture.md` scope item 3

## Context

Airflow 3 orchestrates Spark submissions, dbt runs and Iceberg maintenance. The
executor choice determines task isolation, infrastructure footprint and cost.

`CLAUDE.md` §7 exists because there is a persistent naming confusion here that
produces broken configuration. Setting it straight:

| Name | Reality |
|---|---|
| `KubernetesExecutor` | **Real.** One pod per task. The Airflow 3 default for Kubernetes deployments. |
| `CeleryExecutor` | Real. Needs a broker (Redis/RabbitMQ) and long-lived workers. |
| `CeleryKubernetesExecutor` | Real but **legacy**, and superseded in Airflow 3 by multiple-executor configuration. Not a default. |
| `KubernetesCeleryExecutor` | **Does not exist.** The component-name order is reversed. Configuring it fails at startup. |
| `LocalExecutor` | Real. In-process, no isolation, single host. |

## Options

| Option | Extra infrastructure | Task isolation | Verdict |
|---|---|---|---|
| **`KubernetesExecutor`** | none beyond k3s | per-task pod | **CHOSEN** |
| `CeleryExecutor` | Redis + persistent workers | per-worker | Rejected — a broker and warm workers to pay for |
| `LocalExecutor` | none | none | Rejected — one bad task takes the scheduler down |
| `CeleryKubernetesExecutor` | Redis + workers + k8s | mixed | Rejected — legacy, and no need for two models |

## Decision

`KubernetesExecutor` on Airflow 3, deployed by the official Helm chart onto
single-node k3s (ADR-011).

The reasoning is that this project's tasks are almost entirely **short submitters and
pollers** — `StartJobRun` against EMR Serverless, then wait. They are I/O-bound and
tiny. Celery's advantage is a warm worker pool that avoids pod-start latency, which
matters for high-frequency short tasks; here the actual work happens in EMR
Serverless, so pod-start latency is noise against a multi-minute Spark job. Paying
for Redis and standing workers buys nothing.

## Consequences

- Pod startup adds seconds per task — irrelevant given the above.
- Metadata DB is a Postgres container on a PVC for the lab. `CLAUDE.md` §7 requires
  backup/retention: a `pg_dump` to S3 before window teardown. RDS is the
  production-like option, deliberately not taken on cost grounds.
- DAGs are **orchestration only** (`CLAUDE.md` §7). No transformation in a
  `PythonOperator`. Every Spark job is an external submission with a timeout,
  retries, a run ID and explicit cutoff/partition parameters.
- `catchup = False` by default, `max_active_runs = 1` per DAG, and an explicit pool
  for Iceberg maintenance so cleanup never runs concurrently with a writer
  (risk R14).

## Cost

$0 incremental over the k3s node ($0.1056/hr, ADR-011). Celery would add Redis plus
standing workers — either another instance or a larger one.

## Security

IRSA is unavailable on k3s, so the node instance profile carries the `airflow` role
and pods inherit it via IMDS. This is coarser than IRSA and is recorded as an
accepted lab limitation; `enable_eks` would allow proper per-service-account roles.
IMDSv2 required. Airflow web UI is private-only, reached via SSM port forwarding.

## Rollback

The executor is a Helm value. Switching is a redeploy, not a rewrite, as long as
DAGs stay orchestration-only — which is itself a reason to keep them that way.

## Validation

- `airflow config get-value core executor` returns `KubernetesExecutor`.
- A task creates a pod that is cleaned up on completion.
- A failing task retries per its policy and does not take down the scheduler.
- No DAG contains transformation logic (reviewed, and greppable for
  `PythonOperator` with a non-trivial body).
