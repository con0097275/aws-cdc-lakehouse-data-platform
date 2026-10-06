# Airflow 3 on k3s — orchestration for the CDC Lakehouse

- Session: 12
- Date: 2026-08-14
- Status: **DAGs and Terraform static-validated and PLANNED; nothing deployed**
- Decisions: ADR-011 (k3s runtime), ADR-010 (KubernetesExecutor)

---

## 1. Version matrix — resolved, not assumed

Queried from upstream on **2026-08-14**:

| Component | Pinned | Source |
|---|---|---|
| Airflow Helm chart | **1.22.0** | `airflow.apache.org/index.yaml`, published 2026-06-01 |
| chart sha256 | `1f7d1dfe3d58e2c54899950aba907a43625a18c8b6fb3c54760c21592129a5b6` | same index |
| Airflow | **3.2.2** | the chart's own `appVersion` |
| k3s | **v1.36.3+k3s1** | `update.k3s.io` stable channel |
| PostgreSQL subchart | **13.2.24** | chart dependency |

**Airflow 3.2.2, not PyPI's newer 3.3.1.** The chart is tested against its own `appVersion`;
pushing a newer image into an older chart is an untested combination and `CLAUDE.md` §3.9
forbids that kind of drift. The bootstrap script **verifies the chart digest before
installing** and refuses on mismatch.

## 2. Why k3s and not EKS

Already decided in ADR-011; restated because it is the session's cost question.

The difference is **cost shape, not capability**. An EKS control plane bills continuously
whether or not a DAG ever runs. This node bills only while started, and stopping it is a
scripted one-liner.

Everything the session asks to demonstrate — KubernetesExecutor, per-task pod isolation,
pod resource limits, RBAC, remote logging, DAG sync — is identical on k3s.

**What k3s does not demonstrate**, recorded rather than glossed: multi-node scheduling,
control-plane HA, and node-failure rescheduling. A single node is a single point of failure.
For a portfolio lab that is the right trade; for production it would not be.

`enable_eks` stays `false` and is **not wired to this module** — wiring both would let a
plan create two Kubernetes control planes for one workload.

## 3. Executor

`KubernetesExecutor` (ADR-010). One pod per task, created on demand and reaped on
completion, so nothing idles between DAG runs.

**Not Celery.** CeleryExecutor needs a Redis or RabbitMQ broker running 24/7 plus always-on
workers — pure cost for a lab whose DAGs are mostly waiting on EMR. `redis.enabled: false`
and `flower.enabled: false` are set explicitly so a future edit to `executor` cannot quietly
bring a broker back, and a test asserts it.

> The name `KubernetesCeleryExecutor` does not exist. The real, older name is
> `CeleryKubernetesExecutor`, and it is not Airflow 3's default. `CLAUDE.md` §7 calls this
> out because it is a common invention.

## 4. The DAGs

Eight DAGs, all `is_paused_upon_creation: true` — **nothing starts spending on deploy**.

| DAG | Schedule | Purpose |
|---|---|---|
| `cdc_health_check` | `0 * * * *` | connector liveness, offsets ADVANCING, key distribution |
| `l1_stream_supervisor` | `*/15 * * * *` | ensure the streaming job is alive; restart if not |
| `flow_nrt_mart` | `*/5 * * * *` | NRT provisional mart |
| `flow_auto_correct` | `*/30 * * * *` | recompute the whole of day T |
| `eod_certified_pipeline` | `30 1 * * *` | the certified chain (below) |
| `recovery_rebuild_business_date` | **manual** | rebuild one date after late events or a fix |
| `recovery_replay_l1_to_l2` | **manual** | re-derive L2 from L1 |
| `recovery_quarantine_replay` | **manual** | reprocess DLQ records after a fix |

### 4.1 Health checks read data, not status

Sessions 03–05 recorded that risks R15, R2 and R3 share one property: **the failing
component reports HEALTHY**. A green Connect status means the process is alive, not that
events are flowing — a connector can sit `RUNNING` with a stuck LogMiner session
indefinitely. So the checks assert offsets are *advancing* and that same-PK-same-partition
still holds.

### 4.2 The stream is supervised, not scheduled

Airflow does not run the stream; it checks whether the long-lived EMR Serverless job is
alive and restarts it if not.

Scheduling "run the stream every 5 minutes" would start a **second consumer** of the same
topics. Two writers committing to the same Iceberg table with the same checkpoint is how a
stream corrupts itself — and the idempotency that makes the batch flows safe to overlap
does **not** extend to concurrent streaming writers sharing a checkpoint (`CLAUDE.md` §5.9).

### 4.3 The EOD chain — every arrow is a correctness constraint

```
L1→L2 → L2→L3 → Kimball dims → Kimball facts → EOD flow → full-fill → dbt
                                                                        ↓
                                                                data quality
                                                                        ↓
                                                             reconciliation  (publish gate)
                                                                        ↓
                                                            Iceberg maintenance
```

| Arrow | Why it is not a preference |
|---|---|
| L2 before L3 | L3 dedups from L2's full history; against a half-written L2 it yields a snapshot that is internally consistent and **missing a day** |
| **dims before facts** | A fact resolves surrogate keys by point-in-time join. Built first, every new member lands on `-1` — complete, plausible, and **wrong about who the rows belong to** (S10-4) |
| facts before dbt | The marts aggregate the facts; dbt against a half-written fact table reconciles against itself and not reality |
| DQ before reconciliation | DQ asks "is each table internally valid"; reconciliation asks "do the layers agree". A day can pass every table check and still have **lost events between L1 and L2** |
| reconciliation before maintenance | Maintenance rewrites and expires files — doing that before the day is verified **destroys the evidence** an investigation needs |
| maintenance **last** | `maintenance.sql`: rewriting files under a job that is still committing is how you lose data |

Splitting these into separately scheduled DAGs would replace every guarantee above with a
hope that the cron offsets are far enough apart. That is why this is **one** DAG.

**The reconciliation gate has `retries=0`** — deliberately. A breach is a *data* problem;
retrying the same comparison over the same data produces the same failure more slowly, and
a green retry would mean the data changed underneath, which is worse.

### 4.4 Recovery is manual-only

`schedule=None` on all three. A scheduled recovery either never fires when needed, or fires
when nothing is broken and rewrites good data. All three require an explicit
`business_date` — **a recovery that defaults to "today" turns one incident into two**.

Replay works without a destructive truncate because the L1→L2 MERGE is idempotent on
`event_id` (`CLAUDE.md` §5.3) — which is exactly why §5 forbids deleting L1 before L2 is
validated.

## 5. Retries, timeouts, dates and idempotency

```python
retries: 2, retry_exponential_backoff, max_retry_delay 30m
execution_timeout: 30m default, per-task overrides
catchup: False          max_active_runs: 1        dagrun_timeout: 2h
```

**`catchup=False` everywhere.** A DAG with a past `start_date` and catchup on queues every
missed interval the moment it is unpaused — and bills all of them. Backfill is a deliberate
CLI action (§8), never a side effect of deploying.

**Every task has an `execution_timeout`.** A hung task holds its pool slot forever, and on a
2-vCPU node that stops everything else.

### The business-date contract

Every job takes `--business-date` and `--run-id` and is idempotent for that pair. The DAGs
pass the **logical** date (`{{ ds }}`), never `today`:

- a task computing its own date produces a **different answer on retry** than on its first
  attempt;
- a backfill of an old date would silently process the current one.

`run_id` is **deterministic** (`{{ dag_id }}__{{ ds_nodash }}__{{ ti.try_number }}`), not a
UUID: a retry must reuse the id so the write path recognises the repeat. A fresh UUID per
attempt would defeat the idempotency the jobs implement.

Tests enforce all of this — `test_no_task_computes_its_own_date`,
`test_dated_tasks_pass_a_templated_date`, `test_run_ids_are_deterministic_not_random`.

### Pools

`max_active_tasks` bounds tasks *per DAG*, not across DAGs. Without a shared pool, NRT
(every 5 min), auto-correct (every 30) and a backfill can all run at once and starve the
scheduler.

| Pool | Used by |
|---|---|
| `spark_jobs` | everything submitting to EMR Serverless |
| `maintenance` | compaction/expiry — its own pool so it never contends with a live write |

## 6. Security

| Control | How |
|---|---|
| **UI private only** | ClusterIP, `ingress.enabled: false`, k3s installed with traefik and servicelb **disabled** — there is no ingress controller to expose anything through |
| No public IP | `associate_public_ip_address = false` |
| No SSH | no key pair; SSM Session Manager only (`CLAUDE.md` §3.4) |
| Only inbound rule | port 6443 from the **toolbox SG by id**, never a CIDR |
| No static credentials | node instance profile (role `airflow` from `modules/lake_iam`), default credential chain (`CLAUDE.md` §3.2) |
| No secrets in Git | metadata DB password and git-sync key from SSM SecureString at bootstrap |
| IMDSv2 | `http_tokens = "required"` |
| Chart integrity | sha256 verified before `helm upgrade`; refuses on mismatch |

There is **no ingress rule for 8080 at all** — not even scoped to the VPC. The UI is reached
by SSM port-forward, which needs no inbound rule because the agent dials out. An 8080 rule
"just for the VPC" is how a private UI quietly becomes reachable from every instance in the
account.

```bash
scripts/airflow-node.sh ui      # the ONLY path to the UI
```

## 7. Cost

`docs/PRICE_REFERENCE.md`, `ap-southeast-1`. Budget **$30/month** (ADR-030).

| Scenario | Cost |
|---|---|
| Demo window — 4 hrs/day × 20 days | **$11.33/mo** (37.8% of budget) |
| Light — 2 hrs/day × 10 days | $4.99/mo |
| **Stopped** all month | **$2.88/mo** — EBS only |
| **Destroyed** | $0.00/mo |
| Left running 24/7 | **$79.97/mo — 267% of budget** |

**Stop is cheaper, not free.** A stopped instance bills no compute, but its 30 GiB gp3
volume is billed per provisioned GiB-month regardless. Only *destroy* removes it. That
distinction is the most common lab-cost surprise, and it is why the script prints which
state you are in.

Task pods are deliberately small (100m CPU / 256Mi requests): the tasks are submitters and
pollers, not compute. The actual Spark work runs on EMR Serverless, which auto-stops.

## 8. Operations

```bash
scripts/airflow-node.sh status                 # state + what it costs
scripts/airflow-node.sh start   --execute
scripts/airflow-node.sh stop    --execute      # the cost lever
scripts/airflow-node.sh ui                     # SSM port-forward
scripts/airflow-node.sh backup  --execute      # pg_dump -> S3, BEFORE destroy
scripts/airflow-node.sh destroy --execute      # module-targeted; asks for a typed phrase
```

### Backfill

Deliberate and explicit, because `catchup=False`:

```bash
airflow dags backfill eod_certified_pipeline \
  --start-date 2026-08-01 --end-date 2026-08-07 --reset-dagruns
```

`max_active_runs=1` serialises it, so a week-long backfill cannot run seven days at once and
interleave commits on the same Iceberg partitions.

Or one date, with maintenance skipped:

```bash
airflow dags trigger eod_certified_pipeline \
  --conf '{"business_date": "2026-08-03", "skip_maintenance": true}'
```

### Backup / restore

The Postgres PVC survives a **stop** but not a **terminate**. `backup` exports before
destroy — otherwise DAG run history is lost. DAGs themselves are code in Git and need no
backup; only run history and connections live in the metadata DB.

### Upgrade

1. Resolve the new chart version and record its digest in `docs/VERSIONS.md`.
2. `backup --execute`.
3. Bump `airflow_chart_version` / `airflow_app_version`, `terraform apply`.
4. Re-run `pytest airflow/tests/` — the DAGs must still import against the new Airflow.

## 9. What is tested, and one honest gap

```
airflow/tests/test_dags.py     31 passed
full python suite             242 passed
terraform fmt/validate        Success
terraform plan (saved)        module.airflow_k3s: 5 create, 0 change, 0 destroy
```

Four policy guards were **mutation-checked** — reverted, and the test confirmed to fail:

| Reverted | Caught by |
|---|---|
| `catchup: True` | `test_catchup_is_disabled_everywhere` |
| task computes its own date | `test_dated_tasks_pass_a_templated_date` |
| maintenance before reconciliation | `test_reconciliation_precedes_maintenance`, `test_maintenance_is_last` |
| `executor: CeleryExecutor` | `test_helm_values_select_kubernetes_executor`, `test_no_celery_executor_configured` |

### The DAG tests run against Airflow 2.9.3, not 3.2.2

The locally installed Airflow is **2.9.3**; the deployment target is **3.2.2**. The DAGs
were written to the subset of the API valid in both (`schedule=`, standard operators,
`TaskGroup`, `Param`) and avoid constructs removed in Airflow 3.

But this means **`test_no_import_errors` validates the DAGs against the wrong version**. It
proves the structure and policy are sound; it does **not** prove they import under Airflow
3.2.2. Recorded as **OPEN-22** rather than described as a passing Airflow 3 import test.

Also `NOT_TESTED`: KubernetesExecutor actually launching a task pod (acceptance allows
"or static limitation recorded" — this is that record), remote S3 logging, git-sync, and the
bootstrap script end to end. All need the node deployed.

## 10. Rollback

```bash
scripts/airflow-node.sh backup  --execute
scripts/airflow-node.sh destroy --execute     # targets module.airflow_k3s only
# or simply:
terraform apply -var="enable_airflow=false"   # the flag removes all 5 resources
```

Nothing else in the stack is touched: the module creates exactly 5 resources and the plan
shows **0 destroy, 0 replace** elsewhere.
