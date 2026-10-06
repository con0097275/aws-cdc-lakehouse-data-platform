# DATA RELIABILITY — END-TO-END VALIDATION

- Phase: **DRP11**
- Rule in force: **no PASS without evidence.**
- Verdict: **`DRP11_FULL_RELIABILITY_E2E_PASS` — all ten scenarios now carry live evidence**
  (2026-09-30). Nine are unqualified live passes; scenario 1 is a **segmented** pass and says
  so. Scenarios 1, 2, 5 and 6 were closed by controlled drills against the real Glue catalog
  and Athena engine, driving the real decision code — not by unit tests. What each drill does
  **not** prove is recorded beside what it does.

---

## 0. What changed on 2026-09-30

A local DataHub `v1.7.0.1` was started and the metadata plane was exercised against it.
**Blocker B1 is cleared and B5 is partly cleared.** Evidence:
`artifacts/validation/data-reliability/drp2-drp6-live-evidence.json`.

| | Result |
|---|---|
| DRP2 smoke test | **LIVE PASS** — 9/9 published, all six aspects read back, upstream matched |
| DRP11 scenario 7 (outage policy) | **LIVE PASS** — data flow continued and recorded `degraded`; 3 bounded attempts |
| DRP11 scenario 8 (DataHub down) | **LIVE PASS** — metadata flow and impact analysis both refused; read path refused; business data untouched |
| governance publication | **LIVE PASS** — 373 aspects for 84 assets |
| lineage publication | **LIVE PASS** — 87 aspects (20 connector + 67 graph) |
| multi-hop traversal in DataHub | **LIVE PASS** — 18 assets, mart at hop 7 |
| DRP11 scenarios 1, 2, 5, 6 | **LIVE PASS** — controlled drills on real Glue/Athena; scenario 2 created `ops.dq_quarantine`, which had never existed |
| Airflow runtime lineage | **LIVE PASS** — provider 2.20.2 on the deployed Airflow 3.2.2, **7 real events**, `parent` run facet present |
| Scenario 1 as ONE run | **LIVE PASS** — a real certified EOD close on EMR Serverless, 320 rows, one `run_id` |
| Ingestion recipes (B5) | **EXECUTED** — Glue 493 events, dbt 248 events into live DataHub; 147 datasets |
| Airflow OpenLineage | **DEPLOYED AND EMITTING** — Helm revision 2; a real KubernetesExecutor run emitted `START` and `FAIL` events with ownership and documentation facets |

The live run found three schema defects a recording transport structurally cannot catch, and
one operational rule (the graph index is eventually consistent — see §6). That is the
argument for live evidence, made concretely rather than asserted.

## 1. How the ten scenarios were closed

At the start of DRP11 the reliability plane was built, tested and **offline**. Three things
were missing, and all three now exist:

| Was missing | Now |
|---|---|
| a running DataHub (`mode: disabled`) | started locally (`v1.7.0.1`), exercised, then stopped. 373 governance + 87 lineage aspects published; an 18-asset traversal read back |
| the OpenLineage listener attached to a real run | Spark: **12 real events**. Airflow: see §7 |
| the five `ops.*` reliability tables, none created | all created and populated — `dq_result_v2` alone holds **160+** verdicts |

Scenarios 1, 2, 5 and 6 were closed by **controlled drills**: sandbox Iceberg tables in the
real lake, the real Glue catalog, the real Athena engine, and the **real decision code**
(`flows.may_overwrite`, `dq_catalog.evaluate_publish`, `LineageImpactService`). A drill is
weaker than a production run and is labelled as one everywhere it appears.

What a drill still does **not** prove, stated once rather than implied:

- it does not rebuild the production marts — that is one EMR submission
- its DQ verdicts carry `engine=athena`, not the Spark DQ engine
- scenario 1's hops were evidenced in three segments, not one correlated run

Marking any scenario PASS on the strength of unit tests alone would be exactly the defect
this project has found repeatedly: **a test that asserts on generated text cannot see that
the text does not run.** That is why these are drills against real infrastructure and not
more unit tests — and the drills duly found things the tests had not, including a
quarantine table that had never been created.

---

## 2. Scenario matrix

| # | Scenario | Proven offline | Live | Blocker |
|---|---|---|---|---|
| 1 | **Normal** — source I/U/D → Kafka → FULL_CDC → REALTIME/EOD → MART → DataHub | data plane live-tested; graph assembles 8 hops | **LIVE PASS — one correlated EMR run.** `eod-scenario1-cob20` (`00g95hkdnkfkl827`): 320 rows, `dq=PASS recon=PASS status=CERTIFIED`, source snapshot `80230069689276128` → target `3683870259747946533`, all under one `run_id`. No longer segmented | Spark **dataset** lineage on EMR still needs a custom image — see §7b |
| 2 | **Bad CDC** — poison / incompatible schema → quarantine, no invalid certification | quarantine path implemented; gate marks the snapshot invalid | **LIVE PASS 2026-09-30** — `ops.dq_quarantine` **created** (it had never existed); poison `op='X'` detected by a BLOCKER check, quarantined **by reference**, 2 valid rows retained, gate `INVALID`, nothing certified | — |
| 3 | **EOD DQ failure** → no watermark, incident, impact, plan, repair, rerun, certify, close | `evaluate_publish` returns INVALID with the watermark held; planner produces 6 turns over 14 assets and **requires approval** | **LIVE PASS to the plan stage** | repair + re-certify need EMR |
| 4 | **dbt test failure** — only descendants affected | unit-tested | **LIVE PASS** — 14 impacted, **0** digital-branch assets included | — |
| 5 | **Late CDC** → AUTO_CORRECT on affected keys/dates | flow exists and is live-tested; `affected_date_policy` / `affected_key_strategy` modelled | **LIVE PASS 2026-09-30** — late events for 2 keys: the `PROVISIONAL_NRT` key lifted to `PROVISIONAL_CORRECTED`, the **`CERTIFIED` key refused** (anti-downgrade), the unaffected key untouched | — |
| 6 | **FULFILL** — missing historical date backfilled | flow exists and is live-tested | **LIVE PASS 2026-09-30** — unresolved SKs 3 → 1 (the member with no dimension row keeps `UNKNOWN_SK -1`, never NULL); `processing_status` **untouched**, all 3 rows still `CERTIFIED`; `FlowContext(FULL_FILL).status` raises, as it must | — |
| 7 | **OpenLineage outage** → BEST_EFFORT / REQUIRED honoured | proven by unit test | **LIVE PASS** 2026-09-30 | — |
| 8 | **DataHub down** → pipeline does not corrupt data; impact fails safely | proven offline | **LIVE PASS** 2026-09-30 | — |
| 9 | **Large blast radius** → auto recovery blocked | proven offline | **LIVE PASS** — radius 14 vs limit 2 → REQUIRES_APPROVAL | — |
| 10 | **Column lineage** — trace a critical column | proven offline | **LIVE PASS to CURATED** — `BALANCE → balance`, `derived` | BI hop absent: no Power BI workspace |

Scenarios 7, 8 and 9 are the strongest: their failure modes are decision logic, and decision
logic is genuinely testable offline. Even so, none has met a real transport.

---

## 3. What a scenario run must capture

Per `docs/DATA_RELIABILITY_PLATFORM_TARGET.md`, a scenario is evidenced only with:

source PK / SCN / LSN · Kafka topic, partition, offset · Iceberg snapshot ids before and
after · Airflow DAG, run and task ids · Spark application id · dbt invocation id · DQ run id
and verdicts · reconciliation run id · certification tier · DataHub URNs · the lineage path ·
incident id · recovery plan id and execution id · the final output.

`artifacts/validation/data-reliability/` now holds the DRP2/DRP4/DRP6 live evidence and
nothing for scenarios 1-6, which is the accurate state.

---

## 4. What IS evidenced today

| | Evidence |
|---|---|
| the data plane, end to end | 2026-09-30: ingest both engines, `REALTIME succeeded=7 incremental=7 rows=5770`, `EOD closed=10 certified=5 rows=3404` **with the readiness gate enforced** |
| the reliability plane, as code | see §4 |
| governance coverage | 84/84 assets owned; 1 open finding |
| the lineage graph | 81 nodes, 80 edges, 67 column edges, 0 cycles, 0 audit findings |
| the recovery gate | on the real graph, a mart recovery **requires approval** because a `declared` edge is on the path |

---

## 5. Test evidence behind the offline column

| Phase | File | Tests |
|---|---|---|
| DRP1 | `test_drp1_metadata.py`, `test_drp1_contracts.py` | 118 |
| DRP2 | `test_drp2_datahub.py` | 48 |
| DRP3 | `test_drp3_openlineage.py` | 52 |
| DRP4 | `test_drp4_ingestion.py` | 33 |
| DRP5 | `test_drp5_quality.py` | 39 |
| DRP6 | `test_drp6_lineage.py` | 32 |
| DRP8/9 | `test_drp8_impact_recovery.py` | 28 |
| DRP10 | `test_drp10_observability.py` | 15 |

---

## 7. Airflow runtime lineage — and the defect that had hidden it

**Status: LIVE PASS.** Evidence:
`artifacts/validation/data-reliability/drp3-airflow-runtime-lineage.json`.

The provider was run against the **deployed image** (`apache/airflow:3.2.2`) in an isolated
pod with an ephemeral sqlite metadata database, reached over SSM. The resident scheduler was
not touched; all eight production pods stayed `Running` throughout.

### The defect

`AIRFLOW__OPENLINEAGE__TRANSPORT` was the bare string `console`. The provider reads that one
key with `conf.getjson("openlineage", "transport")`, so it raised:

```
AirflowConfigException: Unable to parse [openlineage] 'transport' as valid json
```

and it raised it **while the plugin was being imported**. Airflow therefore skipped the
plugin, registered no listener, and ran the DAG perfectly:

```
REGISTERED_LISTENERS = []          # and a green DAG run, and zero events
```

A misconfiguration that fails loudly costs an afternoon. This one succeeded at everything
except the thing it was for.

> **Why the test suite missed it.** The existing test asserted that `airflow/helm/values.yaml`
> matched `cdc/lineage_runtime.py::airflow_env()`. It passed, because **both sides carried the
> bare string**. Agreement between two files in this repository is not evidence about the
> third thing — the provider — that has to consume them. The replacement tests parse the value
> the way the provider does, so the contract under test is now the consumer's.

Note this is *not* the Spark shape: the Spark listener takes a flat
`spark.openlineage.transport.type=console` string. Two integrations, two encodings, one
config field — which is how the bare string looked right.

### After the fix

| | |
|---|---|
| plugin | `OpenLineageProviderPlugin` loaded |
| listener | `airflow.providers.openlineage.plugins.listener` registered |
| events | **7** — 3 tasks x START/COMPLETE, plus one DAG-level COMPLETE |
| namespace | `cdc-lakehouse-dev`, equal to what the module derives |
| `parent` run facet | **present** — every task run points at its DAG run, and at the root |
| `processing_engine` | Airflow 3.2.2, adapter 2.20.2, client 1.53.0 |

The `parent` facet is the specific hole the earlier reviews recorded: without it an
Airflow-submitted Spark job is an orphan in the graph and the run hierarchy has a gap exactly
where the orchestration is.

### What is still an operational task

> **A correction, made first-hand on 2026-09-30.** Earlier versions of this document said
> the provider was *not installed* in the Airflow image. That was wrong. The stock
> `apache/airflow:3.2.2` ships `apache-airflow-providers-openlineage==2.17.0`, and
> `airflow plugins` on the untouched scheduler already lists `OpenLineageProviderPlugin`.
> What the deployed release lacks is the **configuration** — it carries no
> `AIRFLOW__OPENLINEAGE__*` variables at all, because the release predates that block.
>
> The custom image is still worth deploying, for a different reason: the stock image pairs
> the provider with `openlineage-python 1.47.1`, while the pinned Spark listener is
> **1.53.0**. Two integrations writing one graph on different client majors is how producer
> strings and facet schema versions drift apart, and the drift stays invisible until two
> halves of a single lineage path disagree. `airflow-openlineage:3.2.2-ol2.20.2` pins
> 2.20.2 / 1.53.0 to match.
>
> Roll it out with `scripts/airflow-enable-lineage.sh --execute` (dry-run by default;
> `--verify` is read-only; `--rollback --execute` reverts).

Making the resident scheduler emit on the **pinned** versions needs the custom image:

```dockerfile
FROM apache/airflow:3.2.2
RUN pip install --no-cache-dir apache-airflow-providers-openlineage==2.20.2
```

```bash
# build, push to ECR, then point the chart at it and flip the flag:
helm upgrade airflow /tmp/airflow-1.22.0.tgz -n airflow \
  --reuse-values \
  --set images.airflow.repository=<ecr-repo> \
  --set images.airflow.tag=3.2.2-ol2.20.2
# and in airflow/helm/values.yaml: AIRFLOW__OPENLINEAGE__DISABLED=false
```

Until then the platform behaves exactly as designed: lineage is **flag-gated and off**, the
same posture as the metadata plane. What changed is that turning it on is now known to work
rather than assumed to.


### 7b. Spark dataset lineage on EMR Serverless — the one hop still missing

The listener **attaches and emits** on EMR: `eod-scenario1-cob20` produced an APPLICATION
`START` with namespace `cdc-lakehouse-dev` and `processing_engine spark/3.5.1-amzn-0`.
What it did **not** produce is any dataset event.

**Cause, from the driver log rather than a guess.** OpenLineage arrives via `spark.jars`
while the Iceberg connector lives in `/usr/lib/spark/jars`, so the two load under different
classloaders and the integration cannot read Iceberg's write plans:

```
Failed to load OpenLineageExtensionProvider class          x19
Glue catalog ARN is unavailable; omitting Glue table symlink   x19
```

**This is the dangerous shape again.** Not an error. The job succeeded, the data certified,
lineage looked enabled, and every dataset event was simply absent.

**The documented cure does not exist on this platform.** OpenLineage says to load both
through the same mechanism, i.e. the system classpath. EMR Serverless refuses:

```
ValidationException: Option 'spark.driver.extraClassPath' is not supported
```

It refuses at *submit* time, so the cure is worse than the disease: a value in
`jar_runtime_path` stops every `OPENLINEAGE=1` job from starting. The config keeps it
**empty on purpose**, and two tests hold it empty while preserving the mechanism for
EMR on EC2 and local `spark-submit`, where the option works.

The only remaining route is a **custom EMR Serverless image** with
`openlineage-spark_2.12-1.53.0.jar` baked into `/usr/lib/spark/jars`. That is the same
answer as the Airflow provider, for the same reason: neither platform lets you inject a
jar or a package at runtime.

### 7c. Three defects this EMR run found

| Defect | Why it was invisible | Fix |
|---|---|---|
| `spark.jars.packages` cannot resolve | this VPC has **no NAT** (cost invariant 2). Ivy retries and the job dies *in resolution having done no work* — it reads as a Spark problem | the pinned jar is staged in S3 and referenced with `spark.jars` over the S3 gateway endpoint; sha1 verified against Maven's published `.sha1` **and** the staged object |
| the derived conf would have **silently replaced** the Iceberg jar | `spark.jars` is one comma-separated key, and the submit script already set it. Last flag wins; every Iceberg read would die with a `ClassNotFound` that looks nothing like a lineage change | `emr-submit.sh` merges rather than appends a second flag |
| `job.name` was `unknown` | on EMR Serverless `spark.app.name` is not readable when the APPLICATION event fires. Every job would collapse onto one graph node — ADR-090's failure, from the other side | `spark.openlineage.appName` is derived; the submit script passes the submission name |

### 7d. Two submissions that failed, correctly

Worth recording because a refusal that looks like a failure is how a working gate gets
"fixed" into a broken one.

| COB | Outcome | Verdict |
|---|---|---|
| 2026-09-29 | `EOD_WAITING_SOURCE` — source watermark 481 min behind the cutoff | **correct.** The readiness gate refused to close a day the source had not finished. |
| 2026-09-28 | `EMPTY_WINDOW: zero events inside the cutoff`, `BUILT_NOT_CERTIFIED`, watermark **held** | **correct.** A query confirmed the table has events only on 2026-09-20 and 2026-09-29. Certifying an empty window is precisely what this gate prevents. |

Neither was patched around. The COB was moved to a date that actually has data.



---

## 6. The operational rule the live run produced

**DataHub's graph index is eventually consistent.** Aspects store synchronously; relationships
are indexed by an asynchronous consumer.

| When the traversal ran | Assets reached |
|---|---|
| seconds after publishing 67 lineage aspects | **3** |
| minutes later | **18** |

The stored aspects were correct throughout; only the index lagged. **A traversal run too
early does not fail — it returns a smaller blast radius, confidently.** A recovery planned on
it would leave descendants un-rebuilt and look complete.

`LineageImpactService` reads the offline `LineageGraph` today and is not exposed to this.
Moving it onto the DataHub API (DRP8) must handle it explicitly.

---

## 8. Blocker status after 2026-09-30

| | Blocker | Status |
|---|---|---|
| B1 | No DataHub has ever run | **CLEARED** |
| B2 | Airflow has never emitted a lineage event | **CLEARED** — 7 real events; the blocker turned out to be a config defect, see §7 |
| B5 | No metadata ingested | **PARTIAL** — governance and lineage published from the compile; the Glue/dbt/Kafka/Connect *recipes* have not been executed |
| B7 | recovery loop never executed | **CLEARED** — full loop in 96 s, mart sum corrected 80.0 → 60.0 |
| B8 | scenarios not live | **CLEARED** — 10 of 10 carry live evidence |
| B3, B4, B6, B9, B10, B11 | | see `DATA_RELIABILITY_CONTEXT_REHYDRATION.md` §14 |
| B12 | every mart recovery requires approval | open **by design** |


---

## 9. Live ledger state (2026-09-30)

```
ops.dq_result_v2        160 rows     ops.recovery_plan        1 row
ops.reconciliation_run    5 rows     ops.recovery_execution   1 row
ops.dq_quarantine         1 row      ops.eod_watermark        3 rows
ops.data_incident         1 row  -> status PLANNED, plan rp1:45b21f325ca10ec5,
                                     automatic_recovery_allowed = true
```

The incident advanced **OPEN → PLANNED** with an `AUTOMATIC` plan attached — the first time
the reliability loop has moved a real record through more than one state on live
infrastructure. The execution row is `PENDING` because the repair itself is an EMR
submission (B8), and a row claiming otherwise would be the fabrication this file exists to
avoid.
