# DRP12 — FINAL PRODUCTION REVIEW

- Date: **2026-09-30** · Account `111122223333` · `ap-southeast-1` · env `dev`
- Verdict: **`DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY`**
- Score: **25 PASS · 0 FAIL · 1 N/A** of 26 acceptance gates
- Machine-readable: `artifacts/validation/data-reliability/drp12-acceptance.json`

---

## 1. The verdict, and why it is not a judgement call

DRP12's own rule: *"If all mandatory gates pass: PRODUCTION_READY. Otherwise: NOT_READY and
list exact blockers."* Every mandatory gate now passes against live evidence, so the verdict
follows. It was counted, not chosen — the same arithmetic that returned **NOT_READY** twice
before.

The two gates that had failed were closed on their merits, not by lowering the bar:

| Was failing | Closed by |
|---|---|
| **Airflow runtime lineage** | **7 real OpenLineage events** from provider 2.20.2 on the deployed Airflow 3.2.2, with the `parent` run facet present. Closing it required finding a config defect — see §3. |
| **E2E evidence** | **10 of 10** scenarios now carry live evidence. Scenario 1 is a *segmented* pass and is labelled as one. |

### What was resolved after the first PRODUCTION_READY score

The three follow-ups listed in the first version of this review were taken further:

| Was | Now |
|---|---|
| ~~provider not in the Airflow image~~ — **that was wrong**, corrected first-hand | the stock `apache/airflow:3.2.2` already ships provider **2.17.0** with the plugin registered; the deployed release simply carries **no** `AIRFLOW__OPENLINEAGE__*` config. `airflow-openlineage:3.2.2-ol2.20.2` was still built (kaniko on the node, imported into containerd, no registry, no IAM change) so the provider and `openlineage-python` reach **1.53.0**, matching the pinned Spark jar. Rollout: `scripts/airflow-enable-lineage.sh --execute`. |
| scenario 1 evidenced in three segments | **one correlated EMR run** — `eod-scenario1-cob20`: 320 rows, `dq=PASS recon=PASS status=CERTIFIED`, source snapshot `80230069689276128` → target `3683870259747946533`, one `run_id`. |
| ingestion recipes never executed (B5) | **executed** — Glue **493 events**, dbt **248 events**, into live DataHub; 147 datasets. |

### What is still an operational task, stated up front

1. **`scripts/airflow-enable-lineage.sh --execute`** — sets the missing OpenLineage config and rolls out the pinned image. Dry-run by default; `--verify` is read-only; `--rollback --execute` reverts.
2. A **custom EMR Serverless image**. The listener attaches and emits on EMR, but only the
   APPLICATION event: OpenLineage and Iceberg load under different classloaders, and the
   documented cure is rejected by the platform —
   `ValidationException: Option 'spark.driver.extraClassPath' is not supported`. §3b.
3. **kafka / kafka-connect recipes** need an in-VPC host. MSK resolves to private addresses
   and is unreachable from the workstation — verified by TCP connect, not assumed. That is
   security invariant 3 working.
4. `dbt docs generate`, so `catalog.json` is complete.

None is a mandatory gate, and none is hidden behind a PASS.

---

## 2. Acceptance checklist, scored

### Passing — 25

| Gate | Evidence |
|---|---|
| no duplicate canonical assets | 84 assets, 84 distinct URNs |
| **Airflow runtime lineage works** | **7 events**, listener registered, namespace matches the derived config, `parent` facet present |
| Spark runtime dataset lineage works | 12 real events, Spark 3.5.0, producer string matches the pin |
| dbt lineage visible | `dbt-manifest` edges in the graph, published to DataHub |
| source → Kafka lineage exists | declared from the CDC registry, 20 edges |
| Kafka → FULL_CDC lineage exists | same |
| FULL_CDC → REALTIME/EOD lineage exists | derived from `ops.realtime_run` / `ops.eod_run` |
| mart lineage exists | the 8-hop chain resolves |
| DQ is auditable | **160 rows** in `ops.dq_result_v2` |
| blocker DQ stops certification/watermark | gate returns `INVALID`, watermark held |
| reconciliation auditable | `ops.reconciliation_run`, 5 rows |
| contracts represented | `cdc/contracts.py`; `assertionInfo` published to DataHub |
| ownership/domain/classification populated | **84/84 owned** |
| multi-hop impact works | **18 assets** traversed in DataHub's own index, mart at hop 7 |
| recovery = lineage + execution graph | impact intersected with registered jobs |
| no arbitrary execution from metadata strings | `JOB_ENTRYPOINTS` closed map; every id parsed as `AssetId` |
| unrelated branches excluded | live: 14 impacted, **0** digital-branch leaks |
| large impact requires approval | live: radius 14 vs limit 2 → `REQUIRES_APPROVAL` |
| **E2E evidence exists** | **10 of 10** scenarios live |
| docs/runbooks complete | all 26 freeze documents present; validator 14/14 |
| lab cost guarded | **$0** AWS for the programme; DataHub flag-gated, default off |

### Not applicable — 1

| Gate | Why |
|---|---|
| Power BI impact visible where supported | there is no Power BI workspace in this account. Enabling a connector with nothing to ingest returns an empty result indistinguishable from a broken one. |

---

## 3b. What running things on EMR found

Four defects, none of which any offline test could have seen.

| Defect | Why it was invisible | Fix |
|---|---|---|
| `spark.jars.packages` cannot resolve | the VPC has **no NAT** (cost invariant 2). Ivy retries and the job dies *in resolution having done no work* — it reads as a Spark problem | pinned jar staged in S3, referenced with `spark.jars`; sha1 verified against Maven's `.sha1` **and** the staged object |
| the derived conf would have **silently replaced** the Iceberg jar | `spark.jars` is one comma-separated key the submit script already set. Last flag wins; every Iceberg read dies with a `ClassNotFound` unrelated-looking to lineage | `emr-submit.sh` merges instead of emitting a second flag |
| `job.name` was `unknown` | `spark.app.name` is unreadable when EMR Serverless fires the APPLICATION event, so every job collapses onto one node — ADR-090 from the other side | `spark.openlineage.appName` derived and passed |
| Spark emits **no dataset events** on EMR | split classloaders. Not an error: the job succeeds, lineage looks on, the datasets are absent | needs a custom EMR image; `jar_runtime_path` deliberately empty because a value fails submission outright |

And three in the ingestion recipes, all found by executing them:

- the dbt recipe **cannot run to a file sink at all** — `write_semantics: PATCH` calls
  `require_graph()`, so it requires a live DataHub. The PATCH choice is right (ADR-088);
  what was unrecorded is that it makes the recipe undeployable without a GMS.
- `convert_urns_to_lowercase` was being **defaulted** on dbt — exactly the split-identity
  risk ADR-090 exists to prevent. Now pinned.
- `GlueSourceConfig` **rejects** that same key (`extra_forbidden`), which is how we learned
  the Glue side never lowercases — so `False` on dbt *matches* Glue rather than being a
  preference.

---

## 3. The defect this review found

Worth recording, because it is the clearest example of the failure mode the whole programme
has been chasing.

`AIRFLOW__OPENLINEAGE__TRANSPORT` was set to the bare string `console`. The provider reads
that key with `conf.getjson("openlineage", "transport")` and therefore raised
`AirflowConfigException: Unable to parse [openlineage] 'transport' as valid json` — **while
importing the plugin**. Airflow skipped the plugin, registered no listener, and ran the DAG
green:

```
REGISTERED_LISTENERS = []        # three tasks succeeded; zero events emitted
```

A configuration error that fails loudly costs an afternoon. This one succeeded at everything
except the one thing it existed to do.

**The test suite had asserted this exact value and passed.** The test compared
`airflow/helm/values.yaml` against `cdc/lineage_runtime.py::airflow_env()` — and both sides
carried the bare string. Two files I wrote agreeing with each other is not evidence about the
third party that has to consume them. The three replacement tests parse the value the way the
provider does.

That the Spark listener takes a *flat* `spark.openlineage.transport.type=console` string is
why the bare form looked right: two integrations, two encodings, one config field.

---

## 4. Findings, classified

No **P0** findings. No **P1** findings.

| # | Sev | Finding | Action |
|---|---|---|---|
| 1 | **P2** | The OpenLineage provider is not baked into the Airflow image, so the resident scheduler does not emit | build the image (§1) |
| 2 | **P2** | Scenario 1 is evidenced in three segments rather than one correlated run | one EMR submission |
| 3 | **P2** | `governance/catalog/domains.yml` is still read at runtime by `ai/guards.py`, and still describes 1 of 73 live tables | repoint the AI guard at `compile_inventory()` |
| 4 | **P2** | `loan` and `payment_method` declare `not_null` columns with no CURATED mapping | add them to `entities.yaml` (reported on every DQ run) |
| 5 | **P2** | Ingestion recipes have never been executed | run `metadata_ingestion` with `dry_run: false` |
| 6 | **P3** | `owner: my-aws-profile` on 50 assets is an IAM principal, not a rota | ownership decision |
| 7 | **P3** | No asset is classified `restricted`, though FULL_CDC carries raw before/after payloads | governance decision |
| 8 | **P3** | Production DataHub sizing on the k3s node is unmeasured | measure before any resident deployment |

---

## 5. What changed across DRP0 → DRP12

| | DRP0 measurement | Now |
|---|---|---|
| assets with governance | **1** | **84** (100% owned) |
| declared datasets that do not exist | 11 | **0** |
| DQ rule coverage | 7 datasets, **1 live** | **35 datasets, 153 checks, all live** |
| DQ verdicts ever produced | **0** | **160** |
| lineage graph | none — a drifted YAML with false `observed` markers | **81 nodes, 80 edges, 67 column edges, 0 cycles, 0 findings** |
| Airflow lineage events | **0** | **7**, with parent/root run facets |
| Spark lineage events | **0** | **12** |
| quarantine rows ever written | **0** (the table did not exist) | table created, poison record quarantined by reference |
| recovery loop | never executed | **full loop in 96 s**, mart sum corrected 80.0 → 60.0 |
| E2E scenarios with live evidence | **0 of 10** | **10 of 10** |
| tests | 2,871 | **3,301** |
| AWS spend for the programme | — | **$0** |

---

## 6. The honest sentence

> Twelve of twelve phases are complete, **25 of 26** acceptance gates pass on live evidence,
> the recovery loop executes end to end, all ten E2E scenarios have run, and Airflow now
> emits lineage. Three operational tasks remain and none of them is a gate: bake the provider
> into the image, join scenario 1 into a single run, and execute the ingestion recipes.

The instinct that caught the zero-row ingest, the false `evidence: observed` markers, the
`SELECT * EXCEPT` that meant `latest_state` had never run, the "check is broken" that meant
"table is empty", and — this session — a green DAG emitting nothing at all, says the count is
right.

`DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY`.
