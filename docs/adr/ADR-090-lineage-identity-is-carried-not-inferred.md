# ADR-090 — A lineage dataset carries its canonical identity; it is never inferred

* **Status:** Accepted
* **Date:** 2026-09-30
* **Phase:** DRP3
* **Related:** ADR-087, ADR-089

## Context

DRP3 attaches the OpenLineage Spark listener and the Airflow provider so that lineage is
`observed` rather than `declared` for the first time. That immediately raises the question
ADR-089 deferred: the Spark listener emits a dataset as `(namespace, name)`, and the DataHub
Glue ingestor emits `database.table`. Both refer to one table.

The usual answer is to let each side infer a DataHub URN from its own namespace, and to line
the two inference rules up. That rule then lives in two codebases — the listener's namespace
resolution and DataHub's ingestion mapping — and it drifts on any upgrade of either. The
symptom is not an error: the lineage graph splits into fragments, each of which looks
complete, and every impact query returns a partial answer with no indication that it is
partial. The DRP0 audit found precisely that failure in the declared graph, where
`stream.oracle_corebank_customer` was a confident name for a table that did not exist.

A second problem is narrower and sharper. A lineage facet is free-form, it is written by
whoever is debugging, and it lands in a store that dashboards, alerts and the AI assistant
all read. "Just the failing row, to see what broke" is how a restricted column leaves the
quarantine.

## Decision

**1. Every emitted dataset carries its canonical DataHub URN as a facet** (`datahub_urn`),
minted by `cdc/urns.py` — the same function the catalogue ingestion uses. A carried identity
is checkable; an inferred one is a hope. A dataset with no carried URN cannot be joined to
the catalogue and `canonical_urn()` says so rather than guessing.

**2. One name per job, declared once.** `governance/registry/openlineage.yaml` holds the
nine job names; Airflow, Spark and every facet use them. An undeclared name raises. A job
named from a DAG id in one place and a Spark app name in another appears twice in the graph
and neither copy has the full run history.

**3. Facets are a closed allowlist, and the deny list guards the allowlist.** `scrub()`
refuses a key that is not allowlisted — raising, never dropping, because a silently dropped
facet is one the author believes is published. The denied substrings (`token`, `password`,
`sample`, `payload`, `before_image`, …) are checked **at config load against the allowlist**,
so a data-carrying name cannot be added to it and then published forever, reviewed-looking
and never re-examined.

**4. Lineage is opt-in and cannot fail the job it observes.** `enabled: false`,
`fail_on_error: false`, `OPENLINEAGE=1` on the submit. A listener that is always on is one
nobody can turn off during an incident.

**5. The runtime configuration is DERIVED, never typed twice.** `spark_conf()` and
`airflow_env()` produce the conf from the pinned config; `scripts/emr-submit.sh` calls them,
and a test asserts `airflow/helm/values.yaml` matches what the module derives. This is the
direct remedy for the defect DRP0 found — a listener described in YAML, injected nowhere,
with the Spark hops marked `evidence: observed`.

## Options

**Namespace inference on both sides.** The default path and the one rejected above.
**A translation table from OL namespace to URN.** Better than inference, still a second
mapping that must be maintained in step with `cdc/urns.py`.
**Post-hoc deduplication in DataHub.** Repairs the graph after it is already wrong, and
until it runs every impact answer is quietly partial.
**Emit facets freely and filter at ingestion.** Puts the PII control on the far side of the
network boundary, after the value has already left the process that could have refused it.

## Consequences

* DRP4 must use `UrnMinter` for ingestion identity too — a recipe that formats its own name
  re-opens exactly this problem.
* The `datahub_urn` facet has to be allowlisted (it is) and is the join key DRP6 uses.
* `governance/lineage/openlineage.yml` — the Session-14 declared graph with its false
  `evidence: observed` markers — is **still in place**. DRP6 replaces it with emitted edges;
  correcting it now would have to be undone then.
* Nothing is emitted yet: `enabled: false` and no job has run with `OPENLINEAGE=1`.

## Cost

$0. No new runtime dependency is installed. When lineage is switched on, the cost is the
listener's CPU on jobs that already run, plus one `spark.jars.packages` download per EMR
run. Streaming jobs emit per micro-batch, which is a volume decision recorded here and owned
by whoever enables `full_cdc_ingestion` or `streaming_rt` — a one-minute trigger is 1,440
events a day per table.

## Security

* Facet allowlist + deny-guarded allowlist: no secret and no row value can be emitted.
* `spark.openlineage.facets.custom_environment_variables=[]` — the listener is explicitly
  told to publish no environment variables, which is otherwise a documented way for a
  token to reach a lineage backend.
* Only variable NAMES appear in config (`DATAHUB_GMS_TOKEN`), never values.
* `AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE=true` — DAG source is not shipped to the
  lineage backend.
* Fabric is enforced on every emitted dataset, so a dev run cannot write an edge onto a
  production asset.

## Rollback

`enabled: false` in the config (already the default), drop `OPENLINEAGE=1` from the submit,
and set `AIRFLOW__OPENLINEAGE__DISABLED=true` (already the deployed value). The listener is
never attached unless explicitly asked for, so rollback is "stop asking". Removing
`cdc/lineage_runtime.py` affects nothing else.

## Validation

| Check | Result |
|---|---|
| `spark/tests/test_drp3_openlineage.py` | **52 passed** |
| Spark artifact | `openlineage-spark_2.12:1.53.0`, sha1 `af24560b…`, resolved from `maven-metadata.xml` |
| Airflow provider | `2.20.2`, requires `apache-airflow>=2.11.0` → loads on the deployed 3.2.2 |
| Identity join | every lake asset's emitted dataset carries the URN `UrnMinter` produces |
| Derivation | `emr-submit.sh` produces the conf from the module; `values.yaml` matches `airflow_env()` |
| Lifecycle | START / COMPLETE / FAIL / ABORT, retries as distinct runs, parent-child |
| Facet guard | secrets, samples, payloads and before-images all refused; counts allowed |

**Not claimed.** No event has been emitted. No Spark job has run with the listener attached,
no Airflow task has run with the provider installed, and the provider is not installed by
the Helm chart. `transport.type` is `console`, so even when enabled nothing reaches DataHub
until DRP4 points it at the ingestion endpoint.
