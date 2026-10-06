# ADR-087 — DataHub as the primary metadata plane, OpenLineage as the runtime standard

* **Status:** **Proposed** — not accepted, not implemented
* **Date:** 2026-09-30
* **Phase:** DRP0
* **Related:** ADR-028, ADR-033, ADR-067, ADR-073, ADR-083, ADR-086
* **Supersedes (if accepted):** the `optional_backends.marquez` position in `governance/lineage/openlineage.yml`

## Context

The data plane is live-tested end to end (2026-09-30: ingest → FULL_CDC → REALTIME/EOD →
Athena, 2,871 tests, EOD `closed=10 certified=5`). The governance plane is not.

DRP0 measured the gap rather than asserting it. Against the live Glue catalog:

* `governance/catalog/domains.yml` declares 12 datasets; **1** exists.
* `governance/dq/rules.yml` declares 7; **1** exists.
* `governance/lineage/openlineage.yml` declares 11 Iceberg datasets; **2** exist, and it
  marks the Spark hops `evidence: observed` when **nothing in the repository injects
  `spark.extraListeners`** and `ops.lineage_event` does not exist in the catalog.
* Inverted: **72 of 73 live tables** have no owner, domain, classification, PII marking,
  retention or DQ rule.

The cause is not neglect. `domains.yml` was written with an explicit argument against this
very failure — *"a second list of PII columns somewhere else is exactly how masking and
policy drift apart while both look maintained"* — and it drifted anyway, because the
platform beneath it became config-driven from `cdc/registry/sources.yaml` (ADR-067…073)
and the hand-maintained YAML could not follow.

Two consequences are already live, not hypothetical:

1. `ai/guards.py:266` and `ai/agent_tools/catalog.py:92` use `domains.yml` as the
   authority for *"is this dataset BI-readable?"*. Against the real catalog that control
   can only answer **deny**, for every real table, while confidently describing eleven
   tables that do not exist.
2. `spark/ops/dq_engine.py:317` reads `governance/dq/rules.yml` at runtime. A DQ run today
   evaluates seven phantom datasets, every check returns `NOT_EVALUATED`, and
   `suite_blocks_publish()` correctly blocks. The engine is sound; its input is three
   naming generations stale.

## Decision

**Adopt DataHub as the single primary catalog / metadata graph / lineage / governance /
impact-analysis platform, and OpenLineage as the runtime lineage transport.** Do not
deploy OpenMetadata alongside it.

Four constraints are part of the decision, not caveats to it.

### 1. The catalog is derived, never hand-maintained

`cdc/registry/sources.yaml` already carries per-table `classification`, `primary_key`,
`dq.not_null`, `dq.freshness_sla_minutes`, `eod.retention_days` and `maintenance`, and it
is the file the platform is actually driven from — so it cannot drift without the pipeline
noticing. It gains `owner`, `domain`, `description`, `glossary_term` and `pii_columns`,
and `domains.yml` becomes **derived output**. DataHub ingests from Glue, dbt and the
registry. A live Glue table absent from the registry is a **finding**, not a silent gap.

Re-authoring `domains.yml` by hand would repeat the experiment that has already been run
here, with the result already measured.

### 2. DataHub may never execute a job

DataHub is read-only with respect to the executor. The impact planner reads the graph,
then resolves each affected node to a *registered executable job* via the OPS job registry
and the Airflow DAG registry; Airflow alone runs anything. Unresolvable nodes become
operator decisions, never silent omissions. A metadata service with a write path into the
executor can destroy data because someone mis-tagged a dataset.

### 3. Evidence class is carried on every edge, and it gates recovery

The existing `declared` / `observed` distinction in `docs/LINEAGE.md` is kept and extended
with `derived` (computed from a producer's own artifact: dbt manifest, Iceberg snapshot
history, OPS ledger). Per `docs/LINEAGE_SOURCE_MATRIX.md` §5:

> A recovery subgraph may execute automatically **only if every edge in it is `observed`
> or `derived`.** One `declared` edge downgrades the whole plan to
> `RECOVERY_PLAN_REQUIRES_APPROVAL`.

Debezium emits no OpenLineage, so hops 1–2 are permanently `declared` — and ADR-086's
STREAM_BATCH `source_tables` link is `declared` too, which correctly makes every
mart-crossing recovery operator-approved.

### 4. Cost posture is inherited, not renegotiated

DataHub is GMS + frontend + Elasticsearch + a metadata store + Kafka — the largest
always-on component this project would have. CLAUDE.md §4.10 already keeps **Marquez** off
for precisely this reason (*"a 24/7 service; `ops.lineage_event` needs nothing running"*).
Rejecting Marquez on cost and then accepting something heavier without saying so would be
incoherent. Therefore:

* `enable_datahub` feature flag, **default `false`** (§4.12), with a destroy + verify script.
* Private-only networking, SSM port-forward for the UI, no `0.0.0.0/0` (§3.3, §3.4).
* No static credentials — IRSA / instance profile (§3.1, §3.2).
* All six required tags including `AutoDestroyAfter` (§4.11).
* **Offline-first:** ingestion recipes must emit to a `file` sink and validate with the
  server down, so DRP4–DRP7 progress at $0 and the checkpoint chain never blocks on an
  always-on service.

## Versions — deliberately NOT pinned here

CLAUDE.md §3.9 forbids `latest`, and `docs/VERSIONS.md:104` already records what pinning
from expectation rather than from a resolve costs: the first EMR wheelhouse was built at
dbt 1.8.9 against a `>=1.9.0` requirement and the run failed. Pinning OpenLineage and
DataHub from a prompt would repeat that.

What DRP0 *did* establish are the constraints the pins must satisfy:

| Integration | Constraint measured in DRP0 |
|---|---|
| OpenLineage Spark | Scala **2.12** artifact, Spark **3.5** (EMR 7.2.0), verified against Iceberg **1.5.2** |
| OpenLineage Airflow | deployed Airflow is **3.2.2** — an Airflow-2-era provider will not load. The workstation's Airflow 2.9.3 is **not** a valid test bed. |
| OpenLineage dbt | must match **dbt-core 1.9.11 / dbt-spark 1.9.3** and be added to the EMR wheelhouse, which must be rebuilt when that pin moves |
| DataHub | server and `acryl-datahub` CLI on the same release line; the chosen release must actually expose the OpenLineage ingestion endpoint |

`pip list` confirms **nothing** is installed today. Resolution, SHA verification and
pinning happen in DRP2/DRP3.

## Options

**OpenMetadata.** Comparable catalog and lineage coverage, generally lighter to run. Not
chosen now, but the brief's instruction is respected literally: if DataHub proves
incompatible with a hard requirement — most plausibly the `lab_low_cost` memory footprint
on the k3s node — a comparison ADR is written and **the programme stops there** rather
than switching mid-flight. Running both is never an option; two catalogs is the failure
this ADR exists to fix, at a larger scale.

**Marquez.** The usual OpenLineage backend and already a documented feature flag. Rejected
as the *primary* plane: it is lineage only — no ownership, no glossary, no classification,
no assertions — so it would leave rows 1–9 and 14–17 of the governance audit unaddressed.

**Keep `ops.lineage_event` as the whole plane.** The cheapest option, and it is what the
current design chose. It fails on the same ground: an Iceberg table can store edges, but
nothing queries a graph, computes forward closure, or holds owners and glossary terms.
The table is retained as a durable, $0 sink for OpenLineage events — the offline path in
constraint 4 — not as the plane.

**Do nothing.** Rejected by the measurement: 72 of 73 tables ungoverned, and a live AI
access control running off a registry that describes one real table.

## Cost

**This ADR authorises no spend.** `enable_datahub` defaults to `false`; DRP2 is a separate
decision carrying its own sizing evidence.

The cost shape when it is enabled, stated so the DRP2 decision is not made blind:

| Component | Cost driver | Posture |
|---|---|---|
| DataHub GMS + frontend + Elasticsearch + metadata store | always-on memory and storage on the k3s node — the largest 24/7 component this project would have | flag-gated, default OFF, `AutoDestroyAfter` tagged |
| DataHub's Kafka usage | reuses the existing MSK cluster; no second cluster | no new spend |
| OpenLineage Spark listener | CPU on jobs that already run; streaming emits per micro-batch | volume decision owned by DRP3 |
| OpenLineage Airflow provider | negligible | — |
| `ops.lineage_event` (Iceberg) | S3 storage only | retained as the $0 offline sink |
| Ingestion recipes with a `file` sink | $0 | the offline path that keeps DRP4–DRP7 unblocked |

CLAUDE.md §4.10 already keeps Marquez off as "a 24/7 service; `ops.lineage_event` needs
nothing running". DataHub is heavier, so it inherits a stricter version of the same rule,
not a looser one. Whether the existing k3s node can host it at all under `lab_low_cost` is
**open question 3** in `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md` §6 and must be answered
before DRP2 spends anything.

## Security

Inherited unchanged from CLAUDE.md §3; none of it is renegotiated by this ADR.

* **§3.3 / §3.4** — private networking only. No `0.0.0.0/0` to GMS, the frontend,
  Elasticsearch or the metadata store. The UI is reached through SSM port-forwarding; no
  SSH, no key pair, no public load balancer.
* **§3.1 / §3.2** — no static credentials and no IAM user or access key. The Glue, Athena
  and S3 ingestion sources authenticate through IRSA or the instance profile.
* **§3.6** — any DataHub secret (metadata-store password, frontend session key) comes from
  Secrets Manager or SSM SecureString at runtime and is never a Terraform output in
  plaintext.
* **§3.9** — every image, chart and Python distribution pinned exactly, with a verified
  checksum; no `latest`.
* **New surface this ADR creates:** DataHub would hold the PII classification for the whole
  lakehouse, which makes the catalog itself a sensitive asset — the map of where the
  regulated data is. It is treated at the classification of the most sensitive thing it
  describes, which under `governance/catalog/domains.yml`'s scheme is `restricted`.
* **The execution boundary is a security control, not only an architectural one.** DataHub
  has no path to trigger a job; Airflow alone executes. Without that, a tag edit in a web
  UI becomes a way to make the platform rewrite data.

## Rollback

* **Before DRP2:** nothing to roll back. This ADR is `Proposed` and changes no running code.
* **DataHub:** `enable_datahub=false` plus the destroy + verify script required by
  CLAUDE.md §4.12. Because the catalog is *derived* from `cdc/registry/sources.yaml`,
  destroying DataHub loses no authored metadata — it can be re-ingested from the registry,
  Glue and the dbt manifest. That is a direct consequence of decision 1 and the main
  operational reason for it.
* **OpenLineage Spark listener:** remove the `spark.extraListeners` configuration. Emission
  is already required to be non-fatal (`fail_on_error: false` in
  `governance/lineage/openlineage.yml`), so removing it cannot fail a job.
* **OpenLineage Airflow provider / `openlineage-dbt`:** uninstall and rebuild the EMR
  wheelhouse at the previous pin.
* **DQ rule source (DRP5):** the move from `governance/dq/rules.yml` to the registry is the
  one change that touches a module which currently blocks publishes. It is staged behind
  its own gate and reverts by pointing `--rules` back at the file.
* **If DataHub fails a hard requirement:** write the comparison ADR and **stop**. Do not
  switch to OpenMetadata mid-programme, and never run both.

## Validation

DRP0 validation — what was actually checked in this session:

| Check | Result |
|---|---|
| Live Glue census vs the three governance files | `domains.yml` 1/12, `dq/rules.yml` 1/7, `openlineage.yml` 2/11 resolve |
| Live tables carrying governance metadata | **1 of 73** |
| `spark.extraListeners` anywhere in `scripts/`, `terraform/`, `airflow/`, `spark/`, `cdc/`, `dbt/` | **0 occurrences** — the `evidence: observed` claim is false |
| `ops.lineage_event` present in the live catalog | **no** (DDL exists, table does not) |
| `openlineage-*` / `acryl-datahub` installed | **no** |
| Runtime versions measured | EMR 7.2.0 / Spark 3.5 / Scala 2.12, Iceberg 1.5.2, Airflow 3.2.2 deployed, dbt-core 1.9.11 pinned |
| Workstation vs deployed drift | Airflow 2.9.3 and dbt 1.9.4 locally — **neither matches the deployed version** |

Validation this ADR is **not** claiming, and which phase owns it:

| Claim | Owner |
|---|---|
| a resolved, SHA-verified pin for each of the four integrations | DRP2 / DRP3 |
| DataHub fits on the k3s node under `lab_low_cost` | DRP2 |
| the file-sink offline path actually produces valid metadata events | DRP4 |
| a real OpenLineage event is emitted by a real run | DRP3 |
| the derivation direction is enforced by a test rather than by discipline | DRP1 |
| Athena query history is retained long enough to ingest | DRP4, open question 1 |

Nothing above is converted from NOT_TESTED to PASS.

## Consequences

* Governance metadata gains a second home (DataHub) alongside its derived source, and the
  derivation direction must be enforced by a test, or this ADR recreates the drift it
  diagnoses.
* The `evidence: observed` markers in `governance/lineage/openlineage.yml` are **false
  today** and must be corrected to `declared` in DRP3 when real emission replaces them.
  DRP0 reports them and leaves them in place; correcting them is not an audit action.
* `ops.lineage_event` needs to be created in the live catalog before it can receive anything.
* The DQ engine's rule source moves from `governance/dq/rules.yml` to the registry, which
  is a behaviour change to a module that currently blocks publishes — it must be staged
  behind the DRP5 gate, not slipped in.
* Accepting this ADR does **not** authorise any spend. `enable_datahub=false` is the
  default and DRP2 is a separate decision with its own sizing evidence.
