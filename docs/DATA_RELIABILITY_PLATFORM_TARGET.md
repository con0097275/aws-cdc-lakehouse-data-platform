# DATA RELIABILITY PLATFORM — TARGET

- Phase: **DRP0** (target stated; nothing below is built)
- Date: **2026-09-30**
- Companions: `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md`, `docs/LINEAGE_SOURCE_MATRIX.md`, ADR-087
- Status of every component below: **PROPOSED.** Not planned, not deployed, not tested.

---

## 1. What is being added, in one sentence

The data plane is proven live end to end; what is missing is the plane that can answer
**"what broke, what did it touch, and what is the smallest safe thing to rerun?"**

```
CDC + Lakehouse + Data Contracts + DQ + Lineage
+ Airflow dependency graph + dbt graph
+ Reconciliation + Certification
+ Impact Analysis + Bounded Recovery
= Data Reliability Platform
```

## 2. Target architecture

```text
DATA PLANE  (exists, live-tested 2026-09-30 — unchanged by this programme)

  Oracle / SQL Server → Debezium → Kafka → CDC Normalizer → FULL_CDC
                                                               │
                                        ┌──────────────────────┴─────────────────────┐
                                        ▼                                            ▼
                                    REALTIME                                        EOD
                                        └──────────────────────┬─────────────────────┘
                                                               ▼
                                                    dbt-Spark → MART → Athena → Power BI


METADATA / GOVERNANCE / RELIABILITY PLANE  (new)

  Connect REST topics ────┐
  Kafka metadata ─────────┤
  Spark OpenLineage ──────┤
  Airflow OpenLineage ────┤
  dbt manifest/run_results┤
  Glue/Iceberg metadata ──┤──→  DataHub  ──→  Catalog · Lineage · Governance
  Athena query history ───┤                          │
  DQ + reconciliation ────┤                          ▼
  OPS run metadata ───────┘             Data Reliability Engine
                                                     │
                            ┌────────────────────────┼────────────────────────┐
                            ▼                        ▼                        ▼
                           DQ                 Reconciliation            Certification
                            └────────────────────────┼────────────────────────┘
                                                     ▼
                                                 Incident
                                                     ▼
                                          Lineage Impact Plan
                                                     ▼
                                          Recovery Subgraph
                                                     ▼
                                     Airflow topological rerun
                                                     ▼
                                        DQ + Reconciliation
                                                     ▼
                                              Re-certify
```

## 3. Separation of concerns — and who may execute

This is the load-bearing table of the whole programme.

| Plane | Owner | Authoritative for | May execute a job? |
|---|---|---|---|
| DataHub | metadata | catalog, lineage graph, impact closure, ownership, glossary, classification | **no — never** |
| OpenLineage | transport | runtime run/job/dataset facets | no |
| OPS (`ops.*`) | operational truth | what actually ran, with what cursor, producing how many rows | no (it is a ledger) |
| Airflow | execution | the only thing that runs jobs | **yes — exclusively** |
| dbt manifest | dbt dependency truth | model→model, model→source | no |
| Spark / dbt | transformation | the data itself | via Airflow |
| Iceberg | storage | snapshots, history, time travel | no |

**DataHub must not be able to trigger a job.** The impact planner reads DataHub, resolves
each affected node to a *registered executable job* through the OPS job registry and the
Airflow DAG registry, and anything that does not resolve becomes an operator decision
rather than a silent omission. A metadata service with a write path into the executor is a
metadata service that can destroy data because someone mis-tagged a dataset.

## 4. The single most important design decision

**The catalog must be derived, never hand-maintained.**

The audit measured what hand-maintenance produced here: `governance/catalog/domains.yml`
describes 12 datasets, of which **1** exists, while **72 of 73** live tables have no
owner, classification, retention or DQ rule. The file was written by careful people with
an explicit argument against exactly this failure — and it still happened, because the
platform underneath it became config-driven and the YAML did not.

So the target inverts the direction:

```
cdc/registry/sources.yaml  (the file the PLATFORM already obeys)
        + owner, domain, description, glossary_term, pii_columns   ← the only new fields
                    │
                    ▼
        derived governance metadata
                    │
     ┌──────────────┴──────────────┐
     ▼                             ▼
 Glue table properties        DataHub ingestion
     │                             │
     └──────────────┬──────────────┘
                    ▼
     DQ rules · masking · access · retention · contracts
```

A dataset that exists in Glue but not in the registry becomes a **finding**, not a silent
gap. That single check is what would have caught this drift 38 sessions ago.

## 5. Phase map — what each checkpoint must deliver

| Checkpoint | Delivers | Gate that proves it |
|---|---|---|
| `DRP0_RELIABILITY_CONTEXT_AUDITED` | this document, the audit, the lineage matrix, ADR-087 | **reached in this session** |
| `DRP1_GOVERNANCE_METADATA_FOUNDATION_READY` | governance fields on `cdc/registry/sources.yaml`; `domains.yml` becomes derived; a check that every live Glue table is registered | the drift number goes from 1/73 to 73/73 |
| `DRP2_DATAHUB_PLATFORM_READY` | DataHub behind `enable_datahub=false`, private-only, destroy+verify script, sizing decided | `terraform destroy` verified; UI reachable only via SSM |
| `DRP3_OPENLINEAGE_RUNTIME_READY` | Spark listener + Airflow provider + `openlineage-dbt`, versions **resolved and SHA-pinned** | a real run emits a real event; `evidence: observed` becomes true for the first time |
| `DRP4_CATALOG_INGESTION_READY` | Glue, dbt, Athena, Kafka ingestion recipes; **file sink works with the server down** | recipes validate offline at $0 |
| `DRP5_DATA_QUALITY_CONTRACTS_READY` | `dq_engine` driven from the registry, not the drifted rules file; results published as DataHub assertions | `ops.dq_result` exists and holds a real run |
| `DRP6_END_TO_END_LINEAGE_READY` | one graph, source column to mart, every edge carrying its evidence class | a single query returns the full path for one column |
| `DRP7_GOVERNANCE_READY` | owner, domain, glossary, PII, retention, contracts on all 73 | no unowned live table |
| `DRP8_IMPACT_RECOVERY_PLANNER_READY` | forward closure → blast radius → recovery subgraph, **planning only** | a plan is produced and executes nothing |
| `DRP9_LINEAGE_DRIVEN_RECOVERY_READY` | Airflow topological rerun of an approved subgraph | a bounded recovery runs and re-certifies |
| `DRP10_RELIABILITY_OBSERVABILITY_READY` | freshness/DQ/incident metrics and alerts | a real breach alerts |
| `DRP11_FULL_RELIABILITY_E2E_PASS` | inject a defect → detect → incident → plan → recover → re-certify | one unbroken live run |
| `DRP12_DATA_RELIABILITY_PLATFORM_PRODUCTION_READY` | the whole chain, live | every gate above at PASS, none converted from NOT_TESTED |

## 6. Auto-recovery — the gate, stated concretely

Automatic recovery runs **only** when every one of these holds:

1. the root dataset is identified,
2. the affected COB / date / window / key set is identified,
3. the repair is deterministic and idempotent,
4. **every edge in the recovery subgraph is `observed` or `derived`** — one `declared`
   edge downgrades the whole plan (`docs/LINEAGE_SOURCE_MATRIX.md` §5),
5. every affected node resolves to a registered executable job,
6. no source-system write is required,
7. no Kafka offset or checkpoint reset is required,
8. blast radius is within the configured limit,
9. the auto-recovery attempt count is not exhausted,
10. cost and security guardrails pass.

Otherwise: **`INCIDENT_OPEN` + `RECOVERY_PLAN_REQUIRES_APPROVAL`**, and the plan is
written down rather than run.

Conditions 6 and 7 are not theoretical here. The 2026-09-29 rebuild produced exactly the
failure they forbid: a Structured Streaming checkpoint outlived the MSK cluster it
referenced, and the ingest job reported `SUCCESS` with `batches: 0, rows: 0`
(`docs/PLATFORM_RESOURCE_INVENTORY.md` §0c). An automatic recovery that resets checkpoints
would have hidden that, not fixed it.

**No infinite loop.** A DQ failure that triggers a rerun that fails DQ again must
terminate at the configured attempt limit and open an incident. A reliability platform
that can loop is an outage generator with good intentions.

## 7. Cost and security posture

Inherited unchanged from CLAUDE.md §3 and §4, and binding on every phase:

- `enable_datahub` defaults **false**; destroy + verify script required (§4.12).
- Private networking only; SSM port-forward for the UI. No `0.0.0.0/0` (§3.3, §3.4).
- No static credentials; IRSA / instance profile for Glue, Athena and S3 access (§3.1, §3.2).
- Every resource tagged `Project`, `Environment`, `ManagedBy`, `Owner`, `CostCenter`,
  `AutoDestroyAfter` (§4.11).
- Every version resolved then pinned, never `latest` (§3.9).
- **DataHub and OpenMetadata are never both deployed.** If DataHub fails a hard
  requirement, ADR-087 is superseded by a comparison ADR — and the work stops there rather
  than switching mid-programme.
- **Offline-first**: ingestion recipes must produce and validate metadata events with the
  server down, so the checkpoint chain never depends on an always-on service to make
  progress.

## 8. What this programme does NOT change

The data plane. FULL_CDC, REALTIME, EOD, the cursor, the rebase, the certification close
and the five reporting flows are live-tested and stay exactly as they are. The reliability
plane observes them; it does not rewrite them. Any phase that finds itself editing
`realtime_engine.py` or `eod_engine.py` to make lineage work has misplaced the boundary.
