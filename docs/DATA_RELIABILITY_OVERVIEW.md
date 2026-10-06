# DATA RELIABILITY — OVERVIEW

- Audience: anyone who has to answer *"is this number right, and if not, what do I rerun?"*
- Status: the **data plane** is live-tested; the **reliability plane** is being built
  phase by phase (DRP0 → DRP12). Each section below says which parts exist today.
- Companions: `PLATFORM_RESOURCE_INVENTORY.md` (what is deployed),
  `LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md` (how to actually rerun),
  `GOVERNANCE_METADATA_CONTRACT.md`, `DATA_QUALITY_MODEL.md`, `CERTIFICATION_MODEL.md`.

---

## 1. The two planes

```text
                         DATA PLANE

Oracle / SQL Server
        │
        ▼
     Debezium
        │
        ▼
       Kafka
        │
        ▼
   CDC Normalizer
        │
        ▼
     FULL_CDC
      /      \
     /        \
REALTIME      EOD
     \        /
      \      /
      dbt-Spark
          │
          ▼
         MART
          │
          ▼
   SERVING / Athena
          │
          ▼
       Power BI


              METADATA / GOVERNANCE / RELIABILITY

Debezium / Kafka Connect metadata ─┐
Kafka metadata ────────────────────┤
Spark OpenLineage ─────────────────┤
Airflow OpenLineage ───────────────┤
dbt manifest/catalog/run_results ──┤
Glue / Iceberg metadata ───────────┤
Athena metadata / usage ───────────┤
Power BI metadata ─────────────────┤
DQ results ────────────────────────┤
Reconciliation ────────────────────┤
OPS execution metadata ────────────┘
                                  │
                                  ▼
                               DataHub
                                  │
                ┌─────────────────┼──────────────────┐
                ▼                 ▼                  ▼
             Catalog            Lineage           Governance
                                  │
                                  ├─ Impact
                                  ├─ Root Cause
                                  ├─ Column Lineage
                                  └─ Run Lineage
                                  │
                                  ▼
                         DATA RELIABILITY ENGINE
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
                   DQ       Reconciliation   Certification
                    │             │             │
                    └─────────────┼─────────────┘
                                  ▼
                               Incident
                                  │
                                  ▼
                         Lineage Impact Analysis
                                  │
                                  ▼
                           Recovery Subgraph
                                  │
                                  ▼
                       Airflow topological rerun
                                  │
                                  ▼
                          DQ + Reconciliation
                                  │
                                  ▼
                            Re-certification
```

### Read the diagram twice

The arrows point the same way in both planes, but they mean different things.

In the **data plane** an arrow is *rows moving*. In the **metadata plane** an arrow is
*a claim about those rows*. That distinction is the whole reason the reliability plane
exists: a pipeline can move rows perfectly and still be wrong, and it can fail to move rows
while every health check stays green. This platform has produced both — an ingest reporting
`SUCCESS` with `batches: 0, rows: 0` (see `PLATFORM_RESOURCE_INVENTORY.md` §0c), and a
"certified" close that had never run a quality check.

### Two structural facts people get wrong

**REALTIME and EOD are siblings, not a chain.** Both are derived from FULL_CDC. There is no
path `REALTIME → EOD`. If you are looking for why an EOD number differs from a REALTIME
number, the answer is never "REALTIME was stale when EOD read it" — EOD never read it.

**FULL_CDC is written straight from Kafka.** There is no intermediate landing layer in the
reporting path. FULL_CDC is the canonical, append-only record of every I/U/D event and is
the replay source for everything above it.

---

## 2. The four questions the reliability plane answers

| Question | Answered by | Where it lives |
|---|---|---|
| *What is this table, who owns it, what is in it?* | **Governance / Catalog** | `cdc/registry/sources.yaml` + `governance/registry/` → DataHub |
| *Where did this number come from?* | **Lineage** | OpenLineage (runtime) + dbt manifest + Iceberg snapshots → DataHub |
| *Is it right?* | **Data Quality** | `spark/ops/dq_engine.py`, dbt tests, `cdc/quality.py` contracts |
| *Does it agree with the layer below?* | **Reconciliation** | `spark/common/reconcile.py`, `ops.reconciliation_run` |

And one more that only exists when all four do:

| *If it is wrong, what is the smallest safe thing to rerun?* | **Impact + bounded recovery** | `cdc/incidents.py`, DataHub downstream lineage, the dbt/job dependency graph |

---

## 3. Data governance — what is actually governed

Governance here is **derived**, not hand-maintained. That is not a style preference: the
DRP0 audit measured a hand-written catalogue describing 12 datasets of which **1 existed**,
while **72 of 73 live tables** carried no owner, classification, retention or SLA.

Today the compiler derives **84 assets** from the files the platform already obeys and
reports `owned 84/84 (100.0%)`:

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.governance_plan import compile_inventory; i = compile_inventory(); \
  print(i.config_version(), i.coverage()); [print(' ', f) for f in i.findings]"
```

| What | Where it comes from |
|---|---|
| owner / technical owner / business owner | `cdc/registry/sources.yaml`, inherited global → domain → source → table |
| domain, subdomain, description | same, checked against `governance/registry/domains.yaml` |
| classification (`public`…`restricted`) | the registry's own `classification:` — **one home**, refused anywhere else |
| PII columns and their **category** | the registry, plus `DimensionSpec.pii_columns` for Kimball dimensions |
| retention band vs executable days | band in governance, days in `eod.retention_days`, **checked** against each other |
| freshness SLO vs SLA | SLO is the promise, SLA is what a check measures; an SLO tighter than its SLA is refused |

**The rule that keeps it honest:** an asset that exists but is not derivable from config is
a *finding*, not a silent gap. One is open today — `curated:dim_customer_bi`, which exists
only in Python.

---

## 4. Data lineage — and how much to trust each edge

Not every edge is equally trustworthy, and pretending otherwise is how an automated
recovery rewrites data on the strength of a YAML file.

| Class | Meaning | May a recovery act on it? |
|---|---|---|
| `observed` | a runtime emitted an event for this specific run | **yes** |
| `derived` | computed from the producer's own artefact (OPS ledger, dbt manifest, Iceberg snapshot history) | **yes** |
| `declared` | asserted from config; no telemetry, no artefact | **no — plan only** |
| `absent` | no edge at all | — |

Per-hop detail is in `LINEAGE_SOURCE_MATRIX.md`. The short version:

- **Debezium hops can never be better than `declared`** — Debezium emits no OpenLineage.
- **The best-instrumented hops today are FULL_CDC → REALTIME and FULL_CDC → EOD**, because
  `ops.realtime_run` and `ops.eod_run` already record input snapshot, output snapshot, rows
  and read mode per table per run. That is `derived` lineage the platform writes for itself.
- **Column lineage** is prioritised where dbt/SQL can give it honestly. A fuzzy inferred
  column mapping is never authoritative for a critical field without validation.

---

## 5. Data quality — six verdicts, and why five is not enough

| Status | Meaning | Blocks a certified publish? |
|---|---|---|
| `PASS` | evaluated and satisfied | no |
| `WARN` | violated, at WARN severity | no |
| `FAIL` | violated, at ERROR severity | **yes** |
| `ERROR` | the check itself could not run | **yes** |
| `NOT_EVALUATED` | it ran and there was nothing to evaluate | **yes** |
| `SKIPPED` | deliberately not run for this interval | no |

`SKIPPED` and `NOT_EVALUATED` are the pair that matters:

> A uniqueness query returning zero rows means "no duplicates" if the table has data, and
> means **nothing at all** if the table is empty, the partition is missing, or the predicate
> matched nothing.

A suite that reports green in the second case is worse than no suite — it actively asserts
health for a pipeline that did not run. So "we could not tell" blocks, and "we chose not to
look" does not.

**An empty suite also blocks.** "No check failed" is not "the data was checked".

---

## 6. Reconciliation — agreement between layers

Reconciliation compares a metric across two datasets for one interval: row counts between
FULL_CDC and EOD, key sets between FULL_CDC and REALTIME, measures between EOD and a mart.

One invariant is worth stating on its own:

> When either side was not measured, `difference` is **NULL**, never `0`.

Rendering an unmeasured comparison as a clean zero is the single most dangerous thing a
reconciliation ledger can do, and the column is nullable in the DDL for that reason.

---

## 7. Certification — the ladder

| Tier | Rank | Meaning |
|---|---|---|
| `REALTIME` | 1 | the bounded overlay; true now, certified never |
| `PROVISIONAL_NRT` | 2 | incremental, watermark-bounded |
| `PROVISIONAL_CORRECTED` | 3 | whole-of-day relook after late events |
| `RECONCILED` | 4 | agreed against a counterpart |
| `CERTIFIED` | 5 | closed, cutoff-bounded, DQ- and recon-clean |

A lower tier may never overwrite a higher one. Equal is allowed, so a correction to a
certified figure can be republished.

**A Spark job exiting 0 is not certification.** Certification requires the gates in
`CERTIFICATION_MODEL.md`: contract declared, DQ evaluated and non-blocking, reconciliation
passed, cutoff bounded, upstream ready, source period closed.

---

## 8. What exists today, honestly

| Capability | State |
|---|---|
| Data plane end to end (Oracle/SQL Server → … → Athena) | **live-tested** 2026-09-30 |
| Governance metadata model and compile (84 assets) | **implemented, offline** (DRP1) |
| Certification ladder and gates | **implemented**; not yet wired into the flows |
| DQ / reconciliation / incident / recovery **contracts** | **implemented**; nothing executed |
| Metadata plane (DataHub) | **mode machinery + pinned local + production design** (DRP2) |
| OpenLineage runtime emission | **not yet** — DRP3 |
| Catalog ingestion, end-to-end lineage, live impact + recovery | **not yet** — DRP4 → DRP9 |

Nothing above is marked PASS that has not run. Where a phase is designed but not executed,
the phase that owns the execution is named.
