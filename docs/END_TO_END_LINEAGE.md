# END-TO-END LINEAGE

- Phase: DRP6 · Code: `cdc/lineage_graph.py` · Config: `governance/registry/lineage_declared.yaml`
- Companions: `LINEAGE_SOURCE_MATRIX.md`, `LINEAGE_NAMING_STANDARD.md`,
  `COLUMN_LINEAGE_STRATEGY.md`, `LINEAGE_TROUBLESHOOTING.md`

---

## 1. The graph, as it stands today

**81 nodes, 80 dataset edges, 67 column edges, 0 cycles, 0 audit findings.**

```
src:oracle.coredb.corebank.account                          declared   (Debezium)
  └─ topic:cdc.oracle.COREBANK.ACCOUNT                      declared   (cdc registry)
      └─ full_cdc:oracle_coredb_corebank_account            derived    (ops.eod_run /
          ├─ realtime:oracle_coredb_corebank_account                    ops.realtime_run)
          └─ eod:oracle_coredb_corebank_account
              └─ curated:banking_account                    derived    (curated/entities.yaml)
                  ├─ curated:dim_account                    derived
                  └─ curated:fact_account_daily_snapshot    DECLARED   (Python only)
                      └─ mart:stg_fact_account_daily_snapshot  derived (dbt manifest)
                          └─ mart:mart_account_balance_daily    derived
```

Eight hops, source column to certified mart. `graph.path()` returns it;
`graph.weakest_evidence()` says **`declared`** — and that single word is what stops an
automatic recovery from crossing it.

## 2. One authority per segment

| Segment | Authority | Evidence |
|---|---|---|
| source DB → Kafka | the CDC registry (`connector_lineage`) | `declared` — Debezium emits nothing |
| Kafka → FULL_CDC | Spark OpenLineage (DRP3) | `declared` until the listener runs |
| FULL_CDC → REALTIME / EOD | `ops.realtime_run`, `ops.eod_run` | **`derived`** |
| EOD → CURATED | `reporting/curated/entities.yaml` | **`derived`** |
| CURATED facts | `governance/registry/lineage_declared.yaml` | `declared` — built in Python |
| dbt models → MART | the dbt manifest | **`derived`**, authoritative |
| MART → Power BI | the Power BI connector | absent — no workspace |

Nothing restates another segment's authority. The dbt manifest is the only source of
model-to-model dependencies, and no second dbt graph is emitted anywhere — that is the
S11-3 objection, and it is exactly what `governance/lineage/openlineage.yml` already did.

## 3. Static and run lineage are separate

**Static / logical** answers *"what feeds this table, ever?"* — the graph above. Impact
analysis reads it.

**Run** answers *"which execution produced the rows I am looking at?"* — a `RunLink` carrying
job, run id, inputs, outputs and facets:

```
run_id  0f1d1c1a-…      job  eod_build
inputs  full_cdc:…account          outputs  eod:…account
facets  cob_date 2026-09-28 · source_snapshot_id 6327604713160137465
        target_snapshot_id 80230069689276128 · config_version gv1:… ·
        dq_run_id dq-1 · spark_app_id 00g8turrhf2ipg27 · airflow_run_id …
```

Everything a post-mortem needs, **as links**. The ledgers stay in OPS — the lineage store
holds pointers, not `ops.eod_run` itself.

## 4. Lineage quality — six rules

`audit()` reports each of these, and the real graph currently returns **zero**:

| Rule | The failure it names |
|---|---|
| `orphan_critical_asset` | critical and no upstream: either a seed or a broken edge, and the two look identical |
| `missing_expected_upstream` | a known parent is absent |
| `unexpected_cycle` | makes a topological rerun impossible and a traversal non-terminating |
| `duplicate_canonical_asset` | one table on two platforms — the graph splits and each half looks complete |
| `broken_dbt_edge` | dbt names an upstream that is not a node |
| `missing_bi_parent` | a BI asset with no parent shows no consumers for a mart change |

A generated dimension declares an **empty** upstream list rather than being left out —
"no parent" is the correct answer for `dim_date`, and leaving it undeclared would make a
real orphan indistinguishable from a deliberate one.

## 5. Traversal is bounded

`descendants(root, max_hops=N)` — and `max_hops` is not a performance guard. An unbounded
traversal means one bad source table nominates every mart, and a blast radius that always
says "everything" is one nobody reads.

## 6. What is still `declared`, and what it costs

| Edge | Why | Consequence |
|---|---|---|
| Debezium hops | emits no OpenLineage | permanent; can never be auto-crossed |
| Kimball fact producers | declared only in Python | **every mart recovery is operator-approved today** |
| `dim_customer_bi` | built in `spark/reporting/`, in no config | also an open `undeclared_asset` finding |

The second row is the live cost of the gap, and it is deliberate: until the fact builders
read a config contract the way `curated_build` reads `entities.yaml`, a human approves.

## 7. Not yet

No event has been emitted — `enabled: false`, nothing installed, no DataHub. The graph above
is assembled **offline from config and artefacts**, which is why it can be tested at all.
DRP3's listener upgrades rows 2–4 of §2 from `declared` to `observed` when it first runs.
