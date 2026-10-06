# COLUMN LINEAGE STRATEGY

- Phase: DRP6 · Code: `cdc/lineage_graph.py` (`ColumnEdge`, `from_curated_entities`,
  `declare_column_lineage`)

---

## 1. The position

> **Column lineage is prioritised, not universal — and a fuzzy mapping on a critical field
> is worse than none.**

Worse, specifically, because it looks authoritative. Someone tracing a wrong balance follows
an inferred edge to a column that never fed it, concludes the upstream is fine, and stops
looking. No column lineage at all would have sent them to read the code.

## 2. Three sources, three evidence classes

| Where the transformation happens | Source | Evidence | Coverage today |
|---|---|---|---|
| SQL, inside dbt | dbt manifest / SQL parse | `declared` until confirmed | every mart model's columns |
| the conformance step (EOD → CURATED) | `reporting/curated/entities.yaml` | **`derived`** | all 8 conformed entities, 67 column edges |
| arbitrary Python in a Spark job | an explicit declaration | `declared` | critical fields only |

The middle row is the interesting one. `entities.yaml` is not a description of what
`curated_build.py` does — it is **the instruction that job follows**. That makes those column
edges `derived` rather than `declared`, and it is the one place in this platform where column
lineage exists through a non-SQL transformation.

```
eod_oracle_coredb_corebank_account.BALANCE   ->   curated.banking_account.balance
                                                 evidence: derived
                                                 source:   curated/entities.yaml
```

## 3. Why dbt's same-name mapping is only `declared`

dbt's manifest names a model's columns. It does **not**, on its own, prove which upstream
column each one came from — `balance` in a mart and `balance` in its source may be the same
value, a rounded one, or a different measure with a convenient name.

Claiming that as `derived` would be exactly the fuzzy inference this document refuses. So
same-name mappings are recorded as `declared` and must be confirmed before a critical field
relies on them. Real SQL column lineage (DataHub's dbt source with `include_column_lineage`)
upgrades them where the compiler can prove the mapping.

## 4. Which columns are critical

A column is critical when a wrong value is externally visible or regulated. Today that is
driven by the governance compile rather than a separate list:

- any column on a `tier_1` asset
- any column carrying `pii_fields`
- any measure a certified mart publishes

For those, an inferred mapping is not accepted as authoritative without validation.

## 5. Declaring a mapping through a Spark transform

```python
declare_column_lineage(graph, [{
    "upstream_dataset":   "<eod urn>",
    "upstream_column":    "BALANCE",
    "downstream_dataset": "<fact urn>",
    "downstream_column":  "closing_balance",
    "source":             "spark/facts/periodic_snapshot.py",
}])
```

Recorded as `declared` on purpose: a human asserted it. Marking it `derived` would claim a
proof that does not exist, and a critical field is precisely where a confident wrong answer
costs the most.

## 6. Querying it

```python
graph.column_upstreams(dataset_urn, "balance")
# [ColumnEdge(BALANCE -> balance, evidence=derived, source=curated/entities.yaml)]
```

Column-scoped impact (`LineageImpactService.downstream(..., column=...)`) narrows the blast
radius where column edges are known and **falls back to dataset scope where they are not** —
narrowing on an unknown mapping would *exclude* affected assets, which is the dangerous
direction.

## 7. What is not covered

| Gap | Owner |
|---|---|
| SQL-parsed column lineage inside dbt models | DRP4's dbt recipe has `include_column_lineage: true`; nothing has ingested yet |
| the Kimball fact builders (`spark/facts/`) | no declaration yet — the dataset edge is declared in `governance/registry/lineage_declared.yaml`, the column edges are not |
| Power BI measure lineage | no workspace exists |
