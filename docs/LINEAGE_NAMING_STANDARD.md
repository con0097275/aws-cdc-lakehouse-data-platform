# LINEAGE NAMING STANDARD

- Phase: DRP6 · Code: `cdc/assets.py`, `cdc/urns.py`, `cdc/lineage_runtime.py`
- Related: ADR-089 (one URN per table), ADR-090 (identity carried, not inferred)

---

## 1. The rule

> **One table, one URN. One job, one name. Both derived, never typed.**

Everything below follows from that, and the reason is single: a second name for one thing
splits the lineage graph into fragments that each look complete, and every impact query then
returns a partial answer with nothing saying so.

## 2. Asset identity

`kind:name`, minted by `cdc/assets.py`:

| Kind | Example |
|---|---|
| `src` | `src:oracle.coredb.corebank.account` |
| `topic` | `topic:cdc.oracle.COREBANK.ACCOUNT` |
| `full_cdc` / `realtime` / `eod` | `eod:oracle_coredb_corebank_account` |
| `curated` / `mart` / `serving` | `mart:mart_customer_360_daily` |
| `bi_dataset` / `bi_report` | `bi_report:balances` |

The three lake layers of one table share one `lineage_key`, which is how impact analysis
moves sideways from `eod:X` to `realtime:X`.

**The kind is part of the identity.** `dim_customer` is CURATED and `customer` is a source
table, a FULL_CDC table, a REALTIME table and an EOD table. The DRP0 audit found that
ambiguity resolved *differently* in different files.

## 3. DataHub URN

```
urn:li:dataset:(urn:li:dataPlatform:<platform>,<name>,<FABRIC>)
```

| Asset kind | platform | name |
|---|---|---|
| `src` (Oracle) | `oracle` | `coredb.corebank.account` |
| `src` (SQL Server) | `mssql` | `digital.dbo.app_user` |
| `topic` | `kafka` | the real topic, engine-cased |
| every lake kind | **`glue`** | `<database>.<table>` |
| BI | `powerbi` | the BI name |

All five lake kinds are `glue`: an Iceberg table registered in Glue is **one** dataset that
happens to be stored in Iceberg.

`UrnMinter.asset_for()` is the exact inverse, because the graph speaks URNs and the
incident/recovery contracts speak `AssetId` — translating at the boundary keeps a URN out of
a field that expects a kind:name.

## 4. Fabric

| Environment | Fabric | Namespace |
|---|---|---|
| `dev` | `DEV` | `cdc-lakehouse-dev` |
| `test` | `QA` | `cdc-lakehouse-test` |
| `prod` | `PROD` | `cdc-lakehouse-prod` |

Two environments may not share one. `assert_fabric()` runs on **every** emit. A dev edge on
a prod dataset makes the production graph confidently wrong and nothing downstream can tell.

Jobs carry the environment as the DataFlow **cluster**
(`urn:li:dataFlow:(airflow,eod_build,dev)`); domains are name-scoped
(`urn:li:domain:core_banking-dev`) because DataHub domains have no fabric component.

## 5. Job names

Nine, declared once in `governance/registry/openlineage.yaml`:

```
full_cdc_ingestion   realtime_materialize   eod_build       dbt_spark_build
auto_correct         fulfill                stream_batch    streaming_rt
iceberg_maintenance
```

An undeclared name raises. A job named from a DAG id in one place and a Spark app name in
another appears twice in the graph and neither copy has the full run history — which reads
as patchy instrumentation rather than a naming bug, so nobody fixes it.

## 6. Run identity

`run_id` is a UUID per run, validated. OpenLineage keys a run by it: a repeated id merges
two runs, and the second run's inputs appear to belong to the first. A retry is a **new
run** with its own id and an `attempt` facet.

Parent-child: the Airflow task is the parent of the Spark app it submits
(`spark.openlineage.parentRunId`). Without it the Spark app is an orphan, and the hole in
the run hierarchy is exactly where the orchestration is.

## 7. Forbidden

| Never | Because |
|---|---|
| a dataset name formatted at a call site | the second spelling is the second entity |
| a job name derived from a DAG id or Spark app name | two jobs, neither with full history |
| a URN whose fabric is not the emitter's | a confident lie about production |
| an aspect name not in `KNOWN_ASPECTS` | DataHub accepts and silently drops it |
| a facet outside the allowlist | reviewed-looking fields nobody reviewed |
| `(`, `)` or `,` inside a URN component | the name cannot be parsed back |
