# FULL_CDC, per source table — the canonical layer

**Contract**: ADR-062 (granularity), ADR-063 (routing and provisioning).
**Code**: `cdc/rowspec.py`, `cdc/provision.py`, `spark/jobs/full_cdc/per_table.py`.
**The monolith it sits beside**: `L1_FULL_CDC.md`. **Migrating between them**:
`CDC_TABLE_MIGRATION.md`.

FULL_CDC is the **canonical durable CDC truth**: every I/U/D with its full envelope and Kafka
metadata, append-oriented, long-retention, rebuild- and replay-capable. REALTIME and EOD are
**siblings** derived from it, never a chain.

---

## One Iceberg table per source table

```
kafka_dev_lab_dev_full_cdc.cdc_oracle_coredb_corebank_account
kafka_dev_lab_dev_stream .rt_oracle_coredb_corebank_account
kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account
```

Names are **derived** from the canonical id `source.database.schema.table`, never typed. That
is what stops the three layers' names for one table drifting apart.

The physical Glue databases keep their deployed names (`_stream`, `_snapshot`) rather than
being renamed to match the logical layers — `reporting/layers.yaml` is the one file that maps
logical to physical, and three of those databases hold data applied 2026-08-15.

## Do not partition a constant

For a table that *is* `oracle.coredb.corebank.account`, the columns `source_system`,
`source_database`, `source_schema` and `source_table` are **constant**. Partitioning by them
creates one partition and buys nothing.

```
monolith   PARTITIONED BY (source_system, event_date)
per-table  PARTITIONED BY (event_date)
```

The columns are still **carried on every row** — dropping them would make the layer
unreadable next to the monolith and break reconciliation. They are stored, not partitioned.

Bucketing (`bucket(N, dv_pk_hash)`) is supported and **off by default**. It needs a measured
justification, and at this project's volumes there is none: see the measured skew in
`CDC_TABLE_PLATFORM_TARGET.md` §12.2.

## The row contract — 21 metadata columns, plus payload

| group | columns |
|---|---|
| identity | `dv_event_id`, `event_id`, `dv_src_event_id`, `dv_pk_hash` |
| source identity | `source_system`, `source_database`, `source_schema`, `source_table` |
| change | `op`, `event_date`, `source_commit_ts` |
| source position | `position_primary`, `position_secondary` |
| Kafka coordinates | `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` |
| provenance | `schema_global_id`, `ingested_at`, `ingest_run_id`, `ingest_job` |

### Two identities, and they answer different questions

* **`dv_event_id`** = `sha2(topic|partition|offset|kafka_timestamp)` — the **transport**
  identity, and the MERGE key. Re-reading the same Kafka record produces the same id, which is
  what makes a rerun idempotent.
* **`dv_src_event_id`** — the **source change** identity. Two Kafka records for one source
  change (a retried produce, a connector restart) share this and differ on the first.

Collapsing them into one would make the platform unable to distinguish "delivered twice" from
"changed twice", which is the difference between a duplicate and a fact.

`dv_pk_hash` is the grain: `sha2` over the primary-key values, coalescing after-image with
before-image, because a delete carries a NULL after-image and its identity lives in `before`.

> **Live caveat, recorded rather than smoothed over**: `dv_pk_hash` is NULL for **100% of
> backfilled rows**, because the legacy monolith has no such column to copy. Live-captured
> rows carry it — both Phase 8 tables are non-NULL throughout. EOD recomputes the grain by the
> identical expression, so closes are correct either way; anything else reading the column on
> backfilled data would collapse every row onto one key. Open issue #2.

## Ordering is source-native

`position_primary` / `position_secondary` hold the engine's own coordinates:

| engine | form | comparison |
|---|---|---|
| Oracle | numeric SCN, e.g. `3287944` | `lpad(pos, 24, '0')` — `'9' > '10'` as strings |
| SQL Server | hex LSN triplet, e.g. `0000002c:00006d50:003a` | `lower(pos)`, already fixed-width |

Both yield a sortable **string**, so one ORDER BY serves both engines and no comparison
depends on a cast that can return NULL. **Kafka offset is never a global comparator** — it is
monotonic only within one partition, so `kafka_partition` precedes `kafka_offset` in every
tie-break (CLAUDE.md §5.4).

## `event_date` is a control, not just a partition key

`event_date IS NULL` is never a legitimate state in this layer — every Debezium envelope
carries `source.ts_ms`. A NULL is a reliable signature that an undecodable record slipped the
quarantine filter, so `dq.event_date_null_tolerance: 0` is a registry **default** and every
table inherits it. Root cause and closure: `CDC_TABLE_PLATFORM_TARGET.md` §1.3.

## The ingest never creates a table

An event whose table is not in the registry is **refused**, and the message names the
registration step:

```
cdc.oracle.COREBANK.MORTGAGE is not in the registry. Register it in
cdc/registry/sources.yaml and provision it BEFORE it is captured --
this platform never creates a table
```

An auto-created table is an unowned, unclassified, unretained data product. Provisioning is a
separate, reviewed act (ADR-063 §F).

## Write properties

| property | value | why |
|---|---|---|
| `write.distribution-mode` | `hash` | avoids one file per task per partition |
| `write.format.default` | `parquet` | with Iceberg v2 |
| `write.parquet.compression-codec` | `zstd` | |
| `write.target-file-size-bytes` | 128 MiB (256 for hot tables) | |
| `write.metadata.metrics.column.payload_*` | `none` | **PII never enters Iceberg column stats**; identity and coordinate columns keep `full` |

Streaming microbatches produce files far under target — the Phase 0 audit measured a **137 KiB
mean against 128 MiB** — which is why maintenance is metric-driven rather than cron-driven.
See `ICEBERG_MAINTENANCE_RUNBOOK.md`.

## Rebuild and replay

FULL_CDC is the thing everything else is rebuilt *from*, so it is the one layer that is not
itself derived. Recovery is a Kafka replay bounded by topic retention (24h here), which is why
the monolith is retained as the reconciliation baseline for every window already captured.

REALTIME can be rebuilt from FULL_CDC with `full_refresh`; EOD with `--fulfill`. Neither needs
Kafka.
