# CDC table config reference

Field-by-field reference for `cdc/registry/sources.yaml`. Implemented in Phase 1
(ADR-062). Companion: `docs/CDC_TABLE_DEVELOPER_WORKFLOW.md`.

```
cdc/
  registry/sources.yaml   the registry — the only place a captured table is declared
  models.py               typed, frozen dataclasses + closed enums
  naming.py               canonical id and derived names (pure functions)
  config_loader.py        inheritance + semantic validation
  table_plan.py           canonical payload + sha256
  compile.py              CLI: --check / --out / --verify
artifacts/cdc/table-plan.json   the compiled plan
```

`cdc` is a **real package with relative imports**, run as `python3 -m cdc.compile`. That is
not cosmetic: `spark/reporting/` uses flat module names (`models`, `config_loader`, `plan`)
and is imported by putting its directory on `sys.path`. When `cdc/` was also flat, adding it
to `sys.path` shadowed those modules and broke 19 unrelated test files at collection.

---

## 1. Precedence

```
defaults:                 <   sources[].defaults:        <   sources[].tables[]:
(global)                      (per source)                   (per table)
```

Merged one level deep, so `realtime: {window_hours: 96}` on a table keeps the inherited
`late_grace_hours` and `retention_hours`. **Lists are replaced wholesale**, never merged
element-wise — otherwise "clear this list" would be inexpressible.

## 2. Identity and derived names

Canonical id: `source_id.database.schema.table`, each part normalised to
`^[a-z][a-z0-9_]*$`. An identifier that will not normalise is **rejected, not mangled** —
silently rewriting a name is how a table ends up somewhere nobody looks for it.

One logical name drives all three layers, so they cannot drift apart:

```
oracle + coredb + bank + account
    logical   oracle_coredb_bank_account
    FULL_CDC  cdc_oracle_coredb_bank_account
    REALTIME  rt_oracle_coredb_bank_account
    EOD       eod_oracle_coredb_bank_account
```

Topic names are engine-specific and **not** normalised — they must match what the connector
actually produces:

| engine | topic |
|---|---|
| Oracle | `{prefix}.{SCHEMA}.{TABLE}` — **UPPERCASE**, because Oracle folds unquoted identifiers |
| SQL Server | `{prefix}.{database}.{schema}.{table}` — includes the DB, no folding |

Getting Oracle's casing wrong is not a loud failure: with `auto.create.topics.enable=false`
the connector reports RUNNING, the topic sits at offset 0, and every health check stays
green. That has happened in this repo, which is why the rule lives in one function.

## 3. Source fields

| field | required | notes |
|---|---|---|
| `source_id` | yes | first component of the canonical id |
| `engine` | yes | `oracle` \| `sqlserver` |
| `connector` | yes | Kafka Connect connector name |
| `database`, `schema` | yes | inheritable defaults for the source's tables |
| `topic_prefix` | yes | e.g. `cdc.oracle` |
| `defaults` | no | applied to every table of this source |
| `tables` | yes | list of table entries |

## 4. Table fields

### Identity and governance

| field | default | notes |
|---|---|---|
| `table` | — | **required** |
| `primary_key` | — | **required** unless `primary_key_unsupported: true`. Also renders `message.key.columns` |
| `primary_key_unsupported` | `false` | must be explicit. An empty PK is a defect, not a shrug |
| `owner` | — | **required** (CLAUDE.md §6) |
| `classification` | `internal` | `public` \| `internal` \| `confidential` \| `restricted`. Drives IAM scope and the AI-plane deny list |
| `domain` | `unassigned` | ADR-028 |
| `enabled` | `true` | `false` keeps the entry and its history without capturing |

### Layer policy

| field | default | validated |
|---|---|---|
| `realtime.enabled` | `true` | |
| `realtime.window_hours` | `72` | must be > 0 |
| `realtime.late_grace_hours` | `24` | must be ≥ 0 |
| `realtime.retention_hours` | `168` | **must be ≥ window + grace** — otherwise the layer expires rows it is still responsible for serving |
| `eod.enabled` | `true` | needs a PK **and** an ordering column |
| `eod.cutoff_policy` | `source_commit_ts` | \| `event_ts` (CLAUDE.md §5.6) |
| `eod.business_timezone` | `UTC` | must be a real IANA zone — a wrong one moves the day boundary (ADR-024) |
| `eod.delete_policy` | `exclude_from_snapshot` | \| `soft_flag`. Explicit by requirement (CLAUDE.md §5.7) |
| `eod.retention_days` | `365` | must be > 0 |

### Storage

| field | default | validated |
|---|---|---|
| `partition_spec` | `[{column: event_date, transform: identity}]` | transform ∈ identity/day/month/bucket; `bucket` requires `num_buckets` 1..4096; `num_buckets` is rejected on any other transform; **identity on a PK column is rejected** — one partition per row (CLAUDE.md §6); the column must be one of `event_date`, `source_commit_ts`, `kafka_timestamp`, `op`, `dv_pk_hash`; `day()`/`month()` need a temporal column; **`source_system` / `source_database` / `source_schema` / `source_table` are rejected** — constant within a per-table target (ADR-063) |
| `write.format_version` | `2` | must be 2 |
| `write.compression` | `zstd` | |
| `write.target_file_size_mb` | `128` | must be > 0 |
| `write.distribution_mode` | `hash` | |
| `schema_evolution` | `additive_only` | \| `full` \| `frozen`. `frozen` makes an unexpected producer column fail the write instead of widening the table |
| `payload.mode` | `json` | \| `typed`. `typed` adds struct columns from `payload.columns` **and keeps the JSON beside them** — a writer version can carry an undeclared field, and typed-only would drop it silently (ADR-063) |
| `payload.columns` | `[]` | required when `mode: typed`; each `{name, type, encoding}`; types are `string`/`boolean`/`tinyint`/`smallint`/`int`/`bigint`/`float`/`double`/`date`/`timestamp`/`binary`/`decimal(p,s)`; **every primary-key column must be declared** — `dv_pk_hash` and the EOD grain read the key out of the payload |
| `payload.columns[].encoding` | `none` | the **Debezium wire form**. `none` \| `timestamp_millis` \| `micro_timestamp` \| `nano_timestamp` \| `date_days` \| `zoned_timestamp`. **Mandatory for a `timestamp` or `date` column** — see below |

### Why a typed temporal must declare its encoding

`decimal.handling.mode: precise` produces `org.apache.kafka.connect.data.Decimal`, a Kafka
Connect **built-in** logical type, so the Avro converter carries it as bytes with an Avro
decimal logical type and `from_avro` hands Spark a real `DecimalType`. Temporals do not work
that way: `io.debezium.time.Timestamp`, `MicroTimestamp`, `NanoTimestamp` and `Date` are
**custom** Connect logical types, which an Avro converter carries as the underlying
primitive. `from_avro` therefore yields a plain `int64`/`int32`.

Casting that number does not fail. Measured on this platform's own Spark:

```
epoch millis 1787356800000   CAST AS timestamp  ->  +58609-…      silently wrong
epoch micros                 CAST AS timestamp  ->  +109081-…     silently wrong
epoch days   (int32)         CAST AS date       ->  AnalysisException, batch dies
```

A year-58609 timestamp partitions, sorts and reconciles like a real value — it is a silent
change of what the data *means*, which the CDC contract forbids. So the compiler **refuses**
a typed `timestamp` or `date` column that declares no encoding, and the writer applies the
declared conversion instead of a cast. There is no default, because every possible default
is wrong for some column.

| encoding | Debezium type | wire form | conversion |
|---|---|---|---|
| `none` | — | already the declared type | plain cast (correct for strings, numbers, booleans, precise decimals) |
| `timestamp_millis` | `io.debezium.time.Timestamp` | int64 epoch millis | `timestamp_millis()` |
| `micro_timestamp` | `io.debezium.time.MicroTimestamp` | int64 epoch micros | `timestamp_micros()` |
| `nano_timestamp` | `io.debezium.time.NanoTimestamp` | int64 epoch nanos | `timestamp_micros(x div 1000)` — truncation is deliberate and visible; Spark has no nanosecond timestamp |
| `date_days` | `io.debezium.time.Date` | int32 days since epoch | `date_add(DATE'1970-01-01', x)` |
| `zoned_timestamp` | `io.debezium.time.ZonedTimestamp` | ISO-8601 string | `to_timestamp()` — parsed, not cast |

Which one a column uses depends on the **connector's `time.precision.mode`**, which is why
the mode is named in the code rather than assumed. As deployed: Oracle
`adaptive_time_microseconds` (DATE and TIMESTAMP(≤3) → millis, TIMESTAMP(4–6) → micros,
TIMESTAMP(7–9) → nanos); SQL Server `adaptive` (DATE → day count, DATETIME and
DATETIME2(≤3) → millis, DATETIME2(4–6) → micros, DATETIME2(7) → nanos, DATETIMEOFFSET →
ISO string). `scripts/cdc-table-new.py --discover` emits the right encoding per column from
the source DDL and these modes.

The JSON images are kept alongside the typed struct, so a wrong encoding is fixable by a
**rerun** rather than a re-capture — the unconverted wire value is still in the row.

### Quality and maintenance

| field | default | notes |
|---|---|---|
| `dq.not_null` | `[]` | |
| `dq.event_date_null_tolerance` | `0` | **Leave at 0.** `event_date IS NULL` is never a legitimate business state in FULL_CDC — it is the signature of an undecodable record that slipped the quarantine filter (`docs/CDC_TABLE_PLATFORM_TARGET.md` §1.3) |
| `dq.freshness_sla_minutes` | `60` | must be > 0 |
| `maintenance.compact_target_mb` | `128` | |
| `maintenance.expire_snapshots_days` | `7` | |
| `maintenance.remove_orphan_files_days` | `3` | **must be ≤ expire_snapshots_days** — a shorter orphan window deletes files a live snapshot still references |

### Overrides

`target.{topic,full_cdc,realtime,eod}` bypass derivation. Use only to adopt a table that
already exists in the catalog under another name. Two tables resolving to the same target
are **rejected** — one would silently overwrite the other.

## 4b. Worked examples

Every block below is **extracted and compiled by
`test_cdc_registry.py::TestDocumentedExamplesCompile`**, so an example that stops being valid
fails the suite rather than misleading the next reader. Examples in documentation rot silently
otherwise — this reference had none for exactly that reason, and a newcomer had to open
`cdc/registry/sources.yaml` to learn the shape.

**The minimum.** Three lines. Everything else is inherited or derived:

```yaml
- table: account
  primary_key: [ACCOUNT_ID]
  dq: {not_null: [ACCOUNT_ID]}
```

**A near-static reference table.** A 72-hour rolling window maintained for a table that
changes four times a day is a standing cost for noise, so it opts out:

```yaml
- table: branch
  primary_key: [BRANCH_ID]
  realtime: {enabled: false}
  dq: {not_null: [BRANCH_ID]}
```

**A high-volume table.** Tighter freshness and larger compaction target than the shared
defaults; `temperature` is stated rather than inferred:

```yaml
- table: transaction
  primary_key: [TRANSACTION_ID]
  dq: {not_null: [TRANSACTION_ID, ACCOUNT_ID], freshness_sla_minutes: 15}
  maintenance: {compact_target_mb: 256, temperature: hot}
```

**A confidential table with a typed payload.** Note `encoding` on the temporal — it is not
optional, and §4 explains why an omitted one produces year 57609 with no error:

```yaml
- table: payment_method
  primary_key: [payment_method_id]
  classification: confidential
  dq: {not_null: [payment_method_id, user_id]}
  payload:
    mode: typed
    columns:
    - {name: payment_method_id, type: "bigint", encoding: none}
    - {name: masked_last4, type: "string", encoding: none}
    - {name: is_default, type: "boolean", encoding: none}
    - {name: created_at, type: "timestamp", encoding: micro_timestamp}
```

**A calendar-day window.** Days rather than hours, so the boundary snaps to midnight in the
table's business timezone instead of moving continuously:

```yaml
- table: loan
  primary_key: [LOAN_ID]
  dq: {not_null: [LOAN_ID, CUSTOMER_ID]}
  realtime:
    recovery: {lookback_days: 5, late_arrival_grace_days: 3, physical_retention_days: 14}
  eod: {schedule: "30 2 * * *"}
  maintenance: {temperature: warm}
```

> Retention must be **at least** lookback + grace, and the compiler enforces it: 14 ≥ 5 + 3.
> A shorter retention deletes rows the window is still supposed to serve, and the result is
> not an error — it is a window that quietly returns less than it claims.

**Restricting maintenance.** List what the table permits. Omit the key for the default set;
an **empty list is refused**, because it is indistinguishable from a mistake and there is no
way to spell "never maintain this table":

```yaml
- table: archive_ledger
  primary_key: [LEDGER_ID]
  dq: {not_null: [LEDGER_ID]}
  maintenance: {actions: [rewrite_data_files, expire_snapshots]}
```

## 5. Semantic validation

Every check runs at compile time, locally, with no AWS:

```
source exists · table id unique · target name collision · PK non-empty unless declared
duplicate PK columns · owner present · EOD needs PK · EOD needs ordering
valid IANA timezone · realtime retention >= window + grace · positive retentions
orphan cleanup <= snapshot expiry · format-version 2 · positive file size
valid transform · valid bucket count · num_buckets only on bucket
no identity partition on a PK column · valid classification / delete policy /
schema evolution / engine
```

`ordering_columns: []` set **explicitly** means "this source provides no ordering" and will
fail an enabled EOD. Omitting the key inherits the engine default. Presence is checked, not
truthiness — `or DEFAULT` would silently replace an explicit empty list and let an
unorderable table enable EOD.

## 6. Compiled plan

`artifacts/cdc/table-plan.json`

```json
{ "plan_hash": "...", "config_version": "cdccfg-...",
  "generated_at": "...", "source_sha256": "...",
  "plan": { "plan_schema_version": 2, "environment": "dev",
            "catalog": {"catalog": "glue_catalog", "databases": {...}},
            "event_index": {"table": "cdc_event_index", "identifier": "..."},
            "sources": [...], "tables": [ { ... "config_hash": "..." } ] } }
```

Each table entry carries resolved source, connector, topic, include-list and key-columns
entries, all three target names **and their fully-qualified identifiers**, the payload
contract, partition spec, write properties, realtime policy, EOD policy, DQ, maintenance,
schema evolution, SLA, governance and its own `config_hash`. **Nothing is inherited at read
time** — a consumer must never need the registry to interpret a plan.

**Schema version 2** adds the resolved catalog (`reporting/layers.yaml`, ADR-033), the
qualified identifiers and the payload contract. The router refuses a version-1 plan rather
than routing against bare table names, which would resolve against whatever database the
session happened to default to. The catalog is inside the hashed payload because it is part
of what the plan *means*: the same registry compiled against different layer bindings
provisions different tables.

`generated_at` and `source_sha256` sit **outside** the hashed payload: the first is
wall-clock and would destroy determinism, the second is provenance about the input file
rather than part of what the plan means.

**Determinism is tested, not asserted.** `--check` recompiles and compares; the suite also
proves that reordering tables in YAML does not change the hash, that a semantic change does,
and that a change to one table moves only that table's `config_hash`.

## 7. Commands

```bash
python3 -m cdc.compile --check                    # validate + prove determinism
python3 -m cdc.compile --out artifacts/cdc/table-plan.json
python3 -m cdc.compile --verify artifacts/cdc/table-plan.json   # drift detection

python3 scripts/cdc-table-new.py --source oracle --schema corebank --table loan
python3 scripts/cdc-table-new.py --source oracle --schema corebank --table loan --discover
python3 scripts/cdc-table-plan.py --table oracle.coredb.corebank.account
python3 scripts/cdc-table-plan.py --against artifacts/cdc/table-plan.json

python3 -m cdc.provision                              # DDL for every target; prints only
python3 -m cdc.provision --table oracle.coredb.corebank.account --layer FULL_CDC
```

All are local and read-only. None calls AWS, mutates a connector, or writes the registry.
`--discover` reads the columns and primary key from `docker/source-lab/<engine>/01-init.sql`
— the DDL that creates the captured tables — so it opens a file and never a connection.

Applying the DDL is a separate, gated step and is **dry-run by default**:

```bash
spark-submit spark/ops/provision_cdc_tables.py --plan artifacts/cdc/table-plan.json \
    --warehouse s3://<lake>/warehouse/                 # prints; creates nothing
spark-submit spark/ops/provision_cdc_tables.py --plan ... --warehouse ... --execute
spark-submit spark/ops/provision_cdc_tables.py --plan ... --warehouse ... --verify
```

`--verify` exits non-zero when a live table disagrees with the registry. It never drops a
column, never changes a type and never evolves a partition spec — those need a person and
a migration (ADR-063).

## 8. Table properties written by the provisioner

Every registry-derived property is namespaced `cdc.`. That is not tidiness: `owner` is a
**reserved table property** in Spark SQL, and `TBLPROPERTIES ('owner' = …)` fails the
statement outright with `UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY`.

| property | source |
|---|---|
| `cdc.table_id`, `cdc.layer`, `cdc.config_hash`, `cdc.config_version` | compiled plan |
| `cdc.owner`, `cdc.classification`, `cdc.domain`, `cdc.grain`, `cdc.primary_key` | governance (CLAUDE.md §6) |
| `cdc.dq.not_null`, `cdc.dq.event_date_null_tolerance`, `cdc.dq.freshness_sla_minutes` | `dq` |
| `cdc.retention_days` / `cdc.retention_hours`, `cdc.eod.*`, `cdc.realtime.*` | layer policy |
| `write.target-file-size-bytes`, `write.parquet.compression-codec`, `write.distribution-mode`, `format-version` | `write` |
| `write.metadata.metrics.column.*` | per column class — `full` on identity/temporal columns, `none` on payloads |

**Target file size is 128 MiB, not Iceberg's 512 MiB default.** Phase 0 measured a mean data
file of 137 KiB against that default: file size is set by commit frequency, not by the
property, so raising it changes nothing except the number a reader compares against.
Compaction is the remedy (`maintenance.compact_target_mb`).

---

## REALTIME shape and write strategy

`refresh_mode` answered two independent questions with one word. They are now two fields.

| field | question | values |
|---|---|---|
| `shape` | **what is in the table** | `event_window`, `latest_state` |
| `write_strategy` | **how it is written** | `overwrite_window`, `append`, `guarded_merge` |

**`shape` is the contract.** It is the only field a downstream reader needs in order to know
whether it may read the table as current state:

* `event_window` — the events themselves, bounded by the window. A row updated three times
  appears three times. A reader wanting current state must collapse it itself.
* `latest_state` — one row per business key. Readable as current state directly, subject to
  the tombstone rule (ADR-081): a deleted key is present with `is_deleted` set, not absent.

**`write_strategy` is the mechanism.** It changes cost and failure behaviour, never meaning.
Not every pair is legal, and the compiler refuses the rest:

| shape | allowed strategies | why the others are refused |
|---|---|---|
| `event_window` | `overwrite_window`, `append` | a guarded merge would collapse events to one row per key — a different product |
| `latest_state` | `guarded_merge` | an append duplicates the key; an overwrite drops the guard that stops a late out-of-order event regressing the row |

```yaml
- table: account
  primary_key: [ACCOUNT_ID]
  dq: {not_null: [ACCOUNT_ID]}
  realtime:
    shape: latest_state
    write_strategy: guarded_merge
    merge:
      ordering_strategy: source_native  # SCN/LSN, then commit ts, then partition, offset
      delete_policy: soft_tombstone
  eod:
    delete_policy: soft_flag            # REQUIRED by latest_state; see below
```

> Not shipped on any table yet — `latest_state` lands in **R2-D**, with the engine and the
> tests that make it safe. The example compiles today, which is what the doc-example test
> above guarantees.

`shape: latest_state` has three compile-time prerequisites:

1. **A usable business key.** No `primary_key`, or `primary_key_unsupported: true`, is
   refused. "One row per business key" has no meaning without the key — every event would
   hash to the same `dv_pk_hash`, the run would report SUCCESS, and the table would hold one
   row. Composite keys are fine.
2. **`eod.delete_policy: soft_flag`.** A tombstone is what the next run's guard compares
   against; removing the row lets a late out-of-order event resurrect a deleted key.
3. **Retention is not a prune.** For `latest_state` the retention numbers are a *recovery
   horizon* — how far back a rebuild reads. They never delete a row. A valid account may not
   change for months, and dropping its row because its last event is old would empty the
   table of exactly the entities that are most stable. `cdc-table-plan` prints this as
   `prunes_by_age`.

### `recovery:` — a spelling, not a second mechanism

`lookback_days`, `late_arrival_grace_days` and `physical_retention_days` may be written
under `recovery:`, which is what they have always meant: how far back a run reads to repair
itself. Identical to declaring them at the top level — proven by `plan_hash`, not by reading
both. Declaring the same field in both places is refused rather than resolved by precedence,
because one of the two would be ignored and nothing would say which.

### `processing.source_progress`

How a run finds the rows it has not handled yet.

| value | status |
|---|---|
| `window_scan` | what runs today: re-filter the whole window every run. Idempotent and self-healing, and it re-reads everything |
| `iceberg_snapshot` | **refused at compile until R2-C.** Accepting it would compile a plan claiming an incremental read while every run still scanned the full window |

`rebase.on_eod_certified: true` is refused the same way, until R2-F. A config value nothing
reads is the defect this project keeps rediscovering — `watermark_type: NONE`, the
`business_date` var, `--catalog`. Each compiled, looked live, and named behaviour that never
happened. The refusal is what gets deleted when the code lands.

### Migrating from `refresh_mode`

Still accepted, with a deprecation warning printed by `make cdc-compile`:

| `refresh_mode` | compiles to |
|---|---|
| `full_refresh` | `shape: event_window`, `write_strategy: overwrite_window` |
| `incremental_merge` | `shape: event_window`, `write_strategy: append` |
| `latest_state` | `shape: latest_state`, `write_strategy: guarded_merge` |

The two spellings **inherit as a group**: a level that declares either one replaces whatever
a less specific level said, in whichever spelling it said it. So a table written as
`refresh_mode:` still works under a `defaults:` block that declares `shape:`. Declaring both
at the *same* level is refused — there the author really did say one thing twice.
