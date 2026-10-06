# DATA CONTRACTS — Normative CDC Column Contract

- Session: 01
- Status: **NORMATIVE**
- Supersedes, on every point of disagreement: `reference/CDC_EVENT_CONTRACT.md`, `reference/ICEBERG_LAYER_SPEC.md`, and the column *names* in `CLAUDE.md` §5 of the guide package
- Closes defects: **D1, D2, D3, D4, D5, D7, D8, D9**
- Enforced by: `scripts/validate-docs.py`

---

## 0. Why this document exists

`docs/GAP_ANALYSIS.md:141` flagged D1–D5 as "the dangerous cluster": five mutually
inconsistent definitions of the same CDC event → L1 → L2 → L3 column lineage,
spread across three documents, in a project where `CLAUDE.md` §5 makes ordering
correctness a **hard invariant**. Concretely, before this document:

- the L3 build SQL at `reference/ICEBERG_LAYER_SPEC.md:79-84` referenced four
  columns (`source_commit_ts`, `source_order_1`, `source_order_2`) that **no layer
  defined**, so it could not have run;
- the ordering key had **three different names** across three documents;
- SQL Server's event-serial-number tie-breaker — required by `CLAUDE.md` §5.5 —
  had **no column to live in**, because `source_commit_position.secondary` was
  dropped when the nested struct was flattened to a string;
- nine envelope fields were specified and then silently dropped from the L1 column
  list, contradicting `CLAUDE.md` §5.2's requirement that L1 preserve all source
  and Kafka metadata;
- the partition spec partitioned on `event_ts` + `source_system`, **neither of
  which appeared in the L1 column list**.

This document is the single source of truth. `scripts/validate-docs.py` fails the
build if any other document reintroduces a rejected name.

**The `CLAUDE.md` invariants themselves are unchanged.** This document does not
relax a single correctness rule; it makes them implementable by giving every rule
a column that exists.

---

## 1. Locked global conventions

| Convention | Value | Rationale |
|---|---|---|
| **Timezone** | **UTC**, everywhere, no exceptions | ADR-024. Closes D8. Debezium `source_ts_ms` is already epoch-UTC; Iceberg timestamps stay unambiguous; no DST or offset arithmetic in any cutoff. `business_date`, `snapshot_date` and all Airflow schedules are UTC. |
| **Timestamp physical type** | `timestamp` = Iceberg `timestamptz` (microsecond, UTC-normalized) | Avoids the Iceberg `timestamp` vs `timestamptz` ambiguity that silently shifts values between Spark and Athena. |
| **Cutoff window** | `[T 00:00:00 UTC, T+1 00:00:00 UTC)` — left-closed, right-open | Half-open intervals cannot double-count or skip a boundary event. |
| **Table format** | Iceberg v2, Parquet, zstd | `CLAUDE.md` §6, ADR-006. |
| **Catalog** | AWS Glue Data Catalog | `CLAUDE.md` §6, ADR-007. |
| **Naming case** | `snake_case`, lower, ASCII | Athena folds identifiers to lowercase; mixed case round-trips badly through Glue. |
| **Decimal handling** | Preserve source precision/scale as Avro `decimal` logical type; never widen to `double` | Silent precision loss in financial data is unrecoverable after the fact. |

### 1.1 Rejected names — never reintroduce

`scripts/validate-docs.py` greps for these and fails if found outside this section.

| Rejected | Appeared in | Replaced by |
|---|---|---|
| `event_order_key` | `reference/CDC_EVENT_CONTRACT.md:63` | `event_order` |
| `source_order_1` | `reference/ICEBERG_LAYER_SPEC.md:79` | `event_order.position_primary` |
| `source_order_2` | `reference/ICEBERG_LAYER_SPEC.md:80` | `event_order.position_secondary` |
| `source_ts` (bare) | `reference/ICEBERG_LAYER_SPEC.md:27` | `source_commit_ts` |
| `event_ts` | `CLAUDE.md` §5.6, `reference/ICEBERG_LAYER_SPEC.md:9` | `source_commit_ts` (deprecated synonym — see §1.2) |
| `source_commit_position` as a nested struct | `reference/CDC_EVENT_CONTRACT.md:42-46` | three flat columns — see §3.2 |

### 1.2 `event_ts` is a deprecated synonym

`CLAUDE.md` §5.6 offers two spellings of the same cutoff — `event_ts < T00:00` **or**
`source_commit_ts <= cutoff`. Both are satisfied by defining:

```text
event_ts  ≡  source_commit_ts        (same column; event_ts is not materialized)
```

The `<` form is the correct one (§1 half-open window). `CLAUDE.md`'s `<=` variant
would include the boundary event in both windows; **use `<`**. This is a defect in
the guide's prose, not a decision, and is recorded as such in `DECISION_LOG.md`.

---

## 2. Domain taxonomy (closes D9)

`ARCHITECTURE.md:71-76` builds S3 paths from `<domain>` and names L3 tables
`<domain>_<entity>`, but nothing ever defined a domain. Normative mapping:

```text
domain = f(source_system, source_database, source_schema)
```

Registered in **one** place — `governance/catalog/domains.yml` — never inferred at
runtime, so a schema rename cannot silently relocate a table's S3 prefix.

| `source_system` | `source_database` | `source_schema` | `domain` |
|---|---|---|---|
| `oracle_core` | `COREDB` | `BANK` | `core_banking` |
| `oracle_core` | `COREDB` | `PARTY` | `party` |
| `sqlserver_ops` | `OPSDB` | `dbo` | `operations` |
| `sqlserver_ops` | `OPSDB` | `sales` | `sales` |

Rules:

1. A domain is a **business** grouping, never a technical one. Do not create a
   domain per source system.
2. `entity` in `snapshot.<domain>_<entity>` is the business entity name, which may
   differ from the physical table name (`CUSTOMER` → `customer`).
3. A `(source_system, source_database, source_schema)` triple with no registered
   domain is a **hard failure** at job start, not a fallback to `unknown`. Silent
   fallbacks put production data in the wrong prefix.
4. Adding a domain is a reviewed change to `domains.yml` plus an S3 prefix and a
   Glue database grant. It is not a code change.

---

## 3. Normalized CDC envelope (Kafka value)

Produced by Debezium plus a Single Message Transform chain; consumed by the L1
Spark job. This is the wire contract.

### 3.1 Topic key

Canonical primary key, deterministic field order (`reference/CDC_EVENT_CONTRACT.md`
is correct here and is carried forward unchanged):

```json
{"customer_id": "C000123"}
```

`CLAUDE.md` §5.1 and §5.4: the key **is** the canonical PK, so the same PK always
lands in the same partition and per-PK ordering is preserved. Never key on the full
row. Never reorder key fields — Avro field order changes the serialized bytes and
therefore the partition assignment, which silently breaks ordering for existing
keys.

### 3.2 Envelope

```json
{
  "event_id": "sha256(...)  see section 3.3",
  "event_version": 1,

  "source_system": "oracle_core",
  "source_database": "COREDB",
  "source_schema": "BANK",
  "source_table": "CUSTOMER",

  "operation": "c|u|d|r",
  "primary_key": {"customer_id": "C000123"},
  "before": {},
  "after": {},

  "source_position_type": "oracle_scn|sqlserver_lsn",
  "source_position_primary": "0000002A:00000C38:0001",
  "source_position_secondary": "3",

  "source_ts_ms": 1786483200000,
  "transaction_id": "3.28.1234",
  "snapshot_flag": "true|false|last",

  "schema_subject": "cdc.oracle.COREDB.BANK.CUSTOMER.v1-value",
  "schema_id": "12345",
  "ingest_ts": "2026-08-12T00:00:00Z",
  "trace_id": "0af7651916cd43dd8448eb211c80319c",

  "kafka_topic": "cdc.oracle.COREDB.BANK.CUSTOMER.v1",
  "kafka_partition": 0,
  "kafka_offset": 918273,
  "headers": {}
}
```

**Change from `reference/CDC_EVENT_CONTRACT.md` (closes D3).** The nested
`source_commit_position {type, value, secondary}` struct is **flattened to three
scalar columns**:

| Was | Now | Type |
|---|---|---|
| `source_commit_position.type` | `source_position_type` | `string` |
| `source_commit_position.value` | `source_position_primary` | `string` |
| `source_commit_position.secondary` | `source_position_secondary` | `string` |

Why flatten: `reference/ICEBERG_LAYER_SPEC.md:26-27` had already half-flattened it
to `source_commit_position string` + `source_position_type string` and **lost
`secondary` entirely** — which is exactly where SQL Server's event serial number
belongs. `CLAUDE.md` §5.5 requires that tie-breaker. Flat scalar columns also sort
and prune far better in Parquet than struct fields.

Types stay `string`, not numeric: Oracle SCN fits in a 64-bit integer but SQL
Server LSN is a 10-byte value with no lossless numeric representation. Ordering is
handled by §4, not by the physical type.

### 3.3 `event_id` — idempotency key

```text
event_id = lower(hex(sha256(
    source_system            || '|' ||
    source_database          || '|' ||
    source_schema            || '|' ||
    source_table             || '|' ||
    canonical_pk_json        || '|' ||
    source_position_type     || '|' ||
    source_position_primary  || '|' ||
    coalesce(source_position_secondary, '') || '|' ||
    operation
)))
```

- `canonical_pk_json` is the PK serialized with **sorted keys, no whitespace**:
  `{"account_id":"A001","transaction_id":"T999"}`. Sorting is what makes it stable
  across connector restarts and schema field-order changes.
- `source_position_secondary` is inside the hash. Without it, two SQL Server
  changes sharing a commit LSN but differing in event serial number collapse to one
  `event_id`, and L2's anti-join would **silently drop a real event**.
- `operation` is inside the hash so a delete and its preceding update at the same
  position stay distinct.
- Never a random UUID (`reference/CDC_EVENT_CONTRACT.md:81`) — replay must
  reproduce the identical `event_id` or L2 idempotency (`CLAUDE.md` §5.3) fails.

### 3.4 Operation semantics

| `operation` | Meaning | `before` | `after` |
|---|---|---|---|
| `c` | insert | null | populated |
| `u` | update | populated | populated |
| `d` | delete | populated | null |
| `r` | snapshot read | null | populated |

Kafka tombstones (null value) exist for log compaction. **L1 must not treat a
tombstone as the authoritative delete when a `d` envelope also exists** — that is
`CLAUDE.md` §5.7's "no ambiguous tombstone" rule. Debezium's
`tombstones.on.delete` is set to `false`; if a tombstone is nonetheless received,
L1 records it in `ops.tombstone_audit` and does not emit an L1 row.

---

## 4. `event_order` — the single ordering contract (closes D2)

One name: **`event_order`**. It is a **struct**, not a scalar, because ordering
requires up to five components and collapsing them into one sortable string is how
subtle ordering bugs get introduced.

```text
event_order struct<
  position_primary   : string,   -- Oracle commit SCN | SQL Server commit LSN
  position_secondary : string,   -- Oracle change SCN | SQL Server event serial number
  source_ts_ms       : long,     -- tie-breaker 1
  kafka_partition    : int,      -- tie-breaker 2
  kafka_offset       : long      -- tie-breaker 3
>
```

### 4.1 Comparison rule — normative

```sql
ORDER BY
  event_order.position_primary   DESC,   -- see 4.2 on collation
  event_order.position_secondary DESC,
  event_order.source_ts_ms       DESC,
  event_order.kafka_partition    DESC,
  event_order.kafka_offset       DESC
```

This is exactly the precedence in `CLAUDE.md` §5.5: source position first, then
`source_ts_ms`, then `kafka_partition`, then `kafka_offset`.

### 4.2 Position comparison must be collation-safe

A naive string compare is **wrong** for both sources:

- Oracle SCN is numeric; `'9' > '10'` lexicographically but `9 < 10` numerically.
- SQL Server LSN is a hex triplet `0000002A:00000C38:0001`; lexicographic compare
  happens to work **only** because every component is zero-padded to fixed width.

Normative rule: `position_primary` and `position_secondary` are stored **already
normalized to a fixed-width, zero-padded, lexicographically-sortable form** by the
L1 job, and the normalization is per `source_position_type`:

| `source_position_type` | Normalization | Example |
|---|---|---|
| `oracle_scn` | decimal, zero-padded to 24 chars | `000000000000000000123456` |
| `sqlserver_lsn` | uppercase hex, colons stripped, zero-padded to 24 chars | `0000002A00000C380001` → `0000002A00000C3800010000` |

The **raw, un-normalized** value is retained separately in
`source_position_primary` / `source_position_secondary` (§3.2) for audit and for
connector debugging. `event_order` holds the normalized form. Mixing the two is a
correctness bug; the column names differ precisely so that reviews catch it.

### 4.3 What ordering must never do

- **Never compare `kafka_offset` across partitions** (`CLAUDE.md` §5.4). Offsets are
  per-partition sequences; a cross-partition offset comparison is meaningless.
  `kafka_partition` precedes `kafka_offset` in the sort **only** to make the sort
  deterministic, never to imply cross-partition order.
- **Never order by timestamp alone** — commit timestamps collide at second/millisecond
  granularity and are subject to clock skew.
- **Never order by `ingest_ts`** — that is pipeline time, not event time.

---

## 5. ~~L1 STREAM~~ — DEPRECATED 2026-08-21 (columns retained for the read-only table)

> Kafka no longer lands here; the streaming append writes FULL_CDC directly. The database
> is retained read-only for Session 33 evidence. The envelope below is now the FULL_CDC
> envelope -- §6 carries the same columns. See `docs/TARGET_ARCHITECTURE.md` §3.

```text
Table:  glue_catalog.stream.<source_system>_<source_schema>_<source_table>
Grain:  one Kafka record = one CDC event
Write:  append-only, no updates, no deletes
```

| Column | Type | Source | Notes |
|---|---|---|---|
| `event_id` | `string` | §3.3 | idempotency key; not enforced unique at L1 |
| `event_version` | `int` | envelope | contract version, for evolution |
| `operation` | `string` | envelope | `c` / `u` / `d` / `r` |
| `primary_key` | `string` | envelope | canonical PK JSON, sorted keys |
| `before` | `string` | envelope | JSON; null for `c`/`r` |
| `after` | `string` | envelope | JSON; null for `d` |
| `source_system` | `string` | envelope | **partition column** |
| `source_database` | `string` | envelope | |
| `source_schema` | `string` | envelope | |
| `source_table` | `string` | envelope | |
| `domain` | `string` | §2 lookup | resolved at write time, never at read time |
| `source_position_type` | `string` | §3.2 | `oracle_scn` / `sqlserver_lsn` |
| `source_position_primary` | `string` | §3.2 | **raw** connector value |
| `source_position_secondary` | `string` | §3.2 | **raw**; SQL Server event serial number |
| `event_order` | `struct` | §4 | **normalized**; the only ordering input |
| `source_commit_ts` | `timestamp` | `source_ts_ms` → UTC | **the business time**; `event_ts` ≡ this |
| `ingest_ts` | `timestamp` | envelope | pipeline time; never used for ordering or cutoffs |
| `event_date` | `date` | `date(source_commit_ts)` | derived, materialized, for pruning |
| `transaction_id` | `string` | envelope | source transaction identifier |
| `snapshot_flag` | `string` | envelope | `true` / `false` / `last` |
| `schema_subject` | `string` | envelope | registry subject |
| `schema_id` | `string` | envelope | registry schema id |
| `trace_id` | `string` | envelope | correlation across Connect → Spark → Airflow |
| `kafka_topic` | `string` | Kafka metadata | |
| `kafka_partition` | `int` | Kafka metadata | |
| `kafka_offset` | `long` | Kafka metadata | |
| `kafka_timestamp` | `timestamp` | Kafka metadata | broker append time; diagnostics only |
| `headers` | `map<string,string>` | envelope | |
| `l1_run_id` | `string` | job | which Spark run wrote this row |
| `l1_write_ts` | `timestamp` | job | when |

**All nine fields D5 reported as dropped are present**: `event_version`,
`source_system`, `source_database`, `source_schema`, `source_table`,
`transaction_id`, `snapshot_flag`, `schema_subject`, `headers`. This is what
`CLAUDE.md` §5.2 requires.

`before` / `after` are JSON `string`, not `struct`. A `struct` would force every
source DDL change to become an Iceberg schema migration on L1; JSON keeps L1
schema-stable and pushes typed projection to L2/L3, where a migration is a
reviewed change rather than a stream outage.

### 5.1 Partition spec (closes D1)

```sql
PARTITIONED BY (source_system, days(source_commit_ts))
```

Both columns now exist — that was D1's entire complaint. Rules:

- **Never partition by `primary_key`** or any high-cardinality column
  (`CLAUDE.md` §6).
- `days()` is Iceberg hidden partitioning; queries filter on `source_commit_ts`
  and prune without naming a partition column.
- `source_system` leads because it is low-cardinality (2 values) and every
  reconciliation query filters on it.
- `event_date` is materialized **in addition** to the hidden partition, because
  Athena's Iceberg support prunes more reliably on an explicit date predicate.

---

## 6. FULL_CDC — canonical layer, written directly from Kafka

```text
Table:  glue_catalog.full_cdc.<source_system>_<source_schema>_<source_table>
Grain:  one unique source CDC event, keyed by event_id
Write:  EOD append, idempotent by event_id
```

Columns: **all L1 columns**, plus:

| Column | Type | Notes |
|---|---|---|
| `business_date` | `date` | the cutoff date `T` this row was assigned to (UTC) |
| `l2_run_id` | `string` | |
| `l2_write_ts` | `timestamp` | |

Partition spec: `PARTITIONED BY (source_system, days(source_commit_ts))` — same as
L1, so the EOD read from L1 and write to L2 prune identically.

### 6.1 EOD load — normative

```sql
-- Idempotent by event_id (CLAUDE.md 5.3). Rerunning the same business_date
-- must not duplicate, and must not collapse I/U/D for the same PK.
MERGE INTO full_cdc.<table> AS t
USING (
  SELECT *
  FROM stream.<table>
  WHERE source_commit_ts >= :cutoff_start      -- T 00:00:00 UTC, inclusive
    AND source_commit_ts <  :cutoff_end        -- T+1 00:00:00 UTC, exclusive
) AS s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT *;
```

- `WHEN NOT MATCHED THEN INSERT` only. **No `WHEN MATCHED THEN UPDATE`** — L2 rows
  are immutable history.
- **No business dedup** (`CLAUDE.md` §5.3): every I/U/D for a PK is retained. The
  `event_id` match is *operational* idempotency for reruns, not deduplication of
  distinct business events.
- Half-open window: `>= start AND < end`. A closed upper bound would assign a
  midnight-boundary event to two business dates.

### 6.2 Late-arriving events

An event whose `source_commit_ts` falls before the current cutoff window but which
arrives in L1 afterwards would be missed by §6.1's window. Normative handling:

```sql
WHERE (source_commit_ts >= :cutoff_start AND source_commit_ts < :cutoff_end)
   OR (l1_write_ts     >= :last_l2_watermark AND source_commit_ts < :cutoff_end)
```

The second clause sweeps anything L1 wrote since the previous L2 run regardless of
its commit time. `event_id` idempotency makes the overlap harmless — this is
exactly what that key buys. The watermark is stored in
`ops.layer_watermark(layer, table_name, watermark_ts, run_id)`.

---

## 7. EOD — as-of T-1, derived from FULL_CDC (closes D4)

```text
Table:  glue_catalog.snapshot.<domain>_<entity>
Grain:  one active record per business PK, as-of snapshot_date
Write:  full overwrite of the snapshot_date partition
```

`CLAUDE.md` §5.6: L3 is state **as-of cutoff T-1**, not "latest regardless of
cutoff". Rebuilding an old `snapshot_date` must reproduce the original result.

| Column | Type | Notes |
|---|---|---|
| *(projected business columns)* | *(typed)* | from `after`, typed per the registered schema |
| `primary_key` | `string` | canonical PK JSON |
| `snapshot_date` | `date` | **partition column**; the `T-1` this snapshot represents |
| `domain` | `string` | §2 |
| `source_system` | `string` | |
| `source_event_id` | `string` | the winning L2 `event_id` — full lineage to one event |
| `source_position_type` | `string` | |
| `source_position_primary` | `string` | winning raw position, for audit |
| `source_commit_ts` | `timestamp` | commit time of the winning event |
| `is_deleted` | `boolean` | always `false` in the active table — see §7.2 |
| `l3_run_id` | `string` | |
| `l3_write_ts` | `timestamp` | |

### 7.1 Build SQL — normative

Every column below exists in §6. `validate-docs.py` enforces that.

```sql
WITH ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY primary_key
      ORDER BY event_order.position_primary   DESC,
               event_order.position_secondary DESC,
               event_order.source_ts_ms       DESC,
               event_order.kafka_partition    DESC,
               event_order.kafka_offset       DESC
    ) AS rn
  FROM full_cdc.<table>
  WHERE source_commit_ts < :cutoff          -- as-of T-1 00:00:00 UTC, exclusive
)
SELECT
  <projected business columns from after>,
  primary_key,
  :snapshot_date  AS snapshot_date,
  :domain         AS domain,
  source_system,
  event_id        AS source_event_id,
  source_position_type,
  source_position_primary,
  source_commit_ts,
  false           AS is_deleted,
  :run_id         AS l3_run_id,
  current_timestamp() AS l3_write_ts
FROM ranked
WHERE rn = 1
  AND operation <> 'd';                     -- delete handling: section 7.2
```

Differences from `reference/ICEBERG_LAYER_SPEC.md:72-93`, all of them defect fixes:

| Was | Now | Why |
|---|---|---|
| `ORDER BY source_order_1, source_order_2, source_ts, ...` | `event_order.*` struct fields | D2/D4 — those three columns never existed |
| `WHERE source_commit_ts < :cutoff` against a column absent from L2 | same predicate, column now defined in §5/§6 | D4 |
| `SELECT <after columns>` unspecified | explicit projection + lineage columns | reproducibility |
| no `is_deleted` | `false AS is_deleted` | makes the contract explicit rather than implied by absence |

### 7.2 Delete semantics — explicit (`CLAUDE.md` §5.7)

Two tables, one decision, no ambiguity:

| Table | Contains | Rule |
|---|---|---|
| `snapshot.<domain>_<entity>` | **active** records only | `WHERE rn = 1 AND operation <> 'd'`. A PK whose latest event as-of cutoff is a delete is **absent**. |
| `snapshot.<domain>_<entity>_history` | every PK including deleted | same ranking, **no** `operation` filter, `is_deleted = (operation = 'd')`. Built only when `enable_snapshot_history = true` for that table. |

The `_history` variant is opt-in per table via `governance/catalog/domains.yml`,
because retaining deleted PII conflicts with erasure obligations and must be a
deliberate, recorded choice rather than a default.

A PK that is deleted and later re-inserted is handled correctly by construction:
the re-insert has a higher `event_order`, wins `rn = 1`, and `operation = 'c'`
passes the filter.

### 7.3 Reproducibility requirement

`MASTER_PLAN.md:69` Gate B requires that rebuilding L3 from L2 for the same cutoff
yields an equivalent checksum. That holds only if the ranking is **total** — no
ties. `event_order`'s five components make ties impossible: two distinct events
cannot share all of `(position_primary, position_secondary, source_ts_ms,
kafka_partition, kafka_offset)`, because the last two alone are unique per Kafka
record. This is the structural reason the tie-breaker chain is not optional.

---

## 8. S3 layout (closes D7)

Adds the missing `ops` prefix and enforces `CLAUDE.md` §5.9 (checkpoints must not
share a prefix with the warehouse).

```text
s3://<lake>/warehouse/stream/<domain>/<table>/
s3://<lake>/warehouse/full_cdc/<domain>/<table>/
s3://<lake>/warehouse/snapshot/<domain>/<table>/
s3://<lake>/warehouse/curated/<domain>/<table>/
s3://<lake>/warehouse/mart/<mart>/<table>/
s3://<lake>/warehouse/ops/<table>/                 <-- D7: was missing
s3://<lake>/warehouse/quarantine/<job>/

s3://<lake>/checkpoints/<job>/<source-topic>/      <-- separate top-level prefix
s3://<lake>/dq-results/<dataset>/<run-date>/
s3://<lake>/quarantine-payloads/<job>/<run-date>/
s3://<lake>/query-results/athena/
s3://<lake>/logs/airflow/
s3://<lake>/logs/emr-serverless/
```

`checkpoints/` is a **top-level sibling** of `warehouse/`, not a child. Iceberg
maintenance — specifically `remove_orphan_files` — walks the table's location and
deletes unreferenced files. A checkpoint under `warehouse/` would be unreferenced
by Iceberg metadata and therefore eligible for deletion, destroying streaming
state. This is the concrete failure `CLAUDE.md` §5.9 prevents.

### 8.1 `ops` tables

| Table | Purpose | Referenced by |
|---|---|---|
| `ops.reconciliation_run` | reconciliation ledger and certification status | `reference/RECONCILIATION_AND_ACCURACY.md` |
| `ops.layer_watermark` | per-layer high-watermark for late-event sweeps | §6.2 |
| `ops.tombstone_audit` | tombstones received when a `d` envelope also existed | §3.4 |
| `ops.dq_result` | DQ rule outcomes per dataset per run | Session 14 |
| `ops.job_run` | run_id, parameters, row counts, duration, outcome | Sessions 06–12 |

---

## 9. DLQ / quarantine contract

Carried forward from `reference/CDC_EVENT_CONTRACT.md:102-118` unchanged — it was
already correct and complete. `CLAUDE.md` §5.10 requires error class, stack hash,
source topic/partition/offset and an original-payload **reference**:

```json
{
  "failed_at": "2026-08-12T03:14:15Z",
  "pipeline": "kafka_to_stream",
  "error_class": "org.apache.avro.AvroTypeException",
  "error_message": "...",
  "stack_hash": "9f2b...",
  "source_topic": "cdc.oracle.COREDB.BANK.CUSTOMER.v1",
  "source_partition": 3,
  "source_offset": 918273,
  "schema_id": "12345",
  "event_id": "...",
  "payload_s3_uri": "s3://<lake>/quarantine-payloads/kafka_to_stream/2026-08-12/...",
  "retry_count": 3,
  "run_id": "..."
}
```

The payload is written to an encrypted S3 object and **referenced**, never inlined
into logs — a poison record frequently contains exactly the PII that must not reach
CloudWatch.

---

## 10. Schema evolution

| Change | Compatibility | Action |
|---|---|---|
| Add optional field with default | compatible | flows through; `before`/`after` are JSON so L1 is unaffected |
| Add required field | **incompatible** | needs a default, or a migration plan |
| Drop field | **incompatible** | keep as optional/deprecated for one contract version |
| Rename field | **incompatible** | add-then-drop across two versions, never in one |
| Widen type (`int` → `long`) | compatible | |
| Narrow type, or change `decimal` precision/scale | **incompatible** | explicit migration; never silent |

- Registry compatibility level: **`BACKWARD`** on the value subject. Consumers
  (Spark L1) are upgraded after producers, which is the ordering `BACKWARD`
  protects.
- Subject naming strategy: `TopicNameStrategy` — `<topic>-key`, `<topic>-value`.
  Fixed, and documented here so it is never changed casually; changing it orphans
  every registered schema.
- `event_version` in the envelope tracks **contract** versions (this document);
  `schema_id` tracks **Avro** schema versions. They are independent and both are
  retained at L1.
- Every DDL change requires a test event through the full path before rollout
  (`reference/CDC_EVENT_CONTRACT.md:97`).

---

## 11. Table metadata — mandatory properties

`CLAUDE.md` §6 requires owner, description, grain, PK/business key, partition
spec, retention, freshness SLA and DQ rules per table. Enforced as Iceberg table
properties so the requirement is machine-checkable, not documentation-only:

```
owner              = <team or person>
domain             = <from domains.yml>
layer              = stream | full_cdc | snapshot | curated | mart | ops
grain              = <one row per ...>
business_key       = <comma-separated>
retention_class    = short | standard | long
pii_class          = none | internal | restricted
freshness_sla_min  = <integer minutes>
dq_rule_set        = <governance/dq/*.yml reference>
```

## 12. Retention and maintenance

| Layer | Business retention | `expire_snapshots` | `rewrite_data_files` |
|---|---|---|---|
| `stream` | 7 days | 3 days | daily |
| `full_cdc` | indefinite | 30 days | weekly |
| `snapshot` | 7 daily partitions | 14 days | after each build |
| `curated` / `mart` | per mart contract | 14 days | weekly |
| `ops` | 90 days | 30 days | monthly |

`remove_orphan_files` retention must exceed **max job duration + retry window**
(`CLAUDE.md` §6, `reference/ICEBERG_LAYER_SPEC.md:108`). With a 2-hour job timeout
and 3 retries, the floor is 8 hours; the normative value is **72 hours**, and
orphan cleanup never runs while a writer for that table may still commit. Getting
this wrong deletes data files that an in-flight commit is about to reference.
