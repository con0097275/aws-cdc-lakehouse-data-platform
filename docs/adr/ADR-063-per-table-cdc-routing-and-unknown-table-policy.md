# ADR-063 — Per-table CDC provisioning, event routing, and the unknown-table policy

* **Status**: Accepted (implementation: Phase 2, provisioning + routing; no cutover)
* **Date**: 2026-09-10
* **Extends**: ADR-062 (CDC table granularity and partitioning), ADR-033 (logical vs
  physical layer naming), ADR-034 (config as the source of truth, applied to capture)
* **Evidence**: `docs/CDC_TABLE_PLATFORM_TARGET.md` (Phase 0 audit),
  `cdc/registry/sources.yaml`, `spark/tests/test_cdc_router.py`,
  `spark/tests/test_cdc_per_table_spark.py`

## Context

ADR-062 decided **that** per-table FULL_CDC tables are the target and **when** to cut over
(concurrent per-table writers, or differentiated retention/PII policy — explicitly *not*
data volume). Phase 1 built the registry and compiler. Neither answered the three questions
an implementation has to answer before a single table can be created:

1. **Who creates the tables, and from what?** Eight source tables across three layers is 21
   Iceberg tables today (three tables disable REALTIME), plus the optional index — and a
   repository that writes those `CREATE TABLE` statements by hand has written the registry's
   content a second time. The second copy is the one that goes stale.
2. **Where does an event go?** The monolith answered this by not asking: everything went to
   one table and `source_table` was a column. Per-table storage makes routing a decision,
   and a decision with a default is a decision that will be made wrongly at some point.
3. **What happens to an event whose table nobody registered?** This is the dangerous one. A
   mistyped topic, an SMT that rewrites a route, or a connector pointed at the wrong schema
   all produce events that look completely normal.

The third question already has a wrong answer available and it is the *convenient* one:
`CREATE TABLE IF NOT EXISTS` in the write path, which both existing ingest jobs already do
for the monolith. Generalised to per-table targets, that turns a typo into a production data
product — with no owner, no classification, no retention, no partition spec and no DQ rule —
and every log line reports success.

## Decision

### 1. Provisioning is generic and config-derived

One generator (`cdc/provision.py`), N tables. FULL_CDC, REALTIME and EOD DDL are pure
functions of a compiled plan entry, and a test asserts that **no per-business-table DDL
exists anywhere in `spark/`**. Governance metadata CLAUDE.md §6 requires per table — owner,
grain, PK, retention, freshness SLA, DQ rules — is emitted into `TBLPROPERTIES`, where a
catalog reader can see it, rather than into a document that only a person can read.

All registry-derived properties are namespaced `cdc.`. Not cosmetic: `owner` is a **reserved
table property** in Spark SQL and `TBLPROPERTIES ('owner' = …)` fails the statement outright.

### 2. The canonical per-table row

Metadata (`dv_event_id`, `dv_src_event_id`, source identity, `op`, source ordering fields,
source time, Kafka coordinates, schema id, ingest metadata) plus a payload. `event_id` is
retained as a mirror of `dv_event_id` so cutover is a table name, not a column rename across
six consumer sites.

Payload representation is per table: `json` (the current contract, the default) or `typed`
(struct columns from the registry's declared column list). **`typed` keeps the JSON beside
the struct.** A writer version can carry a field the registry has not declared, and a
typed-only projection would drop it silently — data loss dressed as a schema improvement.

**A typed temporal column must declare its Debezium `encoding`, and the compiler refuses one
that does not.** This is the sharpest edge in the whole phase.
`decimal.handling.mode: precise` yields `org.apache.kafka.connect.data.Decimal`, a Connect
**built-in** logical type that an Avro converter carries as a logical type, so `from_avro`
gives Spark a real `DecimalType` and a plain cast is correct. Debezium's temporal types are
**custom** Connect logical types, which an Avro converter carries as the underlying
primitive — so `from_avro` yields a plain `int64`/`int32`. Measured on this platform's Spark:

```
epoch millis 1787356800000  CAST AS timestamp -> +58609-…   silently wrong
epoch micros                CAST AS timestamp -> +109081-…  silently wrong
epoch days (int32)          CAST AS date      -> AnalysisException, batch dies
```

A year-58609 timestamp partitions, sorts and reconciles like a real value. The writer
therefore applies the conversion the declared encoding names (`timestamp_millis`,
`micro_timestamp`, `nano_timestamp`, `date_days`, `zoned_timestamp`) rather than casting,
and there is **no default encoding** — every possible default is wrong for some column.
Which encoding a column needs follows the connector's `time.precision.mode`, so that mode is
named in the code rather than assumed.

### 3. Partitioning

Default `identity(event_date)`, which *is* `days(source_commit_ts)`: `event_date` is derived
as the UTC date of `source.ts_ms`. `bucket(N, dv_pk_hash)` only when configured. Partitioning
by `source_system` / `source_database` / `source_schema` / `source_table` is **rejected at
compile time** — a per-table target holds exactly one source table, so those columns are
constant and buy no pruning. The monolith partitions by them because it holds eight tables.

### 4. Write properties: 128 MiB, not 512 MiB

The phase brief permits Iceberg's 512 MiB default "only if consistent with project
benchmarking". It is not. Phase 0 measured a **mean data file of 137 KiB** against that
default — three orders of magnitude under it — because file size is set by commit frequency
(one MERGE per topic per run), not by the target property. Raising the target changes nothing
except the number a reader compares against. 128 MiB is the honest target and **compaction is
the remedy**, which is why `maintenance.compact_target_mb` sits beside it.

Metrics mode is set per column class: `full` on the identity and temporal columns the MERGE
and every window filter use, `none` on the payload columns. Full metrics on a whole JSON
document write both bounds of every document into every manifest, in exchange for min/max on
a string nobody ranges over.

### 5. Routing, and the two refusals

```
normalized event -> canonical table_id -> resolved config -> the ONE target it names
```

| case | outcome | why not something else |
|---|---|---|
| registered + enabled | routed | — |
| **not registered** | quarantine (default) or reject; a metric names what it claimed | never a default target, never a new table |
| **registered but `enabled: false`** | held, with its own outcome | distinct from "unknown": one operator response is "turn it on", the other is "register it" |
| **topic and envelope name different tables** | ambiguous; refused | either choice writes an event into a table that is not its source |

`quarantine` is the default because `reject` stops the seven healthy topics to punish one
misconfiguration. `reject` remains available for the case where continuing would bank more of
a known-bad configuration.

### 6. No table is created because an event appeared

The ingest path **never** issues `CREATE TABLE`. A missing target is a refusal that names
`python3 -m cdc.provision`. Tables are registered in Git, provisioned deliberately, and only
then captured. That order is the entire control.

### 7. Migration modes, default unchanged

`LEGACY_ONLY` (default) | `DUAL_WRITE` | `PER_TABLE_ONLY`. Under the default the jobs behave
exactly as before: the monolith and nothing else, and the `cdc` package is **not imported at
all** — it is staged separately, so an unconditional import would make every existing
submission recipe fail at import the moment the package was absent. Under `DUAL_WRITE` the
per-table columns are dropped before the legacy MERGE, so dual-write cannot widen the
deployed monolith as a side effect. One decode and one set of identity expressions serve both
writes: computing `dv_event_id` twice is how two writers of one event come to disagree about
whether it is one event.

## Options

* **Auto-create the target on first sight of an event.** Rejected — §6. It is the
  convenient answer and it converts a typo into an unowned production table.
* **Route by topic only.** Simpler, and blind to the case where a topic and its envelope
  disagree — the one case where guessing is most expensive. Both are consulted; a conflict
  is refused.
* **Drop unroutable events with a warning.** Rejected. A record that vanishes is
  indistinguishable from one that never existed, and topic counts stop reconciling with no
  way to find out why. This repository already learned that from poison records (G-P1-1).
* **`reject` as the default policy.** Rejected as a default: one unregistered table would
  stop ingest for the other seven. Available as a flag.
* **Typed payloads replacing the JSON.** Rejected — §2. Silent field loss.
* **A default temporal encoding** (say, `micro_timestamp`, the commonest here). Rejected: a
  column that is actually millis would be off by a factor of 1000 and land in 1970 — a
  *plausible* date, which is worse than an absurd one because nothing looks wrong. Requiring
  the declaration makes the question impossible to skip.
* **Inferring the encoding from the decoded Avro type.** Rejected: every one of
  `Timestamp`, `MicroTimestamp` and `NanoTimestamp` decodes to `int64`. The wire type does
  not carry the answer, which is precisely the problem.
* **Forbidding typed temporals outright.** Simpler and it does close the hole, but it would
  push every date column back into a JSON string and give up the per-column statistics that
  are the point of typed mode.
* **Per-table DDL files, one per table.** Rejected — §1. It is the registry, retyped.
* **Cut over to per-table storage in this phase.** Out of scope by ADR-062: neither trigger
  is real yet. This phase builds the machinery and leaves the default alone.

## Consequences

* Onboarding a table is: edit the registry, compile, provision, then capture. The
  provisioner's DDL is readable before anything is created.
* An unregistered topic produces quarantined records and a non-zero
  `CDC_ROUTER_UNROUTED` count instead of a new table. That is a **louder** failure than
  today's, deliberately.
* The compiled plan is now schema version 2 and carries the resolved catalog. A version-1
  plan is refused by the router rather than routed against bare table names.
* Provisioning 22 targets multiplies table count ~3x per source table. At the measured
  volume this makes small files *worse*, not better — compaction is required either way and
  is not a partitioning decision (ADR-062).
* The event index is opt-in and never on in `LEGACY_ONLY`.

## Cost

| item | impact |
|---|---|
| provisioning | **$0** — empty tables; storage is metadata only until a write lands |
| routing | $0 — a dict lookup per topic, built once per run |
| `DUAL_WRITE` | one extra MERGE per topic per window plus transient duplicate storage (~$0.01/month at the lake's 300 MB); the cost is EMR time, not storage |
| event index | coordinates only, no payloads — a small fraction of the per-table tables |
| metrics mode | reduces manifest size versus the default on payload-carrying tables |
| per-table maintenance | 8 compaction jobs instead of 1, each far smaller; independently schedulable and skippable when a table is idle |

Immaterial against the $100/month budget of record (ADR-030 as amended 2026-09-06). The
dominant cost remains platform uptime at ~$1.12/hr.

## Security

* The provisioner makes per-table IAM **expressible** for the first time: today any principal
  that can read `cdc_events` reads all eight source tables, which is why the AI plane is
  denied `warehouse/full_cdc/` wholesale (ADR-060) rather than per table. `cdc.classification`
  on each table is the key a scoped grant would use.
* The registry and the compiled plan carry **no credentials** — table names, owners and
  policy only. Secrets stay in SSM SecureString (CLAUDE.md §3.1/§3.6).
* The event index carries coordinates and no payloads, so it does not widen PII exposure.
* Refusing to create tables closes a real path: without it, anyone who can produce to a Kafka
  topic can cause a table to be created in the lake.
* Provisioning defaults to **dry run**; `--execute` is required, and even then it never drops
  a column, never changes a type and never replaces a table.

## Rollback

* **Routing**: `--migration-mode legacy_only` is the default. Reverting is not passing a
  flag.
* **Provisioned tables**: empty per-table targets can be dropped with no data loss while
  `LEGACY_ONLY` holds, because nothing has written to them.
* **`DUAL_WRITE`**: the monolith stays canonical throughout and is never dropped, so
  reverting is stopping the per-table write. Records already written to per-table targets are
  a superset-safe duplicate, not a divergence — both carry the same `dv_event_id`.
* **`PER_TABLE_ONLY`**: reversible only by replaying the window through `DUAL_WRITE` or
  `LEGACY_ONLY` from Kafka, within the topics' retention. This is the one mode with a
  time-bounded rollback, which is why ADR-062 gates it behind a zero-diff reconciliation.
* **The plan schema bump**: recompiling produces both; a version-1 plan is refused, not
  mis-routed.

## Validation

Phase 2 is accepted on evidence that does not require the platform to be up:

1. `pytest spark/tests/test_cdc_router.py` — routing, refusals, modes, DDL generation,
   drift and the encoding guard, all against a compiled plan. **62 tests.**
2. `pytest spark/tests/test_cdc_per_table_spark.py` — the generated DDL creating real
   Iceberg tables; I/U/D for one key all surviving; rerun not doubling; typed evolution;
   dual-write not widening the monolith; an unprovisioned target refused; and every Debezium
   temporal encoding producing the correct instant, with the bare-cast result pinned as a
   regression guard. **34 tests.**
3. `python3 -m cdc.compile --check` — deterministic, and the shipped registry compiles.
4. `python3 -m cdc.provision` — prints 22 targets (21 layer targets plus the index) and
   mutates nothing.

Cutover to per-table storage is **not** validated here and is not claimed. ADR-062's gate
stands: per table, a full-outer-join diff on `event_id` between the monolith and the
per-table target must return zero rows, computed independently in Athena rather than by the
job that wrote the data.
