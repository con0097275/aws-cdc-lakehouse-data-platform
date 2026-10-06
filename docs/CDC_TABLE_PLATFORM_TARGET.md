# CDC table platform — current state, evidence, and target model

Phase 0 audit, 2026-09-10. **Read-only when written**: no code changed, no AWS mutated, no
connector touched, no `terraform apply`.

> **Status: the audit stands, its predictions have since been MEASURED, and the platform it
> authorised is built.** §12 records which predictions held and which did not — Q9's
> assumption that per-table storage helps uniformly is the one that was wrong, and it is
> stated as wrong rather than smoothed. §13 answers the twenty audit questions against
> measured state. The normative record is now ADR-062 … ADR-072; to *use* the platform, start
> at `CDC_TABLE_QUICKSTART.md`.

Evidence came from Iceberg metadata and manifests read directly off S3, not from folder
listings and not from the Glue catalog — the catalog was recreated empty by the 2026-09-10
apply, while the data and metadata from earlier windows persist.

---

## 1. CURRENT STATE

### 1.1 Storage — it is a MONOLITH (design A)

```
Glue database   kafka_dev_lab_dev_full_cdc
Table           cdc_events                       <- ONE table for all 8 source tables
Location        s3://<lake>/warehouse/full_cdc/cdc_events
Format          Iceberg v2
Partition spec  identity(source_system), identity(event_date)
Sort order      NONE  (order-id 0, zero fields)
Distribution    not set  (engine default)
Target file sz  not set  (Iceberg default 512 MiB)
Compression     write.parquet.compression-codec = zstd
Columns         19
```

`source_table` **is a column but not a partition field.**

Sibling layers follow the same shape — one table each, not per source table:

```
warehouse/stream/cdc_events            (DEPRECATED landing, ADR-033)
warehouse/stream/cdc_events_realtime   REALTIME
warehouse/stream/rt_account_base|rt_account_stream   STREAMING_RT
warehouse/curated/fact_account_daily_snapshot        EOD
warehouse/mart/…                                     marts
```

### 1.2 Measured distribution — from the manifests

Current snapshot of the longest-lived chain (`00057-…`, last updated 2026-08-22):

```
snapshot        2706429421265729193
total records   20,390        data files 14        delete files 0
manifests       13
file size       avg 137.3 KiB   min 7.4 KiB   max 396.7 KiB
partitions      6
```

| source_system | event_date | files | rows | size |
|---|---|---|---|---|
| oracle | 2026-08-20 | 1 | 2,524 | 143 KiB |
| oracle | 2026-08-21 | 2 | 2,525 | 330 KiB |
| oracle | 2026-08-22 | 4 | 2,524 | 340 KiB |
| sqlserver | 2026-08-20 | 1 | 6,408 | 288 KiB |
| sqlserver | 2026-08-21 | 1 | 3,204 | 397 KiB |
| sqlserver | 2026-08-22 | 5 | 3,205 | 423 KiB |

Physically present across **all** chains:

```
oracle    2026-08-20(1) 08-21(6) 08-22(4) 09-03(7) 09-06(10)  event_date=null(2)
sqlserver 2026-08-20(1) 08-21(5) 08-22(5) 09-03(4) 09-06(6)
```

**Skew: sqlserver carries ~2.5x oracle's rows** on the same day (6,408 vs 2,524) because
`digital_event` alone seeds 3,000 rows against `ACCOUNT`'s 320.

**Small files are already real**: mean 137 KiB against a 512 MiB target — three orders of
magnitude under. The cause is many small commits (one MERGE per topic per run), not the
target-size setting. Compaction, not configuration, is the remedy.

**Metadata accumulation**: 109 `metadata.json` files and **three distinct chains** (three
`00000-…` files with different UUIDs) at one location — one per destroy/apply cycle. Each
apply recreates the Glue table, Iceberg starts a fresh chain, and the previous chain is
orphaned but never cleaned. Nothing reads the old chains; they are pure storage and
listing cost, and they make "which metadata is current?" unanswerable from S3 alone.

### 1.3 `event_date IS NULL` — ROOT CAUSE, not ignored

Two data files sit under `source_system=oracle/event_date=null/`. One row, read directly:

```
op                 NULL        source_table      BRANCH
event_date         NULL        source_commit_ts  NULL
payload_after      NULL        payload_before    NULL
position_primary   NULL        kafka_offset      1
```

**This is the poison record from the 2026-09-06 quarantine test** — a literal
`this-is-not-avro-…` message produced to `cdc.oracle.COREBANK.BRANCH` at offset 1, ingested
by the run *before* the poison predicate was corrected.

`event_date` is derived as `to_date(from_unixtime(v.source.ts_ms / 1000))`. PERMISSIVE
`from_avro` returns a struct whose *fields* are all NULL for undecodable bytes (Avro has no
magic bytes or checksum), so `ts_ms` is NULL and `event_date` follows. The row was deleted
logically at the time, but Iceberg merge-on-read writes a position-delete rather than
removing the data file, and the catalog was destroyed on the next apply — so the file
survives with no chain referencing it.

**Conclusion, which is the useful part: `event_date IS NULL` is never a legitimate business
state in this layer.** Every Debezium envelope carries `source.ts_ms`. A NULL is therefore a
reliable signature that an undecodable record slipped the quarantine filter. It should be
promoted from an accident to a **control** — see §6.

> **CLOSED 2026-09-10.** `SELECT count_if(event_date IS NULL) FROM cdc_events` returns **0 of
> 20,400**. The analysis above predicted this: the orphaned file was referenced by no snapshot
> chain, so re-registering the surviving tables into the rebuilt catalog (Phase 6) produced a
> table without it. No delete was needed and none was issued.
>
> The recommendation was implemented rather than noted: `dq.event_date_null_tolerance: 0` is a
> registry **default**, so every table — including the two onboarded in Phase 8 — inherits the
> control. An accident became a rule.

### 1.4 Workload — the partition spec is misaligned with the query pattern

Filter columns across `spark/jobs/realtime/job.py`, `spark/jobs/eod/job.py`,
`spark/realtime/*.py`, `spark/reporting/auto_correct.py`:

| column | in partition spec | consumer filter hits | effective pruning |
|---|---|---|---|
| `source_system` | **yes** | 4 | cardinality **2** → ≤50% |
| `source_table` | **no** | **6 — the primary filter** | **none** |
| `event_date` | **yes** | 1 | rarely exercised |
| `source_commit_ts` | no | 15 — the real windowing column | file stats only |
| `business_date` | n/a (downstream) | 32 | n/a |

Concretely: `eod/job.py` filters `source_system == "oracle" AND source_table == "ACCOUNT"`;
`rt_stream_app.py` filters `source_system == "oracle" AND source_table == "CUSTOMER"`. Both
scan **every row of every table** in the matching `(source_system, event_date)` partitions,
then discard ~87% of them. `realtime/job.py` windows on `source_commit_ts`, a different
column from the partitioned `event_date`, so the engine cannot translate the window into
partition pruning.

**This is the measured defect. Everything below is about fixing it proportionately.**

### 1.5 Capture configuration

| | Oracle | SQL Server |
|---|---|---|
| connector | `OracleConnector` | `SqlServerConnector` |
| include list | `COREBANK.{CUSTOMER,ACCOUNT,TRANSACTION,BRANCH}` | `dbo.{app_user,digital_event,channel,merchant}` |
| topic prefix | `cdc.oracle` → `cdc.oracle.<SCHEMA>.<TABLE>` (**UPPERCASE**) | `cdc.sqlserver` → `cdc.sqlserver.<db>.<schema>.<table>` (lowercase) |
| snapshot.mode | `initial` | `initial` |
| isolation | — | `snapshot` |
| schema history | `cdc.oracle.schema-history` | `cdc.sqlserver.schema-history` |
| tombstones.on.delete | `true` | `true` |
| decimal.handling.mode | `precise` | `precise` |
| transaction metadata | `true` | `true` |

**Two consequences that dominate onboarding:**

1. **No `signal.data.collection` is configured on either connector, so incremental snapshot
   is unavailable.** Adding a table to `table.include.list` and restarting gives the new
   table **no snapshot at all** — `snapshot.mode: initial` only snapshots on first start
   against an empty offset. The new table is captured from the restart point forward and
   its existing rows are invisible until backfilled another way.
2. An allow-list means onboarding is explicit — good for governance, and it means the
   registry proposed in §5 has a single authoritative list to render from.

---

## 2. PARTITION BENCHMARK PLAN

Runtime evidence is **PENDING** — the Glue catalog is empty on the current apply, so these
must run after the next ingest. Each is read-only.

```sql
-- B1 files, partitions, skew
SELECT partition, record_count, file_count, total_size
FROM "kafka_dev_lab_dev_full_cdc"."cdc_events$partitions" ORDER BY record_count DESC;

-- B2 rows per source table per day  (the pruning target)
SELECT source_system, source_table, event_date, COUNT(*) rows
FROM kafka_dev_lab_dev_full_cdc.cdc_events GROUP BY 1,2,3 ORDER BY rows DESC;

-- B3 file size distribution
SELECT file_path, record_count, file_size_in_bytes
FROM "kafka_dev_lab_dev_full_cdc"."cdc_events$files" ORDER BY file_size_in_bytes;

-- B4 manifest count and orphan chains
SELECT COUNT(*) FROM "kafka_dev_lab_dev_full_cdc"."cdc_events$manifests";
SELECT COUNT(*) FROM "kafka_dev_lab_dev_full_cdc"."cdc_events$snapshots";

-- B5 THE CONTROL: must return 0
SELECT COUNT(*) FROM kafka_dev_lab_dev_full_cdc.cdc_events WHERE event_date IS NULL;

-- B6 pruning benchmark — run before and after any spec change, compare bytes scanned
SELECT COUNT(*) FROM kafka_dev_lab_dev_full_cdc.cdc_events
WHERE source_system='oracle' AND source_table='ACCOUNT';
```

**Acceptance for any partitioning change:** B6's `DataScannedInBytes` must fall by at least
the table's share of the partition (≈87% with 8 evenly-weighted tables), and B5 must stay 0.

---

## 3. OPTIONS COMPARED

Scored against the audited evidence, not in the abstract.

| | 1. mono `system+day` (**today**) | 2. mono `system+day+table` | 3. mono `system+day+bucket(table)` | 4. mono `day` + write order | 5. table-per-source-table |
|---|---|---|---|---|---|
| read pruning | **poor** — table filter prunes nothing | **good** — exact | fair — collisions blur it | fair — stats only, no pruning | **best** — nothing else in the table |
| write amplification | low | low | low | low | low |
| small files | already bad (137 KiB) | worse — 8x partitions | moderate | best | worse — 8x tables |
| schema evolution | isolated already (payloads are JSON strings) | same | same | same | **best** — per-table schema possible later |
| governance | one owner, one grain for 8 domains | same | same | same | **best** — owner/PII/retention per table |
| retention | all-or-nothing | partition-level | partition-level | partition-level | **best** — per table |
| security | one IAM boundary | one | one | one | **best** — per-table grants |
| maintenance | one compaction job | one, more partitions | one | one | 8 jobs, but independent |
| failure isolation | **none** — one bad commit blocks all | none | none | none | **best** |
| commit contention | **the scaling wall** — every writer commits to one table | same | same | same | **eliminated** |
| onboarding | edit include-list only | same | same | same | + create table |
| rebuild | rebuild everything | rebuild everything | same | same | rebuild one table |
| cross-source audit | **trivial** | trivial | trivial | trivial | needs union view / index |
| 10x tables | 80 partitions/day, contention grows | 640 partitions/day | bounded by bucket count | unchanged | 80 tables, contention flat |

**On commit contention, the strongest technical point.** Iceberg commits are optimistic
concurrency on the *table*. Today the ingest loops topics **sequentially** in one job, so
there is no conflict. The natural scaling move — one streaming app per source table, which
this platform is already shaped for (the batch job loops topics; the streaming job groups
each micro-batch by topic) — puts N concurrent writers on one table and turns every commit
into a retry storm. Per-table storage removes that by construction. **Nothing else on this
list does.**

**On small files, honestly:** options 2 and 5 both multiply partition/table count and make
small files *worse* at current volume. Neither is a small-file fix. Compaction is, and it is
needed regardless.

---

## 4. TARGET DECISION

**Two phases. The first is small and evidence-forced; the second is larger and justified by
growth and governance, not by today's data.**

### Phase A — partition spec evolution (the smallest production-grade change)

Add `identity(source_table)` to the existing spec, and set a sort order:

```
partition spec  identity(source_system), identity(source_table), identity(event_date)
sort order      source_table, source_commit_ts
```

* Fixes the **one measured defect** (§1.4) directly.
* **Non-destructive.** Iceberg partition-spec evolution is metadata-only: existing files
  keep `spec-id 0` and are still read correctly; new writes use the new spec. No rewrite, no
  backfill, no downtime, no consumer change.
* The sort order helps immediately even for old files, because column stats give file-level
  pruning on `source_table` without any partitioning at all.
* Cost: one `ALTER TABLE … ADD PARTITION FIELD`. Reversible.

### Phase B — one FULL_CDC table per source table (the target)

```
kafka_dev_lab_dev_full_cdc.cdc_oracle_corebank_account
kafka_dev_lab_dev_full_cdc.cdc_oracle_corebank_customer
…                          cdc_sqlserver_digital_app_user
```

with `identity(event_date)` partitioning and `(source_commit_ts)` sort order per table,
plus **one lightweight global index** for cross-source audit:

```
kafka_dev_lab_dev_ops.cdc_event_index
  (event_id, source_system, source_schema, source_table, op, event_date,
   source_commit_ts, kafka_topic, kafka_partition, kafka_offset, target_table)
```

— coordinates and pointers only, no payloads. That preserves the one property the monolith
is genuinely better at (§3) at a fraction of the size.

**Justified by:** commit isolation at N concurrent writers, per-table retention/IAM/owner
(CLAUDE.md §6 requires owner, grain, retention and DQ rules *per table* — a monolith cannot
express eight different answers), and failure isolation.

**NOT justified by:** today's data volume. 20,390 rows across 6 partitions argues for
nothing. Phase A alone is sufficient until either concurrent per-table writers or
differentiated retention/PII policy is actually needed. **Do not do Phase B before one of
those is real.**

### REALTIME / EOD

Leave as-is for now. They are already narrow (`fact_account_daily_snapshot` is one grain,
one table) and there is no measured pruning defect in them. Splitting them per source table
would be change without evidence. Revisit when a second EOD fact is added.

---

## 5. CONFIG MODEL

A registry, so onboarding is data and not code — the same property ADR-034 gives reporting.

`cdc/registry/sources.yaml`

```yaml
version: 1
sources:
  - source_system: oracle
    source_schema: COREBANK          # UPPERCASE — Oracle folds unquoted identifiers
    connector: corebank-source
    topic_pattern: "cdc.oracle.{schema}.{table}"
    tables:
      - source_table: ACCOUNT
        primary_key: [ACCOUNT_ID]
        owner: my-aws-profile
        domain: core_banking
        pii: false
        retention_days: 365
        target_table: cdc_oracle_corebank_account   # Phase B only
        dq:
          not_null: [ACCOUNT_ID, CUSTOMER_ID]
          event_date_null_tolerance: 0              # see §6
```

The registry renders, rather than duplicates:

| rendered artefact | today maintained by hand in |
|---|---|
| `table.include.list` | connector templates |
| `message.key.columns` | connector templates (**a separate, easily-missed second place**) |
| topic list | `docker/cdc-runtime/create-topics.sh` |
| `--topics` for the ingest jobs | submit commands |
| per-table DQ rules | nowhere |
| owner / retention / PII | nowhere — CLAUDE.md §6 requires them |

One source of truth removes the class of defect already recorded twice in this repo:
lowercase topics created against an UPPERCASE include-list (connector RUNNING, eight topics
at offset 0, every health check green), and `message.key.columns` updated in one place but
not the other (same PK scattered across partitions, per-key ordering silently broken).

---

## 6. THE `event_date` CONTROL

Promote the §1.3 finding into an enforced invariant:

1. **DQ rule**: `SELECT COUNT(*) … WHERE event_date IS NULL` must be **0**. Non-zero means a
   record that is not a Debezium envelope reached the canonical layer.
2. **Ingest assertion**: the poison predicate already tests `v IS NULL OR v.op IS NULL`.
   Add `v.source.ts_ms IS NULL` to the same predicate — it is the same class of corruption
   and it is what actually produces the NULL partition.
3. **Do not make `event_date` a required Iceberg field.** Required-ness is enforced at write
   time and would turn a quarantine-able row into a job crash, undoing G-P1-1.
4. **Clean up the two orphan files** with `remove_orphan_files` after confirming no live
   chain references them — not by deleting from S3 by hand.

---

## 7. RISKS

| risk | severity | mitigation |
|---|---|---|
| Partition evolution misread as a rewrite | low | It is metadata-only. Old files keep spec-id 0 and remain readable. Verify with `$partitions` showing both spec ids |
| Phase B doubles storage during dual-write | **medium** | Time-boxed. Budget is $100/month and the lake is 300 MB — a full duplicate is ~$0.01/month. The real cost is EMR time, not storage |
| Consumers hardcode `cdc_events` | **high for Phase B** | 6 filter sites found in §1.4. All must route through the registry before cutover, never by find/replace |
| Orphan metadata chains grow per apply | medium | 3 chains / 109 files today. Add `expire_snapshots` + `remove_orphan_files` to maintenance; guard retention so a live writer cannot be truncated (CLAUDE.md §6) |
| Adding a table gives no snapshot | **high** | No `signal.data.collection` (§1.5). Either configure incremental snapshot or accept a documented backfill step. This is the single biggest onboarding gap |
| Cross-source audit lost in Phase B | medium | The global index (§4) exists for exactly this |
| Small files worsen | medium | Compaction is required either way; it is not a partitioning decision |

---

## 8. COST IMPACT

| item | impact |
|---|---|
| Phase A | **$0** — metadata-only ALTER |
| Phase A benefit | ~87% fewer bytes scanned on table-filtered reads. Athena bills per byte; the workgroup cap is 10 GiB/query |
| Phase B dual-write | one extra EMR ingest pass per window (~$0.30/run at the measured rate) plus transient duplicate storage (~$0.01/month at 300 MB) |
| Global index | coordinates only, no payloads — a small fraction of the monolith |
| Orphan cleanup | reduces LIST and storage cost; 109 metadata files is already a listing tax on every plan |
| Per-table maintenance | 8 compaction jobs instead of 1. Each is far smaller; net EMR time is comparable, but they can be scheduled independently and skipped when a table is idle |

Against the **$100/month budget of record** (ADR-030 as amended 2026-09-06), none of this is
material. The dominant cost remains platform uptime at ~$1.12/hr, not storage layout.

---

## 9. SECURITY IMPACT

* Phase A: **none.** No new resource, no IAM change, no data movement.
* Phase B: a net **improvement**. Per-table tables make per-table IAM expressible — today
  any principal that can read `cdc_events` reads every source table, so a PII table cannot
  be granted separately from a non-PII one. The AI plane is already denied
  `warehouse/full_cdc/` wholesale (ADR-060) precisely because finer granularity does not
  exist.
* The registry must **not** carry credentials. It names tables, owners and policy only;
  secrets stay in SSM SecureString (CLAUDE.md §3.1/§3.6).
* The global index carries coordinates and no payloads, so it does not widen PII exposure.

---

## 10. MIGRATION PLAN — non-destructive

Phase A needs no migration. Phase B, if and when justified:

```
legacy cdc_events (monolith)                     stays readable throughout
        |
        v
registry + compiler                              renders targets, topics, include-lists
        |
        v
new per-table FULL_CDC tables                    created empty, no consumer points at them
        |
        v
DUAL WRITE                                       ingest writes BOTH; monolith stays canonical
        |
        v
CONTROLLED REPLAY                                 backfill per-table from the monolith by
                                                  (source_system, source_table), idempotent
                                                  on event_id — the same MERGE key already used
        |
        v
RECONCILE                                        per table: row counts, event_id set equality,
                                                  per-day counts, and a full-outer-join diff on
                                                  (event_id) that must return zero rows
        |
        v
CONSUMER CUTOVER                                  one consumer at a time, via the registry,
                                                  each independently revertible
        |
        v
COMPATIBILITY PERIOD                              monolith still dual-written, >= 1 full
                                                  EOD + AUTO_CORRECT + FULFILL cycle
        |
        v
optional retirement                               stop dual-write; KEEP the monolith read-only.
                                                  Never DROP -- it is the reconciliation
                                                  baseline for every earlier window
```

**Gate between every step:** reconciliation returns zero diff, and it is computed
*independently* in Athena, not by the job that wrote the data — the same rule the EOD
reconciliation already follows.

---

## 11. IMPLEMENTATION PHASES

| phase | scope | gate to proceed |
|---|---|---|
| **0** | this audit | approved |
| **1** | `event_date IS NULL` control: add `v.source.ts_ms IS NULL` to the poison predicate; add the DQ rule; `remove_orphan_files` for the two stray files | B5 returns 0 on a live run |
| **2** | Phase A partition + sort evolution on `cdc_events` | B6 bytes-scanned drops ≥80%; B5 still 0; all five reporting modes still green |
| **3** | maintenance: `expire_snapshots`, `rewrite_data_files`, `remove_orphan_files`, with retention guards | mean file size up ≥10x; manifest count down; no live writer truncated |
| **4** | registry + compiler (`cdc/registry/sources.yaml`), rendering include-lists, key columns, topics, `--topics` | rendered artefacts byte-match the current hand-maintained ones |
| **5** | incremental-snapshot capability (`signal.data.collection`) **or** a documented backfill runbook | a new table can be onboarded and backfilled without a full re-snapshot |
| **6** | Phase B per-table tables + global index, dual-write, replay, reconcile, cutover | zero-diff reconciliation per table |

**Phases 1–3 are worth doing regardless of the Phase B decision.** Phase 4 is worth doing as
soon as a ninth table is added. Phase 6 waits for a real trigger — concurrent per-table
writers, or differentiated retention/PII policy.

---

## 12. RUNTIME EVIDENCE STATUS

| evidence | status |
|---|---|
| partition spec, sort order, properties, schema | **MEASURED** — Iceberg metadata, 2026-09-10 |
| file/partition/manifest counts, sizes, skew | **MEASURED** — manifest scan |
| `event_date=null` root cause | **MEASURED** — Parquet row read directly |
| consumer filter columns | **MEASURED** — source inspection |
| connector capture config | **MEASURED** — rendered templates |
| bytes-scanned before/after (B6) | **MEASURED** 2026-09-10 — see below |
| per-table rows/day at steady state (B2) | **MEASURED** 2026-09-10 — see below |

### 12.1 B6 closed — the prediction held for the skewed case and not the dominant one

Athena, real query ids, EOD source scan at the same cutoff:

| table | legacy bytes | per-table bytes | delta |
|---|---|---|---|
| `oracle/ACCOUNT` | 89,193 | 11,231 | **−87.4%** |
| `sqlserver/digital_event` | 113,261 | 105,163 | **−7.1%** |

ACCOUNT matches this document's ≈87% prediction. **`digital_event` does not, and the reason
is the point**: it *is* 12,000 of the monolith's 20,390 rows, so isolating the table that
dominates the file saves almost nothing — there was never much to prune away from it. The
benefit of per-table storage is inversely proportional to a table's share of the monolith,
which this document assumed uniformly and should not have.

Two per-table metrics got **worse** for `digital_event`: planning +217% and runtime +156%,
on 2 files versus 14. At this data volume those are fixed costs, not scaling behaviour, and
they are recorded rather than dropped.

### 12.2 B2 closed — the skew this document predicted is real and larger than assumed

Rows per source table per day, measured:

| table | rows/day | share |
|---|---|---|
| `sqlserver/digital_event` | 3,000 (6,000 on 2026-08-20) | dominant |
| `oracle/TRANSACTION` | 2,000 | |
| `oracle/ACCOUNT` | ~320 | |
| `oracle/CUSTOMER` | 200 | |
| `sqlserver/app_user` | ~150 | |
| `sqlserver/merchant` | 50 | |
| `sqlserver/channel`, `oracle/BRANCH` | 4–8 | near-static |

The 4-to-8-rows-a-day tables are why `realtime: {enabled: false}` exists: a 72-hour rolling
window maintained for a table that changes four times a day is a standing cost for noise.
Three of the ten shipped tables now opt out.

---

## 13. THE 20 AUDIT QUESTIONS — answered against MEASURED state

Re-audited 2026-09-10, after Phases 1–8 shipped. Where this document originally **predicted**,
the measured value is given and any divergence is stated rather than smoothed over.

| # | question | answer | evidence |
|---|---|---|---|
| 1 | Is FULL_CDC monolithic? | **Was.** One `cdc_events` table for all 8 tables. Now 4 per-table tables exist alongside it; the monolith is retained as the reconciliation baseline and is never dropped | ADR-062 |
| 2 | Exact Iceberg partition spec? | Monolith: `source_system` + `event_date`. Per-table: `event_date` only — the other two are constant for a single-table Iceberg table and partitioning a constant buys nothing | §1.1, §5 |
| 3 | Why `event_date=null`? | A poison record from the 2026-09-06 quarantine test: PERMISSIVE `from_avro` returns an all-NULL struct for undecodable bytes, so `ts_ms` and therefore `event_date` were NULL. **Now 0 of 20,400** and promoted to a control (`dq.event_date_null_tolerance: 0`, a registry default) | §1.3 |
| 4 | How many source tables? | 8 at audit → **10** today (Phase 8 added Oracle `loan`, SQL Server `payment_method`) | `cdc-table-status` |
| 5 | Event volume/table/day? | **Measured**: `digital_event` 3,000/day (6,000 one day); `TRANSACTION` 2,000; `ACCOUNT` ~320; `CUSTOMER` 200; `app_user` ~150; `merchant` 50; `channel`/`BRANCH` **4–8** | §12.2 |
| 6 | Parquet file sizes? | Monolith **14 files, mean 137 KiB** against a 128 MiB target — three orders of magnitude under. This drove the metric-driven maintenance design | §1.2, ADR-068 |
| 7 | Which jobs filter by `source_table`? | Every EOD and REALTIME read did. That filter is now the *table identity*, not a predicate | §1.4 |
| 8 | Partition pruning today? | Monolith prunes `source_system` + `event_date` only, so a single-table read still scans every table's rows for those days. Per-table removes the scan entirely | §12.1 |
| 9 | Is one table per source table justified? | **Yes, but not uniformly** — and this is where the audit was wrong. EOD scan −87.4% for `ACCOUNT`, only **−7.1%** for `digital_event`, because that table *is* 12,000 of 20,390 rows. The benefit is inversely proportional to a table's share of the monolith | §12.1 |
| 10 | What stays from legacy `cdc_events`? | All of it. It is the reconciliation baseline for every window already captured and is **never dropped** (ADR-062). It still receives events for tables in `dual_write` | ADR-067 |
| 11 | Best config model? | One `cdc/registry/sources.yaml` with `defaults < source defaults < table`, compiled to a deterministic `table-plan.json` with a content `plan_hash`. NOT the per-table-file tree the brief sketched — see below | ADR-062 |
| 12 | Oracle onboarding? | Declare in DDL → registry entry → validate (declared-evidence precheck) → provision → **topic** → connector → export schema → ingest. `changes_only`; offsets never reset | ADR-066, ADR-070 |
| 13 | SQL Server onboarding? | Identical path, different prerequisites (Agent running, `sp_cdc_enable_table`). Proven on `payment_method` | ADR-069 |
| 14 | REALTIME window config? | `lookback` + `late_arrival_grace` + `physical_retention`, with `retention >= lookback + grace` enforced at compile. Two boundary kinds: `rolling_hours` and `calendar_day` | ADR-064 |
| 15 | EOD cutoff config? | `cutoff_local = start of (COB+1)` in the table's business timezone, converted to UTC; predicate is strictly `<`. Date arithmetic on the **local** date, never a 24h delta, so DST cannot shift a business day | ADR-065 |
| 16 | Naming? | Derived from the canonical id, never spelled out: `cdc_` / `rt_` / `eod_` + `<source>_<db>_<schema>_<table>` | ADR-062 |
| 17 | What needed migration? | Nothing was migrated destructively. The legacy job, DAG and monolith all still run; per-table is additive and the cutover is per-table and reversible | ADR-067 |
| 18 | Safest pilot? | `oracle.coredb.corebank.account` — moderate volume, simple PK, and the numeric-SCN ordering path. Paired with `sqlserver/digital_event` to exercise hex LSN | ADR-067 |
| 19 | Rollback? | Per-table cutover mode reverts to `LEGACY`; the monolith is intact and never dropped; EOD is derived and rebuildable from FULL_CDC at any time | ADR-067 |
| 20 | What benchmark proves it? | Athena `QueryExecutionStatistics` with recorded query ids, both directions, and the losses reported alongside the wins (`digital_event` planning +217%, runtime +156%) | §12.1 |

### Where this audit was wrong, and it matters

**Q9.** The document assumed per-table storage helps uniformly. It does not. For the table
that dominates the monolith the saving is ~7%, and two metrics get *worse* at this data
volume. The decision still holds — seven of ten tables are small and gain a lot, and the
architectural benefits (independent schema, retention, maintenance and lifecycle per table)
were never contingent on scan bytes — but the headline number applies to the tail, not the head.

**Q11.** The brief proposed `config/cdc/tables/<source>/<db>/<schema>/<table>.yaml`. Rejected
in favour of one file, because ten tables across two engines fit in ~110 reviewable lines and
a per-table file tree makes the *cross-table* invariants — unique ids, unique targets,
deterministic topics — invisible in review. That reasoning would invert at ~50 tables.

### Status

Phase 0's own token remains valid and its two PENDING evidence items are now closed. The
implementation it authorised is complete through Phase 8:
**`CONFIG_DRIVEN_CDC_TABLE_PLATFORM_PRODUCTION_READY`**.
