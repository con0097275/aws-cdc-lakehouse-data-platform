# ADR-062 — CDC layer table granularity and partitioning

* **Status**: Proposed (Phase 0 design approved, implementation pending)
* **Date**: 2026-09-10
* **Supersedes**: nothing. Refines ADR-033 (logical vs physical layer naming).
* **Evidence**: `docs/CDC_TABLE_PLATFORM_TARGET.md`

## Context

`FULL_CDC` is one monolithic Iceberg table, `kafka_dev_lab_dev_full_cdc.cdc_events`,
partitioned `identity(source_system), identity(event_date)` with **no sort order**, holding
all eight source tables from two engines.

The Phase 0 audit measured the workload against that spec:

| column | partitioned | consumer filter hits | pruning achieved |
|---|---|---|---|
| `source_system` | yes | 4 | cardinality 2 → ≤50% |
| `source_table` | **no** | **6 (primary filter)** | **none** |
| `event_date` | yes | 1 | rarely exercised |
| `source_commit_ts` | no | 15 (real windowing column) | file stats only |

`eod/job.py` and `rt_stream_app.py` both filter `source_system AND source_table`, then scan
every row of every other table in the matching partitions and discard ~87% of them.

Also measured: mean data file 137 KiB against a 512 MiB target; 109 `metadata.json` files
across **three orphaned metadata chains** (one per destroy/apply cycle); and two files under
`source_system=oracle/event_date=null`, traced to a poison record ingested before the
quarantine predicate was corrected.

## Decision

**Two phases, gated on evidence rather than taken together.**

### Phase A — evolve the spec (do now)

```
partition spec  identity(source_system), identity(source_table), identity(event_date)
sort order      source_table, source_commit_ts
```

Metadata-only. Existing files retain `spec-id 0` and stay readable; only new writes use the
new spec. No rewrite, no backfill, no consumer change.

### Phase B — one FULL_CDC table per source table (do when triggered, not before)

Plus a lightweight `ops.cdc_event_index` carrying coordinates and no payloads, to preserve
cross-source audit — the one property the monolith is genuinely better at.

**Triggers, either of which is sufficient:**

1. concurrent per-table writers (one streaming app per source table), or
2. differentiated retention, PII or IAM policy between source tables.

**Explicitly NOT a trigger: data volume.** 20,390 rows across 6 partitions argues for
nothing.

## Rationale

Phase A fixes the one *measured* defect at zero cost and zero risk. Deferring it in favour
of the larger change would leave a known, quantified full-scan in place for months.

Phase B's decisive argument is **commit contention, not size**. Iceberg commits are
optimistic concurrency on the table. Today's ingest loops topics sequentially, so there is
no conflict — but the natural scaling move is one streaming app per source table, which this
platform is already shaped for (the batch job loops topics; the streaming job groups each
micro-batch by topic). That puts N concurrent writers on one table and turns every commit
into a retry storm. Per-table storage removes it by construction; no partitioning scheme
does.

The secondary argument is governance. CLAUDE.md §6 requires owner, grain, retention,
freshness SLA and DQ rules **per table**. A monolith cannot express eight different answers,
which is why the AI plane is denied `warehouse/full_cdc/` wholesale (ADR-060) rather than
per table.

## Options

* **`bucket(source_table)`** — hash collisions blur exactly the pruning being bought, for no
  gain over identity at cardinality 8.
* **Write-ordering only, no new partition field** — gives file-stat pruning but no partition
  pruning; strictly weaker than Phase A, which includes the sort order anyway.
* **Phase B immediately** — multiplies table count 8x at a volume where mean file size is
  already 137 KiB, and spends the migration before either trigger is real.
* **Splitting REALTIME/EOD per source table now** — no measured pruning defect there;
  change without evidence.

## Consequences

* Phase A: none for consumers. Iceberg reads mixed specs transparently.
* Phase B: six consumer filter sites must route through the registry
  (`docs/CDC_TABLE_ONBOARDING_DESIGN.md`) before cutover, never by find/replace.
* The monolith is **never dropped**, even after retirement — it is the reconciliation
  baseline for every window already captured.
* `event_date IS NULL` becomes an enforced DQ control with tolerance 0. It is not a
  legitimate business state; it is the signature of an undecodable record that slipped the
  quarantine filter. `event_date` must **not** become a required Iceberg field — that would
  turn a quarantine-able row into a job crash and undo G-P1-1.

## Cost

| item | impact |
|---|---|
| Phase A | **$0** — metadata-only `ALTER TABLE … ADD PARTITION FIELD` |
| Phase A benefit | ~87% fewer bytes scanned on table-filtered reads; Athena bills per byte and the workgroup caps at 10 GiB/query |
| Phase B dual-write | one extra EMR ingest pass per window (~$0.30 at the measured rate) plus transient duplicate storage (~$0.01/month at the lake's 300 MB) |
| Global index | coordinates only, no payloads |
| Orphan metadata cleanup | reduces LIST cost; 109 metadata files across 3 chains is already a listing tax on every operation |
| Per-table maintenance | 8 compaction jobs instead of 1, each far smaller; schedulable independently and skippable when a table is idle |

Immaterial against the $100/month budget of record (ADR-030 as amended 2026-09-06). The
dominant cost remains platform uptime at ~$1.12/hr, not storage layout.

## Security

* **Phase A: none.** No new resource, no IAM change, no data movement.
* **Phase B: a net improvement.** Per-table tables make per-table IAM expressible. Today any
  principal that can read `cdc_events` reads all eight source tables, so a PII table cannot
  be granted separately — which is precisely why the AI plane is denied
  `warehouse/full_cdc/` wholesale (ADR-060) instead of per table.
* The registry names tables, owners and policy only. **No credentials** — secrets stay in
  SSM SecureString (CLAUDE.md §3.1/§3.6).
* The global index carries coordinates and no payloads, so it does not widen PII exposure.

## Rollback

* **Phase A**: `ALTER TABLE … DROP PARTITION FIELD source_table`. Metadata-only and
  immediate. Files written under the newer spec remain readable — Iceberg reads mixed specs
  transparently, which is the same property that makes the change safe in the first place.
* **Phase B**: the monolith is dual-written throughout and **never dropped**, so rollback is
  repointing consumers at `cdc_events` via the registry. One consumer at a time, each
  independently revertible.
* Retirement never means DROP. The monolith stays read-only as the reconciliation baseline
  for every window already captured.

## Validation

Phase A is accepted only if all four hold on a live run:

1. `B6` bytes-scanned for `source_system='oracle' AND source_table='ACCOUNT'` falls by
   **≥80%** versus the same query before the change.
2. `SELECT COUNT(*) … WHERE event_date IS NULL` returns **0**.
3. `cdc_events$partitions` shows **both** spec ids, proving old files were not rewritten.
4. All five reporting flow modes still run green, and the independent Athena reconciliation
   `expected_from_full_cdc == eod_rows` still matches exactly.

Phase B is accepted only if, per table, a full-outer-join diff on `event_id` between the
monolith and the per-table target returns **zero rows**, computed independently in Athena
rather than by the job that wrote the data.
