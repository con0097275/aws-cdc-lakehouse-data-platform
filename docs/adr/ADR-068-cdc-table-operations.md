# ADR-068 — CDC table operations: metric-driven maintenance, governance, safe evolution

* **Status**: Accepted (implementation: Phase 7)
* **Date**: 2026-09-10
* **Extends**: ADR-062…067
* **Evidence**: `spark/tests/test_cdc_operations.py`, `artifacts/cdc/metrics.json`

## Context

Six phases made the CDC layer config-driven and cut two tables over. Nothing yet decided
**when a table needs maintenance**, and the Phase 0 audit had already measured the problem it
would have to solve: a mean data file of **137 KiB against a 128 MiB target** — three orders
of magnitude under — caused by commit frequency, not elapsed time. Re-measured live on
2026-09-10, the monolith is unchanged: 14 files, 140,546 B mean, 13 manifests, **53
snapshots**.

Nor was there a rule for what a schema change may do, a record of what each table's
governance actually is, or a safe way to retire a table.

## Decision

### 1. Maintenance is decided by MEASURED metrics, bounded by a cadence (§A, §B, §C)

A cron compacts whether or not there is work. The trigger is therefore a threshold on a
measured metric — small-file count, mean file size, manifest count, snapshot count, orphan
count — and the cadence is a **floor** that stops a hot table being compacted continuously.

Temperature (`hot`/`warm`/`cold`, floors 6h/24h/168h) is **inferred** from what the registry
already says — realtime enabled plus freshness SLA — so onboarding needs no guess about a
property the platform can read. An explicit `maintenance.temperature` always wins.

**Both rewrite triggers require enough FILES to be worth rewriting.** Measured live: the
freshly backfilled `oracle/ACCOUNT` holds 2 files averaging 48 KB, far under target — and a
mean-only trigger asked to compact two files into one, an EMR run to eliminate one file, on
every cadence forever. `min_small_files` existed to prevent that and the mean branch was
bypassing it.

**Action order is not arbitrary:** rewrite → manifests → expire → orphans. Compaction writes
new files and leaves the old ones referenced by older snapshots, so expiry *afterwards* is
what frees the space; reversed, storage never drops. Orphan removal is last because it
deletes.

**Batches are bounded** (`select_batch`, default 2, worst-first). Iceberg maintenance commits
to the table; running every table at once turns the maintenance window into peak load on the
same capacity the ingest uses.

### 2. Orphan removal needs a measured count, and is off by default

It is the action that **deletes**. "A week has passed" is not evidence that anything is
orphaned, so the trigger is `orphan_file_count >= 1` — never elapsed time — and the action is
opt-in per table. The `older_than` guard and the compile-time rule that orphan retention may
not outrun snapshot expiry (CLAUDE.md §6) both remain.

### 3. Governance is exposed per table, and `pii` is derived (§D)

`owner`, `domain`, `classification`, `retention`, `SLA`, `DQ`, `schema policy` — all already
in the registry, now surfaced as one record and **propagated into `TBLPROPERTIES`** where a
catalog reader can see them.

`pii` is **derived from `classification`**, not stored beside it. Two fields meaning the same
thing is how a table ends up `pii: false` and `classification: confidential` at once, with
nothing to say which one the AI-plane deny list should believe.

All 8 shipped tables report zero governance gaps.

### 4. Destructive schema change is blocked, not flagged (§F)

| change | verdict |
|---|---|
| new nullable column | **ALLOW** — recoverable; existing rows read NULL, which is accurate |
| widening (`int→bigint`, decimal precision at same scale) | **ALLOW** |
| drop column | **BLOCK** — destroys data no later change restores |
| rename | **BLOCK** — a diff cannot tell a rename from a drop-and-add, and one of them deletes |
| narrowing / cross-family / decimal **scale** change | **BLOCK** — reinterprets data already written |
| primary-key change | **BLOCK** — redefines the grain of every derived layer; snapshots either side are not comparable and both look correct |

Every block carries a remediation. A block with no next step is a wall.

### 5. Decommission is an ordered sequence that deletes nothing (§G)

`disable downstream → stop capture → freeze final state → retention decision → archive →
remove capture (approval)`.

**Downstream first.** Stopping capture first leaves consumers reading a table that has
silently stopped advancing: every query succeeds and every number is quietly stale.
Disabling consumers first makes the staleness an absence instead of a wrong answer.

**No step deletes data**, asserted by test. `remove capture` removes the table from
`table.include.list`; every Iceberg row it captured stays — the same rule ADR-062 fixed for
the monolith, and for the same reason: the moment you need it is the moment someone asks
about a period it covered.

## Options

* **Cron-only maintenance.** Rejected — §1. It does the same work whether or not there is any.
* **Metrics-only, no cadence.** Rejected: a hot table would be compacted continuously.
* **Orphan removal on a schedule.** Rejected — §2. It deletes.
* **Store `pii` as its own field.** Rejected — §3.
* **Auto-apply renames via `ALTER ... RENAME COLUMN`.** Rejected: Iceberg's rename does
  preserve data, but a schema *diff* cannot distinguish it from a drop-and-add, and acting
  on the guess is what makes it destructive.
* **Maintain every table in one nightly run.** Rejected — §1.

## Consequences

* Maintenance requires measured metrics: `cdc-maintenance.py measure` before `plan`, and the
  job **requires** `--metrics`. A run with no measurement is a cron in disguise.
* Orphan counts are reported as `0 = not measured`, and the action stays off until a real
  count exists — counting them means listing the table's location and diffing the manifests.
* Two more OPS concepts (`maintenance_run`, the decommission record) are referenced by the
  observability surface; neither is provisioned yet.

## Cost

| item | impact |
|---|---|
| `measure` | Athena metadata reads (`$files`, `$manifests`, `$snapshots`) — kilobytes, not table scans |
| `plan`, `governance`, `observe`, `decommission` | **$0** — local and pure |
| `rewrite_data_files` | the dominant cost; bounded by batch size and by the file-count floor that stops pointless rewrites |
| `expire_snapshots` | cheap, and the action that actually reclaims storage |
| `remove_orphan_files` | a full location listing; off by default |

The floor that refused to compact a 2-file table is a direct saving: without it, every cadence
would have spent an EMR run per pilot table to eliminate one file.

## Security

* No new resource, no IAM change. Maintenance reads and rewrites tables the Spark role
  already writes.
* Governance propagation puts `cdc.classification` on the table itself, which is what makes a
  per-table grant expressible — the point of ADR-062's trigger 2.
* Decommission deletes nothing, so an offboarding mistake is recoverable.
* The destructive maintenance action is opt-in, measured, and guarded by `older_than`.

## Rollback

* Every decision module is pure and additive; reverting the phase is deleting the new files.
* Maintenance actions are Iceberg operations: a compaction is a new snapshot and is itself
  revertible until snapshots expire. Expiry and orphan removal are **not** revertible, which
  is why one needs a measured count and the other is off by default.
* No schema change is applied by this phase — it only classifies.

## Validation

`pytest spark/tests/test_cdc_operations.py` — **50 tests**: the measured monolith needing all
three non-destructive actions; a 2-file table left alone; thresholds independent per metric;
orphan removal refusing elapsed time; action ordering; temperature inference and the cadence
floor; bounded worst-first batching; the retention guard; every schema verdict including the
decimal-scale and PK cases; governance completeness and propagation into `TBLPROPERTIES`;
every observability signal present with disabled layers marked not-applicable; and the
offboarding order, refusals and approval gate.

Live: `cdc-maintenance.py measure` against all 8 registered tables, and the monolith
re-measured at 14 files / 140,546 B / 13 manifests / 53 snapshots — matching the Phase 0
audit's 14 files and 137.3 KiB.
