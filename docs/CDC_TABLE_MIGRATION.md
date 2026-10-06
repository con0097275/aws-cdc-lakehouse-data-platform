# CDC table migration — legacy monolith to per-table

**Contract**: ADR-062 (why), ADR-067 (how). **Operational runbook**: `CDC_CUTOVER.md` — the
gate, backfill, benchmark and rollback commands live there. This document is the **strategy**:
which mode a table is in, what each guarantees, and why nothing is ever deleted.

---

## The monolith is never dropped

`kafka_dev_lab_dev_full_cdc.cdc_events` stays. It is the **reconciliation baseline for every
window already captured**, and it is the thing a per-table table is compared *against*. A
migration that destroys its own baseline cannot be verified after the fact.

It is also still written to: a table in `DUAL_WRITE` sends events to both paths. The monolith
grew 20,390 → 20,400 during Phase 8 for exactly that reason, which is the mode working rather
than drift.

## Three modes, per table

| mode | ingest writes | readers use | meaning |
|---|---|---|---|
| `LEGACY` | monolith only | monolith | the starting state; per-table table may be empty |
| `DUAL` | **both** | monolith | both paths receive every event, so they can be compared |
| `PER_TABLE` | per-table only | per-table | cut over |

The mode is **per table**, not per platform. Today: 2 of 10 are `PER_TABLE`, the rest
`LEGACY`. There is no flag day.

```bash
python3 scripts/cdc-cutover.py status            # every table and its read path
python3 scripts/cdc-cutover.py plan              # remaining tables, ascending risk
python3 scripts/cdc-cutover.py gate --table <id> # the nine checks
python3 scripts/cdc-cutover.py set --table <id> --mode DUAL
```

`set` writes `cdc/registry/cutover.yaml` — a Git-tracked file a human reviews and commits. It
never touches AWS.

## The order is forced

```
LEGACY  →  DUAL  →  (gate passes)  →  PER_TABLE
```

`DUAL` before `PER_TABLE` is not caution for its own sake: it is what makes the gate's
equivalence checks *possible*. You cannot compare two paths until both have the same events.

**Backfill** fills the per-table table from the monolith for history that predates dual-write.
It preserves `dv_event_id`, `dv_src_event_id`, source ordering, Kafka metadata and operations —
identity is copied, never regenerated, or the two paths could never be shown equal.

> What backfill **cannot** copy is `dv_pk_hash`: the monolith has no such column, so backfilled
> rows carry NULL. Live-captured rows have it. EOD recomputes the grain, so closes are correct
> either way (open issue #2).

## The gate: nine checks, and unmeasured is a failure

A check with no observed value is a **FAIL**, not a skip. "We did not measure it" and "it was
fine" must not reach the same conclusion — that is the whole difference between a gate and a
formality.

The equivalences are computed **in Athena from the two tables as they now stand**, not by
re-running the backfill and trusting its own report. A check computed by the writer confirms
only the writer's belief.

## Benchmark before claiming improvement

Measured, with recorded Athena query ids:

| table | EOD scan, legacy → per-table |
|---|---|
| `oracle/ACCOUNT` | 89,193 → 11,231 bytes (**−87.4%**) |
| `sqlserver/digital_event` | 113,261 → 105,163 bytes (**−7.1%**) |

The second number is the honest one. `digital_event` **is** 12,000 of the monolith's 20,390
rows, so isolating the dominant table saves little — and two of its metrics got *worse*
(planning +217%, runtime +156%) at this volume. The architectural benefits (independent schema,
retention, maintenance and lifecycle per table) were never contingent on scan bytes, but the
headline saving applies to the tail, not the head.

## Rollback

Reversible at every stage, and this is a property of the design rather than a procedure:

1. **From `PER_TABLE`** → set the mode back to `DUAL` or `LEGACY`. The monolith has been
   receiving events all along in `DUAL`, and always has the pre-cutover history.
2. **From `DUAL`** → set `LEGACY`. The per-table table stops being written; nothing read it.
3. **The per-table tables can be dropped entirely** — they hold only what capture and backfill
   produced, both reproducible.
4. **EOD and REALTIME are derived**, so any window or COB can be rebuilt from whichever
   FULL_CDC the mode points at.

The one irreversible act in the whole platform is `expire_snapshots` / `remove_orphan_files`,
which is why they are bounded by retention and opt-in respectively
(`ICEBERG_MAINTENANCE_RUNBOOK.md`).

## Expansion order

Ascending risk, from `cdc-cutover.py plan`: small near-static tables first, the dominant
high-volume table last. The point is that each cutover is a rehearsal for the next one, and the
cheapest rehearsals should come first.

## What migration does NOT change

* no source is modified — the platform only reads
* no connector is reconfigured by cutover; capture is independent of which target is written
* no certified EOD number changes: the same FULL_CDC history through the same engine at the
  same cutoff produces the same close, which is what `--fulfill` convergence demonstrates
