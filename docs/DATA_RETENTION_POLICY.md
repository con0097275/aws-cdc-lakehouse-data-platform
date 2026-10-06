# DATA RETENTION POLICY

- Phase: DRP7 · Six retention domains, deliberately documented **separately**
- Code: `cdc/governance.py` (`RetentionClass`, `RETENTION_MAX_DAYS`), `cdc/maintenance.py`

---

## 1. Why six, not one

Retention is six different questions with six different owners and six different failure
modes. A single "retention policy" number is how a Kafka topic gets deleted while it is still
the only replay source for a layer that has not been validated.

| # | Domain | Set in | Enforced by |
|---|---|---|---|
| 1 | business data (lake tables) | `retention_class` + `eod.retention_days` | Iceberg maintenance |
| 2 | Kafka topics | MSK topic config | the broker |
| 3 | Iceberg snapshot **metadata** | `maintenance.expire_snapshots_days` | `cdc/maintenance.py` |
| 4 | REALTIME window extent | `realtime.retention_hours` | the REALTIME writer |
| 5 | DQ / reconciliation history | `ops.*` table policy | not yet set |
| 6 | lineage metadata | DataHub retention | not yet set |

## 2. Business data — band and number

`retention_class` is the governance **band**; `eod.retention_days` is the executable
**number**. They are checked against each other, never stored twice:

| Band | Cap | Assets |
|---|---|---|
| `transient` | 7 d | 0 |
| `short` | 35 d | 0 |
| `standard` | 400 d | 34 |
| `long` | 2600 d (~7 y) | 50 |
| `regulatory` | unbounded; deletion requires approval | 0 |

`retention_band_mismatch` fires when the executable number exceeds the band's cap — "the
band is what a reviewer reads and the number is what actually deletes".

## 3. Kafka

Topic retention is a **broker** setting, not a lakehouse one, and is deliberately not derived
from `retention_class`. The constraint that matters:

> **A topic may not be aged out while it is the only replay source for a layer that has not
> been validated.**

FULL_CDC is written straight from Kafka and is append-only, so once an event is in FULL_CDC
the topic is no longer the only copy. Until then it is.

## 4. Iceberg snapshot metadata

`expire_snapshots_days: 7`, `remove_orphan_files_days: 3` (registry defaults).

Snapshot expiry destroys **time travel**, which is what `VERSION AS OF` in the EOD-certified
rebase depends on (ADR-085) and what `source_snapshot_id` in a run facet points at. Expiring
to 1 day would make a three-day-old incident un-investigable.

Orphan removal **deletes files**. It must never run with a retention short enough to catch
files a live job is still about to commit (CLAUDE.md §6).

## 5. REALTIME window

`realtime.retention_hours` is the table's physical **extent**, not an archival policy. Under
`append` it must equal the materialised window, or the layer keeps days it never promised and
a consumer counting rows sees the table change shape.

`digital_event` carries `retention_hours: 96` for exactly that reason.

## 6. DQ, reconciliation and incident history

**Not yet set**, and the reason to be careful: these tables are append-only because "why was
COB 28 blocked" is answerable only from the records that were later superseded. A short
retention here does not save much storage and destroys every post-mortem older than it.

Proposed, pending DRP10: DQ and reconciliation results `long`, incidents and recovery plans
`long`, aligned with the business data they describe.

## 7. Lineage metadata

**Not yet set.** Run lineage is high-volume — a streaming job emitting per micro-batch is
1,440 events a day per table. Static lineage is small and should outlive the runs it
describes.

Proposed, pending DRP10: static lineage indefinite, run lineage aligned with the OPS ledgers
it points at. A run link whose ledger row has been deleted is a dangling pointer, and a
dangling pointer is worse than an absent one.

## 8. Deleting is not the opposite of keeping

Two rules that override every number above:

1. **A tombstone is not deletable.** Removing it lets a late out-of-order event resurrect a
   key the source deleted.
2. **FULL_CDC is the replay source.** Deleting it before the layers above are validated
   removes the only way to rebuild — which is why CLAUDE.md §5 makes retention there a
   correctness setting, not a storage one.
