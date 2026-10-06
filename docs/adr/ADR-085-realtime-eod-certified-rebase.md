# ADR-085 — Rebasing the REALTIME overlay onto a certified EOD close

* **Status:** Accepted
* **Date:** 2026-09-28
* **Phase:** R2-F
* **Related:** ADR-076 (the EOD control plane), ADR-081, ADR-082, ADR-083

## Context

A `latest_state` table is an **overlay**, not a standalone truth:

```
current state  =  certified EOD baseline  +  REALTIME changes after its cutoff
```

While the baseline is D-1, every change since D-1's cutoff has to live in REALTIME. When D
certifies, the changes between D-1 and D are now *in* the baseline. Keeping them in the
overlay as well means a consumer composing the two sees them twice — or prefers a stale
overlay row over one the certified close has since corrected.

Nothing rebased. The R2-A audit recorded it as absent, and `realtime_info.baseline_cob_date`
existed with nothing to set it.

## Decision

A rebase is a distinct **operation** on a table, not a mode of the materialiser:
`realtime_engine.py --rebase-cob YYYY-MM-DD`. It removes the overlay rows the newly
certified close already contains and retains everything after its cutoff.

### It refuses far more often than it acts

A rebase **deletes**. If the close is not actually certified, or its cutoff is unknown, or
the baseline would move backwards, the rows removed are rows nothing else holds — and there
is no error afterwards, only a state table that has quietly forgotten a day. So
`rebase_decision()` is a pure function of six guards:

| refused when | because |
|---|---|
| the table did not ask for it | `rebase.on_eod_certified` is false |
| the shape is `event_window` | it keeps EVENTS; there is nothing a close makes redundant, and a rebase would delete events the layer promised |
| no `eod_info` row, or not `CERTIFIED` | rebasing onto uncertified numbers |
| the close recorded no cutoff | *what* the baseline contains is unknown, and the rebase is defined entirely in terms of it |
| the cutoff was not formatted by Spark | see below |
| the baseline is already this COB | idempotency |
| the COB is older than the current baseline | its cutoff is earlier, so it would remove rows the current baseline does not have |

The `event_window` case is also refused at compile.

### The delete matches on `dv_event_id` as well as the key

The overlay keeps accepting events throughout the rebase. The keys to remove are computed
from a **frozen** read — `VERSION AS OF` the target's snapshot when the rebase began — and
the delete is:

```sql
MERGE INTO <target> t USING rt_rebase_keys s
  ON t.dv_pk_hash = s.dv_pk_hash AND t.dv_event_id = s.dv_event_id
WHEN MATCHED THEN DELETE
```

Matching on the key alone would delete a row written *after* the frozen read — a change the
certified baseline does not contain and nothing else holds. Including `dv_event_id` means a
row that has since been superseded no longer matches and survives: **the delete removes only
what it actually looked at.**

A MERGE rather than `DELETE ... WHERE EXISTS`, because a correlated subquery in DELETE is not
supported across the Spark versions this runs on.

### A timestamp that has been through Python is not the timestamp you stored

**Found while writing the tests, and it is a production bug, not a fixture artifact.**

`collect()` renders a Spark TIMESTAMP into a *naive* Python datetime in the **driver's local
zone**. Store midnight UTC, collect it on a machine in UTC+7, get `07:00` with no tzinfo —
and formatting that back into a SQL literal, which Spark reads in the *session* zone, moves
the value by the driver's offset. On an EMR driver in UTC the offset is zero and nothing
happens. From a laptop in UTC+7 the rebase runs against a cutoff seven hours late and deletes
overlay rows the baseline does not contain.

The cutoff is now formatted by `date_format` **inside Spark** and carried as a string. A
decision without that string is refused rather than falling back to the collected timestamp.

The same defect was latent in `last_successful_upper`, whose collected timestamp fed the
day-boundary rebuild comparison: on a UTC+7 driver the rebuild was skipped or run on the
wrong day for the seven hours either side of midnight. Also fixed, and the module docstring
now states the rule.

## Options

**Rebase inside the materialiser, on a flag.** Couples two operations with different
failure semantics into one ledger row; "what removed these rows" becomes unanswerable.

**Delete by cutoff alone, no frozen read.** Simpler, and it deletes concurrent post-cutoff
changes — the exact race §17 of the brief names.

**Have the EOD close do the rebase.** It has the cutoff and the certification in hand. It
would also make EOD depend on REALTIME, which ADR-065 and a standing test forbid.

## Consequences

* `ops.realtime_run` gained `operation` (`materialise` | `rebase`) and
  `prev_baseline_cob_date`. Two genuinely different things happen to the table and both
  leave a row; a history that conflated them could not say what removed what.
* `realtime_info.baseline_cob_date` is now written, so "which close is this overlay on" is
  answerable — which is the precondition for the `CURRENT_STATE` contract.
* EOD remains independent: the rebase reads `eod_info`, never the reverse.

## Cost

One extra Spark job per certified close per state table — four tables here, once a day. It
reads the target's key columns and issues one MERGE. Negligible beside the close itself.

## Security

No change. `eod_info` is read, not written; the rebase touches only the REALTIME target it
already writes.

## Rollback

`rebase: {on_eod_certified: false}` in the registry and recompile; the operation then skips
with `not_enabled`. Nothing needs undoing — a rebase removes only rows that the certified
EOD baseline holds, so the composition stays correct with the overlay un-rebased, merely
larger.

## Validation

`spark/tests/test_realtime_rebase.py` — 18 tests.

Pure (10): every refusal, including a cutoff not formatted by Spark; the delete's match
predicate; that it is a MERGE.

Against real Iceberg (8): pre-cutoff overlays removed and post-cutoff ones retained; both
baselines, the cutoff, the frozen snapshot and the counts recorded in `realtime_info` and in
the ledger under `operation = 'rebase'`; a second rebase onto the same COB removes nothing;
an uncertified close and a missing `eod_info` row each remove nothing; and **an event that
lands for a key between the frozen read and the delete is not removed**.
