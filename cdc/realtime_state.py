"""REALTIME control plane: the cursor, and the table that remembers it. R2-C, ADR-083.

Pure. No Spark, no AWS, no I/O -- the same separation `cdc/realtime.py` and `cdc/eod.py`
make, for the same reason. The decision "which FULL_CDC rows has this table not seen yet"
is where an incremental layer goes silently wrong, and a wrong answer here is not a crash:
it is a REALTIME table that is missing events nobody counted.

WHY AN INCREMENTAL CURSOR IS A TRADE, NOT AN IMPROVEMENT
--------------------------------------------------------
The window scan it replaces re-filters `[grace_lower, upper)` out of FULL_CDC on every run,
144 times a day. That is expensive, and it is also IDEMPOTENT AND SELF-HEALING: it cannot be
incomplete, because it does not depend on remembering anything. Whatever went wrong last
run, this run reads the whole window again and produces the right answer.

An incremental read gives that up. It is correct only while the cursor is correct, and a
cursor can be wrong in ways that produce no error at all -- an expired snapshot, a rollback,
a compaction that rewrote files the append scan would have skipped. So every function below
is written to FALL BACK TO THE FULL SCAN and say why, rather than to read less and hope.

    a run that reads too much is slow.
    a run that reads too little is wrong, and nothing reports it.

THE THREE THINGS THAT MAKE AN INCREMENTAL SCAN UNSAFE
-----------------------------------------------------
Iceberg's incremental append scan returns rows added by APPEND snapshots in (from, to]. It
is defined only over appends, and FULL_CDC is append-oriented -- but it is not append-ONLY:

* `rewrite_data_files` (compaction, run weekly by `cdc_maintenance`) produces a REPLACE
  snapshot. The same rows, in different files. An append scan over a range containing one
  either fails or silently skips, depending on the Iceberg version -- and "silently skips"
  means every event compacted in that window is missing from REALTIME.
* `expire_snapshots` can remove the snapshot the cursor names. The range then has no
  starting point at all.
* A rollback makes the current snapshot an ANCESTOR of the cursor. The cursor is ahead of
  the table.

Each is detected explicitly below and each falls back to the window scan with a recorded
reason. `fallback_reason` is a column, not a log line, because "why did this run cost ten
times the last one" must be answerable a week later.
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import ConfigError

# --------------------------------------------------------------------------- #
# ops.realtime_info -- the CURRENT state of each table, one row, MERGEd in place
# --------------------------------------------------------------------------- #
#
# WHY THIS IS SEPARATE FROM `ops.realtime_run`.
#
# `realtime_run` is an append log: every attempt, forever, failures retained. Asking it
# "where is this table's cursor" means "the latest row, by some ordering, that happens to be
# SUCCEEDED" -- a convention no column enforces and every reader re-implements. That is the
# exact shape ADR-076 removed from the EOD layer, and this is the same fix for the same
# reason: one table answers "what is true now", the other answers "what happened".
#
# `realtime_run` KEEPS its name and stays the run history the R2 brief section 19 asks for.
# A third table called `realtime_run_hist` would be a rename of something that already works.

REALTIME_INFO_TABLE = "realtime_info"

REALTIME_INFO_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("table_id", "STRING", "canonical registry id -- the logical key"),
    ("target_table", "STRING", "REALTIME identifier this row is the state of"),
    ("source_table", "STRING", "FULL_CDC identifier the cursor is into"),
    ("shape", "STRING", "event_window | latest_state"),
    ("write_strategy", "STRING", "overwrite_window | append | guarded_merge"),
    ("source_progress", "STRING", "window_scan | iceberg_snapshot"),
    ("baseline_cob_date", "DATE",
     "the certified EOD close this table's state is an overlay ON. NULL until R2-F"),
    ("last_source_snapshot_id", "BIGINT",
     "THE CURSOR. The FULL_CDC snapshot fully incorporated into the target. Advanced ONLY "
     "after the target commit and the validation both succeeded"),
    ("current_source_snapshot_id", "BIGINT",
     "the FULL_CDC snapshot the last successful run FROZE as its upper bound"),
    ("lower_bound_ts", "TIMESTAMP", "start of what the last run materialised"),
    ("upper_bound_ts", "TIMESTAMP", "the frozen upper bound of the last run"),
    ("target_snapshot_id", "BIGINT", "REALTIME snapshot the last successful run produced"),
    ("last_success_run_id", "STRING", "the run that produced this state"),
    ("input_rows", "BIGINT", "rows READ from FULL_CDC by that run"),
    ("output_rows", "BIGINT", "rows in the target after it"),
    ("read_mode", "STRING", "incremental | window_scan | noop -- how that run actually read"),
    ("fallback_reason", "STRING",
     "why an incremental run fell back to the full window scan. Empty when it did not"),
    ("status", "STRING", "SUCCEEDED | SUCCEEDED_NOOP | FAILED"),
    ("start_time", "TIMESTAMP", ""),
    ("end_time", "TIMESTAMP", ""),
    ("config_version", "STRING", "which compiled plan produced this state"),
    ("err_msg", "STRING", "truncated; the full trace stays in the driver log"),
    ("updated_at", "TIMESTAMP", ""),
)

#: The logical key. ONE row per table -- not per run, not per day. Iceberg does not enforce
#: uniqueness, so the MERGE is what enforces it, and a test asserting a second run UPDATES
#: rather than appends is what proves the MERGE is actually used.
REALTIME_INFO_KEY = ("table_id",)


# --------------------------------------------------------------------------- #
# how a run reads
# --------------------------------------------------------------------------- #

#: Read only the FULL_CDC snapshots appended since the cursor.
MODE_INCREMENTAL = "incremental"
#: Re-filter the whole window. Always correct, never cheap.
MODE_WINDOW_SCAN = "window_scan"
#: Nothing to do: the source has not moved since the cursor.
MODE_NOOP = "noop"
READ_MODES = (MODE_INCREMENTAL, MODE_WINDOW_SCAN, MODE_NOOP)

#: Why a run that WANTED to be incremental was not. Empty string means it was.
FALLBACK_NOT_CONFIGURED = "not_configured"
FALLBACK_NO_CURSOR = "no_cursor"
FALLBACK_CURSOR_EXPIRED = "cursor_expired"
FALLBACK_CURSOR_AHEAD = "cursor_ahead_of_source"
FALLBACK_UNSAFE_SNAPSHOT = "unsafe_snapshot"
FALLBACK_FORCED = "forced_rebuild"
#: The write strategy REPLACES the target's data, so a partial read would delete everything
#: the delta does not contain. Refused at compile (cdc/config_loader.py); this constant is
#: the engine's second line of defence against a plan compiled before that check existed.
FALLBACK_STRATEGY_REBUILDS = "write_strategy_rebuilds"
FALLBACK_REASONS = (FALLBACK_NOT_CONFIGURED, FALLBACK_NO_CURSOR, FALLBACK_CURSOR_EXPIRED,
                    FALLBACK_CURSOR_AHEAD, FALLBACK_UNSAFE_SNAPSHOT, FALLBACK_FORCED,
                    FALLBACK_STRATEGY_REBUILDS)

#: The only Iceberg snapshot operation an incremental APPEND scan is defined over.
#: `replace` is compaction, `overwrite` and `delete` are row-level changes. All three mean
#: rows moved between files, which an append scan does not see.
SAFE_OPERATION = "append"


@dataclass(frozen=True)
class SourceSnapshot:
    """One row of `{table}.snapshots`, in the order Iceberg committed them."""
    snapshot_id: int
    parent_id: "int | None"
    operation: str


@dataclass(frozen=True)
class ReadPlan:
    """What one run decided to read, and why. Recorded, not just acted on."""
    mode: str
    from_snapshot: "int | None" = None
    to_snapshot: "int | None" = None
    fallback_reason: str = ""
    detail: str = ""

    def __post_init__(self) -> None:
        if self.mode not in READ_MODES:
            raise ConfigError(f"read mode {self.mode!r} is not one of "
                              f"{', '.join(READ_MODES)}")
        if self.mode == MODE_INCREMENTAL and self.from_snapshot is None:
            raise ConfigError(
                "an incremental read needs a starting snapshot. Without one Iceberg reads "
                "the whole table as 'appended', which is the full scan wearing the "
                "incremental label -- and it would advance the cursor as if it were one")
        if self.fallback_reason and self.fallback_reason not in FALLBACK_REASONS:
            raise ConfigError(f"fallback reason {self.fallback_reason!r} is not one of "
                              f"{', '.join(FALLBACK_REASONS)}")

    @property
    def is_incremental(self) -> bool:
        return self.mode == MODE_INCREMENTAL

    def describe(self) -> str:
        if self.mode == MODE_NOOP:
            return f"NOOP {self.detail}"
        if self.mode == MODE_INCREMENTAL:
            return f"INCREMENTAL ({self.from_snapshot} -> {self.to_snapshot}]"
        return f"WINDOW_SCAN fallback={self.fallback_reason} {self.detail}".rstrip()


def _lineage_index(lineage) -> dict:
    return {s.snapshot_id: i for i, s in enumerate(lineage)}


def plan_read(*, source_progress: str, last_snapshot, current_snapshot,
              lineage, forced_rebuild: bool = False,
              write_strategy: str = "") -> ReadPlan:
    """Decide how this run reads FULL_CDC. The whole cursor decision, in one pure function.

    `lineage` is `{table}.snapshots` oldest-first, as `SourceSnapshot`s.

    Every branch that is not a clean incremental read FALLS BACK TO THE WINDOW SCAN and
    names why. None of them raise: an unsafe cursor is an expected operational state -- a
    compaction ran, a snapshot expired -- and refusing to run would mean the layer stops
    serving because it could not take the cheap path.
    """
    from .realtime import PROGRESS_ICEBERG_SNAPSHOT

    if current_snapshot is None:
        # A source with no snapshots at all. Not an error: a table provisioned and not yet
        # ingested. Reading it produces nothing, and advancing a cursor past nothing would
        # record progress that did not happen.
        return ReadPlan(mode=MODE_NOOP, detail="source has no snapshots yet")

    from .realtime import WRITE_OVERWRITE_WINDOW

    if write_strategy == WRITE_OVERWRITE_WINDOW:
        # THE DANGEROUS COMBINATION. `overwrite_window` replaces the target's entire
        # contents with what this run produced. Feeding it an incremental delta would
        # delete every row the delta does not contain -- the table would shrink to the last
        # ten minutes of events and the job would report SUCCESS. Refused at compile; this
        # is the second line of defence, for a plan compiled before that check existed.
        return ReadPlan(mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
                        fallback_reason=FALLBACK_STRATEGY_REBUILDS,
                        detail=f"write_strategy={WRITE_OVERWRITE_WINDOW} replaces the "
                               f"target; a partial read would delete the rest")

    if source_progress != PROGRESS_ICEBERG_SNAPSHOT:
        return ReadPlan(mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
                        fallback_reason=FALLBACK_NOT_CONFIGURED,
                        detail=f"source_progress={source_progress}")

    if forced_rebuild:
        # `--rebuild` and the day-boundary rebuild both mean "re-derive from the window".
        # An incremental read cannot express that: it has no way to remove a row.
        return ReadPlan(mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
                        fallback_reason=FALLBACK_FORCED,
                        detail="rebuild re-derives the whole window")

    if last_snapshot is None:
        return ReadPlan(mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
                        fallback_reason=FALLBACK_NO_CURSOR,
                        detail="first run for this table")

    if last_snapshot == current_snapshot:
        return ReadPlan(mode=MODE_NOOP, from_snapshot=last_snapshot,
                        to_snapshot=current_snapshot,
                        detail=f"source unchanged at {current_snapshot}")

    index = _lineage_index(lineage)
    if last_snapshot not in index:
        # `expire_snapshots` removed it, or the table was rebuilt under the same name. Either
        # way there is no range to read: the starting point does not exist.
        return ReadPlan(mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
                        fallback_reason=FALLBACK_CURSOR_EXPIRED,
                        detail=f"cursor snapshot {last_snapshot} is not in the source's "
                               f"snapshot history ({len(lineage)} snapshots)")
    if current_snapshot not in index:
        return ReadPlan(mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
                        fallback_reason=FALLBACK_CURSOR_EXPIRED,
                        detail=f"current snapshot {current_snapshot} is not in the "
                               f"lineage that was read")

    lo, hi = index[last_snapshot], index[current_snapshot]
    if lo > hi:
        # The source was rolled back. Reading (cursor, current] is an empty or backwards
        # range, and treating it as "nothing new" would freeze the layer at a state the
        # source no longer has.
        return ReadPlan(mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
                        fallback_reason=FALLBACK_CURSOR_AHEAD,
                        detail=f"cursor {last_snapshot} is NEWER than the source's current "
                               f"snapshot {current_snapshot}; the source was rolled back")

    unsafe = [s for s in lineage[lo + 1:hi + 1] if s.operation != SAFE_OPERATION]
    if unsafe:
        first = unsafe[0]
        return ReadPlan(
            mode=MODE_WINDOW_SCAN, to_snapshot=current_snapshot,
            fallback_reason=FALLBACK_UNSAFE_SNAPSHOT,
            detail=f"{len(unsafe)} non-append snapshot(s) in the range; first is "
                   f"{first.snapshot_id} operation={first.operation!r}. An append scan "
                   f"does not see rows that moved between files")

    return ReadPlan(mode=MODE_INCREMENTAL, from_snapshot=last_snapshot,
                    to_snapshot=current_snapshot,
                    detail=f"{hi - lo} append snapshot(s)")


# --------------------------------------------------------------------------- #
# the MERGE that keeps `realtime_info` to one row per table
# --------------------------------------------------------------------------- #

def merge_info_sql(info_table: str, source_view: str = "rt_info_src") -> str:
    """UPSERT one table's current state. One row per `table_id`, forever.

    A MERGE rather than an append, and this is the whole difference between a control table
    and a log. An append would make "where is the cursor" mean "the latest row by some
    ordering", which is a convention no column enforces -- exactly what ADR-076 removed from
    the EOD layer.

    `UPDATE SET *` rather than a column list: a column added to `REALTIME_INFO_COLUMNS` and
    forgotten here would keep its old value forever on every table that already has a row,
    and nothing would report it. The wildcard cannot fall behind the schema.
    """
    cols = [c for c, _, _ in REALTIME_INFO_COLUMNS]
    on = " AND ".join(f"t.{k} = s.{k}" for k in REALTIME_INFO_KEY)
    missing = [k for k in REALTIME_INFO_KEY if k not in cols]
    if missing:
        raise ConfigError(f"realtime_info key column(s) {missing} are not in the schema")
    return (f"MERGE INTO {info_table} t USING {source_view} s\n"
            f"  ON {on}\n"
            f"WHEN MATCHED THEN UPDATE SET *\n"
            f"WHEN NOT MATCHED THEN INSERT *")


def advance_cursor(plan: ReadPlan, *, committed: bool, validated: bool,
                   previous):
    """The new cursor value, or the previous one. THE one place this is decided.

    Advanced ONLY when the target commit AND the validation both succeeded (R2 brief §20).
    Either failing leaves the cursor where it was, so the retry starts from the last
    genuinely-incorporated snapshot and re-reads the range that failed.

    Advancing on commit alone is the subtle version of the bug: the rows are in the target,
    so it LOOKS done -- but a validation that failed means we do not know they are right,
    and moving the cursor makes that range unreachable without a full rebuild.
    """
    if not (committed and validated):
        return previous
    if plan.mode == MODE_NOOP:
        # Nothing was read, so there is nothing to have incorporated. Keep the cursor; a
        # NOOP that advanced it would be claiming progress past data it never saw.
        return previous
    return plan.to_snapshot


# --------------------------------------------------------------------------- #
# R2-F: rebasing the overlay onto a newly CERTIFIED EOD close
# --------------------------------------------------------------------------- #
#
# WHAT A REBASE IS FOR.
#
# A `latest_state` table is an OVERLAY on a certified EOD baseline, not a standalone truth:
#
#     current state  =  certified EOD baseline  +  REALTIME changes after its cutoff
#
# While the baseline is D-1, every change since D-1's cutoff has to live in REALTIME. When
# D certifies, the changes between D-1 and D are now IN the baseline, and keeping them in
# the overlay as well means a consumer that composes the two sees them twice -- or, worse,
# sees an overlay row that the certified close has since corrected and prefers the stale one.
#
# So a rebase REMOVES what the new baseline already contains, and retains everything after
# its cutoff. It is not a cleanup; it is what keeps the composition well-defined.

OPERATION_MATERIALISE = "materialise"
OPERATION_REBASE = "rebase"
OPERATIONS = (OPERATION_MATERIALISE, OPERATION_REBASE)

#: Why a rebase did not happen. Empty means it did.
REBASE_NOT_ENABLED = "not_enabled"
REBASE_NOT_LATEST_STATE = "not_latest_state"
REBASE_NOT_CERTIFIED = "eod_not_certified"
REBASE_NO_CUTOFF = "eod_has_no_cutoff"
REBASE_ALREADY_AT_BASELINE = "already_at_this_baseline"
REBASE_BASELINE_WOULD_GO_BACKWARDS = "baseline_would_go_backwards"
REBASE_SKIP_REASONS = (REBASE_NOT_ENABLED, REBASE_NOT_LATEST_STATE, REBASE_NOT_CERTIFIED,
                       REBASE_NO_CUTOFF, REBASE_ALREADY_AT_BASELINE,
                       REBASE_BASELINE_WOULD_GO_BACKWARDS)


@dataclass(frozen=True)
class RebaseDecision:
    """Whether this table rebases onto this COB, and why not when it does not."""
    should_rebase: bool
    skip_reason: str = ""
    prev_baseline = None
    new_baseline = None
    cutoff_utc = None
    #: The cutoff formatted `yyyy-MM-dd HH:mm:ss` in the SESSION zone, BY SPARK. Carried as
    #: a string because a timestamp collected into Python is rendered in the driver's local
    #: zone -- formatting that back into a SQL literal shifts the cutoff by the driver's
    #: offset, and a rebase against a late cutoff deletes rows the baseline does not have.
    cutoff_str: str = ""
    detail: str = ""

    def __post_init__(self) -> None:
        if self.skip_reason and self.skip_reason not in REBASE_SKIP_REASONS:
            raise ConfigError(f"rebase skip reason {self.skip_reason!r} is not one of "
                              f"{', '.join(REBASE_SKIP_REASONS)}")
        if self.should_rebase and self.skip_reason:
            raise ConfigError("a rebase that is happening cannot also have a skip reason")


def rebase_decision(*, shape: str, enabled: bool, eod_row: "dict | None",
                    current_baseline) -> RebaseDecision:
    """Decide whether to rebase, from the EOD control-plane row. Pure.

    EVERY GUARD HERE IS ABOUT NOT REMOVING SOMETHING THE BASELINE DOES NOT HAVE.

    A rebase DELETES overlay rows. If the close it is rebasing onto is not actually
    certified, or its cutoff is unknown, or the baseline would move backwards, then the rows
    removed are rows nothing else holds -- and there is no error afterwards, only a state
    table that has quietly forgotten a day.

    `eod_row` is `ops.eod_info` for `(table_id, cob_date)`, or None when no close exists.
    """
    from .realtime import SHAPE_LATEST_STATE

    # SHAPE FIRST, then the flag. For an event window the shape is the FUNDAMENTAL reason
    # and the flag is merely unset because it cannot be set -- the compiler refuses that
    # pairing outright. Reporting `not_enabled` there would invite an operator to turn it
    # on, and the compile would then refuse them; `not_latest_state` sends them to the real
    # answer the first time.
    if shape != SHAPE_LATEST_STATE:
        # An event window has no baseline to rebase onto: it keeps events, and a consumer
        # composing it with EOD would be double-counting by construction, not by staleness.
        return RebaseDecision(False, REBASE_NOT_LATEST_STATE)
    if not enabled:
        return RebaseDecision(False, REBASE_NOT_ENABLED)
    if not eod_row:
        return RebaseDecision(False, REBASE_NOT_CERTIFIED,
                              detail="no eod_info row for this table and COB")
    if str(eod_row.get("certification_status") or "") != "CERTIFIED":
        return RebaseDecision(
            False, REBASE_NOT_CERTIFIED,
            detail=f"certification_status="
                   f"{eod_row.get('certification_status')!r}")
    cutoff = eod_row.get("cutoff_ts_utc")
    if cutoff is None:
        return RebaseDecision(False, REBASE_NO_CUTOFF,
                              detail="the close recorded no cutoff, so what it contains "
                                     "is unknown and nothing may be removed")
    new_baseline = eod_row.get("cob_date")
    if current_baseline is not None and new_baseline is not None:
        if new_baseline == current_baseline:
            return RebaseDecision(False, REBASE_ALREADY_AT_BASELINE,
                                  detail=f"already rebased onto {new_baseline}")
        if new_baseline < current_baseline:
            # Re-running an older COB's rebase. Its cutoff is EARLIER, so it would remove
            # rows the current baseline does not contain.
            return RebaseDecision(
                False, REBASE_BASELINE_WOULD_GO_BACKWARDS,
                detail=f"current baseline {current_baseline} is newer than {new_baseline}")
    cutoff_str = eod_row.get("cutoff_str") or ""
    if not cutoff_str:
        return RebaseDecision(
            False, REBASE_NO_CUTOFF,
            detail="the close's cutoff was not read in the session zone; comparing a "
                   "driver-local timestamp would shift it by the driver's offset")
    d = RebaseDecision(True, "", cutoff_str=cutoff_str,
                       detail=f"rebase onto {new_baseline} cutoff {cutoff_str}")
    object.__setattr__(d, "prev_baseline", current_baseline)
    object.__setattr__(d, "new_baseline", new_baseline)
    object.__setattr__(d, "cutoff_utc", cutoff)
    return d


def rebase_merge_sql(target: str, source_view: str = "rt_rebase_keys") -> str:
    """Remove exactly the overlay rows the new baseline already contains.

    MATCHED ON `dv_pk_hash` AND `dv_event_id`, AND THAT PAIR IS THE RACE SAFETY.

    The keys are computed from a FROZEN read of the target: the rows whose winning event is
    before the EOD cutoff. Between that read and this delete, a new event can land for one
    of those keys -- the overlay's whole job is to keep accepting them. Matching on the key
    alone would delete that new row, which the certified baseline does NOT contain and
    nothing else holds.

    Including `dv_event_id` means a row that has since been superseded no longer matches,
    so it survives. The delete removes only what it actually looked at.

    A MERGE rather than `DELETE ... WHERE EXISTS`: Iceberg's MERGE with `WHEN MATCHED THEN
    DELETE` is the supported spelling across the Spark versions this runs on, and a DELETE
    with a correlated subquery is not.
    """
    return (f"MERGE INTO {target} t USING {source_view} s\n"
            f"  ON t.dv_pk_hash = s.dv_pk_hash AND t.dv_event_id = s.dv_event_id\n"
            f"WHEN MATCHED THEN DELETE")
