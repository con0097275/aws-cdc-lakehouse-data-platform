"""EOD / snapshot closing: cutoff arithmetic, source-native ordering, delete policy.

Pure. No Spark, no AWS, no I/O -- the same separation `cdc/realtime.py` makes, for the same
reason: a cutoff an hour out or an ordering that compares the wrong things produces a
snapshot of the right SHAPE with the wrong CONTENTS. It reconciles, it partitions, it serves
queries, and the balances are simply not the ones that were true at close of business.

EOD IS A FUNCTION OF FULL_CDC AND A CUTOFF -- NEVER OF REALTIME
----------------------------------------------------------------
REALTIME is a bounded window that ages rows out; EOD is a deterministic function of the
whole history up to an instant. Building EOD from REALTIME would make a historical rebuild
depend on a window that no longer contains the days being rebuilt, and it would make the
snapshot's correctness depend on the *schedule* of a different job. FULFILL (section I) is
the case that proves it: reconstructing a missed 2026-08-14 must not require recreating that
day's REALTIME window first.
"""
from __future__ import annotations

import zoneinfo
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from .models import ConfigError, SourceEngine

# --------------------------------------------------------------------------- #
# section E -- DELETE POLICY
#
# THE EXISTING PROJECT CONTRACT, INSPECTED BEFORE CHOOSING A DEFAULT.
#
#   CLAUDE.md 5.7   "active snapshot loại record đã delete hoặc giữ `_is_deleted=true`
#                    theo contract" -- two policies, the exclusion named first.
#   cdc/models.py   DeletePolicy = EXCLUDE_FROM_SNAPSHOT (default) | SOFT_FLAG.
#   the deployed    spark/jobs/eod/job.py ranks then filters `op != 'd'` -- i.e. it drops a
#   EOD job         key whose winning event is a delete. That IS exclude-latest-delete.
#
# So the default STAYS `exclude_from_snapshot`. It is what the platform already does, and
# changing a delete default is the kind of change that alters certified balances without
# altering a single line of business SQL.
#
# The brief's vocabulary is accepted as ALIASES rather than a rename: the registry, the
# compiled plan hashes and the provisioned table properties already carry the project's
# spellings, and renaming them would rewrite every config hash to say the same thing.
# --------------------------------------------------------------------------- #

DELETE_EXCLUDE = "exclude_from_snapshot"
DELETE_SOFT = "soft_flag"
#: NEW in Phase 4. Distinct from `exclude_from_snapshot`, and the difference only exists
#: once the target retains state across runs: EXCLUDE omits the key from the snapshot being
#: built, while PHYSICAL additionally removes any row for that key left by an EARLIER run.
#: Under ROLLING_HISTORY they are the same thing, because each COB writes its own partition
#: and no earlier row is in scope. Under LATEST_STATE they are not.
DELETE_PHYSICAL = "physical_delete"

DELETE_POLICIES = (DELETE_EXCLUDE, DELETE_SOFT, DELETE_PHYSICAL)

#: The brief's names -> the project's. One direction only: config is written and hashed in
#: the project's vocabulary, so an alias is normalised on the way in and never on the way out.
DELETE_ALIASES = {
    "exclude_latest_delete": DELETE_EXCLUDE,
    "soft_delete": DELETE_SOFT,
    "physical_delete": DELETE_PHYSICAL,
}


def normalise_delete_policy(raw: str, *, what: str) -> str:
    value = str(raw).strip().lower()
    value = DELETE_ALIASES.get(value, value)
    if value not in DELETE_POLICIES:
        allowed = ", ".join(sorted(set(DELETE_POLICIES) | set(DELETE_ALIASES)))
        raise ConfigError(f"{what}: delete policy {raw!r} is not one of {allowed}")
    return value


# --------------------------------------------------------------------------- #
# section F -- SNAPSHOT STATE MODE
# --------------------------------------------------------------------------- #

#: One row per PK per business_date; every closed COB is retained.
SNAPSHOT_ROLLING_HISTORY = "rolling_history"
#: One row per PK -- only the most recently certified COB is retained.
SNAPSHOT_LATEST_STATE = "latest_state"
SNAPSHOT_MODES = (SNAPSHOT_ROLLING_HISTORY, SNAPSHOT_LATEST_STATE)

#: DEFAULT = rolling_history, and this is an inspection result rather than a preference.
#: The provisioned EOD table is already `PARTITIONED BY (business_date)` with a grain of
#: "one row per primary key per business_date" and `eod.retention_days: 365`. A 365-day
#: retention on a table that only ever holds one day is meaningless -- the deployed contract
#: has already committed to history. Defaulting to `latest_state` would make the first run
#: of every table DELETE up to 364 days of certified partitions, which is not a default.
DEFAULT_SNAPSHOT_MODE = SNAPSHOT_ROLLING_HISTORY


# --------------------------------------------------------------------------- #
# section B -- BUSINESS DATE AND CUTOFF
# --------------------------------------------------------------------------- #

CUTOFF_SOURCE_COMMIT_TS = "source_commit_ts"
CUTOFF_EVENT_TS = "event_ts"
CUTOFF_POLICIES = (CUTOFF_SOURCE_COMMIT_TS, CUTOFF_EVENT_TS)


def _zone(tz: str):
    try:
        return zoneinfo.ZoneInfo(tz)
    except Exception:
        raise ConfigError(
            f"business_timezone {tz!r} is not a valid IANA zone. The cutoff is taken in "
            f"that zone, so a wrong one certifies a different set of events than the "
            f"business date names (ADR-024)") from None


@dataclass(frozen=True)
class EodCutoff:
    """The frozen boundary for ONE close, and the evidence of how it was derived.

    `cutoff_local` and `cutoff_utc` are BOTH recorded because they answer different
    questions. The local one is what a business reader checks ("the 22nd closed at midnight
    on the 23rd, Ho Chi Minh time"); the UTC one is what the predicate actually used. When
    they disagree by an unexpected amount, the timezone is wrong -- and only keeping both
    makes that visible.
    """
    table_id: str
    cob_date: date
    business_timezone: str
    cutoff_local: datetime
    cutoff_utc: datetime
    cutoff_policy: str
    #: The UTC dates the window can touch. A PRUNING hint only -- never the exact predicate.
    prune_date_low: date
    prune_date_high: date

    def describe(self) -> str:
        return (f"{self.table_id} COB={self.cob_date.isoformat()} "
                f"cutoff_local={self.cutoff_local.isoformat()} "
                f"cutoff_utc={self.cutoff_utc.isoformat()} tz={self.business_timezone}")


def cob_date_for(run_instant: datetime, *, business_timezone: str,
                 lag_days: int) -> date:
    """Which business date a run closes. `lag_days: 1` closes yesterday.

    The local date is taken in the business zone, not UTC: a run at 02:00 UTC is still the
    previous day in New York, and closing "today" there would close a day that has not
    happened yet.
    """
    if run_instant.tzinfo is None:
        raise ConfigError("run instant must be timezone-aware to derive a business date")
    if lag_days < 0:
        raise ConfigError(f"business_date_lag_days must be >= 0, got {lag_days}")
    local_today = run_instant.astimezone(_zone(business_timezone)).date()
    return local_today - timedelta(days=lag_days)


def resolve_cutoff(cob_date: date, *, table_id: str, business_timezone: str = "UTC",
                   cutoff_policy: str = CUTOFF_SOURCE_COMMIT_TS) -> EodCutoff:
    """COB date D -> the instant the day closes.

        cutoff_local = start of D+1 in the business timezone
        cutoff_utc   = that instant, in UTC

    The comparison is STRICTLY `<`. CLAUDE.md 5.6 permits either `event_ts < T00:00` or
    `source_commit_ts <= cutoff`; those describe the same set with different cutoff values --
    start-of-next-day with `<`, or last-instant-of-day with `<=`. This platform uses the
    first, because "the last instant of a day" has no exact representation and every
    approximation of it (23:59:59, .999, .999999) silently drops events in the gap.

    The date arithmetic is done on the LOCAL date and then converted, never by adding a
    timedelta to an instant: across a DST transition those differ by an hour, and an hour of
    events would be certified into the wrong business date.
    """
    if cutoff_policy not in CUTOFF_POLICIES:
        raise ConfigError(f"{table_id}: eod.cutoff_policy {cutoff_policy!r} is not one of "
                          f"{', '.join(CUTOFF_POLICIES)}")
    if not isinstance(cob_date, date) or isinstance(cob_date, datetime):
        raise ConfigError(f"{table_id}: cob_date must be a date, got {cob_date!r}")
    zone = _zone(business_timezone)
    cutoff_local = datetime.combine(cob_date + timedelta(days=1), time.min, tzinfo=zone)
    cutoff_utc = cutoff_local.astimezone(timezone.utc)
    # The pruning window, deliberately WIDER than the exact predicate. `event_date` is the
    # UTC date of the event, and with a non-UTC business timezone the local business day
    # straddles two UTC dates -- so a prune computed as "event_date == cob_date" would drop
    # real events. One day of slack on each side makes the hint safe under every offset,
    # including the +14 and -12 extremes.
    start_local = datetime.combine(cob_date, time.min, tzinfo=zone)
    return EodCutoff(
        table_id=table_id, cob_date=cob_date, business_timezone=business_timezone,
        cutoff_local=cutoff_local, cutoff_utc=cutoff_utc, cutoff_policy=cutoff_policy,
        prune_date_low=(start_local.astimezone(timezone.utc).date() - timedelta(days=1)),
        prune_date_high=(cutoff_utc.date() + timedelta(days=1)))


# --------------------------------------------------------------------------- #
# section C -- SOURCE-NATIVE ORDER
#
# The two engines need OPPOSITE treatment and one "just pad it" helper would silently
# corrupt one of them (spark/jobs/l1_stream/ordering.py states the contract; this expresses
# the same rule as Spark SQL so one engine, not two, decides what "latest" means).
#
#   Oracle SCN      NUMERIC.  '9' > '10' lexicographically but 9 < 10 numerically.
#                   Left-padded to a fixed width so string compare == numeric compare.
#   SQL Server LSN  HEX triplet aaaaaaaa:bbbbbbbb:cccc, ALREADY fixed-width.
#                   Compared lexicographically -- but only valid BECAUSE of the padding,
#                   so the width is validated rather than assumed.
#
# The deployed EOD job casts `position_primary` to decimal(38,0), which is Oracle-only: a
# SQL Server hex LSN casts to NULL, `desc_nulls_last` sends every row to the back equally,
# and the ranking silently collapses onto `kafka_offset` -- which CLAUDE.md 5.4 forbids as a
# comparator because offsets are monotonic only WITHIN one partition. That job guards itself
# with a refusal and its own comment names this as the fix. This is the fix.
# --------------------------------------------------------------------------- #

#: Matches spark/jobs/l1_stream/ordering.py. The width can never change once data exists:
#: it would re-sort history.
ORACLE_SCN_WIDTH = 24
SQLSERVER_LSN_PATTERN = r"^[0-9a-fA-F]{8}:[0-9a-fA-F]{8}:[0-9a-fA-F]{4}$"
ORACLE_SCN_PATTERN = r"^[0-9]+$"


@dataclass(frozen=True)
class OrderingSpec:
    """How to rank events for one engine, as Spark SQL fragments.

    `sort_key(col)` returns an expression that is SORTABLE AS A STRING for both engines, so
    a single ORDER BY works whatever the source is -- and so the comparison never depends on
    a cast that can yield NULL.
    """
    engine: str
    position_pattern: str
    columns: tuple[str, ...]

    def sort_key(self, column: str) -> str:
        if self.engine == SourceEngine.ORACLE.value:
            # Exactly `normalize_oracle_scn`: zero-pad to fixed width.
            return f"lpad({column}, {ORACLE_SCN_WIDTH}, '0')"
        # Already fixed-width; lower() only so a mixed-case LSN cannot sort apart from an
        # otherwise identical one.
        return f"lower({column})"

    def validation_predicate(self, column: str) -> str:
        """Rows whose position CANNOT be ordered. Non-empty means refuse the run."""
        return f"{column} IS NOT NULL AND NOT {column} RLIKE '{self.position_pattern}'"


ORDERING_BY_ENGINE: dict[str, OrderingSpec] = {
    SourceEngine.ORACLE.value: OrderingSpec(
        engine=SourceEngine.ORACLE.value, position_pattern=ORACLE_SCN_PATTERN,
        # commit_scn then scn -- what spark/jobs/full_cdc/job.py actually writes into
        # position_primary / position_secondary for an Oracle envelope.
        columns=("commit_scn", "scn")),
    SourceEngine.SQLSERVER.value: OrderingSpec(
        engine=SourceEngine.SQLSERVER.value, position_pattern=SQLSERVER_LSN_PATTERN,
        # commit_lsn then change_lsn. NOT event_serial_no: `ordering.py::normalize_position`
        # expects the serial there, but `full_cdc/job.py` writes change_lsn, and the row
        # contract is what actually exists. Documented in ADR-065 as an open discrepancy
        # rather than papered over.
        columns=("commit_lsn", "change_lsn")),
}


def ordering_for(engine: str) -> OrderingSpec:
    try:
        return ORDERING_BY_ENGINE[engine]
    except KeyError:
        raise ConfigError(
            f"no deterministic ordering is defined for engine {engine!r}. 'Last event wins' "
            f"is undefined without one (CLAUDE.md 5.5), so EOD cannot be built for it"
        ) from None


def order_by_clause(engine: str) -> str:
    """The full ranking, in the precedence DATA_CONTRACTS section 4.1 fixes.

    EOD takes the MAXIMUM of the contract's order-key tuple, so every component descends.

        position_primary, position_secondary, source_commit_ts, kafka_partition, kafka_offset

    `kafka_partition` PRECEDES `kafka_offset`, which is the whole of CLAUDE.md 5.4: an offset
    is monotonic only within its own partition, so comparing offsets across partitions is
    meaningless. Ordering by partition first means offsets are only ever compared between
    rows that share one -- and the partition itself is a DETERMINISM tie-break, not a claim
    that a higher partition number happened later.
    """
    spec = ordering_for(engine)
    return ", ".join([
        f"{spec.sort_key('position_primary')} DESC NULLS LAST",
        f"{spec.sort_key('position_secondary')} DESC NULLS LAST",
        "source_commit_ts DESC NULLS LAST",
        "kafka_partition DESC NULLS LAST",
        "kafka_offset DESC NULLS LAST",
    ])


# --------------------------------------------------------------------------- #
# section G -- the run ledger
# --------------------------------------------------------------------------- #

RUN_LEDGER_TABLE = "eod_run"

STATUS_CERTIFIED = "CERTIFIED"
STATUS_BUILT_NOT_CERTIFIED = "BUILT_NOT_CERTIFIED"
STATUS_FAILED = "FAILED"
#: Both gates passed but the ledger row -- the completion marker -- could not be written, so
#: nothing can verify the claim and no rerun can tell the date was closed. The DATA is fine
#: and readable; the certification is not claimable. Distinct from FAILED (which means the
#: close itself broke) and from a failed gate (which means the data is suspect).
STATUS_UNVERIFIED = "UNVERIFIED_NO_EVIDENCE"
STATUS_SKIPPED_DISABLED = "SKIPPED_DISABLED"

RUN_LEDGER_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("run_id", "STRING", "the run this row describes"),
    ("table_id", "STRING", "canonical source identity"),
    ("cob_date", "DATE", "the business date closed"),
    ("business_timezone", "STRING", "the zone the cutoff was taken in"),
    ("cutoff_local", "TIMESTAMP", "start of COB+1 in the business timezone"),
    ("cutoff_utc", "TIMESTAMP", "the same instant in UTC -- the predicate actually used"),
    ("cutoff_policy", "STRING", "source_commit_ts | event_ts"),
    ("snapshot_mode", "STRING", "rolling_history | latest_state"),
    ("delete_policy", "STRING", "exclude_from_snapshot | soft_flag | physical_delete"),
    ("source_table", "STRING", "the FULL_CDC table read"),
    ("target_table", "STRING", "the EOD table written"),
    ("source_snapshot_id", "BIGINT",
     "the FULL_CDC snapshot read. Re-reading THAT snapshot with THIS cutoff must converge "
     "to the same business state (section H)"),
    ("target_snapshot_id", "BIGINT", "the EOD snapshot written"),
    ("max_position_primary", "STRING",
     "the highest source position included. The position/watermark EVIDENCE section G asks "
     "for: it answers 'how far into the source did this close actually reach?'"),
    ("max_source_commit_ts", "TIMESTAMP", "the latest source commit time included"),
    ("input_event_count", "BIGINT", "FULL_CDC events inside the cutoff"),
    ("distinct_key_count", "BIGINT", "distinct primary keys among them"),
    ("row_count", "BIGINT", "rows written to the snapshot"),
    ("delete_count", "BIGINT", "keys whose winning event was a delete"),
    ("dq_status", "STRING", "PASS | FAIL"),
    ("dq_failures", "STRING", "which DQ rules failed, if any"),
    ("reconciliation_status", "STRING", "PASS | FAIL"),
    ("reconciliation_detail", "STRING", "the counts compared, and how they differed"),
    ("status", "STRING",
     "CERTIFIED | BUILT_NOT_CERTIFIED | FAILED | SKIPPED_DISABLED. CERTIFIED requires BOTH "
     "gates to pass -- a failed validation must not publish a completion marker"),
    ("failure_reason", "STRING", "why, when status is FAILED"),
    ("started_at", "TIMESTAMP", "when the run began"),
    ("finished_at", "TIMESTAMP", "when it ended"),
    ("config_version", "STRING", "the compiled plan this run was driven by"),
)


@dataclass(frozen=True)
class ValidationOutcome:
    """DQ + reconciliation, and whether the close may be CERTIFIED.

    Pure, so the gate is testable without building a snapshot -- and so "would this have
    certified?" is a question that can be answered about a set of counts alone.
    """
    dq_status: str
    dq_failures: tuple[str, ...]
    reconciliation_status: str
    reconciliation_detail: str

    @property
    def certified(self) -> bool:
        return self.dq_status == "PASS" and self.reconciliation_status == "PASS"

    @property
    def status(self) -> str:
        return STATUS_CERTIFIED if self.certified else STATUS_BUILT_NOT_CERTIFIED


def validate_close(*, input_event_count: int, distinct_key_count: int, row_count: int,
                   delete_count: int, delete_policy: str,
                   null_key_count: int = 0,
                   not_null_violations: "dict[str, int] | None" = None
                   ) -> ValidationOutcome:
    """The certification gate (section G). Returns ALL failures, not the first.

    The reconciliation is a genuine identity rather than a restatement of the build: under
    an exclusion policy the keys written plus the keys dropped as deletes must account for
    every distinct key inside the cutoff. If they do not, a key vanished between reading and
    writing -- which is the one failure a snapshot cannot show on its face, because the
    result still looks like a perfectly ordinary table.
    """
    failures: list[str] = []
    if null_key_count:
        failures.append(
            f"NULL_PRIMARY_KEY: {null_key_count} event(s) inside the cutoff have no "
            f"primary key value; they cannot be assigned to a snapshot row")
    for column, n in sorted((not_null_violations or {}).items()):
        if n:
            failures.append(f"NOT_NULL[{column}]: {n} snapshot row(s) are null")
    if input_event_count == 0:
        # Legitimate on a quiet day, and it must be DELIBERATE rather than the silent result
        # of a wrong cutoff -- the same rule spark/eod/audit.py already applies.
        failures.append(
            "EMPTY_WINDOW: zero events inside the cutoff. Confirm the cutoff and the "
            "business timezone before certifying this date")
    if row_count < 0 or delete_count < 0:
        failures.append("negative counts are impossible; the audit itself is wrong")

    if delete_policy == DELETE_SOFT:
        # Soft delete keeps the row, so every key is written.
        expected, shape = distinct_key_count, "distinct_keys == rows (soft_flag keeps deletes)"
    else:
        expected, shape = (distinct_key_count - delete_count,
                           "distinct_keys - deletes == rows")
    if row_count != expected:
        recon = (f"FAIL: {shape}; distinct_keys={distinct_key_count} "
                 f"deletes={delete_count} rows={row_count} expected={expected}")
        recon_status = "FAIL"
    else:
        recon = (f"PASS: {shape}; distinct_keys={distinct_key_count} "
                 f"deletes={delete_count} rows={row_count}")
        recon_status = "PASS"

    return ValidationOutcome(
        dq_status="FAIL" if failures else "PASS",
        dq_failures=tuple(failures),
        reconciliation_status=recon_status,
        reconciliation_detail=recon)


# --------------------------------------------------------------------------- #
# Phase D, section 2 -- cadence and sizing (one implementation, cdc/scheduling.py)
# --------------------------------------------------------------------------- #

from . import scheduling as _sched                                    # noqa: E402

#: 01:00 in the table's own business timezone, closing yesterday. The brief's candidate for
#: this lab, and a DEFAULT: a table whose source settles late can say so.
DEFAULT_EOD_SCHEDULE = "0 1 * * *"


def normalise_eod_schedule(raw, *, what: str) -> str:
    return _sched.normalise_schedule(raw, what=what, default=DEFAULT_EOD_SCHEDULE)


def eod_schedule_groups(plan: dict) -> dict:
    """`{cron: [table_id]}` for EOD-enabled tables. One DAG per cadence, never per table."""
    return _sched.schedule_groups(plan, policy_key="eod_policy",
                                  default=DEFAULT_EOD_SCHEDULE)


# --------------------------------------------------------------------------- #
# section 4 -- readiness: clock time alone cannot certify a close
# --------------------------------------------------------------------------- #

#: The source has not caught up to the cutoff yet. NOT a failure: the run is early, and the
#: right response is to wait and look again.
STATUS_WAITING_SOURCE = "WAITING_SOURCE"
#: It never caught up inside the configured SLA. A failure, and deliberately a DIFFERENT one
#: from a build error: nothing is wrong with the job, the data did not arrive.
STATUS_LATE_SOURCE = "LATE_SOURCE"

READINESS_STATUSES = (STATUS_WAITING_SOURCE, STATUS_LATE_SOURCE)

#: How long after the cutoff a close may keep waiting before it gives up. Six hours suits a
#: 01:00 close of a source that settles overnight; a table whose feed is slower says so.
DEFAULT_SOURCE_SLA_MINUTES = 360


@dataclass(frozen=True)
class Readiness:
    """Whether FULL_CDC has demonstrably passed the cutoff for this COB.

    `ready` is the only thing that may unlock a certification, and it is never true because
    a clock said so. Every other field exists so that a WAITING run explains itself: an
    operator at 01:05 needs to know WHICH signal is behind, not that "readiness failed".
    """
    ready: bool
    status: str
    reason: str
    source_watermark: "datetime | None" = None
    lag_minutes: float | None = None
    deadline_passed: bool = False

    @property
    def should_retry(self) -> bool:
        return self.status == STATUS_WAITING_SOURCE


def assess_readiness(*, cutoff_utc: datetime, now: datetime,
                     source_watermark: "datetime | None",
                     sla_minutes: int = DEFAULT_SOURCE_SLA_MINUTES,
                     ingestion_status: str | None = None,
                     connectors_healthy: bool | None = None,
                     ingest_watermark: "datetime | None" = None,
                     table_id: str = "") -> Readiness:
    """Can COB be certified yet? Pure, so the DAG and the job reach one verdict.

    THE RULE: FULL_CDC must hold at least one event committed AT OR AFTER the cutoff, which
    is the only positive evidence that everything before the cutoff has arrived. A watermark
    merely "close to" the cutoff proves nothing -- the missing minute may hold the day's last
    thousand transactions.

    A source that is simply quiet after the cutoff looks identical to a broken one from the
    watermark alone, which is why the streaming state and connector health are inputs too: a
    stalled ingest with a stale watermark must not be waited on until the SLA expires and
    then certified by a human who assumes the day was empty.
    """
    if now.tzinfo is None or cutoff_utc.tzinfo is None:
        raise ConfigError(f"{table_id}: readiness needs timezone-aware instants")
    deadline = cutoff_utc + timedelta(minutes=int(sla_minutes))
    passed = now >= deadline

    def late(reason: str) -> Readiness:
        return Readiness(False, STATUS_LATE_SOURCE, reason, source_watermark,
                         None, True)

    def waiting(reason: str) -> Readiness:
        return Readiness(False, STATUS_WAITING_SOURCE, reason, source_watermark,
                         None, False)

    if connectors_healthy is False:
        msg = "capture is not healthy, so FULL_CDC cannot be assumed complete"
        return late(msg) if passed else waiting(msg)

    if ingestion_status is not None and str(ingestion_status).upper() in ("FAILED", "STALE"):
        msg = f"the streaming ingest is {ingestion_status}"
        return late(msg) if passed else waiting(msg)

    # EITHER signal is sufficient evidence that the day is complete.
    #
    # A table's OWN watermark is the strongest: an event committed after the cutoff proves
    # everything before it has arrived. But a quiet table has no such event -- `channel`
    # holds four rows and may not change for weeks -- and requiring one would put every
    # reference table into LATE_SOURCE on every close, every day. The PLATFORM watermark
    # (`ops.streaming_app_state.source_watermark_ts`) answers the same question one level
    # up: the ingest itself has consumed past the cutoff, so a table with nothing after it
    # genuinely had nothing.
    marks = [m.replace(tzinfo=timezone.utc) if m is not None and m.tzinfo is None else m
             for m in (source_watermark, ingest_watermark)]
    best = max([m for m in marks if m is not None], default=None)

    if best is None:
        msg = ("neither FULL_CDC nor the streaming ingest reports a watermark, so nothing "
               "proves the day arrived")
        return late(msg) if passed else waiting(msg)

    mark = best
    if mark < cutoff_utc:
        behind = (cutoff_utc - mark).total_seconds() / 60.0
        msg = (f"source watermark {mark.isoformat()} is {behind:.0f} min behind the cutoff "
               f"{cutoff_utc.isoformat()}")
        r = late(msg) if passed else waiting(msg)
        return Readiness(False, r.status, msg, mark, behind, passed)

    return Readiness(True, "", f"source watermark {mark.isoformat()} is at or past the cutoff",
                     mark, 0.0, passed)


# --------------------------------------------------------------------------- #
# section 7 -- ops.eod_info: current authoritative state, one row per (table, COB)
# --------------------------------------------------------------------------- #

EOD_INFO_TABLE = "eod_info"

#: WHY THIS EXISTS SEPARATELY FROM `eod_run`.
#:
#: `eod_run` is an append log: every attempt, forever. Asking it "what is the current
#: certified state of this table for this date" means "the latest row, by some ordering,
#: that happens to be CERTIFIED" -- a convention no column enforces and every reader has to
#: re-implement. The Phase A audit found exactly that: one table doing two jobs, with
#: "latest row wins" implicit.
#:
#: `eod_info` answers the question directly: ONE row per (table_id, cob_date), MERGEd in
#: place, holding the watermark PAIR -- what the close moved FROM and TO. "What did this
#: close actually advance" is then answerable, which it is not from a cutoff alone.
EOD_INFO_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("table_id", "STRING", "canonical registry id"),
    ("source_table", "STRING", "FULL_CDC identifier the close read"),
    ("target_table", "STRING", "EOD identifier the close wrote"),
    ("cob_date", "DATE", "the business date this row is the state of"),
    ("business_timezone", "STRING", "the zone the cutoff was taken in"),
    ("cutoff_ts_utc", "TIMESTAMP", "half-open upper bound; events < this are in"),
    ("prev_watermark_ts", "TIMESTAMP", "what the previous successful close advanced TO"),
    ("watermark_ts", "TIMESTAMP", "what this close advanced to"),
    ("source_position_json", "STRING", "max source-native position, JSON: SCN or LSN"),
    ("source_snapshot_id", "BIGINT", "FULL_CDC snapshot the close read"),
    ("target_snapshot_id", "BIGINT", "EOD snapshot the close produced"),
    ("last_success_run_id", "STRING", "the run that produced this state"),
    ("row_count", "BIGINT", "rows in the snapshot"),
    ("deleted_count", "BIGINT", "keys excluded or flagged by the delete policy"),
    ("dq_status", "STRING", "PASS | FAIL | NOT_RUN"),
    ("reconciliation_status", "STRING", "PASS | FAIL | NOT_RUN"),
    ("certification_status", "STRING", "CERTIFIED | NOT_CERTIFIED"),
    ("status", "STRING", "the close's terminal status"),
    ("start_time", "TIMESTAMP", ""),
    ("end_time", "TIMESTAMP", ""),
    ("err_msg", "STRING", "truncated; the full trace stays in the driver log"),
    ("config_version", "STRING", "which compiled plan produced this state"),
    ("updated_at", "TIMESTAMP", ""),
)

#: The logical key. Iceberg does not enforce uniqueness, so the MERGE below is what enforces
#: it -- and `test_a_second_close_of_one_cob_updates_rather_than_appends` is what proves the
#: MERGE is actually used.
EOD_INFO_KEY = ("table_id", "cob_date")


# --------------------------------------------------------------------------- #
# section 8 -- ops.eod_run_hist: one row per ATTEMPT, never overwritten
# --------------------------------------------------------------------------- #

RUN_HIST_TABLE = "eod_run_hist"

RUN_HIST_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("run_id", "STRING", "unique per attempt"),
    ("table_id", "STRING", ""),
    ("cob_date", "DATE", ""),
    ("attempt", "INT", "1-based, counted from history, not from the process"),
    ("status", "STRING", "CERTIFIED | BUILT_NOT_CERTIFIED | FAILED | WAITING_SOURCE | "
                         "LATE_SOURCE | SKIPPED_DISABLED | UNVERIFIED_NO_EVIDENCE"),
    ("cutoff_ts_utc", "TIMESTAMP", ""),
    ("prev_watermark_ts", "TIMESTAMP", "the watermark this attempt started from"),
    ("watermark_ts", "TIMESTAMP", "the watermark it reached, NULL when it did not"),
    ("source_snapshot_id", "BIGINT", ""),
    ("target_snapshot_id", "BIGINT", ""),
    ("input_event_count", "BIGINT", ""),
    ("row_count", "BIGINT", ""),
    ("deleted_count", "BIGINT", ""),
    ("dq_status", "STRING", ""),
    ("reconciliation_status", "STRING", ""),
    ("spark_app_id", "STRING", "which application ran it; finds the driver log"),
    ("config_version", "STRING", ""),
    ("err_msg", "STRING", ""),
    ("start_time", "TIMESTAMP", ""),
    ("end_time", "TIMESTAMP", ""),
    ("duration_seconds", "DOUBLE", ""),
)


def next_attempt(previous_attempts: int | None) -> int:
    """Attempt numbers come from HISTORY, not from the process.

    A retry that counted in-process would call itself attempt 1 forever, and "this close has
    failed nine times today" -- the signal that something is actually wrong -- would never
    appear anywhere.
    """
    try:
        n = int(previous_attempts)
    except (TypeError, ValueError):
        return 1
    return max(1, n + 1)


# --------------------------------------------------------------------------- #
# section 8b -- what the exit code means
# --------------------------------------------------------------------------- #


def close_exit_code(statuses, *, cutoffs, skip_readiness: bool,
                    now: datetime) -> int:
    """0 when the run did what was asked, 1 when something is wrong.

    `statuses` and `cutoffs` are parallel sequences: one status and one cutoff instant per
    table the run considered (SKIPPED_DISABLED already removed).

    THE ONE EXCEPTION IS ABOUT TELLING THE OPERATOR THE TRUTH. Closing a COB whose cutoff
    has not passed cannot certify by design, and asking for that build is an explicit act --
    it requires `--skip-readiness`. Returning FAILED for doing exactly what was asked turns
    a correct run into a red row, and a console full of red rows that mean "working as
    intended" is how a real failure gets missed. Without `--skip-readiness` the same
    situation IS a fault: a schedule firing before its own cutoff. The INTENT distinguishes
    them, not the outcome.
    """
    pairs = list(zip(statuses, cutoffs))
    not_certified = [(s, c) for s, c in pairs if s != STATUS_CERTIFIED]
    if not not_certified:
        return 0
    if skip_readiness and all(s == STATUS_BUILT_NOT_CERTIFIED and c > now
                              for s, c in not_certified):
        return 0
    return 1


# --------------------------------------------------------------------------- #
# section 9 -- legacy compatibility, as a VIEW
# --------------------------------------------------------------------------- #

LEGACY_VIEW_TABLE = "eod_info_legacy_v"


def legacy_view_sql(view_identifier: str, info_identifier: str) -> str:
    """`pre_datelastmaint` / `datelastmaint` for consumers that speak the old dialect.

    Applied through **Athena**, not through the Spark provisioner: Iceberg view support is
    catalog-dependent, and the consumers that speak this dialect read through Athena. The
    statement is ANSI enough for both, so nothing here is Athena-specific except where it
    runs.

    A VIEW, never columns on `eod_info`. Those names are ambiguous -- "maintenance date" says
    nothing about whether it is the date a close moved FROM or TO, and the reference
    implementation's own jobs disagree about it. Making them canonical would bake that
    ambiguity into the platform's control table; a view keeps the unambiguous pair
    authoritative and lets a legacy reader keep its spelling.
    """
    return (f"CREATE OR REPLACE VIEW {view_identifier} AS\n"
            f"SELECT table_id,\n"
            f"       cob_date,\n"
            f"       prev_watermark_ts AS pre_datelastmaint,\n"
            f"       watermark_ts      AS datelastmaint,\n"
            f"       certification_status,\n"
            f"       status,\n"
            f"       row_count,\n"
            f"       updated_at\n"
            f"FROM {info_identifier}")
