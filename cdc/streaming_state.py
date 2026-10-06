"""`ops.streaming_app_state`: what the resident ingest is doing, written by the ingest.

    ingestion.STREAMING_STATE_COLUMNS   the CONTRACT  (DDL, provisioned)
    streaming_state.StateRow            the VALUE     (one row, in memory)
    streaming_state.merge_sql           the WRITE     (update in place)
    streaming_state.health              the READ      (what the monitor decides)

WHY THIS MODULE EXISTS (Phase B, section 7)
-------------------------------------------
ADR-073 defined the state contract and `cdc/provision.py` created the table. Nothing wrote
to it. The gap is the same shape as the defect Phase A found and worse in consequence: a
batch ingest that has finished is indistinguishable from a stream that is caught up, and an
EMPTY state table is indistinguishable from a healthy app that simply has nothing to say.
An operator asking "is the ingest alive and where has it got to" had no table to ask.

So the ingest writes its own state, and this module is the only place that knows how. It
holds NO Spark import: every decision here -- what a row contains, when to write one, which
transitions are legal, whether an app is healthy -- is testable without a cluster, and the
Airflow monitor (which has no SparkSession) calls the same `health()` the CLI does. Two
implementations of "is it alive" would disagree exactly once, at 03:00.

THE HEARTBEAT IS THROTTLED, AND THAT IS THE DESIGN
--------------------------------------------------
One row per app, MERGEd in place (ADR-073 section 6) -- but "in place" is about ROWS, not
about COMMITS. A resident app on a 1-minute trigger writing state every batch still commits
1,440 Iceberg snapshots a day carrying nothing but a timestamp, on a table whose whole
content is a handful of rows. Iceberg's expire-snapshots would then spend its budget on the
state table rather than on the data.

So a write happens when it CARRIES INFORMATION -- the status changed, rows moved, an error
appeared -- or when the heartbeat interval has elapsed and silence itself is the news.
"""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

from . import ingestion as _ing
from .models import ConfigError

# --------------------------------------------------------------------------- #
# section 7a -- legal status transitions
# --------------------------------------------------------------------------- #

#: A closed transition table, in the same spirit as `cdc/lifecycle.py`. The point is not
#: bureaucracy: `FAILED -> RUNNING` without passing through `STARTING` would mean a process
#: that recovered without restarting, which cannot happen and therefore signals that two
#: processes are writing the same app_id -- two ingests on one checkpoint, which Spark will
#: also refuse, but hours later and with a less legible message.
ALLOWED_TRANSITIONS: dict[str, tuple[str, ...]] = {
    _ing.STATUS_STARTING: (_ing.STATUS_RUNNING, _ing.STATUS_FAILED, _ing.STATUS_STOPPED),
    _ing.STATUS_RUNNING: (_ing.STATUS_RUNNING, _ing.STATUS_STOPPED, _ing.STATUS_FAILED),
    _ing.STATUS_STOPPED: (_ing.STATUS_STARTING,),
    _ing.STATUS_FAILED: (_ing.STATUS_STARTING,),
}

#: How much of a failure to keep. A Spark stack trace is tens of kilobytes; the state table
#: is read by an operator at 03:00 and by a monitor every few minutes, and neither wants a
#: megabyte of Java. The full trace stays in the EMR driver log, which the row points at by
#: carrying the deployment id.
ERROR_LIMIT = 2000

#: Default heartbeat. Five minutes is 288 state commits a day against 1,440 for a per-batch
#: write, and it is well inside any monitor interval that would page a human. The value
#: lives in `cdc/ingestion.py` with the other CONFIG defaults -- re-declaring it here would
#: be a second source of truth for one number, which is the drift this whole phase is about.
DEFAULT_HEARTBEAT_SECONDS = _ing.parse_trigger(_ing.DEFAULT_HEARTBEAT,
                                               what="heartbeat",
                                               floor=_ing.MIN_HEARTBEAT_SECONDS)
MIN_HEARTBEAT_SECONDS = _ing.MIN_HEARTBEAT_SECONDS


def transition(old: str | None, new: str) -> str:
    """Validate a status move. `old is None` is a first sighting, and only `STARTING`."""
    if new not in _ing.STREAMING_STATUSES:
        raise ConfigError(f"streaming status {new!r} is not one of "
                          f"{', '.join(_ing.STREAMING_STATUSES)}")
    if old is None:
        if new != _ing.STATUS_STARTING:
            raise ConfigError(
                f"an app with no state row can only appear as {_ing.STATUS_STARTING}, "
                f"not {new!r}: a row that begins as RUNNING hides whether the process "
                f"ever started cleanly")
        return new
    if old not in _ing.STREAMING_STATUSES:
        raise ConfigError(f"unknown current streaming status {old!r}")
    if new not in ALLOWED_TRANSITIONS[old]:
        raise ConfigError(
            f"illegal streaming status transition {old} -> {new}; from {old} the legal "
            f"moves are {', '.join(ALLOWED_TRANSITIONS[old])}")
    return new


def restart_count_after(previous: Mapping[str, Any] | None) -> int:
    """How many times this app id has been STARTED, including this start.

    Counted from the stored value rather than from the checkpoint directory: a checkpoint
    that was reset (an audited act -- `scripts/streaming-reset.sh`) must not make the app
    look brand new, because "this app has restarted 40 times today" is the signal that a
    crash loop is being papered over by the lifecycle DAG.
    """
    if not previous:
        return 1
    raw = previous.get("restart_count")
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return 1
    return max(1, n + 1)


# --------------------------------------------------------------------------- #
# section 7b -- Kafka offsets out of a query progress
# --------------------------------------------------------------------------- #

def offsets_json(progress: Mapping[str, Any] | None) -> str | None:
    """`{topic: {partition: offset}}` from a Spark `StreamingQueryProgress` dict.

    COORDINATES AND COUNTS, NEVER PAYLOAD -- the same rule the event index and the run
    ledgers follow, and the reason this is safe to read from a dashboard.

    Spark reports `endOffset` per source as a JSON STRING, and a source that has not yet
    read anything reports `null`. Both are handled here rather than at three call sites.
    """
    if not progress:
        return None
    merged: dict[str, dict[str, int]] = {}
    for source in progress.get("sources") or ():
        raw = source.get("endOffset") if isinstance(source, Mapping) else None
        if not raw:
            continue
        parsed = json.loads(raw) if isinstance(raw, str) else raw
        for topic, partitions in (parsed or {}).items():
            if not isinstance(partitions, Mapping):
                continue
            slot = merged.setdefault(str(topic), {})
            for part, off in partitions.items():
                if off is None:
                    continue
                slot[str(part)] = int(off)
    if not merged:
        return None
    # sort_keys: the row is diffed by humans and by tests. An offsets blob whose key order
    # follows dict insertion makes two identical positions look like a change.
    return json.dumps(merged, sort_keys=True, separators=(",", ":"))


def offset_total(offsets: str | None) -> int:
    """Sum of every partition's end offset. A single number an operator can watch move."""
    if not offsets:
        return 0
    try:
        parsed = json.loads(offsets)
    except (TypeError, ValueError):
        return 0
    return sum(int(v) for parts in parsed.values() for v in parts.values())


# --------------------------------------------------------------------------- #
# section 7c -- the row
# --------------------------------------------------------------------------- #

@dataclass
class StateRow:
    """One application's current state. Field names ARE the column names."""
    app_id: str
    deployment_id: str
    mode: str
    profile: str
    trigger_interval: str
    checkpoint_location: str
    status: str
    last_batch_id: int | None = None
    kafka_offsets: str | None = None
    source_watermark_ts: dt.datetime | None = None
    last_commit_ts: dt.datetime | None = None
    target_snapshot_id: int | None = None
    rows_last_batch: int | None = None
    rows_total: int | None = None
    restart_count: int = 1
    config_version: str | None = None
    error: str | None = None
    updated_at: dt.datetime | None = None
    #: Not a column: what this row was written for, carried so the caller can log it.
    reason: str = field(default="", compare=False)

    def __post_init__(self) -> None:
        if not self.app_id or "/" in self.app_id:
            raise ConfigError(f"app_id {self.app_id!r} must be a single non-empty segment")
        if self.status not in _ing.STREAMING_STATUSES:
            raise ConfigError(f"streaming status {self.status!r} is not one of "
                              f"{', '.join(_ing.STREAMING_STATUSES)}")
        if self.mode not in _ing.INGESTION_MODES:
            raise ConfigError(f"ingestion mode {self.mode!r} is not one of "
                              f"{', '.join(_ing.INGESTION_MODES)}")
        if self.error:
            self.error = str(self.error)[:ERROR_LIMIT]

    def as_dict(self) -> dict[str, Any]:
        return {name: getattr(self, name) for name, _t, _c in _ing.STREAMING_STATE_COLUMNS}


# --------------------------------------------------------------------------- #
# section 7d -- the write
# --------------------------------------------------------------------------- #

def _literal(value: Any, sql_type: str) -> str:
    """One value as SQL, typed. A NULL is CAST so the MERGE's source column keeps its type.

    Without the cast Spark types a literal NULL as void, and `UPDATE SET last_batch_id =
    s.last_batch_id` then fails to resolve against a BIGINT column -- on the first batch
    that had nothing to report, which is to say not in any test that wrote data.
    """
    t = sql_type.upper()
    if value is None:
        return f"CAST(NULL AS {t})"
    if t in ("BIGINT", "INT"):
        return str(int(value))
    if t == "TIMESTAMP":
        if isinstance(value, dt.datetime):
            # UTC, always (ADR-024). A naive datetime here would be interpreted in the
            # session zone, and the state table is read next to `source_commit_ts`.
            aware = value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)
            text = aware.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            text = str(value)
        return f"TIMESTAMP '{text}'"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def merge_sql(identifier: str, row: StateRow) -> str:
    """UPDATE IN PLACE, keyed on `app_id`. Never an append.

    Built column-by-column from `ingestion.STREAMING_STATE_COLUMNS`, so a column added to
    the contract cannot be silently left unwritten: the DDL and the writer read the same
    tuple. `UPDATE SET *` would have been shorter and would bind the writer to the physical
    column ORDER instead.
    """
    if not identifier:
        raise ConfigError("streaming state write needs a table identifier")
    values = row.as_dict()
    names = [n for n, _t, _c in _ing.STREAMING_STATE_COLUMNS]
    select = ",\n         ".join(
        f"{_literal(values[n], t)} AS {n}" for n, t, _c in _ing.STREAMING_STATE_COLUMNS)
    assignments = ",\n      ".join(f"{n} = s.{n}" for n in names if n != "app_id")
    return (f"MERGE INTO {identifier} t\n"
            f"USING (SELECT {select}) s\n"
            f"ON t.app_id = s.app_id\n"
            f"WHEN MATCHED THEN UPDATE SET\n      {assignments}\n"
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(names)})\n"
            f"VALUES ({', '.join('s.' + n for n in names)})")


def state_query(identifier: str, *, app_id: str | None = None
                ) -> tuple[str, str, tuple[str, ...]]:
    """`(database, sql, columns)` for reading the state table from ATHENA.

    Built here rather than at each caller because there are two of them -- the Airflow
    monitor and `scripts/cdc-stream.py` -- and a monitor that asked a slightly different
    question than the operator's CLI is a monitor whose disagreement with the operator is
    invisible until the night it matters.

    The Spark catalog prefix is stripped: `glue_catalog.db.table` is how a SparkSession
    addresses the table and `"db"."table"` is how Athena addresses the same Glue entry.
    """
    parts = [p for p in str(identifier or "").split(".") if p]
    if len(parts) < 2:
        raise ConfigError(
            f"unusable streaming state identifier {identifier!r}; expected "
            f"catalog.database.table from the compiled plan's ingestion.state_table")
    database, table = parts[-2], parts[-1]
    columns = tuple(n for n, _t, _c in _ing.STREAMING_STATE_COLUMNS)
    if app_id is not None and ("'" in app_id or "/" in app_id):
        raise ConfigError(f"app_id {app_id!r} must be a single plain segment")
    where = f" WHERE app_id = '{app_id}'" if app_id else ""
    sql = f'SELECT {", ".join(columns)} FROM "{database}"."{table}"{where}'
    return database, sql, columns


def heartbeat_due(*, last_written: dt.datetime | None, now: dt.datetime,
                  interval_seconds: int, rows: int = 0, status_changed: bool = False,
                  error: bool = False) -> bool:
    """Should this batch write state?

    Information first, clock second: a batch that moved rows, changed status or failed is
    news and is written immediately. Otherwise the write waits for the interval, because a
    resident app that is quiet for an hour needs to say so 12 times, not 60.
    """
    if status_changed or error or rows:
        return True
    if last_written is None:
        return True
    if interval_seconds < MIN_HEARTBEAT_SECONDS:
        raise ConfigError(
            f"heartbeat interval {interval_seconds}s is below the {MIN_HEARTBEAT_SECONDS}s "
            f"floor; at that cadence the state table commits more snapshots than the data")
    return (now - last_written).total_seconds() >= interval_seconds


# --------------------------------------------------------------------------- #
# section 7e -- the read (one implementation, two callers)
# --------------------------------------------------------------------------- #

HEALTH_HEALTHY = "HEALTHY"
HEALTH_STALE = "STALE"
HEALTH_FAILED = "FAILED"
HEALTH_STOPPED = "STOPPED"
HEALTH_ABSENT = "ABSENT"
HEALTH_STARTING = "STARTING"

#: How many heartbeat intervals of silence before a RUNNING app is called stale. Three,
#: not one: a single missed heartbeat is a slow batch or a retried commit, and a monitor
#: that restarts an application for that turns a hiccup into a restart loop.
STALE_INTERVALS = 3


@dataclass(frozen=True)
class Health:
    verdict: str
    reason: str
    #: True only for a verdict the lifecycle should act on by restarting. STOPPED is not
    #: one: an operator stopped it, and an orchestrator that "recovers" a deliberate stop
    #: is an orchestrator nobody can turn off.
    restartable: bool = False

    @property
    def ok(self) -> bool:
        return self.verdict in (HEALTH_HEALTHY, HEALTH_STARTING)


def health(row: Mapping[str, Any] | None, *, now: dt.datetime,
           heartbeat_seconds: int = DEFAULT_HEARTBEAT_SECONDS,
           stale_intervals: int = STALE_INTERVALS) -> Health:
    """What the monitor and the CLI both decide from one row.

    ABSENT is deliberately NOT healthy and NOT restartable-by-default: no row means either
    "never started" or "the state write itself is broken", and those need a human to tell
    apart. Silence read as health is how Phase A's lag went unnoticed for a session.
    """
    if not row:
        return Health(HEALTH_ABSENT, "no state row for this app id: it has never started, "
                                     "or the state write is failing")
    status = str(row.get("status") or "").upper()
    updated = row.get("updated_at")
    if isinstance(updated, str):
        updated = dt.datetime.fromisoformat(updated.replace("Z", "+00:00"))
    if updated is not None and updated.tzinfo is None:
        updated = updated.replace(tzinfo=dt.timezone.utc)
    age = None if updated is None else (now - updated).total_seconds()

    if status == _ing.STATUS_FAILED:
        return Health(HEALTH_FAILED, f"app failed: {row.get('error') or 'no error recorded'}",
                      restartable=True)
    if status == _ing.STATUS_STOPPED:
        return Health(HEALTH_STOPPED, "stopped cleanly; the checkpoint is intact and a "
                                      "start resumes from it")
    if status == _ing.STATUS_STARTING:
        return Health(HEALTH_STARTING, "starting")
    if status != _ing.STATUS_RUNNING:
        return Health(HEALTH_ABSENT, f"unreadable status {status!r}")

    limit = max(heartbeat_seconds, 1) * max(stale_intervals, 1)
    if age is None:
        return Health(HEALTH_STALE, "RUNNING with no updated_at", restartable=True)
    if age > limit:
        return Health(
            HEALTH_STALE,
            f"RUNNING but last heartbeat {int(age)}s ago, over the {int(limit)}s limit "
            f"({stale_intervals} x {heartbeat_seconds}s)", restartable=True)
    return Health(HEALTH_HEALTHY, f"RUNNING, heartbeat {int(age)}s ago")
