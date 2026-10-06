"""Iceberg table maintenance, driven by config and by MEASURED file metrics.

    table config + observed metrics  ->  which actions this table needs, right now

Pure. No Spark, no AWS -- the decision is separated from the doing so that "why did this
table get compacted?" is answerable from a config and a metrics row, without a cluster.

WHY THRESHOLDS AND NOT ONLY CRON (section C)
---------------------------------------------
A cron that compacts every table nightly does the same work whether or not there is work to
do. The Phase 0 audit measured the actual problem -- a mean data file of 137 KiB against a
128 MiB target, three orders of magnitude under -- and it is caused by COMMIT FREQUENCY, not
by elapsed time. A table nobody wrote to since the last run has nothing to compact, and a
table that took a thousand small commits in an hour needs compacting before its cadence says
so. So a cadence gives the OPPORTUNITY to act and the metrics decide WHETHER to.

The cadence is still a real bound: it is what stops a hot table being compacted continuously
and spending more on maintenance than on ingest.
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import ConfigError

# --------------------------------------------------------------------------- #
# section A -- the actions
# --------------------------------------------------------------------------- #

REWRITE_DATA_FILES = "rewrite_data_files"
REWRITE_MANIFESTS = "rewrite_manifests"
EXPIRE_SNAPSHOTS = "expire_snapshots"
REMOVE_ORPHAN_FILES = "remove_orphan_files"

ACTIONS = (REWRITE_DATA_FILES, REWRITE_MANIFESTS, EXPIRE_SNAPSHOTS, REMOVE_ORPHAN_FILES)

#: The order actions must run in, and it is not arbitrary.
#:
#: Compaction WRITES new files and leaves the old ones referenced by older snapshots, so
#: expiring snapshots afterwards is what actually frees the space -- run the other way round
#: and the expiry has nothing to collect and the storage never drops. Orphan removal runs
#: LAST because it deletes files no snapshot references, and it must not run while a
#: rewrite it cannot see is still in flight.
ACTION_ORDER = (REWRITE_DATA_FILES, REWRITE_MANIFESTS, EXPIRE_SNAPSHOTS,
                REMOVE_ORPHAN_FILES)

# --------------------------------------------------------------------------- #
# section B -- temperature
# --------------------------------------------------------------------------- #

HOT = "hot"
WARM = "warm"
COLD = "cold"
TEMPERATURES = (HOT, WARM, COLD)

#: Minimum hours between maintenance runs for a table of each temperature. A FLOOR, not a
#: schedule: the scheduler may offer a table more often, and this is what refuses.
MIN_INTERVAL_HOURS = {HOT: 6, WARM: 24, COLD: 168}

#: Derived from the table's own declared policy when not stated. A table with a tight
#: freshness SLA commits often, which is precisely what makes small files; a table with no
#: realtime layer is written once a day at most.
def default_temperature(entry: dict) -> str:
    """Infer a temperature from what the registry already says about the table.

    Inferred rather than required, so onboarding a table does not need a guess about a
    property the platform can read. An explicit `maintenance.temperature` always wins.
    """
    dq = entry.get("dq") or {}
    rt = (entry.get("realtime_policy") or {}).get("enabled", True)
    freshness = int(dq.get("freshness_sla_minutes", 60))
    if rt and freshness <= 15:
        return HOT
    if rt:
        return WARM
    return COLD


# --------------------------------------------------------------------------- #
# section C -- the thresholds
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Thresholds:
    """When a metric means "there is work to do".

    Defaults are anchored to the Phase 0 measurement, not to taste: mean file 137 KiB,
    109 metadata.json files across three chains, 13 manifests for 14 data files.
    """
    #: A file smaller than this fraction of the target is "small". 0.25 of 128 MiB = 32 MiB.
    small_file_fraction: float = 0.25
    #: How many small files justify a rewrite. Two small files are not a problem; a rewrite
    #: that runs for two files costs more than it saves.
    min_small_files: int = 8
    #: Rewrite when the mean file is below this fraction of target even if the count is low.
    min_avg_size_fraction: float = 0.10
    #: More manifests than this and manifest rewriting pays for itself.
    max_manifests: int = 10
    #: More snapshots than this and expiry has something to do.
    max_snapshots: int = 20
    #: Orphan removal is the one that DELETES, so it needs a stronger reason than "a while
    #: has passed": a measured count of unreferenced files.
    min_orphan_files: int = 1


@dataclass(frozen=True)
class TableMetrics:
    """What was MEASURED about one table. Every field is observed, never estimated."""
    table_id: str
    identifier: str
    file_count: int = 0
    small_file_count: int = 0
    avg_file_size_bytes: float = 0.0
    manifest_count: int = 0
    snapshot_count: int = 0
    orphan_file_count: int = 0
    hours_since_last_maintenance: float | None = None
    #: None means "never measured", which is different from zero and must stay different.
    total_bytes: int | None = None


@dataclass(frozen=True)
class Decision:
    action: str
    needed: bool
    reason: str

    def __str__(self) -> str:                                          # pragma: no cover
        return f"{self.action}: {'RUN' if self.needed else 'skip'} -- {self.reason}"


@dataclass(frozen=True)
class MaintenancePlan:
    table_id: str
    temperature: str
    decisions: tuple
    due: bool
    interval_reason: str

    @property
    def actions(self) -> tuple:
        """The actions to run, in the order they must run."""
        if not self.due:
            return ()
        chosen = {d.action for d in self.decisions if d.needed}
        return tuple(a for a in ACTION_ORDER if a in chosen)

    @property
    def needed(self) -> bool:
        return bool(self.actions)

    def payload(self) -> dict:
        return {"table_id": self.table_id, "temperature": self.temperature,
                "due": self.due, "interval_reason": self.interval_reason,
                "actions": list(self.actions),
                "decisions": [{"action": d.action, "needed": d.needed, "reason": d.reason}
                              for d in self.decisions]}


def _enabled_actions(entry: dict) -> set:
    m = entry.get("maintenance") or {}
    declared = m.get("actions")
    if declared is not None and len(declared) == 0:
        # Refused rather than obeyed. An empty list is what a mistake produces (a dropped
        # value, a plan that serialised "unset" as `[]`), and it is indistinguishable from
        # a deliberate "never maintain this table" -- for which the platform has no
        # setting. Obeying it turns off compaction, snapshot expiry and manifest rewriting
        # on a streaming table, and nothing reports that it happened.
        raise ConfigError(
            f"{entry.get('table_id')}: maintenance.actions is empty. Omit the key to get "
            f"the default set ({', '.join(sorted(set(ACTIONS) - {REMOVE_ORPHAN_FILES}))}), "
            f"or list the actions this table permits. There is no way to spell "
            f"'no maintenance at all'.")
    if declared is None:
        # Everything except orphan removal, which deletes files and is opt-in per table.
        return set(ACTIONS) - {REMOVE_ORPHAN_FILES}
    unknown = [a for a in declared if a not in ACTIONS]
    if unknown:
        raise ConfigError(f"{entry.get('table_id')}: unknown maintenance action(s) "
                          f"{unknown}. Known: {', '.join(ACTIONS)}")
    return set(declared)


def plan_maintenance(entry: dict, metrics: TableMetrics,
                     thresholds: Thresholds | None = None) -> MaintenancePlan:
    """Decide what one table needs. Pure: config in, metrics in, decisions out."""
    t = thresholds or Thresholds()
    m = entry.get("maintenance") or {}
    temperature = m.get("temperature") or default_temperature(entry)
    if temperature not in TEMPERATURES:
        raise ConfigError(f"{entry.get('table_id')}: maintenance.temperature "
                          f"{temperature!r} is not one of {', '.join(TEMPERATURES)}")

    floor = MIN_INTERVAL_HOURS[temperature]
    since = metrics.hours_since_last_maintenance
    if since is None:
        due, interval_reason = True, "never maintained"
    elif since >= floor:
        due, interval_reason = True, f"{since:.1f}h since last run, floor {floor}h"
    else:
        due, interval_reason = False, (f"only {since:.1f}h since last run; {temperature} "
                                       f"tables wait {floor}h")

    target_bytes = int((entry.get("write") or {}).get("target_file_size_mb", 128)) * 1024 * 1024
    enabled = _enabled_actions(entry)
    decisions = []

    if REWRITE_DATA_FILES in enabled:
        # BOTH triggers require enough FILES to be worth rewriting.
        #
        # The value of a compaction is roughly the number of files it eliminates, and a
        # rewrite costs an EMR run whatever it finds. Measured live: the freshly backfilled
        # `oracle/ACCOUNT` holds 2 files averaging 48 KB -- far under the 128 MiB target, so
        # a mean-only trigger fired and asked to compact two files into one. That is an
        # EMR run to remove one file, on every cadence, forever. `min_small_files` existed
        # to prevent exactly that and the mean branch was bypassing it.
        enough_files = metrics.file_count >= t.min_small_files
        small_enough = enough_files and metrics.small_file_count >= t.min_small_files
        mean_low = (enough_files and metrics.avg_file_size_bytes > 0
                    and metrics.avg_file_size_bytes < target_bytes * t.min_avg_size_fraction)
        needed = small_enough or mean_low
        why = []
        if small_enough:
            why.append(f"{metrics.small_file_count} small files >= {t.min_small_files}")
        if mean_low:
            why.append(f"mean {metrics.avg_file_size_bytes:,.0f}B is under "
                       f"{t.min_avg_size_fraction:.0%} of the {target_bytes:,}B target")
        if not needed:
            why.append(
                f"{metrics.file_count} files ({metrics.small_file_count} small), mean "
                f"{metrics.avg_file_size_bytes:,.0f}B -- "
                + ("too few files for a rewrite to pay for itself"
                   if not enough_files else "nothing to gain"))
        decisions.append(Decision(REWRITE_DATA_FILES, needed, "; ".join(why)))

    if REWRITE_MANIFESTS in enabled:
        needed = metrics.manifest_count > t.max_manifests
        decisions.append(Decision(
            REWRITE_MANIFESTS, needed,
            f"{metrics.manifest_count} manifests vs threshold {t.max_manifests}"))

    if EXPIRE_SNAPSHOTS in enabled:
        needed = metrics.snapshot_count > t.max_snapshots
        decisions.append(Decision(
            EXPIRE_SNAPSHOTS, needed,
            f"{metrics.snapshot_count} snapshots vs threshold {t.max_snapshots}"))

    if REMOVE_ORPHAN_FILES in enabled:
        # A MEASURED count, never elapsed time. This action deletes files, and "a week has
        # passed" is not evidence that anything is orphaned.
        needed = metrics.orphan_file_count >= t.min_orphan_files
        decisions.append(Decision(
            REMOVE_ORPHAN_FILES, needed,
            f"{metrics.orphan_file_count} unreferenced files"
            if needed else "no measured orphans; elapsed time is not a reason to delete"))

    return MaintenancePlan(table_id=entry.get("table_id", ""), temperature=temperature,
                           decisions=tuple(decisions), due=due,
                           interval_reason=interval_reason)


# --------------------------------------------------------------------------- #
# section A -- "do not run all tables at the same time"
# --------------------------------------------------------------------------- #

def select_batch(plans: list, *, max_tables: int = 2) -> list:
    """The tables to maintain in ONE run, worst first.

    Iceberg maintenance rewrites data and commits to the table, so running every table at
    once turns a maintenance window into the platform's peak load -- on the same EMR
    capacity the ingest uses, and against tables a live writer may also be committing to.
    A bounded batch keeps the window predictable.

    Ordered by how much work each table needs, so a bound never means the worst table waits
    behind a healthier one.
    """
    if max_tables < 1:
        raise ConfigError(f"max_tables must be >= 1, got {max_tables}")
    ranked = sorted((p for p in plans if p.needed),
                    key=lambda p: (-len(p.actions), p.table_id))
    return ranked[:max_tables]


def retention_guard(entry: dict) -> tuple:
    """(ok, reason). Orphan cleanup must never outrun snapshot expiry.

    CLAUDE.md 6: a shorter orphan window deletes files a live snapshot still references, and
    the table then fails to read with no way back -- the snapshots that could time-travel it
    reference the same missing files. The compiler already enforces this; it is re-checked
    here because this module is what actually orders the delete.
    """
    m = entry.get("maintenance") or {}
    expire = int(m.get("expire_snapshots_days", 7))
    orphan = int(m.get("remove_orphan_files_days", 3))
    if orphan > expire:
        return False, (f"remove_orphan_files_days={orphan} exceeds "
                       f"expire_snapshots_days={expire}: orphan cleanup would delete files "
                       f"a live snapshot still references (CLAUDE.md 6)")
    return True, f"orphan {orphan}d <= expire {expire}d"


# --------------------------------------------------------------------------- #
# Phase G section 5 -- partition-scoped compaction, per layer
# --------------------------------------------------------------------------- #

#: Layers this framework maintains. MART is here because a mart is an Iceberg table with the
#: same small-file problem; it is NOT in the CDC registry, so its entry is supplied by the
#: caller (the reporting plan) rather than derived from a source table.
LAYER_FULL_CDC = "FULL_CDC"
LAYER_REALTIME = "REALTIME"
LAYER_EOD = "EOD"
LAYER_MART = "MART"
MAINTAINED_LAYERS = (LAYER_FULL_CDC, LAYER_REALTIME, LAYER_EOD, LAYER_MART)

#: How many recent partitions a compaction touches by default, per layer.
#:
#: WHY SCOPE AT ALL. `rewrite_data_files` with no predicate rewrites the WHOLE table. On
#: FULL_CDC that is the canonical history of every event ever captured -- rewriting it nightly
#: to fix today's small files costs the entire table's bytes to repair one day's, and each
#: rewrite is a new snapshot holding the old files until expiry. The small files are almost
#: always in the RECENT partitions, because that is where the writes are.
DEFAULT_RECENT_DAYS = {LAYER_FULL_CDC: 3, LAYER_REALTIME: 2, LAYER_EOD: 7, LAYER_MART: 7}


@dataclass(frozen=True)
class CompactionScope:
    """WHERE a rewrite runs, and why that is the right blast radius."""
    where: str | None
    reason: str
    #: False means "the whole table", which is correct for a small or latest-state table and
    #: must be a decision rather than an omission.
    scoped: bool = True

    def options(self) -> dict:
        return {"where": self.where} if self.where else {}


def compaction_scope(entry: dict, *, layer: str, now=None,
                     recent_days: int | None = None) -> CompactionScope:
    """The partition predicate for one table's rewrite, from its LAYER and its policy.

    Each layer's answer follows from what the layer IS, not from a preference:

    * **FULL_CDC** is append-only canonical history partitioned on `event_date`. Only recent
      days receive writes, so only recent days accumulate small files. History is never
      rewritten -- that is the layer's contract (CLAUDE.md 5.2/5.3), and a rewrite that
      touched it would rewrite bytes nobody changed.
    * **REALTIME** is a bounded rolling window. Its ACTIVE partitions are the window; beyond
      the retention bound the prune deletes rows anyway, so compacting there is work whose
      result is about to be discarded.
    * **EOD** depends on the snapshot mode. `latest_state` holds one COB and is small, so it
      compacts whole. `rolling_history` accumulates a partition per business date and only
      the recent ones move.
    * **MART** has no single shape, so the default is recent business dates and a mart that
      knows its hotspot overrides it.
    """
    import datetime as _dt

    if layer not in MAINTAINED_LAYERS:
        raise ConfigError(f"maintenance layer {layer!r} is not one of "
                          f"{', '.join(MAINTAINED_LAYERS)}")
    policy = (entry.get("maintenance") or {})
    now = now or _dt.datetime.now(_dt.timezone.utc)
    # `or` would swallow an explicit 0: `policy.get(...) or DEFAULT` turns
    # "compact the WHOLE table" into "compact 3 days", silently, because 0 is
    # falsy -- the same shape that made `maintenance.actions: []` inherit the
    # full default set (ADR-070).
    configured = policy.get("compact_recent_days")
    if recent_days is not None:
        days = int(recent_days)
    elif configured is not None:
        days = int(configured)
    else:
        days = DEFAULT_RECENT_DAYS[layer]
    if days <= 0:
        return CompactionScope(None, "compact_recent_days <= 0 means the whole table",
                               scoped=False)

    if layer == LAYER_EOD:
        mode = (entry.get("eod_policy") or {}).get("snapshot_mode", "rolling_history")
        if mode == "latest_state":
            return CompactionScope(
                None, "EOD latest_state holds one COB; the whole table IS the recent data",
                scoped=False)
        column = "business_date"
    elif layer == LAYER_REALTIME:
        column = "event_date"
    elif layer == LAYER_MART:
        column = str(policy.get("compact_partition_column") or "date_of_data")
    else:
        column = "event_date"

    cutoff = (now - _dt.timedelta(days=days)).date()
    return CompactionScope(
        f"{column} >= DATE '{cutoff.isoformat()}'",
        f"{layer}: the last {days} day(s) by {column} -- where the writes, and therefore "
        f"the small files, are")


# --------------------------------------------------------------------------- #
# section 6 -- orphan removal must outlive the longest writer
# --------------------------------------------------------------------------- #

#: The longest a write may plausibly be in flight. `remove_orphan_files` deletes files that
#: are unreferenced AND older than its threshold -- and a file written by a job that has not
#: committed yet is exactly that. Deleting it destroys a run that was about to succeed.
DEFAULT_MAX_WRITER_HOURS = 24

#: Multiplied onto the writer duration. A threshold equal to the longest writer is a
#: threshold that fails the first time a writer is slower than its longest observed run.
ORPHAN_SAFETY_FACTOR = 2


def orphan_min_age_hours(entry: dict, *, max_writer_hours: int | None = None) -> float:
    """The floor below which orphan removal is unsafe for this table."""
    policy = (entry.get("maintenance") or {})
    hours = int(max_writer_hours if max_writer_hours is not None
                else policy.get("max_writer_hours") or DEFAULT_MAX_WRITER_HOURS)
    return hours * ORPHAN_SAFETY_FACTOR


def validate_orphan_age(entry: dict, *, table_id: str = "",
                        max_writer_hours: int | None = None) -> None:
    """Refuse a configuration whose orphan threshold could delete a live writer's files."""
    policy = (entry.get("maintenance") or {})
    configured = float(int(policy.get("remove_orphan_files_days", 3)) * 24)
    floor = orphan_min_age_hours(entry, max_writer_hours=max_writer_hours)
    if configured < floor:
        raise ConfigError(
            f"{table_id or entry.get('table_id')}: remove_orphan_files_days="
            f"{configured / 24:g} is {configured:g}h, below the {floor:g}h floor "
            f"({ORPHAN_SAFETY_FACTOR}x the {floor / ORPHAN_SAFETY_FACTOR:g}h longest "
            f"writer). Orphan removal deletes unreferenced files, and a file an in-flight "
            f"job has written but not yet committed is unreferenced -- the delete would "
            f"destroy a run that was about to succeed")
