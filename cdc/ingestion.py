"""How Kafka reaches FULL_CDC: execution mode, trigger, checkpoint identity, app state.

    profile + config  ->  IngestionPolicy  ->  the one thing stream_job reads

WHY THIS MODULE EXISTS (ADR-073)
--------------------------------
Phase A found that production behaviour was being INFERRED from a CLI flag. `stream_job.py`
opened a real `processingTime` query and then killed it on a `--run-seconds` wall-clock
budget:

    while query.isActive and (time.monotonic() - started) < args.run_seconds:

That is a bounded streaming SESSION wearing a streaming name, and nothing in config said so.
Worse, the job actually being run was the BATCH one -- measured live on 2026-09-17, Kafka
`cdc.oracle.COREBANK.LOAN` held 124 events while FULL_CDC held 65, and nothing reported a
problem because a batch ingest that has finished is indistinguishable from a stream that is
caught up.

So the mode becomes a DECLARED value with a profile-chosen default, and `--run-seconds`
stops being a lifecycle mechanism.

THE TENSION THIS RESOLVES, RATHER THAN HIDES
--------------------------------------------
The production target is a resident application. CLAUDE.md section 4 says nothing runs 24/7
without a warning and `lab_low_cost` is the default profile. Both are right. Hardcoding
either one would make the other a bug, so the PROFILE picks the default and the registry can
override per deployment -- and turning on a resident app is then a costed decision someone
made, not a side effect of which entry point got submitted.
"""
from __future__ import annotations

from dataclasses import dataclass

from .models import ConfigError

# --------------------------------------------------------------------------- #
# section 1 -- execution modes
# --------------------------------------------------------------------------- #

#: A resident Structured Streaming query. Commits on its trigger interval, forever, until an
#: operator stops it. This is the production target.
MODE_CONTINUOUS = "continuous_microbatch"

#: `Trigger.AvailableNow`: drain what Kafka currently holds, commit, exit. Finite, so EMR
#: capacity is released and the lab costs nothing between runs. NOT a lesser implementation
#: -- it runs the identical transformation and routing code, and differs only in the trigger
#: and in whether the process stays up.
MODE_AVAILABLE_NOW = "available_now"

INGESTION_MODES = (MODE_CONTINUOUS, MODE_AVAILABLE_NOW)

#: Deployment profiles, and the mode each defaults to. The profile is the cost decision;
#: the mode is its consequence. Keeping them separate is what makes "we are paying for a
#: resident app" reviewable in one place.
PROFILE_PRODUCTION = "production"
PROFILE_LAB = "lab_low_cost"
PROFILES = (PROFILE_PRODUCTION, PROFILE_LAB)

DEFAULT_MODE_FOR_PROFILE = {
    PROFILE_PRODUCTION: MODE_CONTINUOUS,
    PROFILE_LAB: MODE_AVAILABLE_NOW,
}

#: The production trigger. One minute, not thirty seconds: every trigger is an Iceberg
#: commit, every commit is a snapshot and at least one data file, and the Phase A measurement
#: found a mean file of 81 KB against a 128 MiB target. Halving the interval doubles the
#: small-file problem it already has.
DEFAULT_TRIGGER = "1 minute"

#: Accepted `processingTime` spellings. Spark parses far more than this; the narrow set is
#: deliberate, because `trigger_interval: "1"` silently meaning one MILLISECOND is the kind
#: of config that is only discovered from a bill.
_TRIGGER_UNITS = ("second", "seconds", "minute", "minutes", "hour", "hours")

#: Below this a trigger is refused outright. A 5-second commit cadence on Iceberg produces
#: 17,280 snapshots a day per table and a maintenance job that can never catch up.
MIN_TRIGGER_SECONDS = 30

#: How often a resident app writes "still alive" when it has nothing else to say. The state
#: row is MERGEd in place, but in-place is about ROWS, not COMMITS: writing every trigger is
#: 1,440 Iceberg snapshots a day on a table holding a handful of rows, and expire-snapshots
#: would then spend its budget on the state table rather than on the data
#: (`cdc/streaming_state.py`, section 7).
DEFAULT_HEARTBEAT = "5 minutes"

#: Below this the throttle stops throttling anything.
MIN_HEARTBEAT_SECONDS = 60


def parse_trigger(raw: str, *, what: str, floor: int = MIN_TRIGGER_SECONDS) -> int:
    """`"1 minute"` -> 60. Refuses anything it cannot read UNAMBIGUOUSLY."""
    text = str(raw).strip().lower()
    parts = text.split()
    if len(parts) != 2:
        raise ConfigError(
            f"{what}: trigger_interval {raw!r} must be '<number> <unit>', e.g. '1 minute'. "
            f"A bare number is refused because Spark would read it as milliseconds.")
    value, unit = parts
    try:
        n = float(value)
    except ValueError:
        raise ConfigError(f"{what}: trigger_interval {raw!r} has a non-numeric value "
                          f"{value!r}") from None
    if unit not in _TRIGGER_UNITS:
        raise ConfigError(f"{what}: trigger_interval unit {unit!r} is not one of "
                          f"{', '.join(sorted(set(_TRIGGER_UNITS)))}")
    seconds = int(n * {"second": 1, "seconds": 1, "minute": 60, "minutes": 60,
                       "hour": 3600, "hours": 3600}[unit])
    if seconds < floor:
        raise ConfigError(
            f"{what}: trigger_interval {raw!r} is {seconds}s, below the {floor}s "
            f"floor. Every trigger is an Iceberg commit; at this cadence the table produces "
            f"more snapshots per day than maintenance can expire.")
    return seconds


# --------------------------------------------------------------------------- #
# section 4 -- checkpoint identity
# --------------------------------------------------------------------------- #

#: Checkpoints live OUTSIDE the warehouse. CLAUDE.md section 5.9: a checkpoint under
#: `warehouse/` is a file an Iceberg maintenance job will eventually consider an orphan.
CHECKPOINT_ROOT = "checkpoints/full_cdc"


def checkpoint_path(lake_bucket: str, app_id: str) -> str:
    """The durable checkpoint location for one streaming application.

    DERIVED FROM IDENTITY, NEVER FROM A RUN. A checkpoint keyed on a run id is a new
    checkpoint on every restart, which means every restart replays from
    `startingOffsets` and re-delivers everything -- the MERGE on `dv_event_id` makes that
    non-destructive, but it is a full re-read that looks like normal operation.
    """
    if not lake_bucket:
        raise ConfigError("checkpoint_path needs the lake bucket")
    if not app_id or "/" in app_id:
        raise ConfigError(f"streaming app id {app_id!r} must be a single non-empty segment")
    return f"s3://{lake_bucket}/{CHECKPOINT_ROOT}/{app_id}"


def app_id_for(config_version: str, engine: str = "") -> str:
    """A stable streaming app id.

    NOT the config_version alone: the checkpoint must survive a config change that does not
    change what is being read. Onboarding a table changes `config_version`, and if that
    changed the checkpoint, adding one table would silently re-ingest every other table's
    history. The id is therefore the ENGINE (whose topics the app subscribes to), and the
    config version is recorded in state as evidence instead.
    """
    return f"full-cdc-{engine}" if engine else "full-cdc-all"


def app_id_for_topics(plan: dict | None, topics) -> str:
    """The app id for the topics this process actually SUBSCRIBES to.

    Identity follows the subscription, because that is what the checkpoint describes: a
    checkpoint records an offset per (topic, partition), so two processes reading different
    topic sets must not share one, and one process reading the same topic set across a
    redeploy must keep its own.

    Derived from the plan's topic -> source mapping rather than from a submit-time flag,
    for the reason ADR-070 records four times over: a derived artifact kept in sync BY HAND
    is one that is eventually out of sync, and here being out of sync means replaying a
    topic from `startingOffsets` while every dashboard says the app restarted normally.
    """
    wanted = [t for t in (topics or []) if t]
    if not wanted:
        raise ConfigError("cannot derive a streaming app id from an empty topic list")
    payload = (plan or {}).get("plan") or plan or {}
    by_topic: dict[str, str] = {}
    for entry in payload.get("tables") or ():
        topic = (entry.get("capture") or {}).get("topic")
        engine = (entry.get("source") or {}).get("engine")
        if topic and engine:
            by_topic[str(topic)] = str(engine)
    unknown = sorted(t for t in wanted if t not in by_topic)
    if unknown:
        # REFUSE rather than key the checkpoint on the topics that happen to be known.
        # Deriving `full-cdc-oracle` from "four oracle topics and one the plan has never
        # heard of" would hand this process the ORACLE APP'S CHECKPOINT -- two processes,
        # one checkpoint, which Spark discovers later and less legibly than this does.
        raise ConfigError(
            f"cannot derive a streaming app id: the plan does not describe "
            f"{', '.join(unknown[:3])}{' ...' if len(unknown) > 3 else ''}. Register the "
            f"table, or pass --app-id explicitly for a deliberate one-off subscription.")
    engines = sorted({by_topic[t] for t in wanted})
    if len(engines) == 1:
        return app_id_for("", engines[0])
    # A deliberately mixed subscription gets ONE stable id rather than one invented per
    # topic set: an id derived from the set changes the checkpoint the first time someone
    # reorders the --topics argument, and a changed checkpoint is a full replay.
    return app_id_for("", "")


# --------------------------------------------------------------------------- #
# section 7 -- streaming app state (hot, not an append log)
# --------------------------------------------------------------------------- #

STREAMING_STATE_TABLE = "streaming_app_state"

#: ONE ROW PER APP, updated in place -- not one row per heartbeat. A heartbeat every trigger
#: for a resident app is 1,440 Iceberg commits a day writing nothing but a timestamp, on a
#: table whose only reader asks "is it alive and where is it".
STREAMING_STATE_COLUMNS = (
    ("app_id", "STRING", "stable identity; also the checkpoint segment"),
    ("deployment_id", "STRING", "which deployment started this process"),
    ("mode", "STRING", "continuous_microbatch | available_now"),
    ("profile", "STRING", "production | lab_low_cost"),
    ("trigger_interval", "STRING", "as configured, e.g. '1 minute'"),
    ("checkpoint_location", "STRING", "durable path; changing this replays from scratch"),
    ("status", "STRING", "STARTING | RUNNING | STOPPED | FAILED"),
    ("last_batch_id", "BIGINT", "Spark's own batch counter"),
    ("kafka_offsets", "STRING", "JSON topic -> partition -> offset, from the query progress"),
    ("source_watermark_ts", "TIMESTAMP", "max source_commit_ts committed; BUSINESS time"),
    ("last_commit_ts", "TIMESTAMP", "wall clock of the last successful commit"),
    ("target_snapshot_id", "BIGINT", "Iceberg snapshot after the last commit"),
    ("rows_last_batch", "BIGINT", ""),
    ("rows_total", "BIGINT", ""),
    ("restart_count", "INT", "how many times this app id has been started"),
    ("config_version", "STRING", "evidence, NOT part of the checkpoint identity"),
    ("error", "STRING", ""),
    ("updated_at", "TIMESTAMP", "heartbeat"),
)

STATUS_STARTING = "STARTING"
STATUS_RUNNING = "RUNNING"
STATUS_STOPPED = "STOPPED"
STATUS_FAILED = "FAILED"
STREAMING_STATUSES = (STATUS_STARTING, STATUS_RUNNING, STATUS_STOPPED, STATUS_FAILED)


@dataclass(frozen=True)
class IngestionPolicy:
    """Everything the ingest needs to know about HOW to run, resolved once."""
    mode: str
    profile: str
    trigger_interval: str
    trigger_seconds: int
    app_id: str
    checkpoint: str
    heartbeat_interval: str = DEFAULT_HEARTBEAT
    heartbeat_seconds: int = 300

    @property
    def is_continuous(self) -> bool:
        return self.mode == MODE_CONTINUOUS

    @property
    def is_production(self) -> bool:
        return self.profile == PROFILE_PRODUCTION

    def payload(self) -> dict:
        return {"mode": self.mode, "profile": self.profile,
                "trigger_interval": self.trigger_interval,
                "trigger_seconds": self.trigger_seconds,
                "heartbeat_interval": self.heartbeat_interval,
                "heartbeat_seconds": self.heartbeat_seconds,
                "app_id": self.app_id, "checkpoint": self.checkpoint}


def resolve(raw: dict | None, *, profile: str, lake_bucket: str, engine: str = "",
            what: str = "ingestion") -> IngestionPolicy:
    """Config + profile -> the resolved policy. Every default is explicit here."""
    raw = raw or {}
    if profile not in PROFILES:
        raise ConfigError(f"{what}: deployment profile {profile!r} is not one of "
                          f"{', '.join(PROFILES)}")
    mode = str(raw.get("mode") or DEFAULT_MODE_FOR_PROFILE[profile]).strip().lower()
    if mode not in INGESTION_MODES:
        raise ConfigError(f"{what}: ingestion mode {mode!r} is not one of "
                          f"{', '.join(INGESTION_MODES)}")
    trigger = str(raw.get("trigger_interval") or DEFAULT_TRIGGER)
    seconds = parse_trigger(trigger, what=what)
    heartbeat = str(raw.get("heartbeat_interval") or DEFAULT_HEARTBEAT)
    heartbeat_seconds = parse_trigger(heartbeat, what=f"{what}.heartbeat_interval",
                                      floor=MIN_HEARTBEAT_SECONDS)
    app_id = str(raw.get("app_id") or app_id_for("", engine))
    return IngestionPolicy(mode=mode, profile=profile, trigger_interval=trigger,
                           trigger_seconds=seconds, app_id=app_id,
                           checkpoint=checkpoint_path(lake_bucket, app_id),
                           heartbeat_interval=heartbeat,
                           heartbeat_seconds=heartbeat_seconds)
