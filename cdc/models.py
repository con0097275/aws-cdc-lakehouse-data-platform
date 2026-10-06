"""Typed model for the CDC capture registry.

Deliberately mirrors `spark/reporting/models.py` in shape -- frozen dataclasses, enums for
closed sets, one `ConfigError` raised at compile time -- because this repository already has
a config compiler and a second, differently-shaped one would be a second thing to learn and
a second place for drift to hide (ADR-034's argument, applied to capture).

Deliberately a SEPARATE file set from reporting. They meet at exactly one point: a reporting
job's EOD_TABLE dependency names a layer this registry ultimately feeds. Merging them would
couple capture cadence to mart scheduling, which are different decisions with different
owners (docs/CDC_TABLE_ONBOARDING_DESIGN.md section 7).
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum


class ConfigError(ValueError):
    """A configuration defect. Fatal at compile time, never at 02:00."""


class SourceEngine(str, Enum):
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"


class Classification(str, Enum):
    """Data classification. Drives IAM scope and the AI-plane deny list, so it is a closed
    set rather than a free string -- a typo in `pii` must not silently downgrade a table."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class DeletePolicy(str, Enum):
    """CLAUDE.md 5.7 requires deletes to be handled EXPLICITLY, not left ambiguous.

    The brief's vocabulary (`exclude_latest_delete`, `soft_delete`, `physical_delete`) is
    accepted as ALIASES on the way in -- see `cdc/eod.py::DELETE_ALIASES`. The project's
    spellings stay canonical because the registry, the compiled plan hashes and the
    provisioned table properties already carry them, and renaming would rewrite every config
    hash to say the same thing.
    """
    EXCLUDE_FROM_SNAPSHOT = "exclude_from_snapshot"   # active snapshot drops the key
    SOFT_FLAG = "soft_flag"                            # keep the row, _is_deleted = true
    #: Additionally removes any row for that key left by an EARLIER run. Identical to
    #: EXCLUDE under `rolling_history`, where each COB writes its own partition and no
    #: earlier row is in scope; different under `latest_state`, where one is.
    PHYSICAL_DELETE = "physical_delete"


class SchemaEvolutionPolicy(str, Enum):
    ADDITIVE_ONLY = "additive_only"   # new nullable columns absorbed; drops/type changes fail
    FULL = "full"                     # any compatible change absorbed
    FROZEN = "frozen"                 # any change fails the ingest


class PartitionTransform(str, Enum):
    """Only transforms this platform actually supports and has tested."""
    IDENTITY = "identity"
    DAY = "day"
    MONTH = "month"
    BUCKET = "bucket"


class SnapshotCutoffPolicy(str, Enum):
    EVENT_TS = "event_ts"                  # event_ts < T00:00
    SOURCE_COMMIT_TS = "source_commit_ts"  # source_commit_ts <= cutoff  (CLAUDE.md 5.6)


#: Ordering columns each engine can actually provide (CLAUDE.md 5.5). An EOD table needs one
#: of these, or "last event wins" is undefined -- see `validate_semantics`.
ORDERING_BY_ENGINE: dict[SourceEngine, tuple[str, ...]] = {
    SourceEngine.ORACLE: ("commit_scn", "scn"),
    SourceEngine.SQLSERVER: ("commit_lsn", "change_lsn", "event_serial_no"),
}


@dataclass(frozen=True)
class PartitionField:
    column: str
    transform: PartitionTransform = PartitionTransform.IDENTITY
    #: Required for BUCKET, forbidden otherwise.
    num_buckets: int | None = None

    def spec(self) -> str:
        if self.transform is PartitionTransform.BUCKET:
            return f"bucket({self.num_buckets}, {self.column})"
        if self.transform is PartitionTransform.IDENTITY:
            return self.column
        return f"{self.transform.value}({self.column})"


@dataclass(frozen=True)
class RealtimePolicy:
    """The rolling window this table serves, and how a run materialises it.

    TWO BOUNDARY MODES, and they are not interchangeable. `rolling_hours` measures back from
    the run instant, so the window holds partial days and shifts every run.
    `calendar_day` measures back from MIDNIGHT in `business_timezone`, so it holds whole days
    and every run inside a day agrees on its contents. "3 days" is not 72 hours, and the
    platform refuses to guess which one a policy meant (cdc/realtime.py).

    Hours are what every table in the registry already uses, so an hours-configured table
    keeps `rolling_hours` and behaves exactly as before. Declaring any of the `_days` fields
    switches the table to `calendar_day` unless `boundary` says otherwise.
    """
    enabled: bool = True

    #: WHICH cadence this table wants, as a 5-field cron. Empty means the platform default.
    #: Until Phase C this compiled into the plan and nothing read it (ADR-075).
    schedule_raw: str = ""
    #: `small` | `medium` | `large` -- sizing AND the Airflow pool, resolved at compile.
    resource_profile: str = ""

    # -- rolling_hours ------------------------------------------------------ #
    window_hours: int = 72
    late_grace_hours: int = 24
    #: Physical retention of the REALTIME table. MUST cover window + grace, or the layer
    #: expires rows it is still logically responsible for serving.
    retention_hours: int = 168

    # -- calendar_day ------------------------------------------------------- #
    #: Whole business days served, INCLUDING the day the run falls in: 1 means "today".
    lookback_days: int | None = None
    #: Extra days materialised below the logical bound, so an event that committed earlier
    #: but arrived late is present rather than silently absent.
    late_arrival_grace_days: int | None = None
    #: Days kept physically. Enforced >= lookback + grace (section F).
    physical_retention_days: int | None = None

    #: Which of the two above applies. Resolved by the loader; never guessed at runtime.
    boundary: str = "rolling_hours"
    # -- shape and write strategy (R2-B) ------------------------------------ #
    #: WHAT IS IN THE TABLE. `event_window` keeps the events; `latest_state` keeps one row
    #: per business key. This is the CONTRACT -- the only field a downstream reader needs
    #: in order to know whether it may read this table as current state.
    shape: str = "event_window"
    #: HOW IT IS WRITTEN. `overwrite_window` | `append` | `guarded_merge`. Changes cost and
    #: failure behaviour, never meaning. Constrained by `shape` at compile time.
    write_strategy: str = "overwrite_window"
    #: How a run finds rows it has not processed: `window_scan` (filter the whole window,
    #: what runs today) or `iceberg_snapshot` (the incremental cursor, R2-C).
    source_progress: str = "window_scan"
    #: How two events for the same key are ranked. `source_native` -- SCN/LSN, then
    #: commit ts, then partition, then offset (cdc/realtime.py::order_key_expr).
    ordering_strategy: str = "source_native"
    #: What a DELETE does to a `latest_state` row: `soft_tombstone` keeps it, flagged.
    merge_delete_policy: str = "soft_tombstone"
    #: Whether an EOD certification rebases this table's baseline (R2-F). False until then.
    rebase_on_eod_certified: bool = False
    #: Cron, for the scheduler. Carried in config so the cadence and the window it serves
    #: are declared in ONE place -- a 72h window refreshed daily is not the same product as
    #: a 72h window refreshed every ten minutes, and nothing else records the difference.
    schedule: str = ""
    #: Freshness target for the LAYER, distinct from the capture SLA in `dq`.
    sla_minutes: int = 60
    #: Partition spec override for the REALTIME target. Empty means "same as FULL_CDC".
    partition_spec: tuple = ()

    @property
    def refresh_mode(self) -> str:
        """The deprecated single-word spelling, DERIVED from shape + write_strategy.

        Kept so the Spark engine, the plan and every existing test keep reading one name
        while R2-E migrates them to `shape`. Derived rather than stored: two fields that
        must agree are two fields that eventually will not.
        """
        from .realtime import REFRESH_MODE_FOR
        return REFRESH_MODE_FOR[(self.shape, self.write_strategy)]

    @property
    def prunes_by_age(self) -> bool:
        """Whether the retention bound DELETES rows, or is only a recovery horizon.

        False for `latest_state`: a valid entity may not change for months, and dropping
        its row because its last event is old would empty the table of exactly the
        entities that are most stable.
        """
        from .realtime import prunes_by_age
        return prunes_by_age(self.shape)


@dataclass(frozen=True)
class EodPolicy:
    """How a business date is closed for this table.

    EOD is a deterministic function of FULL_CDC and a cutoff (ADR-065), so everything that
    decides WHICH events are in the close lives here: the zone the day boundary is taken in,
    which day a scheduled run closes, and which timestamp the cutoff is applied to.
    """
    enabled: bool = True
    cutoff_policy: SnapshotCutoffPolicy = SnapshotCutoffPolicy.SOURCE_COMMIT_TS
    business_timezone: str = "UTC"
    delete_policy: DeletePolicy = DeletePolicy.EXCLUDE_FROM_SNAPSHOT
    retention_days: int = 365

    #: Which business date a scheduled run closes: 1 means "close yesterday". Zero is legal
    #: and means "close today", which only makes sense for a source that is quiet by the
    #: time the job runs -- so it is expressible, not the default.
    business_date_lag_days: int = 1
    #: rolling_history (default) keeps every closed COB; latest_state keeps only the most
    #: recently certified one. See cdc/eod.py for why the default is history.
    snapshot_mode: str = "rolling_history"
    #: Cron, so the cadence and the cutoff it closes are declared in ONE place.
    schedule: str = ""
    #: `small` | `medium` | `large` -- sizing AND the Airflow pool (ADR-075).
    resource_profile: str = ""
    #: How long past the cutoff a close may WAIT for the source before it gives up
    #: and reports LATE_SOURCE. Clock time alone cannot certify (ADR-076).
    source_sla_minutes: int = 360


@dataclass(frozen=True)
class DqPolicy:
    not_null: tuple[str, ...] = ()
    #: `event_date IS NULL` is never a legitimate business state in FULL_CDC -- it is the
    #: signature of an undecodable record that slipped the quarantine filter
    #: (docs/CDC_TABLE_PLATFORM_TARGET.md section 1.3). Default zero tolerance.
    event_date_null_tolerance: int = 0
    freshness_sla_minutes: int = 60


@dataclass(frozen=True)
class MaintenancePolicy:
    compact_target_mb: int = 128
    expire_snapshots_days: int = 7
    remove_orphan_files_days: int = 3
    #: hot | warm | cold. Empty means INFER it from the table's own realtime/freshness
    #: policy (cdc/maintenance.py). Carried through the compiled plan, because a runtime
    #: consumer that fell back to the inference would silently ignore an explicit choice --
    #: and would agree with it often enough to hide that it was doing so.
    temperature: str = ""
    #: Which maintenance actions this table permits.
    #:
    #: `None` means NOT DECLARED -> the default set (everything except orphan removal, which
    #: deletes). An empty tuple means DECLARED EMPTY, which is refused at compile time.
    #:
    #: The two were the same value once (`()` for both) and the information was destroyed at
    #: load: a registry saying `actions: []` compiled clean, serialised as `null`, and the
    #: runtime handed that table the FULL default set. The user asked for no maintenance and
    #: silently got all of it -- the same silent-drop defect as `temperature`, in the same
    #: block. Absence and emptiness have to stay distinguishable to be checkable.
    actions: tuple | None = None


@dataclass(frozen=True)
class PayloadColumn:
    """One declared source column, for the typed payload representation.

    `encoding` names the DEBEZIUM wire form the value arrives in. It is not decoration: a
    temporal arrives as a plain int64 and casting it to `timestamp` yields a year-57609
    value silently (cdc/rowspec.py). `none` means the decoded value already is the declared
    type, which is correct for strings, numbers, booleans and precise decimals.
    """
    name: str
    type: str
    encoding: str = "none"


@dataclass(frozen=True)
class PayloadPolicy:
    """How the before/after images are stored (rowspec.py explains the trade).

    `json` is the current contract and the default. `typed` ADDS struct columns built from
    `columns` and keeps the JSON beside them -- a writer version can carry a field the
    registry has not declared, and a typed-only projection would drop it silently.
    """
    mode: str = "json"
    columns: tuple[PayloadColumn, ...] = ()


@dataclass(frozen=True)
class WriteProperties:
    format_version: int = 2
    compression: str = "zstd"
    target_file_size_mb: int = 128
    distribution_mode: str = "hash"


@dataclass(frozen=True)
class OnboardingPolicy:
    """How this table's EXISTING rows are brought in when capture starts (ADR-066).

    Default `changes_only` describes what the platform does today, not what is best:
    both connectors run `snapshot.mode: initial`, which snapshots only on first start
    against an empty offset, so adding a table to a running connector captures it from the
    restart point forward. `incremental_snapshot` is the better answer and is REFUSED by the
    precheck until a connector configures `signal.data.collection`.
    """
    mode: str = "changes_only"
    #: Set once a backfill has actually been done by another route, so the warning about
    #: invisible history stops firing for a table where it is no longer true.
    backfilled: bool = False


@dataclass(frozen=True)
class TableConfig:
    """One captured source table, AFTER inheritance is resolved."""
    source_id: str
    engine: SourceEngine
    database: str
    schema: str
    table: str

    primary_key: tuple[str, ...]
    owner: str
    classification: Classification
    domain: str

    connector: str
    topic: str
    full_cdc_table: str
    realtime_table: str
    eod_table: str

    partition_spec: tuple[PartitionField, ...]
    write: WriteProperties
    realtime: RealtimePolicy
    eod: EodPolicy
    dq: DqPolicy
    maintenance: MaintenancePolicy
    schema_evolution: SchemaEvolutionPolicy

    #: How the before/after images are stored. Defaulted, so every table that does not
    #: declare columns keeps the JSON contract it has today.
    payload: PayloadPolicy = PayloadPolicy()
    onboarding: OnboardingPolicy = OnboardingPolicy()
    enabled: bool = True
    #: Set when the source genuinely has no PK (append-only event log). Must be explicit --
    #: an EMPTY primary_key with this False is a defect, not a shrug.
    primary_key_unsupported: bool = False
    ordering_columns: tuple[str, ...] = ()

    @property
    def table_id(self) -> str:
        """Canonical identity: source_id.database.schema.table (section B)."""
        return f"{self.source_id}.{self.database}.{self.schema}.{self.table}"


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    engine: SourceEngine
    connector: str
    database: str
    schema: str
    topic_prefix: str
    tables: tuple[TableConfig, ...] = ()


@dataclass(frozen=True)
class LoadedCdcConfig:
    environment: str
    sources: tuple[SourceConfig, ...]
    #: The raw `ingestion:` block, validated at load and resolved per engine at plan time
    #: (ADR-073). Kept raw here because the resolved policy needs the lake bucket, which is
    #: a deployment fact rather than a registry one.
    ingestion: dict = field(default_factory=dict)
    #: Deprecated spellings found in the registry, one message each. Carried on the config
    #: rather than printed by the loader so a test can assert on them and a caller that
    #: loads twice does not print them twice. `cdc/compile.py` is what shows them.
    deprecations: tuple = ()

    @property
    def tables(self) -> tuple[TableConfig, ...]:
        return tuple(t for s in self.sources for t in s.tables)

    def enabled_tables(self) -> tuple[TableConfig, ...]:
        return tuple(t for t in self.tables if t.enabled)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigError(message)
