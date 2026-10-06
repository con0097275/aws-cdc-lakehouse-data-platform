"""Load `cdc/registry/sources.yaml`, apply inheritance, validate semantics.

PRECEDENCE (section D), lowest to highest:

    global defaults  <  source defaults  <  table overrides

Each level is a partial mapping; `_merge` walks them in order so a table only states what it
actually differs on. That is what keeps a new table to a handful of lines instead of a
hundred-line copy of its neighbour -- and a copy is where drift starts.
"""
from __future__ import annotations

import zoneinfo
from dataclasses import replace
from pathlib import Path
from typing import Any

import yaml

from . import connector_plan, eod, naming, realtime, rowspec
from .models import (Classification, ConfigError, DeletePolicy, DqPolicy, EodPolicy,
                    LoadedCdcConfig, MaintenancePolicy, ORDERING_BY_ENGINE,
                    PartitionField, PartitionTransform, PayloadColumn, PayloadPolicy,
                    RealtimePolicy, SchemaEvolutionPolicy, SnapshotCutoffPolicy,
                    OnboardingPolicy, SourceConfig, SourceEngine, TableConfig,
                    WriteProperties, _require)

MAX_BUCKETS = 4096

#: Columns of the canonical row a partition field may name. Deliberately a SHORT list: a
#: partition column must be low-cardinality or a transform of a temporal column, and every
#: other column of the row is neither.
PARTITIONABLE_COLUMNS = frozenset({"event_date", "source_commit_ts", "kafka_timestamp",
                                   "op", "dv_pk_hash"})
TEMPORAL_COLUMNS = frozenset({"event_date", "source_commit_ts", "kafka_timestamp"})
#: Constant within a per-table target, so partitioning by them creates one partition value
#: and no pruning. The MONOLITH partitions by these because it holds eight source tables.
REDUNDANT_PARTITION_COLUMNS = frozenset({"source_system", "source_database",
                                         "source_schema", "source_table"})


def _merge(*layers: dict | None) -> dict:
    """Shallow-merge per top-level key, deep-merge one level into nested mappings.

    One level deep is deliberate. `realtime: {window_hours: 96}` on a table must inherit the
    other realtime fields from the source, but a LIST (partition_spec, not_null) is replaced
    wholesale -- merging lists element-wise would make "clear this list" unexpressible.
    """
    out: dict[str, Any] = {}
    for layer in layers:
        for k, v in (layer or {}).items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = {**out[k], **v}
            else:
                out[k] = v
    return out


def _enum(cls, value, *, what: str):
    try:
        return cls(value)
    except ValueError:
        allowed = ", ".join(sorted(m.value for m in cls))
        raise ConfigError(f"{what}: {value!r} is not valid. Allowed: {allowed}") from None


def _partition_spec(raw: list | None, *, what: str) -> tuple[PartitionField, ...]:
    out: list[PartitionField] = []
    for i, item in enumerate(raw or []):
        if not isinstance(item, dict) or "column" not in item:
            raise ConfigError(f"{what}[{i}]: each partition field needs a `column`")
        tr = _enum(PartitionTransform, item.get("transform", "identity"),
                   what=f"{what}[{i}].transform")
        n = item.get("num_buckets")
        if tr is PartitionTransform.BUCKET:
            if not isinstance(n, int) or n < 1 or n > MAX_BUCKETS:
                raise ConfigError(
                    f"{what}[{i}]: bucket() needs num_buckets in 1..{MAX_BUCKETS}, got {n!r}")
        elif n is not None:
            raise ConfigError(
                f"{what}[{i}]: num_buckets is only valid with transform=bucket, "
                f"not {tr.value}")
        out.append(PartitionField(column=item["column"], transform=tr, num_buckets=n))
    return tuple(out)


def _payload(raw: dict | None, *, what: str) -> PayloadPolicy:
    """`payload: {mode: json|typed, columns: [{name, type}]}`.

    Types and names are normalised HERE rather than at DDL time, so a bad type is a compile
    error naming the table and not an unreadable Glue table with data already in it.
    """
    raw = raw or {}
    mode = raw.get("mode", "json")
    if mode not in rowspec.PAYLOAD_MODES:
        raise ConfigError(f"{what}.mode: {mode!r} is not one of "
                          f"{', '.join(rowspec.PAYLOAD_MODES)}")
    cols: list[PayloadColumn] = []
    for i, c in enumerate(raw.get("columns") or []):
        if not isinstance(c, dict):
            raise ConfigError(f"{what}.columns[{i}]: expected a mapping with name and type")
        col_type = rowspec.normalise_type(c.get("type"),
                                          what=f"{what}.columns[{i}].type")
        encoding = str(c.get("encoding", rowspec.DEFAULT_ENCODING)).strip().lower()
        rowspec.validate_encoding(col_type, encoding, what=f"{what}.columns[{i}]")
        cols.append(PayloadColumn(
            name=rowspec.normalise_column_name(c.get("name"),
                                               what=f"{what}.columns[{i}].name"),
            type=col_type, encoding=encoding))
    names = [c.name for c in cols]
    if len(set(names)) != len(names):
        raise ConfigError(f"{what}.columns: duplicate column names {sorted(names)}")
    return PayloadPolicy(mode=mode, columns=tuple(cols))


#: Declaring any of these means the policy is written in DAYS, so the boundary becomes
#: calendar_day unless `boundary` says otherwise. An hours-only table is untouched, which is
#: what makes this change a no-op for every table already in the registry.
_DAY_FIELDS = ("lookback_days", "late_arrival_grace_days", "physical_retention_days")


#: The two spellings of the same decision. They INHERIT as a group: a level that declares
#: either one replaces whatever a less specific level said, in whichever spelling it said it.
_SHAPE_SPELLING_LEGACY = ("refresh_mode",)
_SHAPE_SPELLING_CURRENT = ("shape", "write_strategy")


def _reconcile_shape_spelling(rt_raw: dict, levels: list, *, what: str) -> dict:
    """Let the MOST SPECIFIC level decide which spelling applies, and drop the other.

    Without this, putting `shape: event_window` in the registry's `defaults:` -- which is
    the point of R2-B -- would break every table still written as `refresh_mode:`. The
    merge would hand both to the parser, and the table would be refused for a contradiction
    it did not write: its own `refresh_mode` against an inherited `shape`.

    Inheritance means the specific level wins. So a table that says `refresh_mode` overrides
    an inherited `shape`, and a table that says `shape` overrides an inherited
    `refresh_mode`. Declaring both at the SAME level is still a contradiction, and still
    refused -- there the author really did say one thing twice.
    """
    winner = None
    for level, label in levels:
        if not isinstance(level, dict):
            continue
        rt = level.get("realtime")
        if not isinstance(rt, dict):
            continue
        has_legacy = any(k in rt for k in _SHAPE_SPELLING_LEGACY)
        has_current = any(k in rt for k in _SHAPE_SPELLING_CURRENT)
        if has_legacy and has_current:
            raise ConfigError(
                f"{what}: `refresh_mode` and `shape`/`write_strategy` are both declared in "
                f"{label}. They are two spellings of one decision; `refresh_mode` is the "
                f"deprecated one. Remove it.")
        if has_legacy:
            winner = "legacy"
        elif has_current:
            winner = "current"
    if winner == "legacy":
        return {k: v for k, v in rt_raw.items() if k not in _SHAPE_SPELLING_CURRENT}
    if winner == "current":
        return {k: v for k, v in rt_raw.items() if k not in _SHAPE_SPELLING_LEGACY}
    return rt_raw


def _realtime(raw: dict, *, what: str, warn: list | None = None) -> RealtimePolicy:
    """Resolve the realtime policy, INCLUDING which boundary mode it is written in.

    The mode is decided here, once, rather than inferred at runtime from which fields happen
    to be non-null -- an inference that would silently change a table's window the day
    someone added a `lookback_days` alongside the hours it already had.

    R2-B: the day fields may also be written under a `recovery:` block, which is what they
    have always MEANT -- how far back a run reads to repair itself, not how far back it
    reads normally. Both spellings resolve to the same fields; declaring the same field in
    both places is refused rather than resolved by precedence.
    """
    raw = raw or {}
    recovery = raw.get("recovery") or {}
    if not isinstance(recovery, dict):
        raise ConfigError(f"{what}.recovery: expected a mapping")
    overlap = sorted(set(_DAY_FIELDS) & {k for k in recovery if raw.get(k) is not None})
    if overlap:
        raise ConfigError(
            f"{what}: {', '.join(overlap)} declared both at the top level and under "
            f"`recovery:`. One of the two would be ignored, and nothing would say which.")
    # `recovery:` is a SPELLING of the same fields, so flatten it before anything reads them.
    raw = {**raw, **{k: v for k, v in recovery.items() if k in _DAY_FIELDS}}
    unknown_recovery = sorted(set(recovery) - set(_DAY_FIELDS))
    if unknown_recovery:
        raise ConfigError(f"{what}.recovery: unknown field(s) {', '.join(unknown_recovery)}. "
                          f"Known: {', '.join(_DAY_FIELDS)}")

    day_fields = [f for f in _DAY_FIELDS if raw.get(f) is not None]
    default_boundary = (realtime.BOUNDARY_CALENDAR_DAY if day_fields
                        else realtime.BOUNDARY_ROLLING_HOURS)
    boundary = raw.get("boundary", default_boundary)
    if boundary not in realtime.BOUNDARIES:
        raise ConfigError(f"{what}.boundary: {boundary!r} is not one of "
                          f"{', '.join(realtime.BOUNDARIES)}")

    shape, write_strategy, deprecation = realtime.resolve_shape(raw, what=what)
    if deprecation and warn is not None:
        warn.append(deprecation)

    # -- processing: how a run finds the rows it has not handled yet -------- #
    processing = raw.get("processing") or {}
    if not isinstance(processing, dict):
        raise ConfigError(f"{what}.processing: expected a mapping")
    unknown = sorted(set(processing) - {"source_progress"})
    if unknown:
        raise ConfigError(f"{what}.processing: unknown field(s) {', '.join(unknown)}. "
                          f"Known: source_progress")
    source_progress = str(processing.get("source_progress",
                                         realtime.DEFAULT_SOURCE_PROGRESS)).strip().lower()
    if source_progress not in realtime.SOURCE_PROGRESS_MODES:
        raise ConfigError(f"{what}.processing.source_progress: {source_progress!r} is not "
                          f"one of {', '.join(realtime.SOURCE_PROGRESS_MODES)}")
    # R2-C implemented the cursor, so the "not implemented yet" refusal that stood here is
    # gone -- as ADR-082 said it would be. What replaces it is narrower and permanent.
    if (source_progress == realtime.PROGRESS_ICEBERG_SNAPSHOT
            and write_strategy == realtime.WRITE_OVERWRITE_WINDOW):
        raise ConfigError(
            f"{what}: `source_progress: {realtime.PROGRESS_ICEBERG_SNAPSHOT}` cannot be "
            f"combined with `write_strategy: {realtime.WRITE_OVERWRITE_WINDOW}`. An "
            f"overwrite REPLACES the target's entire contents with what the run produced, "
            f"so feeding it an incremental delta would delete every row the delta does not "
            f"contain -- the table would shrink to the last few minutes of events and the "
            f"job would report SUCCESS. Use `write_strategy: {realtime.WRITE_APPEND}` "
            f"(event_window) or `{realtime.WRITE_GUARDED_MERGE}` (latest_state).")

    # -- merge: how a latest_state row is chosen and how a delete lands ----- #
    merge = raw.get("merge") or {}
    if not isinstance(merge, dict):
        raise ConfigError(f"{what}.merge: expected a mapping")
    unknown = sorted(set(merge) - {"strategy", "ordering_strategy", "delete_policy"})
    if unknown:
        raise ConfigError(f"{what}.merge: unknown field(s) {', '.join(unknown)}. "
                          f"Known: strategy, ordering_strategy, delete_policy")
    # `merge.strategy: guarded` is the brief's spelling of `write_strategy: guarded_merge`.
    merge_strategy = merge.get("strategy")
    if merge_strategy is not None:
        if str(merge_strategy).strip().lower() != "guarded":
            raise ConfigError(f"{what}.merge.strategy: {merge_strategy!r} is not "
                              f"supported; the only guard is 'guarded'")
        if write_strategy != realtime.WRITE_GUARDED_MERGE:
            raise ConfigError(
                f"{what}: `merge.strategy: guarded` contradicts "
                f"`write_strategy: {write_strategy}`. A guard only exists in "
                f"{realtime.WRITE_GUARDED_MERGE!r}.")
    ordering_strategy = str(merge.get("ordering_strategy",
                                      realtime.ORDERING_SOURCE_NATIVE)).strip().lower()
    if ordering_strategy not in realtime.ORDERING_STRATEGIES:
        raise ConfigError(f"{what}.merge.ordering_strategy: {ordering_strategy!r} is not "
                          f"one of {', '.join(realtime.ORDERING_STRATEGIES)}")
    merge_delete = str(merge.get("delete_policy",
                                 realtime.MERGE_DELETE_SOFT_TOMBSTONE)).strip().lower()
    if merge_delete not in realtime.MERGE_DELETE_POLICIES:
        raise ConfigError(f"{what}.merge.delete_policy: {merge_delete!r} is not one of "
                          f"{', '.join(realtime.MERGE_DELETE_POLICIES)}")

    # -- rebase: R2-F ------------------------------------------------------- #
    rebase = raw.get("rebase") or {}
    if not isinstance(rebase, dict):
        raise ConfigError(f"{what}.rebase: expected a mapping")
    unknown = sorted(set(rebase) - {"on_eod_certified"})
    if unknown:
        raise ConfigError(f"{what}.rebase: unknown field(s) {', '.join(unknown)}. "
                          f"Known: on_eod_certified")
    # R2-F implemented the rebase, so the "not implemented yet" refusal is gone. What
    # replaces it is the one combination that has no meaning: an EVENT WINDOW has no
    # baseline to rebase onto. It keeps events, so a consumer composing it with EOD would
    # be double-counting by construction rather than by staleness, and a rebase would
    # silently delete events the layer promised to serve.
    rebase_on_certified = bool(rebase.get("on_eod_certified", False))
    if rebase_on_certified and shape != realtime.SHAPE_LATEST_STATE:
        raise ConfigError(
            f"{what}.rebase.on_eod_certified: only `shape: "
            f"{realtime.SHAPE_LATEST_STATE}` has a baseline to rebase. An "
            f"{realtime.SHAPE_EVENT_WINDOW} keeps EVENTS, so there is nothing a certified "
            f"close makes redundant -- a rebase would delete events the layer promises to "
            f"serve, and report SUCCESS.")

    def _day(name: str, default: int) -> int | None:
        v = raw.get(name)
        return default if v is None else int(v)

    calendar = boundary == realtime.BOUNDARY_CALENDAR_DAY
    return RealtimePolicy(
        enabled=bool(raw.get("enabled", True)),
        window_hours=int(raw.get("window_hours", 72)),
        late_grace_hours=int(raw.get("late_grace_hours", 24)),
        retention_hours=int(raw.get("retention_hours", 168)),
        # Defaulted only in calendar mode. Leaving them None in hours mode keeps "this table
        # is not day-configured" expressible, which the plan and the plan command both show.
        lookback_days=_day("lookback_days", 3) if calendar else raw.get("lookback_days"),
        late_arrival_grace_days=(_day("late_arrival_grace_days", 1) if calendar
                                 else raw.get("late_arrival_grace_days")),
        physical_retention_days=(_day("physical_retention_days", 7) if calendar
                                 else raw.get("physical_retention_days")),
        boundary=boundary,
        shape=shape,
        write_strategy=write_strategy,
        source_progress=source_progress,
        ordering_strategy=ordering_strategy,
        merge_delete_policy=merge_delete,
        rebase_on_eod_certified=rebase_on_certified,
        # Validated HERE so an unreadable cron costs a red terminal, not a DAG that fails
        # to import and vanishes from the UI.
        schedule=realtime.normalise_schedule(raw.get("schedule"), what=what),
        schedule_raw=str(raw.get("schedule", "") or ""),
        resource_profile=realtime.resolve_resource_profile(
            raw.get("resource_profile"), what=what)["name"],
        sla_minutes=int(raw.get("sla_minutes", 60)),
        partition_spec=_partition_spec(raw.get("partition_spec"),
                                       what=f"{what}.partition_spec"))


def _realtime_checked(rt_raw: dict, eod_raw: dict, *, what: str,
                      primary_key: tuple = (), primary_key_unsupported: bool = False,
                      warn: list | None = None) -> RealtimePolicy:
    """The realtime policy, cross-checked against the things it depends on elsewhere.

    Two cross-field rules, both about `shape: latest_state`, and both refused at COMPILE
    rather than discovered in a mart.

    1. IT NEEDS A USABLE BUSINESS KEY (R2 brief §10). "One row per key" is meaningless
       without a key. A table with no declared PK, or one whose PK the source cannot
       supply (`primary_key_unsupported`), would collapse every event onto the same
       `dv_pk_hash` -- the job would succeed and the table would hold one row.

    2. IT NEEDS `delete_policy: soft_flag`. `latest_state` keeps a deleted key as a
       TOMBSTONE between runs. It has to: physically removing the row leaves nothing for
       the next batch's order-key guard to compare against, so a late out-of-order event
       RESURRECTS a key the source deleted.

       That makes `latest_state` and `delete_policy: exclude_from_snapshot` contradictory --
       one says "a deleted key is retained, flagged", the other says "a deleted key is
       excluded". Allowing the pair would put tombstones into a layer whose readers,
       correctly, do not filter them today, and a deleted row would reach a mart.
    """
    policy = _realtime(rt_raw, what=f"{what}.realtime", warn=warn)
    if policy.shape != realtime.SHAPE_LATEST_STATE:
        return policy

    if primary_key_unsupported:
        raise ConfigError(
            f"{what}: shape 'latest_state' needs a business key, but this table declares "
            f"`primary_key_unsupported: true`. Every event would hash to the same key and "
            f"the target would hold one row. Use shape "
            f"'{realtime.SHAPE_EVENT_WINDOW}' for a table without a usable key.")
    if not primary_key:
        raise ConfigError(
            f"{what}: shape 'latest_state' needs `primary_key`, and none is declared. "
            f"'One row per business key' has no meaning without the key; the run would "
            f"succeed and materialise a single row.")
    if len(set(primary_key)) != len(primary_key):
        raise ConfigError(
            f"{what}: shape 'latest_state' needs a primary key with no repeated column; "
            f"got {list(primary_key)}")

    delete_policy = str(eod_raw.get("delete_policy", "exclude_from_snapshot"))
    if delete_policy != eod.DELETE_SOFT:
        raise ConfigError(
            f"{what}: shape 'latest_state' needs delete_policy '{eod.DELETE_SOFT}', "
            f"got {delete_policy!r}. latest_state keeps a deleted key as a tombstone "
            f"between runs -- removing it would let a late out-of-order event resurrect "
            f"the key -- so the table's readers must expect flagged deletes. Set "
            f"`eod: {{delete_policy: {eod.DELETE_SOFT}}}` on this table, or use shape "
            f"'{realtime.SHAPE_EVENT_WINDOW}'.")
    return policy


def _eod(raw: dict, *, what: str) -> EodPolicy:
    """Resolve the EOD policy, normalising the delete-policy vocabulary on the way in."""
    raw = raw or {}
    mode = str(raw.get("snapshot_mode", eod.DEFAULT_SNAPSHOT_MODE)).strip().lower()
    if mode not in eod.SNAPSHOT_MODES:
        raise ConfigError(f"{what}.snapshot_mode: {mode!r} is not one of "
                          f"{', '.join(eod.SNAPSHOT_MODES)}")
    lag = int(raw.get("business_date_lag_days", 1))
    if lag < 0:
        raise ConfigError(f"{what}.business_date_lag_days must be >= 0, got {lag}. A "
                          f"negative lag would close a business date that has not happened")
    return EodPolicy(
        enabled=bool(raw.get("enabled", True)),
        cutoff_policy=_enum(SnapshotCutoffPolicy,
                            raw.get("cutoff_policy", "source_commit_ts"),
                            what=f"{what}.cutoff_policy"),
        business_timezone=raw.get("business_timezone", "UTC"),
        delete_policy=DeletePolicy(eod.normalise_delete_policy(
            raw.get("delete_policy", "exclude_from_snapshot"),
            what=f"{what}.delete_policy")),
        retention_days=int(raw.get("retention_days", 365)),
        business_date_lag_days=lag,
        snapshot_mode=mode,
        # Validated at COMPILE: an unreadable cron discovered by Airflow is a DAG that fails
        # to import, and such a DAG disappears from the UI (ADR-075/076).
        schedule=eod.normalise_eod_schedule(raw.get("schedule"), what=f"{what}.schedule"),
        resource_profile=realtime.resolve_resource_profile(
            raw.get("resource_profile"), what=f"{what}.resource_profile")["name"],
        source_sla_minutes=int(raw.get("source_sla_minutes",
                                       eod.DEFAULT_SOURCE_SLA_MINUTES)))


def _build_table(raw: dict, *, src: dict, glob: dict, source_id: str,
                 engine: SourceEngine, where: str,
                 warn: list | None = None) -> TableConfig:
    m = _merge(glob, src, raw)
    for req in ("table", "owner"):
        _require(bool(m.get(req)), f"{where}: `{req}` is required")

    database = m.get("database") or src.get("database") or ""
    schema = m.get("schema") or src.get("schema") or ""
    table = m["table"]
    _require(bool(database), f"{where}: `database` must come from the source or the table")
    _require(bool(schema), f"{where}: `schema` must come from the source or the table")

    pk = tuple(m.get("primary_key") or ())
    pk_unsupported = bool(m.get("primary_key_unsupported", False))

    rt_raw = _reconcile_shape_spelling(
        m.get("realtime") or {},
        [(glob, "the registry defaults"), (src, "the source defaults"),
         (raw, "this table")],
        what=where)
    eod_raw = m.get("eod") or {}
    dq_raw = m.get("dq") or {}
    mt_raw = m.get("maintenance") or {}
    wr_raw = m.get("write") or {}

    # Target names are DERIVED, never spelled out -- unless an override is explicitly given,
    # which exists for migrating a table that already has a name in the catalog.
    ov = m.get("target") or {}
    args = (source_id, database, schema, table)

    return TableConfig(
        source_id=naming.normalize_ident(source_id, what=f"{where}.source_id"),
        engine=engine,
        database=naming.normalize_ident(database, what=f"{where}.database"),
        schema=naming.normalize_ident(schema, what=f"{where}.schema"),
        table=naming.normalize_ident(table, what=f"{where}.table"),
        primary_key=pk,
        owner=m["owner"],
        classification=_enum(Classification, m.get("classification", "internal"),
                             what=f"{where}.classification"),
        domain=m.get("domain", "unassigned"),
        connector=src.get("connector", ""),
        topic=ov.get("topic") or naming.topic_for(
            engine, topic_prefix=src.get("topic_prefix", ""),
            database=database, schema=schema, table=table),
        full_cdc_table=ov.get("full_cdc") or naming.full_cdc_table(*args),
        realtime_table=ov.get("realtime") or naming.realtime_table(*args),
        eod_table=ov.get("eod") or naming.eod_table(*args),
        partition_spec=_partition_spec(m.get("partition_spec"),
                                       what=f"{where}.partition_spec"),
        write=WriteProperties(
            format_version=int(wr_raw.get("format_version", 2)),
            compression=wr_raw.get("compression", "zstd"),
            target_file_size_mb=int(wr_raw.get("target_file_size_mb", 128)),
            distribution_mode=wr_raw.get("distribution_mode", "hash")),
        realtime=_realtime_checked(rt_raw, eod_raw, what=where,
                                   primary_key=pk,
                                   primary_key_unsupported=pk_unsupported,
                                   warn=warn),
        eod=_eod(eod_raw, what=f"{where}.eod"),
        dq=DqPolicy(
            not_null=tuple(dq_raw.get("not_null") or ()),
            event_date_null_tolerance=int(dq_raw.get("event_date_null_tolerance", 0)),
            freshness_sla_minutes=int(dq_raw.get("freshness_sla_minutes", 60))),
        maintenance=MaintenancePolicy(
            compact_target_mb=int(mt_raw.get("compact_target_mb", 128)),
            expire_snapshots_days=int(mt_raw.get("expire_snapshots_days", 7)),
            remove_orphan_files_days=int(mt_raw.get("remove_orphan_files_days", 3)),
            temperature=str(mt_raw.get("temperature", "") or "").strip().lower(),
            # `None` when the key is absent; a tuple (possibly empty) when it is present.
            # `.get(...) or ()` collapsed those two into one value and lost the difference.
            actions=(None if "actions" not in mt_raw
                     else tuple(mt_raw.get("actions") or ()))),
        schema_evolution=_enum(SchemaEvolutionPolicy,
                               m.get("schema_evolution", "additive_only"),
                               what=f"{where}.schema_evolution"),
        payload=_payload(m.get("payload"), what=f"{where}.payload"),
        onboarding=OnboardingPolicy(
            mode=connector_plan.normalise_mode(
                (m.get("onboarding") or {}).get("mode",
                                                connector_plan.DEFAULT_ONBOARDING_MODE),
                what=f"{where}.onboarding.mode"),
            backfilled=bool((m.get("onboarding") or {}).get("backfilled", False))),
        enabled=bool(m.get("enabled", True)),
        primary_key_unsupported=pk_unsupported,
        # PRESENCE, not truthiness. `ordering_columns: []` must mean "this source provides
        # no ordering column" -- which is exactly what the EOD validation needs to catch.
        # `m.get(...) or DEFAULT` would silently replace an explicit empty list with the
        # engine default and let an unorderable table enable EOD.
        ordering_columns=tuple(m["ordering_columns"] if "ordering_columns" in m
                               else ORDERING_BY_ENGINE.get(engine, ())),
    )


def _validate_ingestion(raw: dict, *, what: str) -> None:
    """Fail at COMPILE time, not when a streaming app is already holding capacity.

    An unreadable trigger or an unknown profile discovered at submit time costs an EMR
    acquisition and a confusing stack trace; discovered here it costs a red terminal.
    """
    from . import ingestion as _ing
    profile = str(raw.get("profile") or _ing.PROFILE_LAB).strip().lower()
    _require(profile in _ing.PROFILES,
             f"{what}.profile: {profile!r} is not one of {', '.join(_ing.PROFILES)}")
    if raw.get("mode"):
        mode = str(raw["mode"]).strip().lower()
        _require(mode in _ing.INGESTION_MODES,
                 f"{what}.mode: {mode!r} is not one of {', '.join(_ing.INGESTION_MODES)}")
    # parse_trigger raises ConfigError with its own message, which is more specific than
    # anything _require could say here.
    _ing.parse_trigger(raw.get("trigger_interval") or _ing.DEFAULT_TRIGGER, what=what)
    _ing.parse_trigger(raw.get("heartbeat_interval") or _ing.DEFAULT_HEARTBEAT,
                       what=f"{what}.heartbeat_interval",
                       floor=_ing.MIN_HEARTBEAT_SECONDS)
    # An unknown key here is a TYPO with consequences: `trigger: "5 minutes"` (no `_interval`)
    # loads clean, resolves to the 1-minute default and costs five times the commits the
    # author asked for -- silently, because every value present is valid.
    known = {"profile", "mode", "trigger_interval", "heartbeat_interval", "app_id"}
    unknown = sorted(set(raw) - known)
    _require(not unknown,
             f"{what}: unknown key(s) {', '.join(unknown)}; expected one of "
             f"{', '.join(sorted(known))}")


def load_config(path: Path, *, environment: str = "dev") -> LoadedCdcConfig:
    if not path.exists():
        raise ConfigError(f"{path}: registry not found")
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top level must be a mapping")

    # Deprecations are COLLECTED, not printed from in here. A pure loader that wrote to
    # stderr would make the warning invisible to a test and duplicated by every caller that
    # loads the registry twice.
    warn: list[str] = []

    glob = raw.get("defaults") or {}
    sources_raw = raw.get("sources")
    _require(isinstance(sources_raw, list) and bool(sources_raw),
             f"{path}: `sources` must be a non-empty list")

    sources: list[SourceConfig] = []
    for si, s in enumerate(sources_raw):
        where_s = f"{path}: sources[{si}]"
        for req in ("source_id", "engine", "connector"):
            _require(bool(s.get(req)), f"{where_s}: `{req}` is required")
        engine = _enum(SourceEngine, s["engine"], what=f"{where_s}.engine")
        src_defaults = s.get("defaults") or {}
        # The source's own identity fields are inheritable defaults for its tables.
        src_layer = _merge(src_defaults, {k: s[k] for k in ("database", "schema")
                                          if k in s})
        tables = tuple(
            _build_table(t, src={**s, **src_layer}, glob=glob, source_id=s["source_id"],
                         engine=engine, warn=warn,
                         where=f"{where_s}.tables[{ti}] ({t.get('table', '?')})")
            for ti, t in enumerate(s.get("tables") or []))
        sources.append(SourceConfig(
            source_id=naming.normalize_ident(s["source_id"], what=f"{where_s}.source_id"),
            engine=engine, connector=s["connector"],
            database=s.get("database", ""), schema=s.get("schema", ""),
            topic_prefix=s.get("topic_prefix", ""), tables=tables))

    ing = raw.get("ingestion") or {}
    if not isinstance(ing, dict):
        raise ConfigError(f"{path}: `ingestion` must be a mapping, got {type(ing).__name__}")
    _validate_ingestion(ing, what=f"{path}: ingestion")
    config = LoadedCdcConfig(environment=environment, sources=tuple(sources),
                             ingestion=ing, deprecations=tuple(dict.fromkeys(warn)))
    validate_semantics(config)
    return config


def validate_semantics(config: LoadedCdcConfig) -> None:
    """Section E. Every check here is one a compile can make and a 02:00 run cannot."""
    seen_ids: dict[str, str] = {}
    seen_targets: dict[str, str] = {}
    source_ids = {s.source_id for s in config.sources}

    for t in config.tables:
        tid = t.table_id

        # source exists / table id unique
        _require(t.source_id in source_ids,
                 f"{tid}: source {t.source_id!r} is not declared")
        if tid in seen_ids:
            raise ConfigError(f"duplicate table id {tid!r} (already defined once)")
        seen_ids[tid] = tid

        # target name collision -- two different source tables must never derive or
        # override to the same physical table, or one silently overwrites the other
        for layer, name in (("full_cdc", t.full_cdc_table),
                            ("realtime", t.realtime_table),
                            ("eod", t.eod_table)):
            key = f"{layer}:{name}"
            if key in seen_targets and seen_targets[key] != tid:
                raise ConfigError(
                    f"{tid}: {layer} target {name!r} collides with {seen_targets[key]!r}")
            seen_targets[key] = tid

        # PK non-empty unless explicitly unsupported
        if not t.primary_key:
            _require(t.primary_key_unsupported,
                     f"{tid}: `primary_key` is empty. Set it, or set "
                     f"`primary_key_unsupported: true` to state that the source genuinely "
                     f"has none -- an empty PK also means no message.key.columns entry, "
                     f"which scatters the key across partitions (CLAUDE.md 5.1)")
        else:
            _require(not t.primary_key_unsupported,
                     f"{tid}: primary_key_unsupported is true but a primary_key is set")
            _require(len(set(t.primary_key)) == len(t.primary_key),
                     f"{tid}: primary_key has duplicate columns: {t.primary_key}")

        # owner required
        _require(bool(t.owner and t.owner.strip()), f"{tid}: `owner` is required")

        # EOD needs usable ordering AND a key to dedup on
        if t.eod.enabled:
            _require(bool(t.ordering_columns),
                     f"{tid}: EOD is enabled but no ordering columns are available for "
                     f"engine {t.engine.value}. 'Last event wins' is undefined without "
                     f"one (CLAUDE.md 5.5)")
            _require(bool(t.primary_key),
                     f"{tid}: EOD is enabled but there is no primary key to dedup on "
                     f"(CLAUDE.md 5.6)")
            try:
                zoneinfo.ZoneInfo(t.eod.business_timezone)
            except Exception:
                raise ConfigError(
                    f"{tid}: eod.business_timezone {t.eod.business_timezone!r} is not a "
                    f"valid IANA zone. A wrong zone moves the day boundary and certifies "
                    f"a different set of events than the cutoff names (ADR-024)") from None
            _require(t.eod.retention_days > 0,
                     f"{tid}: eod.retention_days must be positive, got {t.eod.retention_days}")
            # Section D. An engine with no deterministic ordering cannot answer "which event
            # won", so EOD for it is undefined -- and undefined here means a snapshot that
            # silently picks an arbitrary event per key and looks entirely normal.
            try:
                eod.ordering_for(t.engine.value)
            except ConfigError as exc:
                raise ConfigError(f"{tid}: EOD is enabled but {exc}") from None
            # The PK must be usable as a grain, not merely present.
            _require(len(set(t.primary_key)) == len(t.primary_key),
                     f"{tid}: EOD needs a primary key with no repeated column; got "
                     f"{t.primary_key}")
            # NO typed-payload PK check here: the one below fires for EVERY typed table,
            # enabled EOD or not, so it is strictly stronger. A second check saying the same
            # thing would only shadow the first one's message.

        # REALTIME retention must cover window + late grace, in whichever unit the policy
        # is written in (section F). Checking only the hours fields would leave a
        # day-configured table unvalidated while APPEARING to be checked -- the hours
        # defaults are always present, so the assertion would pass against numbers the
        # table does not use.
        rt = t.realtime
        if rt.enabled:
            if rt.boundary == realtime.BOUNDARY_CALENDAR_DAY:
                _require(rt.lookback_days is not None and rt.lookback_days > 0,
                         f"{tid}: realtime.lookback_days must be a positive number of "
                         f"whole business days")
                _require(rt.late_arrival_grace_days is not None
                         and rt.late_arrival_grace_days >= 0,
                         f"{tid}: realtime.late_arrival_grace_days must be >= 0")
                need = rt.lookback_days + rt.late_arrival_grace_days
                _require(rt.physical_retention_days is not None
                         and rt.physical_retention_days >= need,
                         f"{tid}: realtime.physical_retention_days="
                         f"{rt.physical_retention_days} is below lookback_days + "
                         f"late_arrival_grace_days = {need}. The prune would delete rows "
                         f"the same run just materialised, and the layer would expire data "
                         f"it is still responsible for serving (section F)")
                try:
                    zoneinfo.ZoneInfo(t.eod.business_timezone)
                except Exception:
                    raise ConfigError(
                        f"{tid}: realtime boundary is calendar_day but business_timezone "
                        f"{t.eod.business_timezone!r} is not a valid IANA zone. The day "
                        f"boundary is taken in that zone, so a wrong one materialises a "
                        f"different set of events than the policy names") from None
            else:
                need = rt.window_hours + rt.late_grace_hours
                _require(rt.retention_hours >= need,
                         f"{tid}: realtime.retention_hours={rt.retention_hours} is "
                         f"below window_hours + late_grace_hours = {need}. The layer would "
                         f"expire rows it is still responsible for serving")
                _require(rt.window_hours > 0 and rt.late_grace_hours >= 0,
                         f"{tid}: realtime window must be positive and grace non-negative")
            _require(rt.sla_minutes > 0,
                     f"{tid}: realtime.sla_minutes must be positive")
            for pf in rt.partition_spec:
                # Redundancy first, matching the FULL_CDC check: for a constant column that
                # is the ACCURATE diagnosis, and "not part of the canonical row" would send
                # the reader looking for a typo that is not there.
                _require(pf.column not in REDUNDANT_PARTITION_COLUMNS,
                         f"{tid}: realtime.partition_spec by {pf.column!r} is redundant -- "
                         f"a per-table REALTIME target holds exactly one source table")
                _require(pf.column in PARTITIONABLE_COLUMNS,
                         f"{tid}: realtime.partition_spec column {pf.column!r} is not part "
                         f"of the canonical row. Partitionable columns: "
                         f"{', '.join(sorted(PARTITIONABLE_COLUMNS))}")

        # write properties
        _require(t.write.format_version == 2,
                 f"{tid}: only Iceberg format-version 2 is supported (CLAUDE.md 6)")
        _require(t.write.target_file_size_mb > 0,
                 f"{tid}: write.target_file_size_mb must be positive")

        # maintenance retention must be positive, and orphan cleanup must not outrun
        # snapshot expiry -- a shorter orphan window can delete files a live snapshot
        # still references (CLAUDE.md 6)
        _require(t.maintenance.expire_snapshots_days > 0
                 and t.maintenance.remove_orphan_files_days > 0,
                 f"{tid}: maintenance retentions must be positive")
        _require(t.maintenance.remove_orphan_files_days <= t.maintenance.expire_snapshots_days,
                 f"{tid}: remove_orphan_files_days "
                 f"({t.maintenance.remove_orphan_files_days}) must not exceed "
                 f"expire_snapshots_days ({t.maintenance.expire_snapshots_days}) -- orphan "
                 f"cleanup would delete files a live snapshot still references")

        # maintenance temperature / actions must be values the engine understands, or the
        # config is accepted and silently ignored -- which is how `temperature` behaved
        # before it reached the compiled plan at all.
        if t.maintenance.temperature:
            from . import maintenance as _maint
            _require(t.maintenance.temperature in _maint.TEMPERATURES,
                     f"{tid}: maintenance.temperature {t.maintenance.temperature!r} is not "
                     f"one of {', '.join(_maint.TEMPERATURES)}")
        if t.maintenance.actions is not None:
            from . import maintenance as _maint2
            # FAIL BEFORE RUNTIME (Phase 1 section 13). `actions: []` used to reach the
            # maintenance job, which refuses it -- but only once a job was already running.
            # An empty list is what a mistake produces and is indistinguishable from a
            # deliberate "never maintain this table", for which there is no setting.
            _require(len(t.maintenance.actions) > 0,
                     f"{tid}: maintenance.actions is empty. Omit the key to get the default "
                     f"set ({', '.join(sorted(set(_maint2.ACTIONS) - {_maint2.REMOVE_ORPHAN_FILES}))}), "
                     f"or list the actions this table permits. There is no way to spell "
                     f"'no maintenance at all'.")
            unknown = [a for a in t.maintenance.actions if a not in _maint2.ACTIONS]
            _require(not unknown,
                     f"{tid}: unknown maintenance action(s) {unknown}. Known: "
                     f"{', '.join(_maint2.ACTIONS)}")

        # DQ
        _require(t.dq.event_date_null_tolerance >= 0,
                 f"{tid}: dq.event_date_null_tolerance must be >= 0")
        _require(t.dq.freshness_sla_minutes > 0,
                 f"{tid}: dq.freshness_sla_minutes must be positive")

        # typed payload: the declared columns must at least cover the PK, because the
        # snapshot layer and `dv_pk_hash` both read the key OUT of the payload. A typed
        # table whose key is not declared produces a NULL hash and an EOD row whose grain
        # cannot be reconstructed -- silently, since every other column still looks right.
        if t.payload.mode == "typed":
            _require(bool(t.payload.columns),
                     f"{tid}: payload.mode=typed needs `payload.columns`. Typed columns "
                     f"are DERIVED FROM CONFIG -- inferring them from whatever the last "
                     f"writer version happened to carry is how two runs produce two "
                     f"schemas for one table")
            declared = {c.name for c in t.payload.columns}
            missing = [c for c in t.primary_key if c not in declared]
            _require(not missing,
                     f"{tid}: payload.mode=typed but the primary-key column(s) "
                     f"{missing} are not declared in payload.columns. The key is read "
                     f"out of the payload by dv_pk_hash and by the EOD snapshot")

        # partitioning: every field must name a column the row contract actually has, and
        # must not re-partition by an identity this table already IS (section C)
        for pf in t.partition_spec:
            if pf.transform is PartitionTransform.IDENTITY and pf.column in t.primary_key:
                raise ConfigError(
                    f"{tid}: partitioning identity({pf.column}) on a primary-key column "
                    f"creates one partition per row (CLAUDE.md 6). Use bucket() if you "
                    f"need to distribute by key")
            if pf.column in REDUNDANT_PARTITION_COLUMNS:
                raise ConfigError(
                    f"{tid}: partitioning by {pf.column!r} is redundant -- a per-table "
                    f"FULL_CDC table holds exactly ONE source table, so every row has the "
                    f"same value. It buys no pruning and multiplies the partition count "
                    f"(ADR-062, phase brief section C). The monolith partitions by it "
                    f"because it holds eight tables; this one does not")
            if pf.column not in PARTITIONABLE_COLUMNS:
                raise ConfigError(
                    f"{tid}: partition column {pf.column!r} is not part of the canonical "
                    f"row. Partitionable columns: "
                    f"{', '.join(sorted(PARTITIONABLE_COLUMNS))}")
            if pf.transform in (PartitionTransform.DAY, PartitionTransform.MONTH):
                _require(pf.column in TEMPORAL_COLUMNS,
                         f"{tid}: {pf.transform.value}({pf.column}) needs a date or "
                         f"timestamp column; {pf.column!r} is not one")
            if pf.transform is PartitionTransform.BUCKET:
                _require(pf.column != "event_date",
                         f"{tid}: bucket() on event_date scatters one day across "
                         f"{pf.num_buckets} partitions and prunes nothing by date")
                if pf.column == "dv_pk_hash":
                    _require(bool(t.primary_key),
                             f"{tid}: bucket(N, dv_pk_hash) needs a primary key -- the "
                             f"hash is NULL without one, so every row lands in one bucket")
