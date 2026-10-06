"""Compile the registry into a deterministic plan (section F).

    YAML  ->  inheritance  ->  semantic validation  ->  canonical payload  ->  sha256

`canonical_json` and `plan_hash` are IMPORTED from `spark/reporting/plan.py`, not
reimplemented. Two hashing functions that are "the same" until one of them changes its
separators is exactly the drift this platform keeps finding, and the reporting compiler's
version is already covered by determinism tests.

`generated_at` and `source_sha256` sit OUTSIDE the hashed payload, for the reason the
reporting plan documents: the first is wall-clock and would destroy determinism, the second
is provenance about the input file rather than part of what the plan MEANS.
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# CANONICAL JSON + HASH — kept BYTE-IDENTICAL to spark/reporting/plan.py.
#
# Importing them would have been the first choice, and it does not work: the reporting
# package uses FLAT module names (`plan`, `config_loader`, `models`) and so does this one,
# so whichever directory is earlier on sys.path wins and the import silently resolves to the
# wrong module. Loading by file path fails the same way one level down, because
# reporting/plan.py itself does `from config_loader import ...`.
#
# So the two functions are duplicated ON PURPOSE, and
# `test_cdc_registry.py::test_canonical_json_matches_the_reporting_implementation`
# asserts the two produce identical output for the same input. A duplicate that is pinned
# equal by a test is safer here than an import that can bind to the wrong module.
def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def plan_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


from . import naming
from .catalog import Catalog                          # noqa: E402
from . import ingestion as _ing                                  # noqa: E402
from .eod import RUN_LEDGER_TABLE as EOD_RUN_LEDGER            # noqa: E402
from . import eod as _eodm                                     # noqa: E402


def _ing_profile(config) -> str:
    return str((config.ingestion or {}).get("profile") or _ing.PROFILE_LAB).strip().lower()


def _ing_mode(config) -> str:
    """The RESOLVED mode: an explicit override if present, else the profile's default."""
    raw = (config.ingestion or {}).get("mode")
    if raw:
        return str(raw).strip().lower()
    return _ing.DEFAULT_MODE_FOR_PROFILE[_ing_profile(config)]
from .realtime import RUN_LEDGER_TABLE as REALTIME_RUN_LEDGER   # noqa: E402
from . import realtime_state as _rtstate                       # noqa: E402
from . import realtime as _rt                                   # noqa: E402
from . import scheduling as _sched                              # noqa: E402
from .models import LoadedCdcConfig, TableConfig      # noqa: E402

#: 10 adds the EOD control plane (`eod_info`, `eod_run_hist`, the legacy view) and the
#: resolved EOD cadence, profile and source SLA. A consumer on 9 has one append ledger
#: and no way to ask what the current certified state of a business date is.
#: 9 adds the RESOLVED realtime cadence and resource profile. A consumer on 8 has a
#: `schedule` it must default itself and no sizing at all, which is the split-brain
#: Phase C closes: the DAG's env var and the registry disagreed and the registry lost.
#: 8 adds the streaming app map (`ingestion.apps`) and the state heartbeat. A consumer on 7
#: knows the mode and the checkpoint ROOT but not which app id a subscription belongs to, so
#: it has to derive the checkpoint at submit time -- and a derived path that disagrees with
#: the plan's is a replay from `startingOffsets` that looks like a normal restart.
#: 7 adds the ingestion policy: resolved mode, trigger and the streaming state table. A
#: consumer on 6 finds no `ingestion` block and falls back to its own default, which is
#: precisely the inference ADR-073 removes.
#: 6 adds the maintenance temperature and action list, which the config accepted and
#: the plan silently dropped -- so a runtime consumer fell back to the inferred
#: temperature and ignored an explicit one, agreeing with it often enough to hide it.
#: 5 adds the onboarding mode, so a plan carries how a table's existing rows were (or were
#: not) brought in -- a fact the capture diff cannot show and that changes what "this table
#: is complete" means.
#: 4 adds the resolved EOD close policy (lag, snapshot mode, ordering engine) and the EOD
#: ledger identifier. A consumer on 3 would close a date without knowing which business day
#: a run is for, or which engine's ordering decides "last event wins".
#: 3 adds the resolved realtime window policy (boundary, day bounds, refresh mode) and the
#: run-ledger identifier. A consumer on 2 would read `window_hours` off a table whose policy
#: is written in calendar days and materialise a different window than the config names.
#: 2 added the resolved catalog, the qualified target identifiers and the payload contract.
#: A consumer that only understands 1 would route to a BARE table name and resolve it
#: against whatever database its session happened to default to, so the version is a gate,
#: not a note.
PLAN_SCHEMA_VERSION = 10

#: The optional global event index (ADR-063 section G). One table for the platform, in OPS.
EVENT_INDEX_TABLE = "cdc_event_index"
CONFIG_VERSION_PREFIX = "cdccfg"
CONFIG_VERSION_HASH_CHARS = 16


def table_payload(t: TableConfig, catalog: Catalog) -> dict[str, Any]:
    """One resolved table, fully expanded. Nothing here is inherited at read time -- the
    consumer of a plan must never need the registry to interpret it."""
    return {
        "table_id": t.table_id,
        "enabled": t.enabled,
        "source": {
            "source_id": t.source_id, "engine": t.engine.value,
            "connector": t.connector, "database": t.database,
            "schema": t.schema, "table": t.table,
        },
        "capture": {
            "topic": t.topic,
            "include_list_entry": naming.include_list_entry(
                t.engine, schema=t.schema, table=t.table),
            "key_columns_entry": (
                naming.key_columns_entry(t.engine, schema=t.schema, table=t.table,
                                         primary_key=t.primary_key)
                if t.primary_key else None),
        },
        # BARE names and QUALIFIED identifiers both, deliberately. The bare name is what a
        # human reads and what the collision check compares; the identifier is what a job
        # writes to, and a job that had to assemble it would be assembling it from a
        # database name it got from somewhere else (ADR-033).
        "targets": {
            "full_cdc": t.full_cdc_table,
            "realtime": t.realtime_table,
            "eod": t.eod_table,
            "full_cdc_identifier": catalog.qualify("FULL_CDC", t.full_cdc_table),
            "realtime_identifier": catalog.qualify("REALTIME", t.realtime_table),
            "eod_identifier": catalog.qualify("EOD", t.eod_table),
        },
        "payload": {
            "mode": t.payload.mode,
            "columns": [{"name": c.name, "type": c.type, "encoding": c.encoding}
                        for c in t.payload.columns],
        },
        "identity": {
            "primary_key": list(t.primary_key),
            "primary_key_unsupported": t.primary_key_unsupported,
            "ordering_columns": list(t.ordering_columns),
        },
        "partition_spec": [
            {"column": p.column, "transform": p.transform.value,
             "num_buckets": p.num_buckets, "spec": p.spec()}
            for p in t.partition_spec
        ],
        "write": {
            "format_version": t.write.format_version,
            "compression": t.write.compression,
            "target_file_size_mb": t.write.target_file_size_mb,
            "distribution_mode": t.write.distribution_mode,
        },
        "realtime_policy": {
            "enabled": t.realtime.enabled,
            "boundary": t.realtime.boundary,
            # SHAPE is the downstream CONTRACT: whether this table may be read as current
            # state. `refresh_mode` is the deprecated single-word spelling, DERIVED from
            # the pair, and kept in the plan only until R2-E migrates the Spark engine.
            "shape": t.realtime.shape,
            "write_strategy": t.realtime.write_strategy,
            "source_progress": t.realtime.source_progress,
            "ordering_strategy": t.realtime.ordering_strategy,
            "merge_delete_policy": t.realtime.merge_delete_policy,
            "rebase_on_eod_certified": t.realtime.rebase_on_eod_certified,
            #: False for latest_state -- the retention bound is a recovery horizon there,
            #: not a prune. A state row is not stale because the entity did not change.
            "prunes_by_age": t.realtime.prunes_by_age,
            "refresh_mode": t.realtime.refresh_mode,
            # RESOLVED, never the raw string: a consumer that had to apply the default
            # would be a second implementation of it, and the one in the DAG is exactly
            # what Phase C removes.
            "schedule": t.realtime.schedule,
            "schedule_declared": bool(t.realtime.schedule_raw),
            "resource_profile": _rt.resolve_resource_profile(
                t.realtime.resource_profile, what=f"{t.table_id}: realtime"),
            "sla_minutes": t.realtime.sla_minutes,
            "window_hours": t.realtime.window_hours,
            "late_grace_hours": t.realtime.late_grace_hours,
            "retention_hours": t.realtime.retention_hours,
            "lookback_days": t.realtime.lookback_days,
            "late_arrival_grace_days": t.realtime.late_arrival_grace_days,
            "physical_retention_days": t.realtime.physical_retention_days,
            "partition_spec": [
                {"column": p.column, "transform": p.transform.value,
                 "num_buckets": p.num_buckets, "spec": p.spec()}
                for p in t.realtime.partition_spec
            ],
            # The business timezone the calendar boundary is taken in. Copied from the EOD
            # policy rather than duplicated in config: one table cannot have two different
            # ideas of when its day starts without the two layers disagreeing about which
            # events belong to it.
            "business_timezone": t.eod.business_timezone,
        },
        "eod_policy": {
            "enabled": t.eod.enabled,
            "cutoff_policy": t.eod.cutoff_policy.value,
            "business_timezone": t.eod.business_timezone,
            "delete_policy": t.eod.delete_policy.value,
            "retention_days": t.eod.retention_days,
            "business_date_lag_days": t.eod.business_date_lag_days,
            "snapshot_mode": t.eod.snapshot_mode,
            "schedule": t.eod.schedule,
            "schedule_declared": t.eod.schedule != _eodm.DEFAULT_EOD_SCHEDULE,
            "resource_profile": _sched.resolve_resource_profile(
                t.eod.resource_profile, what=f"{t.table_id}: eod"),
            "source_sla_minutes": t.eod.source_sla_minutes,
            # The engine the ordering contract is chosen by. Carried so a runtime consumer
            # needs the plan and nothing else -- re-deriving it from `source.engine` would
            # work and would be a second place for the two to disagree.
            "ordering_engine": t.engine.value,
        },
        "dq": {
            "not_null": list(t.dq.not_null),
            "event_date_null_tolerance": t.dq.event_date_null_tolerance,
            "freshness_sla_minutes": t.dq.freshness_sla_minutes,
        },
        "maintenance": {
            "compact_target_mb": t.maintenance.compact_target_mb,
            "expire_snapshots_days": t.maintenance.expire_snapshots_days,
            "remove_orphan_files_days": t.maintenance.remove_orphan_files_days,
            "temperature": t.maintenance.temperature,
            # `None`, NOT `[]`. The runtime reads a declared list as "permit ONLY these",
            # so an empty list means "permit nothing" -- and emitting `[]` for every table
            # that simply did not declare any silently disabled maintenance platform-wide.
            # Absence of a choice and a choice of nothing are different facts and the plan
            # has to keep them different.
            "actions": (list(t.maintenance.actions)
                        if t.maintenance.actions is not None else None),
        },
        "schema_evolution": t.schema_evolution.value,
        "onboarding": {"mode": t.onboarding.mode, "backfilled": t.onboarding.backfilled},
        "governance": {
            "owner": t.owner,
            "classification": t.classification.value,
            "domain": t.domain,
        },
        "sla": {"freshness_minutes": t.dq.freshness_sla_minutes},
    }


def build_payload(config: LoadedCdcConfig,
                  catalog: Catalog | None = None) -> dict[str, Any]:
    """The hashed, semantic content. Tables sorted by id so ordering in YAML is not
    semantic -- moving a table up the file must not change the hash.

    The CATALOG is part of the hashed payload because it is part of what the plan MEANS: the
    same registry compiled against a different layer binding provisions different tables, and
    a plan whose hash did not move would claim otherwise.
    """
    catalog = catalog or Catalog.from_layers_file()
    tables = sorted((table_payload(t, catalog) for t in config.tables),
                    key=lambda e: e["table_id"])
    for entry in tables:
        entry["config_hash"] = hashlib.sha256(
            canonical_json(entry).encode("utf-8")).hexdigest()[:CONFIG_VERSION_HASH_CHARS]
    return {
        "plan_schema_version": PLAN_SCHEMA_VERSION,
        "environment": config.environment,
        "catalog": catalog.payload(),
        "event_index": {
            "table": EVENT_INDEX_TABLE,
            "identifier": catalog.qualify("OPS", EVENT_INDEX_TABLE),
        },
        # The REALTIME run ledger (Phase 3 section D). One table for the platform, in OPS.
        "realtime_run_ledger": {
            "table": REALTIME_RUN_LEDGER,
            "identifier": catalog.qualify("OPS", REALTIME_RUN_LEDGER),
        },
        # The REALTIME CONTROL PLANE (R2-C / ADR-083). `realtime_run` stays the append
        # log of every attempt; `realtime_info` answers "where is this table's cursor"
        # directly, instead of by the convention "the latest SUCCEEDED row by some
        # ordering" that every reader would otherwise re-implement.
        "realtime_info": {
            "table": _rtstate.REALTIME_INFO_TABLE,
            "identifier": catalog.qualify("OPS", _rtstate.REALTIME_INFO_TABLE),
            "key": list(_rtstate.REALTIME_INFO_KEY),
        },
        # The EOD run ledger (Phase 4 section G).
        "eod_run_ledger": {
            "table": EOD_RUN_LEDGER,
            "identifier": catalog.qualify("OPS", EOD_RUN_LEDGER),
        },
        # The EOD CONTROL PLANE (Phase D / ADR-076). `eod_run` stays an append log;
        # `eod_info` answers "what is the current certified state" without a convention,
        # and `eod_run_hist` keeps every attempt including the failures.
        "eod_info": {
            "table": _eodm.EOD_INFO_TABLE,
            "identifier": catalog.qualify("OPS", _eodm.EOD_INFO_TABLE),
            "key": list(_eodm.EOD_INFO_KEY),
        },
        "eod_run_hist": {
            "table": _eodm.RUN_HIST_TABLE,
            "identifier": catalog.qualify("OPS", _eodm.RUN_HIST_TABLE),
        },
        "eod_info_legacy_view": {
            "table": _eodm.LEGACY_VIEW_TABLE,
            "identifier": catalog.qualify("OPS", _eodm.LEGACY_VIEW_TABLE),
        },
        # HOW the ingest runs (ADR-073). In the plan rather than on the submit command,
        # because production behaviour inferred from a CLI flag is exactly what Phase A
        # found: a `--run-seconds` budget silently turning a stream into a session.
        # `mode` is resolved from the profile here so a consumer never has to redo that.
        "ingestion": {
            "profile": _ing_profile(config),
            "mode": _ing_mode(config),
            "trigger_interval": (config.ingestion or {}).get(
                "trigger_interval", _ing.DEFAULT_TRIGGER),
            "trigger_seconds": _ing.parse_trigger(
                (config.ingestion or {}).get("trigger_interval") or _ing.DEFAULT_TRIGGER,
                what="ingestion"),
            "heartbeat_interval": (config.ingestion or {}).get(
                "heartbeat_interval", _ing.DEFAULT_HEARTBEAT),
            "heartbeat_seconds": _ing.parse_trigger(
                (config.ingestion or {}).get("heartbeat_interval")
                or _ing.DEFAULT_HEARTBEAT,
                what="ingestion.heartbeat_interval", floor=_ing.MIN_HEARTBEAT_SECONDS),
            "checkpoint_root": _ing.CHECKPOINT_ROOT,
            "state_table": catalog.qualify("OPS", _ing.STREAMING_STATE_TABLE),
            # One streaming application per ENGINE, named here so the submit path reads an
            # identity instead of deriving one. `checkpoint_suffix` is relative because the
            # plan is bucket-agnostic by design (the same plan compiles for any
            # environment); the job joins it to the lake bucket it was pointed at.
            "apps": sorted(
                [{"app_id": _ing.app_id_for("", s.engine.value),
                  "engine": s.engine.value,
                  "checkpoint_suffix": f"{_ing.CHECKPOINT_ROOT}/"
                                       f"{_ing.app_id_for('', s.engine.value)}",
                  "topics": sorted(t.topic for t in s.tables)}
                 for s in config.sources],
                key=lambda a: a["app_id"]),
        },
        "sources": sorted(
            [{"source_id": s.source_id, "engine": s.engine.value,
              "connector": s.connector, "topic_prefix": s.topic_prefix,
              "table_count": len(s.tables)} for s in config.sources],
            key=lambda e: e["source_id"]),
        "tables": tables,
    }


def config_version(payload: dict[str, Any]) -> str:
    return f"{CONFIG_VERSION_PREFIX}-{plan_hash(payload)[:CONFIG_VERSION_HASH_CHARS]}"


def source_fingerprint(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_plan(config: LoadedCdcConfig, *, generated_at: datetime,
               source_sha256: str | None = None,
               catalog: Catalog | None = None) -> dict[str, Any]:
    payload = build_payload(config, catalog)
    return {
        "plan_hash": plan_hash(payload),
        "config_version": config_version(payload),
        "generated_at": generated_at.isoformat(),
        "source_sha256": source_sha256,
        # OUTSIDE `payload`, so a deprecation warning never changes `plan_hash`. A warning
        # that moved the hash would make `--verify` fail on a registry nobody edited.
        "deprecations": list(config.deprecations),
        "plan": payload,
    }
