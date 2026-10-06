#!/usr/bin/env python3
"""Generic per-table provisioning: compiled plan -> Iceberg DDL. No AWS, no Spark.

    python3 -m cdc.provision                              # DDL for every enabled table
    python3 -m cdc.provision --table oracle.coredb.corebank.account
    python3 -m cdc.provision --plan artifacts/cdc/table-plan.json --layer FULL_CDC

ONE generator, N tables (phase brief section A). There is deliberately no per-business-table
DDL anywhere in this repository: a hand-written CREATE TABLE per source table is the same
fact spelled twice -- once in the registry and once in SQL -- and the second copy is the one
that goes stale. Every statement below is a pure function of a compiled plan entry, which is
itself hashed, so "what should this table look like?" has exactly one answer.

WHY 128 MiB AND NOT 512 MiB (section D)
---------------------------------------
Iceberg's own default target is 512 MiB and the phase brief allows it "only if consistent
with project benchmarking". It is not. The Phase 0 audit measured the deployed monolith at
a MEAN DATA FILE OF 137 KiB against that 512 MiB default -- three orders of magnitude under
it -- because the file size is set by how often the job commits (one MERGE per topic per
run), not by the target property. Raising the target to 512 MiB would change nothing except
the number a reader compares against. The registry's 128 MiB is kept as the honest target
and COMPACTION is the actual remedy, which is why `maintenance.compact_target_mb` exists
next to it.

METRICS MODE is set per column class rather than left at the default, for a measured reason:
the payload columns are whole JSON documents, and full metrics on them write both bounds of
every document into the manifest. That inflates manifest size and every plan that reads it,
in exchange for min/max on a string nobody ranges over. They get `none`; the identity and
temporal columns -- the ones the MERGE and every window filter actually use -- get `full`.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import eod as _eod                        # noqa: E402
from .models import ConfigError
from .eod import RUN_LEDGER_COLUMNS as EOD_LEDGER_COLUMNS
from .realtime import RUN_LEDGER_COLUMNS, RUN_LEDGER_TABLE
from . import realtime_state as _rtstate
from .rowspec import (EVENT_INDEX_COLUMNS, column_names, ddl_column_list, eod_columns,
                      full_cdc_columns, realtime_columns)

#: Columns whose bounds are worth keeping in every manifest: the MERGE key, the identity
#: columns, and the columns every window filter and snapshot cutoff uses.
FULL_METRICS_COLUMNS = ("dv_event_id", "event_id", "dv_src_event_id", "dv_pk_hash",
                        "event_date", "source_commit_ts", "kafka_offset",
                        "kafka_timestamp", "op", "business_date")
#: Whole JSON documents. Bounds on them are manifest bloat for a filter nobody writes.
NO_METRICS_COLUMNS = ("payload_after", "payload_before",
                      "payload_after_json", "payload_before_json", "pk_json")
DEFAULT_METRICS_MODE = "truncate(16)"

#: Default write order per layer. ADR-062 fixes `source_commit_ts` for a per-table FULL_CDC
#: table -- the column every consumer windows on. EOD is one row per key, so it clusters by
#: the key instead.
DEFAULT_SORT_ORDER = {
    "FULL_CDC": ("source_commit_ts",),
    "REALTIME": ("source_commit_ts",),
    "EOD": ("dv_pk_hash",),
    "OPS": ("event_date",),
}

LAYERS = ("FULL_CDC", "REALTIME", "EOD")


@dataclass(frozen=True)
class TargetPlan:
    """Everything needed to create or verify ONE table, and nothing about how to run it."""
    table_id: str
    layer: str
    identifier: str
    columns: tuple[tuple[str, str, str], ...]
    partition_spec: tuple[str, ...]
    sort_order: tuple[str, ...]
    properties: dict[str, str]
    comment: str
    enabled: bool = True
    statements: tuple[str, ...] = field(default_factory=tuple)


def _lit(value: Any) -> str:
    """A SQL string literal. Single quotes doubled -- an owner or description with an
    apostrophe would otherwise end the literal and produce a syntax error at apply time,
    against a plan that read fine."""
    return "'" + str(value).replace("'", "''") + "'"


def partition_spec(entry: dict, layer: str) -> tuple[str, ...]:
    """The Iceberg partition expressions for one layer.

    FULL_CDC and REALTIME take the registry's spec, which defaults to `event_date` --
    identity on a DATE column, which is the same partitioning as `days(source_commit_ts)`
    and is what the deployed table already uses. `bucket(N, dv_pk_hash)` applies only when
    the registry says so (section C).

    EOD is partitioned by `business_date` regardless: its grain is one row per key per
    snapshot date, so the source's event partitioning does not describe it.
    """
    if layer == "EOD":
        return ("business_date",)
    if layer == "OPS":
        return ("event_date",)
    if layer == "REALTIME":
        # A REALTIME override exists because the two layers are read differently: FULL_CDC
        # holds all history and is filtered by date, while REALTIME is already bounded to a
        # few days, so day partitioning can leave it with a handful of tiny partitions.
        # Empty means "same as FULL_CDC", which is the default and needs no config.
        override = tuple(p["spec"] for p in
                         (entry.get("realtime_policy") or {}).get("partition_spec") or ())
        if override:
            return override
    spec = tuple(p["spec"] for p in entry.get("partition_spec") or ())
    if not spec:
        raise ConfigError(
            f"{entry.get('table_id')}: no partition spec. An unpartitioned CDC table "
            f"scans its whole history for every windowed read")
    return spec


def sort_order(entry: dict, layer: str) -> tuple[str, ...]:
    declared = tuple((entry.get("write") or {}).get("sort_by") or ())
    return declared or DEFAULT_SORT_ORDER[layer]


def table_properties(entry: dict, layer: str, columns, *,
                     config_version: str | None = None) -> dict[str, str]:
    """Iceberg write properties AND the governance metadata CLAUDE.md section 6 requires
    on every table: owner, grain, PK, retention, freshness SLA and DQ rules. They live in
    TBLPROPERTIES rather than a side document because a catalog reader -- Athena, Glue, the
    AI plane's deny check -- can see them there and cannot see a markdown file."""
    write = entry.get("write") or {}
    dq = entry.get("dq") or {}
    gov = entry.get("governance") or {}
    maint = entry.get("maintenance") or {}
    ident = entry.get("identity") or {}

    props: dict[str, str] = {
        "format-version": str(write.get("format_version", 2)),
        "write.format.default": "parquet",
        "write.parquet.compression-codec": str(write.get("compression", "zstd")),
        "write.target-file-size-bytes":
            str(int(write.get("target_file_size_mb", 128)) * 1024 * 1024),
        "write.distribution-mode": str(write.get("distribution_mode", "hash")),
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        # Snapshot retention. `expire_snapshots` still has to RUN -- this only says what it
        # may remove when it does. Orphan cleanup is deliberately NOT expressed as a table
        # property: it deletes files, and a property that quietly authorises a delete is
        # not something a reader of the DDL would notice (CLAUDE.md 6).
        "history.expire.max-snapshot-age-ms":
            str(int(maint.get("expire_snapshots_days", 7)) * 86_400_000),
        # EVERY registry-derived key is namespaced `cdc.`, including the governance ones.
        # Two reasons, and the first is not cosmetic: `owner` is a RESERVED table property
        # in Spark SQL -- `TBLPROPERTIES ('owner' = ...)` fails the statement outright with
        # UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY, so the DDL would not even parse. The
        # second is that a namespaced key cannot collide with a future engine property, and
        # a reader can tell at a glance which properties this registry owns.
        "cdc.table_id": str(entry.get("table_id", "")),
        "cdc.layer": layer,
        "cdc.config_hash": str(entry.get("config_hash", "")),
        "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.contract": "docs/CDC_TABLE_CONFIG_REFERENCE.md",
        "cdc.owner": str(gov.get("owner", "")),
        "cdc.classification": str(gov.get("classification", "")),
        "cdc.domain": str(gov.get("domain", "")),
        "cdc.grain": _grain(entry, layer),
        "cdc.primary_key": ",".join(ident.get("primary_key") or ()),
        "cdc.dq.not_null": ",".join(dq.get("not_null") or ()),
        "cdc.dq.event_date_null_tolerance": str(dq.get("event_date_null_tolerance", 0)),
        "cdc.dq.freshness_sla_minutes": str(dq.get("freshness_sla_minutes", 60)),
    }
    if config_version:
        props["cdc.config_version"] = config_version
    if layer == "EOD":
        eod = entry.get("eod_policy") or {}
        props["cdc.retention_days"] = str(eod.get("retention_days", 365))
        props["cdc.eod.delete_policy"] = str(eod.get("delete_policy",
                                                     "exclude_from_snapshot"))
        props["cdc.eod.cutoff_policy"] = str(eod.get("cutoff_policy",
                                                     "source_commit_ts"))
    if layer == "REALTIME":
        rt = entry.get("realtime_policy") or {}
        boundary = rt.get("boundary", "rolling_hours")
        props["cdc.realtime.boundary"] = str(boundary)
        props["cdc.realtime.refresh_mode"] = str(rt.get("refresh_mode", "full_refresh"))
        props["cdc.realtime.sla_minutes"] = str(rt.get("sla_minutes", 60))
        if rt.get("schedule"):
            props["cdc.realtime.schedule"] = str(rt["schedule"])
        # Only the fields the table's OWN boundary uses. Writing both sets would put
        # `window_hours=72` on a table whose window is three calendar days -- a property a
        # reader would reasonably believe.
        if boundary == "calendar_day":
            props["cdc.realtime.lookback_days"] = str(rt.get("lookback_days"))
            props["cdc.realtime.late_arrival_grace_days"] = str(
                rt.get("late_arrival_grace_days"))
            props["cdc.retention_days"] = str(rt.get("physical_retention_days"))
            props["cdc.realtime.business_timezone"] = str(
                rt.get("business_timezone", "UTC"))
        else:
            props["cdc.retention_hours"] = str(rt.get("retention_hours", 168))
            props["cdc.realtime.window_hours"] = str(rt.get("window_hours", 72))
            props["cdc.realtime.late_grace_hours"] = str(rt.get("late_grace_hours", 24))

    present = set(column_names(columns))
    for col in FULL_METRICS_COLUMNS:
        if col in present:
            props[f"write.metadata.metrics.column.{col}"] = "full"
    for col in NO_METRICS_COLUMNS:
        if col in present:
            props[f"write.metadata.metrics.column.{col}"] = "none"
    return props


def _grain(entry: dict, layer: str) -> str:
    tid = entry.get("table_id", "")
    if layer == "FULL_CDC":
        return f"one row per CDC event of {tid}"
    if layer == "REALTIME":
        rt = entry.get("realtime_policy") or {}
        return (f"one row per CDC event of {tid} within the last "
                f"{rt.get('window_hours', 72)}h")
    if layer == "EOD":
        return f"one row per primary key of {tid} per business_date"
    return "one row per CDC event, coordinates only"


def _comment(entry: dict, layer: str) -> str:
    tid = entry.get("table_id", "")
    gov = entry.get("governance") or {}
    if layer == "FULL_CDC":
        what = ("canonical CDC history: every I/U/D/R preserved, no business dedup "
                "(CLAUDE.md 5.3)")
    elif layer == "REALTIME":
        rt = entry.get("realtime_policy") or {}
        what = f"bounded {rt.get('window_hours', 72)}h window of the FULL_CDC history"
    else:
        eod = entry.get("eod_policy") or {}
        what = (f"state as of the cutoff, deduplicated by primary key, deletes "
                f"{eod.get('delete_policy', 'exclude_from_snapshot')} (CLAUDE.md 5.6/5.7)")
    return (f"{tid} -- {what}. Owner {gov.get('owner', '?')}, "
            f"{gov.get('classification', '?')}. Provisioned from "
            f"cdc/registry/sources.yaml; do not edit by hand")


def create_table_sql(identifier: str, columns, spec: tuple[str, ...],
                     properties: dict[str, str], comment: str) -> str:
    """IF NOT EXISTS, always. Provisioning must be safe to re-run against a table that has
    data in it -- and section F forbids creating anything that is not in the registry, so a
    CREATE that could replace one is a bigger lever than this tool should own."""
    props = ",\n    ".join(f"{_lit(k)} = {_lit(v)}"
                           for k, v in sorted(properties.items()))
    # An EMPTY spec means UNPARTITIONED, and the clause must then be omitted entirely:
    # `PARTITIONED BY ()` is a syntax error, not an empty partitioning. Every target had a
    # spec until `streaming_app_state` -- one row per app, where partitioning would cost a
    # directory per value and buy nothing -- so this path had never been generated.
    partition_clause = f"PARTITIONED BY ({', '.join(spec)})\n" if spec else ""
    return (f"CREATE TABLE IF NOT EXISTS {identifier} (\n"
            f"  {ddl_column_list(columns)}\n"
            f")\nUSING iceberg\n"
            f"{partition_clause}"
            f"COMMENT {_lit(comment)}\n"
            f"TBLPROPERTIES (\n    {props}\n)")


def write_order_sql(identifier: str, order: tuple[str, ...]) -> str:
    return f"ALTER TABLE {identifier} WRITE ORDERED BY {', '.join(order)}"


def target_plan(entry: dict, layer: str, *,
                config_version: str | None = None) -> TargetPlan:
    """One layer of one table. `enabled` is carried rather than filtered: a caller that
    needs to know a target is deliberately off should be told, not left to infer it from
    an absence."""
    if layer not in LAYERS:
        raise ConfigError(f"unknown layer {layer!r}; expected one of {', '.join(LAYERS)}")
    targets = entry.get("targets") or {}
    identifier = targets.get(f"{layer.lower()}_identifier")
    if not identifier:
        raise ConfigError(
            f"{entry.get('table_id')}: the plan carries no {layer} identifier. It was "
            f"compiled by an older schema version -- recompile with `python3 -m "
            f"cdc.compile --out ...`")
    columns = {"FULL_CDC": full_cdc_columns, "REALTIME": realtime_columns,
               "EOD": eod_columns}[layer](entry)
    spec = partition_spec(entry, layer)
    order = sort_order(entry, layer)
    props = table_properties(entry, layer, columns, config_version=config_version)
    comment = _comment(entry, layer)
    enabled = bool(entry.get("enabled", True))
    if layer == "REALTIME":
        enabled = enabled and bool((entry.get("realtime_policy") or {}).get("enabled", True))
    if layer == "EOD":
        enabled = enabled and bool((entry.get("eod_policy") or {}).get("enabled", True))
    return TargetPlan(
        table_id=str(entry.get("table_id")), layer=layer, identifier=identifier,
        columns=columns, partition_spec=spec, sort_order=order, properties=props,
        comment=comment, enabled=enabled,
        statements=(create_table_sql(identifier, columns, spec, props, comment),
                    write_order_sql(identifier, order)))


def event_index_plan(plan: dict) -> TargetPlan:
    """The optional global index (section G): coordinates and pointers, no payload.

    It exists because per-table storage loses the one property the monolith was genuinely
    better at -- "where did this event go, across every source?" -- and answering that from
    eight tables means eight scans. Carrying payloads here would rebuild the monolith under
    a new name and widen PII exposure, which ADR-062 rules out.
    """
    index = (plan.get("plan") or plan).get("event_index") or {}
    identifier = index.get("identifier")
    if not identifier:
        raise ConfigError("the plan carries no event_index identifier; recompile it")
    props = {
        "format-version": "2",
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd",
        "write.distribution-mode": "hash",
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        "cdc.layer": "OPS",
        "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.grain": "one row per CDC event, coordinates only",
        "cdc.owner": "my-aws-profile",
        "cdc.classification": "internal",
    }
    for col in FULL_METRICS_COLUMNS:
        if col in column_names(EVENT_INDEX_COLUMNS):
            props[f"write.metadata.metrics.column.{col}"] = "full"
    comment = ("Cross-source CDC event index: coordinates and the routed target only, "
               "never a payload (ADR-062 section G). Provisioned from the registry")
    spec = ("event_date",)
    order = DEFAULT_SORT_ORDER["OPS"]
    return TargetPlan(
        table_id="*", layer="OPS", identifier=identifier, columns=EVENT_INDEX_COLUMNS,
        partition_spec=spec, sort_order=order, properties=props, comment=comment,
        statements=(create_table_sql(identifier, EVENT_INDEX_COLUMNS, spec, props, comment),
                    write_order_sql(identifier, order)))


def run_ledger_plan(plan: dict) -> TargetPlan:
    """The REALTIME run ledger (Phase 3 section D).

    Provisioned like any other target rather than created by the job, for the same reason
    the data tables are: a ledger the job creates on demand is one whose schema is whatever
    the last version of the job thought it was.
    """
    payload = plan.get("plan") or plan
    identifier = (payload.get("realtime_run_ledger") or {}).get("identifier")
    if not identifier:
        raise ConfigError(
            "the plan carries no realtime_run_ledger identifier -- it was compiled by an "
            "older schema version. Recompile: python3 -m cdc.compile --out ...")
    props = {
        "format-version": "2",
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd",
        "write.distribution-mode": "hash",
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        "cdc.layer": "OPS",
        "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.grain": "one row per REALTIME materialisation run per table",
        "cdc.owner": "my-aws-profile",
        "cdc.classification": "internal",
    }
    comment = ("REALTIME run ledger: the frozen window, the snapshots read and written, and "
               "the outcome of every materialisation (ADR-064 section D)")
    # Partitioned by the run's upper bound, which is what every question of this table is
    # asked in terms of ("what did the 22nd look like?"). `run_id` would be unique per row.
    spec = ("days(upper_bound)",)
    order = ("table_id", "upper_bound")
    return TargetPlan(
        table_id="*", layer="OPS", identifier=identifier, columns=RUN_LEDGER_COLUMNS,
        partition_spec=spec, sort_order=order, properties=props, comment=comment,
        statements=(create_table_sql(identifier, RUN_LEDGER_COLUMNS, spec, props, comment),
                    write_order_sql(identifier, order)))


def realtime_info_plan(plan: dict) -> TargetPlan:
    """`ops.realtime_info` -- the CURRENT state and CURSOR of each REALTIME table. ADR-083.

    Unpartitioned, and it has to be: one row per table, MERGEd in place. Ten rows for this
    platform, a few hundred for a large one. A partition here would cost a directory per
    value to prune nothing, and the MERGE would rewrite a partition per run.
    """
    payload = plan.get("plan") or plan
    identifier = (payload.get("realtime_info") or {}).get("identifier")
    if not identifier:
        raise ConfigError(
            "the plan carries no realtime_info identifier -- it was compiled by an older "
            "schema version. Recompile: python3 -m cdc.compile --out ...")
    props = {
        "format-version": "2", "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd", "write.distribution-mode": "hash",
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        "cdc.layer": "OPS", "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.grain": "one row per table -- CURRENT cursor and state, MERGEd in place",
        "cdc.owner": "my-aws-profile", "cdc.classification": "internal",
    }
    cols = _rtstate.REALTIME_INFO_COLUMNS
    order = _rtstate.REALTIME_INFO_KEY
    comment = ("REALTIME control plane: the incremental cursor and last successful state "
               "per table (ADR-083)")
    return TargetPlan(
        table_id="*", layer="OPS", identifier=identifier, columns=cols,
        partition_spec=(), sort_order=order, properties=props, comment=comment,
        statements=(create_table_sql(identifier, cols, (), props, comment),
                    write_order_sql(identifier, order)))


def streaming_state_plan(plan: dict) -> TargetPlan:
    """The streaming app state table (ADR-073 section 7).

    UNPARTITIONED AND TINY ON PURPOSE. It holds one row per streaming application, updated
    in place -- not one row per heartbeat. A resident app triggering every minute would
    append 1,440 rows a day carrying nothing but a timestamp, on a table whose only question
    is "is it alive and how far has it got". Partitioning a table with a handful of rows
    costs a directory per value and buys nothing.
    """
    from . import ingestion as _ing
    payload = plan.get("plan") or plan
    identifier = (payload.get("ingestion") or {}).get("state_table")
    if not identifier:
        raise ConfigError(
            "the plan carries no ingestion.state_table identifier -- it was compiled by an "
            "older schema version. Recompile: python3 -m cdc.compile --out ...")
    props = {
        "format-version": "2",
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd",
        "write.distribution-mode": "hash",
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        "cdc.layer": "OPS",
        "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.grain": "one row per streaming application, updated in place",
        "cdc.owner": "my-aws-profile",
        "cdc.classification": "internal",
    }
    comment = ("Streaming ingestion app state: mode, checkpoint, offsets, watermark and "
               "liveness for the Kafka -> FULL_CDC application (ADR-073)")
    # The (name, type, comment) triples verbatim: `ddl_column_list` carries the comment
    # into the DDL, and dropping it here would make this the one OPS table whose columns
    # arrive undocumented in the catalog.
    cols = _ing.STREAMING_STATE_COLUMNS
    return TargetPlan(
        table_id="*", layer="OPS", identifier=identifier, columns=cols,
        partition_spec=(), sort_order=("app_id",), properties=props, comment=comment,
        statements=(create_table_sql(identifier, cols, (), props, comment),
                    write_order_sql(identifier, ("app_id",))))


def eod_ledger_plan(plan: dict) -> TargetPlan:
    """The EOD run ledger (Phase 4 section G): what each close did, and whether it was
    certified. Provisioned rather than created by the job, for the same reason every other
    target is -- a ledger the job creates on demand has whatever schema the last version of
    the job believed in."""
    payload = plan.get("plan") or plan
    identifier = (payload.get("eod_run_ledger") or {}).get("identifier")
    if not identifier:
        raise ConfigError(
            "the plan carries no eod_run_ledger identifier -- it was compiled by an older "
            "schema version. Recompile: python3 -m cdc.compile --out ...")
    props = {
        "format-version": "2",
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd",
        "write.distribution-mode": "hash",
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        "cdc.layer": "OPS",
        "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.grain": "one row per EOD close attempt per table per business date",
        "cdc.owner": "my-aws-profile",
        "cdc.classification": "internal",
    }
    comment = ("EOD run ledger: the cutoff, the snapshots read and written, the position "
               "evidence, the DQ and reconciliation outcomes, and whether the close was "
               "CERTIFIED (ADR-065 section G)")
    spec = ("cob_date",)
    order = ("table_id", "cob_date")
    return TargetPlan(
        table_id="*", layer="OPS", identifier=identifier, columns=EOD_LEDGER_COLUMNS,
        partition_spec=spec, sort_order=order, properties=props, comment=comment,
        statements=(create_table_sql(identifier, EOD_LEDGER_COLUMNS, spec, props, comment),
                    write_order_sql(identifier, order)))


def eod_info_plan(plan: dict) -> TargetPlan:
    """`ops.eod_info` -- the CURRENT state of each (table, COB). Phase D section 7.

    Unpartitioned on purpose: one row per table per business date is thousands of rows a
    year, and a partition per date would cost a directory per day to prune nothing. Sorted
    on the logical key so the MERGE reads one file.
    """
    payload = plan.get("plan") or plan
    identifier = (payload.get("eod_info") or {}).get("identifier")
    if not identifier:
        raise ConfigError(
            "the plan carries no eod_info identifier -- it was compiled by an older schema "
            "version. Recompile: python3 -m cdc.compile --out ...")
    props = {
        "format-version": "2", "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd", "write.distribution-mode": "hash",
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        "cdc.layer": "OPS", "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.grain": "one row per table per business date -- CURRENT state, MERGEd in place",
        "cdc.owner": "my-aws-profile", "cdc.classification": "internal",
    }
    cols = _eod.EOD_INFO_COLUMNS
    order = _eod.EOD_INFO_KEY
    return TargetPlan(
        table_id="*", layer="OPS", identifier=identifier, columns=cols,
        partition_spec=(), sort_order=order, properties=props,
        comment="EOD control plane: current certified state per table per COB (ADR-076)",
        statements=(create_table_sql(identifier, cols, (), props,
                                     "EOD control plane: current certified state per table "
                                     "per COB (ADR-076)"),
                    write_order_sql(identifier, order)))


def eod_run_hist_plan(plan: dict) -> TargetPlan:
    """`ops.eod_run_hist` -- one row per ATTEMPT, appended and never overwritten.

    Partitioned by COB date: history is queried "show me everything that happened for this
    business date", and it grows without bound while `eod_info` does not.
    """
    payload = plan.get("plan") or plan
    identifier = (payload.get("eod_run_hist") or {}).get("identifier")
    if not identifier:
        raise ConfigError(
            "the plan carries no eod_run_hist identifier -- recompile the plan.")
    props = {
        "format-version": "2", "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd", "write.distribution-mode": "hash",
        "write.metadata.metrics.default": DEFAULT_METRICS_MODE,
        "cdc.layer": "OPS", "cdc.managed_by": "cdc/registry/sources.yaml",
        "cdc.grain": "one row per EOD attempt -- append only, failures retained",
        "cdc.owner": "my-aws-profile", "cdc.classification": "internal",
    }
    cols = _eod.RUN_HIST_COLUMNS
    spec = ("days(cob_date)",)
    order = ("table_id", "cob_date", "attempt")
    comment = "EOD control plane: every attempt, including the failed ones (ADR-076)"
    return TargetPlan(
        table_id="*", layer="OPS", identifier=identifier, columns=cols,
        partition_spec=spec, sort_order=order, properties=props, comment=comment,
        statements=(create_table_sql(identifier, cols, spec, props, comment),
                    write_order_sql(identifier, order)))


def eod_legacy_view_plan(plan: dict) -> TargetPlan:
    """The `pre_datelastmaint` / `datelastmaint` compatibility view (section 9)."""
    payload = plan.get("plan") or plan
    info = (payload.get("eod_info") or {}).get("identifier")
    view = (payload.get("eod_info_legacy_view") or {}).get("identifier")
    if not (info and view):
        raise ConfigError("the plan carries no eod_info_legacy_view identifier -- recompile.")
    return TargetPlan(
        table_id="*", layer="OPS", identifier=view, columns=(), partition_spec=(),
        sort_order=(), properties={},
        comment="Legacy spelling of the watermark pair. A VIEW, never canonical columns.",
        statements=(_eod.legacy_view_sql(view, info),))


def plan_targets(plan: dict, *, layers: tuple[str, ...] = LAYERS,
                 table_ids: tuple[str, ...] | None = None,
                 include_disabled: bool = False,
                 include_event_index: bool = True) -> list[TargetPlan]:
    """Every target the compiled plan asks for, in a deterministic order."""
    payload = plan.get("plan") or plan
    version = plan.get("config_version")
    out: list[TargetPlan] = []
    for entry in sorted(payload.get("tables") or [], key=lambda e: e["table_id"]):
        if table_ids and entry["table_id"] not in table_ids:
            continue
        for layer in layers:
            tp = target_plan(entry, layer, config_version=version)
            if tp.enabled or include_disabled:
                out.append(tp)
    if include_event_index and not table_ids:
        out.append(event_index_plan(plan))
    if not table_ids:
        # The EOD control plane. Provisioned like every other target, for the
        # reason every other one is: a control table the job creates on demand
        # has whatever schema the last version of the job believed in.
        # NOT `eod_legacy_view_plan`: view support is CATALOG-dependent (the Hadoop catalog
        # used by the tests refuses outright, and Glue's depends on the Iceberg version), and
        # the consumers that want the old spelling read through Athena anyway. The SQL is
        # still compiled and printed -- `make cdc-eod-legacy-view` -- and applied there.
        for builder in (eod_info_plan, eod_run_hist_plan, realtime_info_plan):
            try:
                out.append(builder(plan))
            except ConfigError:
                # A plan compiled before schema 10 has no control plane. Skipping
                # is right here and refusing is right in the JOB: provisioning an
                # old plan must not fail wholesale, but closing against one must.
                pass
        out.append(run_ledger_plan(plan))
        out.append(eod_ledger_plan(plan))
        out.append(streaming_state_plan(plan))
    return out


# --------------------------------------------------------------------------- #
# drift: what a live table has vs what the registry says it should have
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ColumnDrift:
    missing: tuple[tuple[str, str], ...] = ()      # in the contract, not in the table
    unexpected: tuple[tuple[str, str], ...] = ()   # in the table, not in the contract
    type_changed: tuple[tuple[str, str, str], ...] = ()   # (name, expected, observed)

    @property
    def clean(self) -> bool:
        return not (self.missing or self.unexpected or self.type_changed)


def diff_columns(expected, observed: dict[str, str]) -> ColumnDrift:
    """Pure comparison, so the drift check is testable without a catalog.

    `unexpected` is REPORTED, never dropped. A column the registry does not know about is
    either an older contract or someone's manual ALTER, and both need a person -- dropping
    it would destroy data to make a report green.
    """
    exp = {name: sql_type.lower().replace(" ", "") for name, sql_type, _ in expected}
    obs = {k: v.lower().replace(" ", "") for k, v in observed.items()}
    missing = tuple((n, t) for n, t in exp.items() if n not in obs)
    unexpected = tuple((n, t) for n, t in obs.items() if n not in exp)
    changed = tuple((n, exp[n], obs[n]) for n in exp if n in obs and exp[n] != obs[n])
    return ColumnDrift(missing=missing, unexpected=unexpected, type_changed=changed)


def additive_alter_sql(identifier: str, drift: ColumnDrift) -> tuple[str, ...]:
    """ADD COLUMN for what is missing, and nothing else.

    Deliberately no DROP and no type change: `schema_evolution: additive_only` is the
    registry's default, an Iceberg type change rewrites nothing but reinterprets everything,
    and a dropped column is unrecoverable. Those need a person and a migration, not a flag.
    """
    return tuple(f"ALTER TABLE {identifier} ADD COLUMN {name} {sql_type}"
                 for name, sql_type in drift.missing)


# --------------------------------------------------------------------------- #
# CLI -- prints DDL, mutates nothing
# --------------------------------------------------------------------------- #

def _default_plan_path() -> Path:
    return Path(__file__).resolve().parents[1] / "artifacts" / "cdc" / "table-plan.json"


def load_plan(path: Path | None = None) -> dict:
    """Read a compiled plan, or compile one on the fly when none is written yet."""
    path = path or _default_plan_path()
    if path.exists():
        return json.loads(path.read_text())
    from datetime import datetime, timezone

    from .compile import compile_plan, DEFAULT_REGISTRY
    return compile_plan(DEFAULT_REGISTRY, generated_at=datetime.now(timezone.utc))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", type=Path, default=None,
                    help="compiled plan (default artifacts/cdc/table-plan.json, "
                         "compiled on the fly if absent)")
    ap.add_argument("--table", action="append", default=[],
                    help="canonical table id; repeatable")
    ap.add_argument("--layer", action="append", default=[], choices=list(LAYERS))
    ap.add_argument("--include-disabled", action="store_true")
    ap.add_argument("--no-event-index", action="store_true")
    args = ap.parse_args(argv)

    try:
        plan = load_plan(args.plan)
        targets = plan_targets(
            plan, layers=tuple(args.layer) or LAYERS,
            table_ids=tuple(args.table) or None,
            include_disabled=args.include_disabled,
            include_event_index=not args.no_event_index)
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        return 2

    for tp in targets:
        flag = "" if tp.enabled else "   [DISABLED — printed for review only]"
        print(f"-- {tp.layer:<9} {tp.table_id}{flag}")
        for stmt in tp.statements:
            print(f"{stmt};\n")
    print(f"-- {len(targets)} target(s). Nothing was created; no AWS call was made.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
