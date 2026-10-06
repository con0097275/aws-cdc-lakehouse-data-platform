"""Debezium envelope -> L1 STREAM row.

docs/DATA_CONTRACTS.md §5 is the normative column list. Two properties this module
exists to guarantee:

  1. NOTHING is dropped. D5 found nine envelope fields silently discarded by the guide's
     original L1 list. CLAUDE.md §5.2 requires L1 to preserve all source and Kafka
     metadata, so `assert_no_field_dropped` fails loudly rather than quietly narrowing.
  2. `before`/`after` stay JSON STRINGS, never structs (S01-16). A struct would turn every
     source DDL change into an Iceberg migration on the streaming table.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from ordering import normalize_position, POSITION_TYPE_ORACLE, POSITION_TYPE_SQLSERVER

# Every column in DATA_CONTRACTS §5, in order.
L1_COLUMNS = [
    "event_id", "event_version", "operation", "primary_key", "before", "after",
    "source_system", "source_database", "source_schema", "source_table", "domain",
    "source_position_type", "source_position_primary", "source_position_secondary",
    "event_order", "source_commit_ts", "ingest_ts", "event_date", "transaction_id",
    "snapshot_flag", "schema_subject", "schema_id", "trace_id",
    "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "headers",
    "l1_run_id", "l1_write_ts",
]

# The nine D5 reported as dropped. Named explicitly so a regression is obvious.
D5_REQUIRED = [
    "event_version", "source_system", "source_database", "source_schema",
    "source_table", "transaction_id", "snapshot_flag", "schema_subject", "headers",
]

# Of those nine, the two the L1 schema declares NOT NULL (job.py L1_SCHEMA). The other
# seven are legitimately nullable -- a snapshot record carries no transaction_id, and a
# connector need not report a database or schema -- so demanding a value there would
# quarantine valid events. Kept as its own list because the previous check read
# `... and c == "event_version"`, which made a guard advertised as covering nine fields
# examine exactly one, and that one is defaulted to 1 on the line that builds it, so the
# check could never fire at all (governance review 2026-09-03, finding G-P2-1).
D5_NOT_NULL = ["event_version", "source_system"]


def canonical_primary_key(pk: dict) -> str:
    """PK as JSON with SORTED keys.

    Sorting is not cosmetic: an unsorted dict gives a different string for the same key
    depending on field order, so event_id would differ across runs and CLAUDE.md §5.3's
    idempotency-by-event_id would silently fail on rerun.
    """
    return json.dumps(pk, sort_keys=True, separators=(",", ":"))


def compute_event_id(topic: str, partition: int, offset: int) -> str:
    """Deterministic idempotency key (DATA_CONTRACTS §3.3).

    Derived from Kafka coordinates, which are stable across replays of the same record.
    A UUID would make every rerun produce new ids and defeat idempotency entirely.
    """
    return hashlib.sha256(f"{topic}:{partition}:{offset}".encode()).hexdigest()


def detect_position_type(source: dict) -> str:
    if "scn" in source or "commit_scn" in source:
        return POSITION_TYPE_ORACLE
    if "commit_lsn" in source or "change_lsn" in source:
        return POSITION_TYPE_SQLSERVER
    raise ValueError(f"cannot determine position type from source keys: {sorted(source)}")


def raw_positions(position_type: str, source: dict):
    """Raw connector values, before normalisation. DATA_CONTRACTS §5 stores BOTH."""
    if position_type == POSITION_TYPE_ORACLE:
        return str(source.get("commit_scn")), str(source.get("scn"))
    return str(source.get("commit_lsn")), str(source.get("event_serial_no"))


def to_l1_row(record: dict, domain: str, run_id: str, now=None) -> dict:
    """Map one Kafka record (Debezium envelope + Kafka metadata) to an L1 row."""
    now = now or datetime.now(timezone.utc)
    env = record["value"]
    src = env["source"]

    ptype = detect_position_type(src)
    raw_primary, raw_secondary = raw_positions(ptype, src)
    norm_primary, norm_secondary = normalize_position(ptype, raw_primary, raw_secondary)

    commit_ts = datetime.fromtimestamp(src["ts_ms"] / 1000.0, tz=timezone.utc)
    event_id = compute_event_id(record["topic"], record["partition"], record["offset"])

    row = {
        "event_id": event_id,
        "event_version": env.get("event_version", 1),
        "operation": env["op"],
        "primary_key": canonical_primary_key(record["key"]),
        # JSON strings, never structs (S01-16).
        "before": json.dumps(env["before"], sort_keys=True) if env.get("before") is not None else None,
        "after": json.dumps(env["after"], sort_keys=True) if env.get("after") is not None else None,
        "source_system": src["connector"],
        "source_database": src.get("db"),
        "source_schema": src.get("schema"),
        "source_table": src.get("table"),
        "domain": domain,
        "source_position_type": ptype,
        "source_position_primary": raw_primary,
        "source_position_secondary": raw_secondary,
        "event_order": {
            "position_primary": norm_primary,
            "position_secondary": norm_secondary,
            "source_ts_ms": src["ts_ms"],
            "kafka_partition": record["partition"],
            "kafka_offset": record["offset"],
        },
        # source_commit_ts is the BUSINESS time and the only cutoff input (ADR-024, UTC).
        "source_commit_ts": commit_ts,
        # ingest_ts is pipeline time. NEVER used for ordering or cutoffs.
        "ingest_ts": now,
        "event_date": commit_ts.date(),
        "transaction_id": (env.get("transaction") or {}).get("id"),
        "snapshot_flag": str(src.get("snapshot", "false")),
        "schema_subject": record.get("schema_subject"),
        "schema_id": record.get("schema_id"),
        "trace_id": (record.get("headers") or {}).get("trace_id"),
        "kafka_topic": record["topic"],
        "kafka_partition": record["partition"],
        "kafka_offset": record["offset"],
        "kafka_timestamp": record.get("timestamp"),
        "headers": record.get("headers") or {},
        "l1_run_id": run_id,
        "l1_write_ts": now,
    }
    assert_no_field_dropped(row)
    return row


def assert_no_field_dropped(row: dict) -> None:
    """Fail loudly if the L1 row is missing a contract column (CLAUDE.md §5.2, defect D5)."""
    missing = [c for c in L1_COLUMNS if c not in row]
    if missing:
        raise ValueError(f"L1 row is missing contract columns: {missing}")
    nulled = [c for c in D5_NOT_NULL if row.get(c) is None]
    if nulled:
        raise ValueError(f"D5 fields must not be null: {nulled}")
