"""The canonical row contract for the per-table CDC layers, derived from config.

ONE definition, three consumers: the provisioner emits DDL from it, the Spark writer
projects to it, and the drift check compares a live table against it. A hand-written DDL per
business table -- which is what section A of the phase brief forbids -- would be an eighth
place for the same fact to be spelled differently.

WHAT A FULL_CDC ROW MUST CARRY (phase brief section B)
------------------------------------------------------
Every column below is metadata the platform ALREADY produces or already needs; nothing here
invents a semantic. The three additions over the legacy monolith's projection are named as
additions and explained where they are defined:

    source_database   the monolith carries system/schema/table but not database
    schema_global_id  the writer-schema id `apicurio.value.globalId` -- "schema version".
                      The decode has always selected per globalId (finding E2) and then
                      thrown the id away, so a mis-decoded row could not be traced to the
                      schema that decoded it
    dv_pk_hash        the canonical PK, hashed. Required for `bucket(N, dv_pk_hash)`
                      (section C) and the cheap way to ask "every event for this key"
    ingest_run_id     which run wrote the row; `ingested_at` alone cannot separate two runs
    ingest_job        which JOB wrote it -- batch and stream both write this layer

`event_id` is retained as a mirror of `dv_event_id`. It is the MERGE key on the legacy
table and the column every downstream reader names, so keeping it makes the eventual cutover
a table-name change rather than a column rename in six consumer sites at once.

PAYLOAD REPRESENTATION -- AND WHY TYPED MODE ALSO KEEPS THE JSON
----------------------------------------------------------------
`json` is the current contract and the default: `payload_after` / `payload_before` are the
Debezium images serialised with `to_json`. It survives multiple writer versions on one topic
because the projection is a string either way.

`typed` adds real struct columns built from the registry's declared column list, which is
what gives Parquet per-column statistics and lets a reader project one field instead of
parsing a document. It does NOT replace the JSON: a writer version can carry a field the
registry has not declared yet, and a typed-only projection would drop it silently -- data
loss dressed as a schema improvement. So typed mode writes BOTH, and the JSON stays the
lossless record. At this lake's size (300 MB) the duplication is measured in cents; a
dropped column is not recoverable at any price.
"""
from __future__ import annotations

import re

from .models import ConfigError

#: A column the canonical row carries: (name, SQL type, why it exists).
#: ORDER IS THE DDL ORDER, and it is stable -- a reordering would rewrite every generated
#: DDL and make a drift check noisy for no semantic change.
FULL_CDC_METADATA: tuple[tuple[str, str, str], ...] = (
    ("dv_event_id", "STRING",
     "identity of the DELIVERED RECORD: sha2(topic|partition|offset|kafka_timestamp). "
     "The MERGE key -- rerun idempotency (CLAUDE.md 5.3)"),
    ("event_id", "STRING",
     "mirror of dv_event_id under the name every existing consumer and the legacy "
     "monolith use. Kept so cutover is a table name, not a column rename"),
    ("dv_src_event_id", "STRING",
     "identity of the SOURCE CHANGE: carries no Kafka coordinate, so the same commit "
     "redelivered through a rebuilt cluster is still recognisable as one business event"),
    ("dv_pk_hash", "STRING",
     "sha2 of the canonical primary key, after-image first and before-image for a delete. "
     "NULL when the source has no PK. Enables bucket(N, dv_pk_hash)"),
    ("source_system", "STRING", "engine identity: oracle | sqlserver"),
    ("source_database", "STRING", "source database -- absent from the legacy monolith"),
    ("source_schema", "STRING", "source schema, normalised"),
    ("source_table", "STRING", "source table, normalised"),
    ("op", "STRING", "Debezium operation: c | u | d | r"),
    ("event_date", "DATE",
     "UTC date of source.ts_ms. The partition column. NEVER legitimately NULL -- a NULL "
     "is the signature of an undecodable record that slipped quarantine (ADR-062)"),
    ("source_commit_ts", "TIMESTAMP", "source commit time, the real windowing column"),
    ("position_primary", "STRING",
     "engine ordering field: Oracle commit_scn, SQL Server commit_lsn (CLAUDE.md 5.5)"),
    ("position_secondary", "STRING",
     "engine ordering tie-break: Oracle scn, SQL Server change_lsn"),
    ("kafka_topic", "STRING", "Kafka coordinates: topic"),
    ("kafka_partition", "INT", "Kafka coordinates: partition"),
    ("kafka_offset", "BIGINT",
     "Kafka coordinates: offset. Monotonic WITHIN a partition only (CLAUDE.md 5.4)"),
    ("kafka_timestamp", "TIMESTAMP", "broker-assigned record time"),
    ("schema_global_id", "BIGINT",
     "apicurio.value.globalId of the writer schema that decoded this row -- the schema "
     "version. Without it a mis-decode cannot be traced to a schema"),
    ("ingested_at", "TIMESTAMP", "when this platform wrote the row"),
    ("ingest_run_id", "STRING", "which run wrote it"),
    ("ingest_job", "STRING", "which job wrote it: full_cdc_batch | full_cdc_stream"),
)

#: Payload columns per mode. `typed` is metadata + these + the typed structs.
PAYLOAD_JSON_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("payload_after", "STRING", "after image, to_json of the Debezium envelope"),
    ("payload_before", "STRING", "before image, to_json. NULL for an insert"),
)
PAYLOAD_TYPED_JSON_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("payload_after_json", "STRING",
     "after image as JSON, kept ALONGSIDE the typed struct: a writer version may carry a "
     "field the registry has not declared, and typed-only would drop it silently"),
    ("payload_before_json", "STRING", "before image as JSON, for the same reason"),
)

#: The global event index (section G): coordinates and pointers, never a payload. Keeping a
#: payload here would recreate the monolith under another name and widen PII exposure --
#: which is precisely what ADR-062 says the index must not do.
EVENT_INDEX_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("dv_event_id", "STRING", "delivered-record identity, the MERGE key"),
    ("dv_src_event_id", "STRING", "source-change identity"),
    ("table_id", "STRING", "canonical source identity: source.database.schema.table"),
    ("source_system", "STRING", "engine identity"),
    ("source_database", "STRING", "source database"),
    ("source_schema", "STRING", "source schema"),
    ("source_table", "STRING", "source table"),
    ("op", "STRING", "Debezium operation"),
    ("event_date", "DATE", "UTC date of source.ts_ms; the partition column"),
    ("source_commit_ts", "TIMESTAMP", "source commit time"),
    ("position_primary", "STRING", "engine ordering field"),
    ("kafka_topic", "STRING", "Kafka coordinates: topic"),
    ("kafka_partition", "INT", "Kafka coordinates: partition"),
    ("kafka_offset", "BIGINT", "Kafka coordinates: offset"),
    ("kafka_timestamp", "TIMESTAMP", "broker-assigned record time"),
    ("schema_global_id", "BIGINT", "writer schema id"),
    ("target_table", "STRING", "the per-table FULL_CDC table this event was routed to"),
    ("ingested_at", "TIMESTAMP", "when the index row was written"),
    ("ingest_run_id", "STRING", "which run wrote it"),
)

#: EOD is one row per PK as of a cutoff (CLAUDE.md 5.6/5.7). It is NOT the FULL_CDC shape:
#: the winning event's coordinates are kept for provenance, but the grain is the key.
EOD_METADATA: tuple[tuple[str, str, str], ...] = (
    ("business_date", "DATE", "the snapshot date. The partition column"),
    ("dv_pk_hash", "STRING", "the key this row is the state of"),
    ("pk_json", "STRING",
     "the primary-key values as JSON. Readable without knowing the source's column types, "
     "which the registry only knows when typed columns are declared"),
    ("op", "STRING", "operation of the WINNING event"),
    ("is_deleted", "BOOLEAN",
     "true when the winning event is a delete. Under delete_policy=exclude_from_snapshot "
     "such rows are not written at all; under soft_flag they are, with this set "
     "(CLAUDE.md 5.7 -- explicit either way, never ambiguous)"),
    ("dv_event_id", "STRING", "which FULL_CDC event this state came from"),
    ("source_system", "STRING", "engine identity"),
    ("source_database", "STRING", "source database"),
    ("source_schema", "STRING", "source schema"),
    ("source_table", "STRING", "source table"),
    ("source_commit_ts", "TIMESTAMP", "commit time of the winning event"),
    ("position_primary", "STRING", "ordering field of the winning event"),
    ("position_secondary", "STRING", "ordering tie-break of the winning event"),
    ("snapshot_run_id", "STRING", "which snapshot run produced this row"),
    ("snapshot_ts", "TIMESTAMP", "when it was produced"),
)

PAYLOAD_MODES = ("json", "typed")

# --------------------------------------------------------------------------- #
# DEBEZIUM SOURCE ENCODINGS -- the reason a typed temporal column needs one
#
# `decimal.handling.mode=precise` produces `org.apache.kafka.connect.data.Decimal`, which is
# a Kafka Connect BUILT-IN logical type, so the Avro converter carries it as bytes with an
# Avro decimal logical type and `from_avro` hands Spark a real DecimalType. Temporals do not
# work that way. Debezium's temporal types -- `io.debezium.time.Timestamp`,
# `MicroTimestamp`, `NanoTimestamp`, `Date` -- are CUSTOM Connect logical types, and an Avro
# converter carries a custom logical type as its underlying primitive. `from_avro` therefore
# yields a plain int64/int32, not a timestamp.
#
# Casting that number to `timestamp` does not fail. MEASURED on this platform's own Spark:
#
#     epoch millis 1755820800000  CAST AS timestamp  ->  +57609-09-30 00:00:00
#     epoch micros                CAST AS timestamp  ->  +109081-07-08 05:06:32
#     epoch days   (int32)        CAST AS date       ->  AnalysisException, batch dies
#
# A year-57609 timestamp in the canonical layer is a silent data-semantics change of exactly
# the kind the contract forbids -- it partitions, sorts and reconciles as a real value. So a
# typed temporal column MUST declare which Debezium encoding it arrives in, and the writer
# converts explicitly rather than casting. The compiler REFUSES a temporal column with no
# encoding; there is no default, because every possible default is wrong for some column.
# --------------------------------------------------------------------------- #

#: encoding -> (Spark expression template, the declared type it produces).
#: `{c}` is substituted with the raw decoded column reference.
SOURCE_ENCODINGS: dict[str, tuple[str, str]] = {
    # No conversion: the decoded value already IS the declared type. Correct for strings,
    # numbers, booleans and `decimal.handling.mode=precise` decimals.
    "none": ("{c}", ""),
    # io.debezium.time.Timestamp -- epoch MILLIS as int64. Oracle DATE and TIMESTAMP(0-3);
    # SQL Server DATETIME and DATETIME2(0-3).
    "timestamp_millis": ("timestamp_millis({c})", "timestamp"),
    # io.debezium.time.MicroTimestamp -- epoch MICROS as int64. Oracle TIMESTAMP(4-6) under
    # `adaptive_time_microseconds`; SQL Server DATETIME2(4-6) under `adaptive`.
    "micro_timestamp": ("timestamp_micros({c})", "timestamp"),
    # io.debezium.time.NanoTimestamp -- epoch NANOS as int64. Integer division, not a cast:
    # Spark has no nanosecond timestamp, so the sub-microsecond digits are dropped
    # DELIBERATELY and visibly here rather than silently by a narrowing conversion.
    "nano_timestamp": ("timestamp_micros({c} div 1000)", "timestamp"),
    # io.debezium.time.Date -- DAYS since epoch as int32. A direct cast is not merely wrong,
    # it is rejected by Spark ("cannot cast INT to DATE"), which kills the batch.
    "date_days": ("date_add(DATE'1970-01-01', {c})", "date"),
    # io.debezium.time.ZonedTimestamp -- ISO-8601 STRING. Parsed, not cast.
    "zoned_timestamp": ("to_timestamp({c})", "timestamp"),
}

#: Declared types that cannot be reached by a plain cast from what Debezium actually sends.
TEMPORAL_DECLARED_TYPES = frozenset({"timestamp", "date"})

DEFAULT_ENCODING = "none"


def encoding_expression(encoding: str, column_ref: str, *, what: str) -> str:
    """The Spark SQL expression that converts one decoded field. Pure; no Spark import."""
    try:
        template, _ = SOURCE_ENCODINGS[encoding]
    except KeyError:
        raise ConfigError(
            f"{what}: source encoding {encoding!r} is not one this platform converts. "
            f"Allowed: {', '.join(sorted(SOURCE_ENCODINGS))}") from None
    return template.format(c=column_ref)


def validate_encoding(sql_type: str, encoding: str, *, what: str) -> None:
    """Reject the combinations that would silently change what a value MEANS."""
    if encoding not in SOURCE_ENCODINGS:
        raise ConfigError(
            f"{what}: source encoding {encoding!r} is not one this platform converts. "
            f"Allowed: {', '.join(sorted(SOURCE_ENCODINGS))}")
    base = sql_type.split("(")[0]
    if encoding == DEFAULT_ENCODING:
        if base in TEMPORAL_DECLARED_TYPES:
            raise ConfigError(
                f"{what}: a typed {base} column must declare `encoding`. Debezium sends "
                f"temporals as plain int64/int32 (its semantic types are CUSTOM Connect "
                f"logical types, which an Avro converter carries as the underlying "
                f"primitive), so casting to {base} yields a year-57609 value SILENTLY or "
                f"fails the batch. Choose the encoding the connector actually produces: "
                f"{', '.join(e for e in sorted(SOURCE_ENCODINGS) if e != DEFAULT_ENCODING)}")
        return
    produced = SOURCE_ENCODINGS[encoding][1]
    if produced != base:
        raise ConfigError(
            f"{what}: encoding {encoding!r} produces a {produced}, but the column is "
            f"declared {sql_type!r}. Declare the type the conversion actually yields -- a "
            f"mismatch here is a wrong value, not a failed one")


#: Types the platform will generate DDL for. An allow-list rather than a passthrough: an
#: unrecognised type string reaches Glue as a table that Athena cannot read, and the failure
#: surfaces at query time against a table that already has data in it.
_SIMPLE_TYPES = frozenset({"string", "boolean", "tinyint", "smallint", "int", "bigint",
                           "float", "double", "date", "timestamp", "binary"})
_DECIMAL = re.compile(r"^decimal\(\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\)$")
#: Source column names are used verbatim as struct fields, so they must be spellable.
_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def normalise_type(raw: str, *, what: str) -> str:
    """Canonical lower-case SQL type, or a ConfigError naming the field."""
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError(f"{what}: a column needs a `type`")
    t = " ".join(raw.strip().lower().split())
    if t in _SIMPLE_TYPES:
        return t
    m = _DECIMAL.match(t)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        if not (1 <= p <= 38 and 0 <= s <= p):
            raise ConfigError(
                f"{what}: decimal({p},{s}) is out of range -- precision 1..38 and "
                f"scale 0..precision")
        return f"decimal({p},{s})"
    raise ConfigError(
        f"{what}: type {raw!r} is not one this platform generates DDL for. "
        f"Allowed: {', '.join(sorted(_SIMPLE_TYPES))}, decimal(p,s)")


def normalise_column_name(raw: str, *, what: str) -> str:
    if not isinstance(raw, str) or not _COLUMN_NAME.match(raw.strip()):
        raise ConfigError(
            f"{what}: column name {raw!r} is not a spellable identifier "
            f"(^[A-Za-z_][A-Za-z0-9_]*$). Source columns are used VERBATIM as struct "
            f"fields, so a name that needs quoting is rejected rather than mangled")
    return raw.strip()


def struct_type(columns: tuple[tuple[str, str], ...]) -> str:
    """`STRUCT<NAME: type, ...>` for the typed payload columns."""
    inner = ", ".join(f"{n}: {t}" for n, t in columns)
    return f"STRUCT<{inner}>"


def full_cdc_columns(entry: dict) -> tuple[tuple[str, str, str], ...]:
    """The FULL_CDC column list for ONE compiled plan entry.

    Takes the plan ENTRY (a dict), not a TableConfig, so the runtime path needs only the
    compiled artifact and the standard library -- the writer runs on EMR, where `yaml` and
    the loader are not guaranteed to be on the path, and where re-reading the registry would
    mean running against a config nobody hashed.
    """
    payload = entry.get("payload") or {}
    mode = payload.get("mode", "json")
    if mode not in PAYLOAD_MODES:
        raise ConfigError(f"{entry.get('table_id')}: payload.mode {mode!r} is not one of "
                          f"{', '.join(PAYLOAD_MODES)}")
    cols = list(FULL_CDC_METADATA)
    if mode == "json":
        cols += list(PAYLOAD_JSON_COLUMNS)
        return tuple(cols)

    declared = tuple((c["name"], c["type"]) for c in payload.get("columns") or ())
    if not declared:
        raise ConfigError(
            f"{entry.get('table_id')}: payload.mode=typed needs `payload.columns`. "
            f"Typed columns are DERIVED FROM CONFIG -- inferring them from whatever the "
            f"last writer version happened to carry is how two runs produce two schemas")
    st = struct_type(declared)
    cols += [("payload_after", st, "after image as a typed struct of declared columns"),
             ("payload_before", st, "before image as a typed struct. NULL for an insert")]
    cols += list(PAYLOAD_TYPED_JSON_COLUMNS)
    return tuple(cols)


def eod_columns(entry: dict) -> tuple[tuple[str, str, str], ...]:
    """EOD keeps the snapshot metadata plus the winning event's payload, in the same
    representation FULL_CDC uses for that table -- a snapshot in a different payload shape
    than its own history is a join nobody can write."""
    payload = entry.get("payload") or {}
    cols = list(EOD_METADATA)
    if payload.get("mode", "json") == "typed":
        st = struct_type(tuple((c["name"], c["type"]) for c in payload.get("columns") or ()))
        cols.append(("payload_after", st, "state as a typed struct"))
        cols.append(("payload_after_json", "STRING", "state as JSON, the lossless record"))
    else:
        cols.append(("payload_after", "STRING", "state as JSON"))
    return tuple(cols)


def realtime_columns(entry: dict) -> tuple[tuple[str, str, str], ...]:
    """REALTIME is a BOUNDED WINDOW of FULL_CDC, not a reshape of it -- `realtime/job.py`
    filters on `source_commit_ts` and writes the projection straight through. Same columns,
    deliberately: a window with a different schema than its source is a second contract to
    keep in step."""
    return full_cdc_columns(entry)


def column_names(columns: tuple[tuple[str, str, str], ...]) -> tuple[str, ...]:
    return tuple(c[0] for c in columns)


def ddl_column_list(columns: tuple[tuple[str, str, str], ...]) -> str:
    return ",\n  ".join(f"{name} {sql_type}" for name, sql_type, _ in columns)
