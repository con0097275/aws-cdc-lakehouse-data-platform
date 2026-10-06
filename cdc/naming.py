"""Deterministic identity and target-name derivation (sections B and C).

Every name this module produces is a PURE function of the canonical table id. Nothing is
typed twice, so nothing can disagree -- which is the whole point: the three silent failure
modes recorded in docs/CDC_TABLE_ONBOARDING_DESIGN.md section 1 are all "the same fact
written in two places, and one of them was wrong".
"""
from __future__ import annotations

import re

from .models import ConfigError, SourceEngine

#: Iceberg/Glue identifiers: lowercase, alphanumeric + underscore, must not start with a
#: digit. Anything else is rejected rather than mangled -- silently rewriting a name is how
#: a table ends up somewhere nobody looks for it.
_SAFE = re.compile(r"^[a-z][a-z0-9_]*$")


def normalize_ident(raw: str, *, what: str) -> str:
    """Fold to the catalog's canonical form, then REJECT anything still unsafe.

    Oracle folds unquoted identifiers to UPPERCASE and SQL Server does not, so the same
    logical table arrives spelled two ways. Normalising here means the rest of the platform
    never has to care -- and `topic_for` below deliberately does NOT use this, because Kafka
    topic names must match what the connector actually produces, not what reads nicely.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError(f"{what}: must be a non-empty string")
    ident = raw.strip().lower().replace("-", "_").replace(".", "_")
    if not _SAFE.match(ident):
        raise ConfigError(
            f"{what}: {raw!r} normalises to {ident!r}, which is not a safe catalog "
            f"identifier (need ^[a-z][a-z0-9_]*$). Rename the source object or set an "
            f"explicit override rather than relying on mangling.")
    return ident


def table_id(source_id: str, database: str, schema: str, table: str) -> str:
    """Canonical identity. THE key for collision detection (section B)."""
    return ".".join([
        normalize_ident(source_id, what="source_id"),
        normalize_ident(database, what="database"),
        normalize_ident(schema, what="schema"),
        normalize_ident(table, what="table"),
    ])


def logical_name(source_id: str, database: str, schema: str, table: str) -> str:
    """The ONE logical identifier shared by FULL_CDC, REALTIME and EOD (section C).

        oracle + coredb + bank + account  ->  oracle_coredb_bank_account

    Layer prefixes are applied by the callers below, never spelled out per table, so the
    three layer names for a table cannot drift apart.
    """
    return "_".join(table_id(source_id, database, schema, table).split("."))


def full_cdc_table(source_id: str, database: str, schema: str, table: str) -> str:
    return f"cdc_{logical_name(source_id, database, schema, table)}"


def realtime_table(source_id: str, database: str, schema: str, table: str) -> str:
    return f"rt_{logical_name(source_id, database, schema, table)}"


def eod_table(source_id: str, database: str, schema: str, table: str) -> str:
    return f"eod_{logical_name(source_id, database, schema, table)}"


def topic_for(engine: SourceEngine, *, topic_prefix: str, database: str,
              schema: str, table: str) -> str:
    """The topic the CONNECTOR will actually produce to. Casing is engine-specific.

    Oracle folds unquoted identifiers to UPPERCASE, so `cdc.oracle.corebank.account` is a
    topic Debezium never writes to: with `auto.create.topics.enable=false` the connector
    reports RUNNING, the topic sits at offset 0, and every health check stays green. That
    exact failure is in this repo's history, which is why the rule lives in one function.

    SQL Server includes the DATABASE component and does not fold.
    """
    if engine is SourceEngine.ORACLE:
        return f"{topic_prefix}.{schema.upper()}.{table.upper()}"
    return f"{topic_prefix}.{database}.{schema}.{table}"


def include_list_entry(engine: SourceEngine, *, schema: str, table: str) -> str:
    """The `table.include.list` element, in the engine's own spelling."""
    if engine is SourceEngine.ORACLE:
        return f"{schema.upper()}.{table.upper()}"
    return f"{schema}.{table}"


def key_columns_entry(engine: SourceEngine, *, schema: str, table: str,
                      primary_key: tuple[str, ...]) -> str:
    """The `message.key.columns` element -- the SECOND place a new table must be declared,
    and the one most often forgotten. Omitting it does not error: Debezium keys by its own
    default and the same PK scatters across partitions, silently breaking per-key ordering
    (CLAUDE.md 5.1)."""
    ident = include_list_entry(engine, schema=schema, table=table)
    cols = ",".join(primary_key)
    return f"{ident}:{cols}"
