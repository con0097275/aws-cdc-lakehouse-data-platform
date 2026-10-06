"""Read-only discovery of a source table's columns and primary key (section G).

The authority is `docker/source-lab/<engine>/01-init.sql` -- the DDL that CREATES the tables
the connectors capture. Parsing it is read-only *by construction*: this module opens a file
and never a connection, so `--discover` works with the lab torn down, with no AWS credential
and at zero cost, and it cannot mutate the source under any argument.

WHY NOT QUERY THE LIVE SOURCE
------------------------------
The previous implementation claimed to: its docstring said it "reads the source's catalog
READ-ONLY (columns and primary key) over SSM". It did not. It built a SQL string, never used
it, invoked `aws ssm send-command` with no `--instance-ids` and no `--parameters` (a call
that cannot succeed), and returned `([], [])` unconditionally -- so `--discover` silently did
nothing while reporting success. A stub that lies is worse than an absent feature: the
operator believes the primary key was verified against the source when nothing was verified
at all.

Querying the live source is still the more authoritative answer and remains a legitimate
future addition; it is not this one, because it cannot be written honestly without a live
lab to test it against, and an untested SSM path is how the stub above came to exist.

THE ENCODING IS NOT OPTIONAL FOR A TEMPORAL
-------------------------------------------
Discovery returns a Debezium ENCODING alongside each type, derived from the column's source
type AND the connector's `time.precision.mode`. Without it a scaffolded `timestamp` column
would compile to a plain cast, and a Debezium epoch-millis int64 cast to `timestamp` is a
year-57609 value that no error reports (see `cdc/rowspec.py`). A column whose safe encoding
cannot be determined is EXCLUDED from the typed block with a reason, never guessed -- the
JSON images are always kept, so excluding a column loses nothing.

THE TYPES ARE A STARTING POINT, NOT A CONTRACT
-----------------------------------------------
The final authority on a column's type in the lake is the CONNECTOR's mapping, not the
source DDL: `decimal.handling.mode=precise` and Debezium's own temporal mapping both sit
between the two. So discovered types are emitted as a scaffold the operator reviews, and the
scaffold says so. What discovery is genuinely authoritative about is the COLUMN NAMES and
the PRIMARY KEY -- the two fields whose omission this platform has already been bitten by
(`message.key.columns` missing, scattering a key across partitions).
"""
from __future__ import annotations

import re
from pathlib import Path

from .models import ConfigError, SourceEngine
from .rowspec import DEFAULT_ENCODING

#: The DDL that creates each engine's source tables.
DDL_BY_ENGINE = {
    SourceEngine.ORACLE: Path("docker/source-lab/oracle/01-init.sql"),
    SourceEngine.SQLSERVER: Path("docker/source-lab/sqlserver/01-init.sql"),
}

_REPO = Path(__file__).resolve().parents[1]

#: `CREATE TABLE <schema>.<table> ( ... );` -- non-greedy to the first `);` at line start,
#: which is how both files terminate a table and no other statement in them does.
_CREATE = re.compile(
    r"CREATE\s+TABLE\s+(?:\[?(?P<schema>\w+)\]?\.)?\[?(?P<table>\w+)\]?\s*\((?P<body>.*?)\n\s*\)\s*;",
    re.IGNORECASE | re.DOTALL)
_PK = re.compile(r"PRIMARY\s+KEY\s*\((?P<cols>[^)]*)\)", re.IGNORECASE)
#: A column line: name, then the type up to the first comma at depth 0. Constraint lines are
#: excluded by the keyword test in `_columns`, not by this pattern.
_COLUMN = re.compile(r"^\s*\[?(?P<name>\w+)\]?\s+(?P<type>[A-Za-z_][\w]*(?:\s*\([^)]*\))?)")

#: Lines that open a table-level constraint or index rather than a column.
_NOT_A_COLUMN = re.compile(
    r"^\s*(CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK|INDEX|KEY)\b", re.IGNORECASE)


def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are NOT inside parentheses.

    A naive `split(",")` breaks `NUMBER(18,2)` in half and turns a decimal column into two
    unparseable fragments -- silently, because the fragments simply fail to match and the
    column vanishes from the scaffold.
    """
    parts, depth, current = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    parts.append("".join(current))
    return parts


def _strip_comments(text: str) -> str:
    """Drop `--` comments. Both files carry commentary INSIDE the column list, and a comment
    line that happens to start with a word would otherwise parse as a column."""
    return "\n".join(re.sub(r"--.*$", "", line) for line in text.splitlines())


# Oracle. NUMBER is the interesting one: with a scale it is a decimal and with none it is an
# integer, and getting that backwards makes every downstream sum non-reproducible -- which
# is the exact reason 01-init.sql itself carries a comment about NUMBER(18,2) vs FLOAT.
def _oracle_type(raw: str) -> tuple[str, str] | None:
    """(declared type, Debezium encoding), or None when no SAFE mapping exists.

    Encodings follow the deployed connector's `time.precision.mode:
    adaptive_time_microseconds` (connectors/oracle/corebank-source.json.tmpl): Oracle DATE
    and TIMESTAMP(<=3) arrive as epoch MILLIS, TIMESTAMP(4-6) as MICROS, TIMESTAMP(7-9) as
    NANOS. Changing that connector setting changes these answers, which is why the mode is
    named here rather than assumed.
    """
    t = raw.strip().upper()
    base = t.split("(")[0].strip()
    args = re.findall(r"\d+", t[len(base):])
    if base == "DATE":
        # Oracle DATE carries a TIME component and Debezium sends it as epoch millis.
        # Mapping it to `date` would truncate the time on every row.
        return "timestamp", "timestamp_millis"
    if base == "TIMESTAMP":
        precision = int(args[0]) if args else 6
        if precision <= 3:
            return "timestamp", "timestamp_millis"
        if precision <= 6:
            return "timestamp", "micro_timestamp"
        return "timestamp", "nano_timestamp"
    if base == "NUMBER":
        if len(args) >= 2 and int(args[1]) > 0:
            return f"decimal({args[0]},{args[1]})", DEFAULT_ENCODING
        precision = int(args[0]) if args else 38
        if precision <= 9:
            return "int", DEFAULT_ENCODING
        if precision <= 18:
            return "bigint", DEFAULT_ENCODING
        return f"decimal({precision},0)", DEFAULT_ENCODING
    if base in ("FLOAT", "BINARY_DOUBLE"):
        return "double", DEFAULT_ENCODING
    if base == "BINARY_FLOAT":
        return "float", DEFAULT_ENCODING
    if base in ("VARCHAR2", "NVARCHAR2", "VARCHAR", "CHAR", "NCHAR", "CLOB", "NCLOB"):
        return "string", DEFAULT_ENCODING
    if base in ("BLOB", "RAW"):
        return "binary", DEFAULT_ENCODING
    # INTERVAL, ROWID, XMLTYPE, TIMESTAMP WITH TIME ZONE and anything else: no safe mapping.
    # Excluded from the typed block rather than flattened to a string, which would be a
    # semantic claim this module cannot support.
    return None


def _sqlserver_type(raw: str) -> tuple[str, str] | None:
    """(declared type, Debezium encoding), or None when no SAFE mapping exists.

    Encodings follow `time.precision.mode: adaptive`
    (connectors/sqlserver/digital-source.json.tmpl): DATE arrives as a DAY COUNT, DATETIME
    and DATETIME2(<=3) as millis, DATETIME2(4-6) as micros, DATETIME2(7) as nanos.
    """
    t = raw.strip().upper()
    base = t.split("(")[0].strip()
    args = re.findall(r"\d+", t[len(base):])
    if base in ("DECIMAL", "NUMERIC") and len(args) >= 2:
        return f"decimal({args[0]},{args[1]})", DEFAULT_ENCODING
    if base == "MONEY":
        return "decimal(19,4)", DEFAULT_ENCODING
    if base == "BIGINT":
        return "bigint", DEFAULT_ENCODING
    if base == "INT":
        return "int", DEFAULT_ENCODING
    if base == "SMALLINT":
        return "smallint", DEFAULT_ENCODING
    if base == "TINYINT":
        return "tinyint", DEFAULT_ENCODING
    if base == "BIT":
        return "boolean", DEFAULT_ENCODING
    if base in ("FLOAT", "REAL"):
        return "double", DEFAULT_ENCODING
    if base == "DATE":
        return "date", "date_days"
    if base in ("DATETIME", "SMALLDATETIME"):
        return "timestamp", "timestamp_millis"
    if base == "DATETIME2":
        precision = int(args[0]) if args else 7
        if precision <= 3:
            return "timestamp", "timestamp_millis"
        if precision <= 6:
            return "timestamp", "micro_timestamp"
        return "timestamp", "nano_timestamp"
    if base == "DATETIMEOFFSET":
        # io.debezium.time.ZonedTimestamp -- an ISO-8601 STRING, parsed rather than cast.
        return "timestamp", "zoned_timestamp"
    if base in ("VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT", "NTEXT", "XML",
                "UNIQUEIDENTIFIER"):
        return "string", DEFAULT_ENCODING
    if base in ("BINARY", "VARBINARY", "IMAGE"):
        return "binary", DEFAULT_ENCODING
    # TIME arrives as micros SINCE MIDNIGHT, which is not any type this platform declares;
    # excluded rather than turned into a meaningless timestamp.
    return None


_MAPPERS = {SourceEngine.ORACLE: _oracle_type, SourceEngine.SQLSERVER: _sqlserver_type}


class DiscoveredTable:
    """Columns and primary key, as read from the source DDL."""

    def __init__(self, schema: str, table: str,
                 columns: tuple[tuple[str, str, str], ...], primary_key: tuple[str, ...],
                 unmapped: tuple[str, ...] = ()):
        self.schema = schema
        self.table = table
        #: (name, declared type, Debezium encoding)
        self.columns = columns
        self.primary_key = primary_key
        #: Columns present in the DDL that have no SAFE typed mapping. Reported, never
        #: guessed -- the JSON images keep them regardless.
        self.unmapped = unmapped

    def __repr__(self) -> str:                                          # pragma: no cover
        return (f"DiscoveredTable({self.schema}.{self.table}, "
                f"{len(self.columns)} cols, pk={self.primary_key})")


def _columns(body: str, engine: SourceEngine):
    """(mapped columns, names with no safe mapping)."""
    mapper = _MAPPERS[engine]
    out, unmapped = [], []
    for part in _split_top_level(body):
        if not part.strip() or _NOT_A_COLUMN.match(part):
            continue
        m = _COLUMN.match(part)
        if not m:
            continue
        mapped = mapper(m.group("type"))
        if mapped is None:
            unmapped.append(m.group("name"))
            continue
        out.append((m.group("name"), mapped[0], mapped[1]))
    return tuple(out), tuple(unmapped)


def _primary_key(body: str, columns) -> tuple[str, ...]:
    """The declared PK, with each column spelled as the COLUMN LIST spells it.

    A constraint may name a column in a different case than its declaration, and the PK
    columns are copied verbatim into `message.key.columns` -- where a case mismatch is not
    an error, it is a key Debezium resolves differently.
    """
    m = _PK.search(body)
    if not m:
        return ()
    declared = {c[0].lower(): c[0] for c in columns}
    names = [c.strip().strip("[]") for c in m.group("cols").split(",") if c.strip()]
    return tuple(declared.get(n.lower(), n) for n in names)


def parse_ddl(text: str, engine: SourceEngine) -> dict[str, DiscoveredTable]:
    """Every table in one DDL file, keyed `schema.table` lower-cased. Pure: no I/O."""
    text = _strip_comments(text)
    found: dict[str, DiscoveredTable] = {}
    for m in _CREATE.finditer(text):
        schema = (m.group("schema") or "").strip()
        table = m.group("table").strip()
        body = m.group("body")
        columns, unmapped = _columns(body, engine)
        if not columns:
            continue
        found[f"{schema}.{table}".lower()] = DiscoveredTable(
            schema=schema, table=table, columns=columns,
            primary_key=_primary_key(body, columns), unmapped=unmapped)
    return found


def discover(engine: SourceEngine, schema: str, table: str, *,
             repo: Path | None = None) -> DiscoveredTable | None:
    """One table, or None when the DDL has nothing by that name.

    None rather than a raise, and rather than a guess: an unreachable or silent source must
    not block scaffolding, and a WRONG primary key is worse than no primary key -- it is
    accepted without question and scatters the key across partitions (CLAUDE.md 5.1). The
    caller falls back to a placeholder the operator has to fill in, which is a prompt rather
    than a lie.
    """
    path = (repo or _REPO) / DDL_BY_ENGINE[engine]
    if not path.exists():
        return None
    tables = parse_ddl(path.read_text(), engine)
    return tables.get(f"{schema}.{table}".lower())


def fold_identifier(engine: SourceEngine, name: str) -> str:
    """Spell a column the way the CONNECTOR will see it.

    Oracle folds unquoted identifiers to UPPERCASE, so `01-init.sql` declares `account_id`
    and Debezium emits `ACCOUNT_ID`. The primary key is copied verbatim into
    `message.key.columns`, where the wrong case is not an error -- Debezium simply resolves
    a different key, the same PK scatters across partitions, and per-key ordering breaks
    with every health check green (CLAUDE.md 5.1). This is the same engine rule
    `naming.topic_for` encodes for topics, applied to columns.
    """
    return name.upper() if engine is SourceEngine.ORACLE else name


def discovery_source(engine: SourceEngine) -> str:
    """What a scaffold should cite as the provenance of its discovered fields."""
    if engine not in DDL_BY_ENGINE:
        raise ConfigError(f"no DDL registered for engine {engine!r}")
    return str(DDL_BY_ENGINE[engine])
