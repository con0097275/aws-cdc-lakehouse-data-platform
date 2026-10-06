"""Read-only source precheck (section A). Opens files; never a connection, never a mutation.

WHAT IT CHECKS AND WHY IT CAN CHECK IT OFFLINE
-----------------------------------------------
The things that make a table capturable are declared in Git, in the same scripts that
configure the source:

    docker/source-lab/oracle/01-init.sql       the table and its PRIMARY KEY
    docker/source-lab/oracle/02-enable-cdc.sql ARCHIVELOG, supplemental logging PER TABLE,
                                               and the LogMiner grants the capture user has
    docker/source-lab/sqlserver/01-init.sql    the table and its PRIMARY KEY
    docker/source-lab/sqlserver/02-enable-cdc.sql  sp_cdc_enable_db, the per-table capture
                                               list, and the capture role
    connectors/<engine>/*.json.tmpl            schema history topic, connector privileges

So the precheck runs with the lab torn down, with no AWS credential, at zero cost -- and
every finding names the file and the line an operator has to change. A precheck that needs
the platform up is one nobody runs before the platform is up, which is exactly when
onboarding decisions are made.

WHAT IT CANNOT CHECK, AND SAYS SO
----------------------------------
It reads DECLARED state, not LIVE state. A table can be declared with supplemental logging
in Git and not have it in a database somebody rebuilt by hand. Every result carries
`evidence_kind: "declared"` for exactly that reason, and `docs/CDC_TABLE_ONBOARDING.md`
carries the live queries that confirm it. Claiming the live check when only the declared one
ran would be the stub-that-lies defect this repository has already fixed once.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from .models import SourceEngine

_REPO = Path(__file__).resolve().parents[1]

SEVERITY_BLOCK = "BLOCK"     # onboarding must not proceed
SEVERITY_WARN = "WARN"       # proceed knowingly; the operator is told what they lose
SEVERITY_PASS = "PASS"


@dataclass(frozen=True)
class Finding:
    check: str
    severity: str
    detail: str
    #: The file an operator changes to fix it. Named because "supplemental logging missing"
    #: without a path is a finding that costs twenty minutes to act on.
    remediation: str = ""
    evidence_kind: str = "declared"

    @property
    def blocking(self) -> bool:
        return self.severity == SEVERITY_BLOCK


@dataclass
class PrecheckResult:
    table_id: str
    engine: str
    findings: list

    @property
    def blocking(self) -> tuple:
        return tuple(f for f in self.findings if f.blocking)

    @property
    def warnings(self) -> tuple:
        return tuple(f for f in self.findings if f.severity == SEVERITY_WARN)

    @property
    def passed(self) -> bool:
        return not self.blocking

    def payload(self) -> dict:
        return {"table_id": self.table_id, "engine": self.engine,
                "passed": self.passed,
                "findings": [{"check": f.check, "severity": f.severity,
                              "detail": f.detail, "remediation": f.remediation,
                              "evidence_kind": f.evidence_kind} for f in self.findings]}


def _read(path: Path) -> str:
    return path.read_text() if path.exists() else ""


def _connector_config(engine: str, repo: Path) -> dict:
    """The connector template's config, with placeholders neutralised.

    `${...}` values are replaced before parsing rather than after: the template is not valid
    JSON with them in place, and a partial parse would silently drop the very keys the
    checks below look for.
    """
    name = {"oracle": "connectors/oracle/corebank-source.json.tmpl",
            "sqlserver": "connectors/sqlserver/digital-source.json.tmpl"}.get(engine)
    if not name:
        return {}
    raw = _read(repo / name)
    if not raw:
        return {}
    try:
        return json.loads(re.sub(r"\$\{[^}]+\}", "PLACEHOLDER", raw)).get("config", {})
    except Exception:                                                  # noqa: BLE001
        return {}


def _schema_history_finding(cfg: dict, engine: str) -> Finding:
    topic = cfg.get("schema.history.internal.kafka.topic")
    if topic:
        return Finding("schema_history", SEVERITY_PASS,
                       f"schema history topic configured: {topic}")
    return Finding(
        "schema_history", SEVERITY_BLOCK,
        "no `schema.history.internal.kafka.topic` on the connector. Debezium replays the "
        "schema history on every restart; without it the connector cannot reconstruct the "
        "DDL for a captured table and will not start.",
        remediation=f"connectors/{engine}/*.json.tmpl")


def _pk_finding(table_id: str, primary_key, discovered) -> Finding:
    if not primary_key:
        return Finding(
            "primary_key", SEVERITY_BLOCK,
            f"{table_id} has no primary_key in the registry. An omitted "
            f"`message.key.columns` entry does not error: Debezium keys by its own default, "
            f"the same key scatters across partitions, and per-key ordering breaks with "
            f"every health check green (CLAUDE.md 5.1).",
            remediation="cdc/registry/sources.yaml")
    if discovered is None:
        return Finding(
            "primary_key", SEVERITY_WARN,
            f"the registry declares {list(primary_key)} but the table is not in the source "
            f"DDL, so the key could not be corroborated.",
            remediation="docker/source-lab/<engine>/01-init.sql")
    expected = tuple(discovered)
    if tuple(primary_key) != expected:
        return Finding(
            "primary_key", SEVERITY_BLOCK,
            f"the registry declares {list(primary_key)} but the source DDL declares "
            f"{list(expected)}. The registry's value is copied verbatim into "
            f"`message.key.columns`, so a mismatch keys the topic by the wrong column.",
            remediation="cdc/registry/sources.yaml")
    return Finding("primary_key", SEVERITY_PASS,
                   f"registry and source DDL agree: {list(primary_key)}")


def _table_exists_finding(table_id: str, schema: str, table: str, discovered) -> Finding:
    if discovered is None:
        return Finding(
            "table_exists", SEVERITY_BLOCK,
            f"{schema}.{table} is not declared in the source DDL. Capturing a table that "
            f"does not exist gives a connector that reports RUNNING and produces nothing.",
            remediation="docker/source-lab/<engine>/01-init.sql")
    return Finding("table_exists", SEVERITY_PASS,
                   f"{schema}.{table} declared with {len(discovered.columns)} columns")


# --------------------------------------------------------------------------- #
# Oracle
# --------------------------------------------------------------------------- #

ORACLE_CDC_SQL = "docker/source-lab/oracle/02-enable-cdc.sql"
#: The LogMiner grants Debezium's Oracle connector documents as required. Checked as a set
#: rather than a count: a missing LOGMINING is a different failure from a missing
#: SELECT ANY TRANSACTION, and both are silent until the first capture attempt.
ORACLE_REQUIRED_GRANTS = ("LOGMINING", "SELECT ANY TRANSACTION", "SELECT_CATALOG_ROLE",
                          "EXECUTE ON DBMS_LOGMNR", "SET CONTAINER", "FLASHBACK ANY TABLE")


def _oracle_checks(schema: str, table: str, repo: Path) -> list:
    sql = _read(repo / ORACLE_CDC_SQL)
    out = []

    # Supplemental logging PER TABLE. Database-level MIN is not enough: without ALL COLUMN
    # logging an UPDATE arrives with unchanged columns NULL, so every before/after
    # comparison downstream is silently wrong.
    pattern = re.compile(
        rf"ALTER\s+TABLE\s+{re.escape(schema)}\.{re.escape(table)}\s+"
        rf"ADD\s+SUPPLEMENTAL\s+LOG\s+DATA\s*\(\s*ALL\s*\)\s*COLUMNS",
        re.IGNORECASE)
    if pattern.search(sql):
        out.append(Finding("supplemental_logging", SEVERITY_PASS,
                           f"ALL COLUMN supplemental logging declared for {schema}.{table}"))
    else:
        out.append(Finding(
            "supplemental_logging", SEVERITY_BLOCK,
            f"no `ALTER TABLE {schema}.{table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS`. "
            f"Database-level MIN is not enough: without it an UPDATE arrives with unchanged "
            f"columns NULL and every before/after comparison downstream is wrong -- and it "
            f"fails as wrong data, never as an error.",
            remediation=ORACLE_CDC_SQL))

    # LogMiner needs redo to mine. Without ARCHIVELOG the connector produces NOTHING while
    # reporting healthy -- risk R15, recorded in the script's own header.
    if re.search(r"ALTER\s+DATABASE\s+ARCHIVELOG", sql, re.IGNORECASE):
        out.append(Finding("logminer_archivelog", SEVERITY_PASS,
                           "ARCHIVELOG enablement is declared"))
    else:
        out.append(Finding(
            "logminer_archivelog", SEVERITY_BLOCK,
            "no ARCHIVELOG enablement found. LogMiner has no redo to mine, so the connector "
            "produces nothing while reporting healthy (risk R15).",
            remediation=ORACLE_CDC_SQL))

    missing = [g for g in ORACLE_REQUIRED_GRANTS if g.upper() not in sql.upper()]
    if missing:
        out.append(Finding(
            "capture_privileges", SEVERITY_BLOCK,
            f"the capture user is missing grant(s): {', '.join(missing)}",
            remediation=ORACLE_CDC_SQL))
    else:
        out.append(Finding("capture_privileges", SEVERITY_PASS,
                           f"all {len(ORACLE_REQUIRED_GRANTS)} LogMiner grants declared"))
    return out


# --------------------------------------------------------------------------- #
# SQL Server
# --------------------------------------------------------------------------- #

SQLSERVER_CDC_SQL = "docker/source-lab/sqlserver/02-enable-cdc.sql"


def _sqlserver_checks(schema: str, table: str, repo: Path) -> list:
    sql = _read(repo / SQLSERVER_CDC_SQL)
    out = []

    if re.search(r"sp_cdc_enable_db", sql, re.IGNORECASE):
        out.append(Finding("cdc_enabled_database", SEVERITY_PASS,
                           "sys.sp_cdc_enable_db is declared"))
    else:
        out.append(Finding(
            "cdc_enabled_database", SEVERITY_BLOCK,
            "no `sys.sp_cdc_enable_db`. Table-level CDC cannot be enabled until the "
            "database is.", remediation=SQLSERVER_CDC_SQL))

    # The per-table capture list. `sp_cdc_enable_table` SUCCEEDS even when SQL Server Agent
    # is not running -- the script's own header calls that risk R15 in its most dangerous
    # form -- so being in the list is necessary and not sufficient, which the detail says.
    in_list = re.search(rf"'{re.escape(table)}'", sql) is not None
    enables = re.search(r"sp_cdc_enable_table", sql, re.IGNORECASE) is not None
    if in_list and enables:
        out.append(Finding(
            "cdc_enabled_table", SEVERITY_PASS,
            f"{table} is in the sp_cdc_enable_table list (a capture instance is created "
            f"per table). Note this is DECLARED state: sp_cdc_enable_table succeeds even "
            f"when SQL Server Agent is not running, so confirm the capture job live."))
    else:
        out.append(Finding(
            "cdc_enabled_table", SEVERITY_BLOCK,
            f"{table} is not in the per-table CDC enablement list. Debezium reads the "
            f"change table; without a capture instance there is nothing to read and the "
            f"connector reports RUNNING while producing nothing.",
            remediation=SQLSERVER_CDC_SQL))

    role = re.search(r"@role_name\s*=\s*N?'(\w+)'", sql)
    if role:
        out.append(Finding("capture_instance_role", SEVERITY_PASS,
                           f"capture instances are gated by role {role.group(1)!r}"))
    else:
        out.append(Finding(
            "capture_instance_role", SEVERITY_WARN,
            "no @role_name on sp_cdc_enable_table: the change tables are readable by any "
            "principal with table access rather than by a named role.",
            remediation=SQLSERVER_CDC_SQL))
    return out


# --------------------------------------------------------------------------- #

def precheck(entry: dict, *, repo: Path | None = None) -> PrecheckResult:
    """Every section-A check for one compiled plan entry. Pure apart from reading files."""
    from . import source_schema

    repo = repo or _REPO
    source = entry.get("source") or {}
    engine = source.get("engine", "")
    schema = source.get("schema", "")
    table = source.get("table", "")
    table_id = entry.get("table_id", f"{schema}.{table}")
    primary_key = tuple((entry.get("identity") or {}).get("primary_key") or ())

    try:
        engine_enum = SourceEngine(engine)
        discovered = source_schema.discover(engine_enum, schema, table, repo=repo)
    except Exception:                                                  # noqa: BLE001
        engine_enum, discovered = None, None

    findings = [_table_exists_finding(table_id, schema, table, discovered)]
    discovered_pk = None
    if discovered is not None and engine_enum is not None:
        discovered_pk = tuple(source_schema.fold_identifier(engine_enum, c)
                              for c in discovered.primary_key)
    findings.append(_pk_finding(table_id, primary_key, discovered_pk))

    if engine == SourceEngine.ORACLE.value:
        findings += _oracle_checks(schema, table, repo)
    elif engine == SourceEngine.SQLSERVER.value:
        findings += _sqlserver_checks(schema, table, repo)
    else:
        findings.append(Finding(
            "engine", SEVERITY_BLOCK,
            f"engine {engine!r} has no precheck. Onboarding a source this platform cannot "
            f"verify is onboarding it blind.", remediation="cdc/precheck.py"))

    findings.append(_schema_history_finding(_connector_config(engine, repo), engine))
    return PrecheckResult(table_id=table_id, engine=engine, findings=findings)
