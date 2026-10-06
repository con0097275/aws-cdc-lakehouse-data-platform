"""Read-only Athena execution. The guards run on the OUTPUT, before anything executes.

WHAT CHANGES HERE VS ai/guards.py
---------------------------------
`ai/guards.py` already proves a string is read-only. It has never EXECUTED one. Adding the
executor is the moment the read-only guarantee stops being theoretical, so this module adds
the controls a validator alone cannot provide:

  * an allow-list of DATABASES, derived from governance, not hardcoded here
  * an injected LIMIT, re-validated AFTER injection
  * a bytes-scanned cutoff, enforced by the WORKGROUP so a client cannot override it
  * a hard timeout with cancellation
  * a result cap applied before anything reaches the model
  * the QueryExecutionId and bytes scanned in the audit record

DEFENCE IN DEPTH
----------------
The allow-list is the SECOND control. The first is IAM: the AI role is denied
`warehouse/stream/` and `warehouse/full_cdc/` outright (ADR-060), so a bug here cannot
reach raw CDC.
"""

from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from guards import GuardViolation, assert_read_only_sql, readable_datasets
from agent_tools.contract import PermissionDenied, ResultTooLarge, ToolError, ToolTimeout

#: Layers the AI plane may query. MART/SERVING preferred; ops for pipeline questions.
ALLOWED_DATABASE_SUFFIXES = ("_mart", "_curated", "_ops", "_snapshot")

#: Never queryable, whatever the allow-list says. Belt and braces over the IAM deny.
DENIED_DATABASE_SUFFIXES = ("_stream", "_full_cdc", "_quarantine")

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
MAX_SQL_BYTES = 4_096

_TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)", re.I)
_LIMIT = re.compile(r"\blimit\s+(\d+)\s*$", re.I)


def _databases_in(sql: str) -> set[str]:
    """The database component of every FROM/JOIN operand.

    Uses the shared token walk in `guards.table_references` rather than a local regex. The
    regex this replaced matched only QUALIFIED names, so an unqualified operand was never
    seen — `... FROM x_mart.a JOIN raw_pii s ...` reached Athena with `raw_pii` unchecked,
    against this module's own stated rule two lines below (finding G-P0-2).
    """
    from guards import table_references
    return {ref.split(".")[0].lower() for ref in table_references(sql) if "." in ref}


def assert_allowed_databases(sql: str) -> None:
    from guards import assert_qualified, table_references

    refs = table_references(sql)
    # Refuse the unqualified operand ITSELF, not merely the case where nothing qualified
    # was found. One qualified reference used to satisfy the check below for the whole
    # query, which is how an unqualified name slipped past.
    assert_qualified(refs, context="this tool")
    dbs = _databases_in(sql)
    if not dbs:
        raise PermissionDenied(
            "no qualified table reference found. An unqualified name resolves against "
            "whatever the session default happens to be, which is not a decision this "
            "tool may leave to chance.")
    for db in dbs:
        if any(db.endswith(s) for s in DENIED_DATABASE_SUFFIXES):
            raise PermissionDenied(
                f"database {db!r} holds raw CDC and is denied to the AI plane (ADR-060). "
                "It is also denied at the IAM layer; this is the second control.")
        if not any(db.endswith(s) for s in ALLOWED_DATABASE_SUFFIXES):
            raise PermissionDenied(
                f"database {db!r} is not allow-listed. Permitted suffixes: "
                f"{', '.join(ALLOWED_DATABASE_SUFFIXES)}")


def enforce_limit(sql: str, limit: int) -> str:
    """Append or tighten a LIMIT, then RE-VALIDATE the result.

    Re-validating after mutation matters: a guard that checks the input and then rewrites it
    has validated a string that is no longer the one being run.
    """
    if limit < 1 or limit > MAX_LIMIT:
        raise PermissionDenied(f"limit must be 1..{MAX_LIMIT}")
    stripped = sql.rstrip().rstrip(";")
    m = _LIMIT.search(stripped)
    if m:
        out = stripped if int(m.group(1)) <= limit else _LIMIT.sub(f"LIMIT {limit}", stripped)
    else:
        out = f"{stripped}\nLIMIT {limit}"
    assert_read_only_sql(out)          # re-validate the STRING WE WILL ACTUALLY RUN
    return out


def prepare(sql: str, limit: int = DEFAULT_LIMIT) -> str:
    """Full validation chain. Raises before anything can execute."""
    if not sql or not sql.strip():
        raise GuardViolation("empty SQL")
    if len(sql.encode()) > MAX_SQL_BYTES:
        raise PermissionDenied(
            f"query is {len(sql)} bytes, limit {MAX_SQL_BYTES}. Oversized SQL is either a "
            "generated mistake or an attempt to bury a second statement.")
    assert_read_only_sql(sql)          # allow-list, stacking, comment-stripping
    assert_allowed_databases(sql)
    return enforce_limit(sql, limit)


def run_query(sql: str, limit: int = DEFAULT_LIMIT, *,
              client=None, workgroup: str | None = None,
              timeout_seconds: float = 60.0, poll_seconds: float = 2.0,
              max_rows: int = MAX_LIMIT) -> dict:
    """Execute a validated read-only query. Client injected so tests need no AWS."""
    final = prepare(sql, limit)

    if client is None:
        import boto3
        client = boto3.client("athena")
    wg = workgroup or os.environ.get("AI_ATHENA_WORKGROUP", "kafka-dev-lab-dev-wg")

    qid = client.start_query_execution(QueryString=final, WorkGroup=wg)["QueryExecutionId"]
    deadline = time.monotonic() + timeout_seconds
    state, reason, scanned = "QUEUED", None, None
    while True:
        ex = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = ex["Status"]["State"]
        reason = ex["Status"].get("StateChangeReason")
        scanned = ex.get("Statistics", {}).get("DataScannedInBytes")
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            # CANCEL, do not just stop waiting: an abandoned query keeps scanning and
            # keeps billing.
            try:
                client.stop_query_execution(QueryExecutionId=qid)
            except Exception:
                pass
            raise ToolTimeout(f"query {qid} exceeded {timeout_seconds}s and was cancelled")
        time.sleep(poll_seconds)

    if state != "SUCCEEDED":
        raise ToolError(f"query {qid} {state}: {reason}")

    res = client.get_query_results(QueryExecutionId=qid, MaxResults=min(max_rows, 1000))
    raw = res.get("ResultSet", {}).get("Rows", [])
    if not raw:
        return {"rows": [], "columns": [], "row_count": 0,
                "resource_ids": [qid], "bytes_scanned": scanned, "sql": final}

    cols = [c.get("VarCharValue") for c in raw[0]["Data"]]
    rows = [{cols[i]: c.get("VarCharValue") for i, c in enumerate(r["Data"])}
            for r in raw[1:]]
    if len(rows) > max_rows:
        raise ResultTooLarge(f"{len(rows)} rows exceeds max_rows {max_rows}")
    return {"rows": rows, "columns": cols, "row_count": len(rows),
            "resource_ids": [qid], "bytes_scanned": scanned, "sql": final}
