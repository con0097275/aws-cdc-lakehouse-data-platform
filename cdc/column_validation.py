"""Validate declared column lineage against the REAL schemas in Glue.

WHY THIS EXISTS
---------------
`reporting/curated/entities.yaml` and the dbt manifest DECLARE column mappings. Declared is
not observed, so `cdc/lineage_graph.py` marks those edges `DERIVED` -- and the AI copilot
refuses to narrow a recovery to a column on anything weaker than `VALIDATED`.

There are two ways to make column-narrowed recovery possible. One is to relabel the edges,
which changes a string and nothing else. The other is to check that both endpoints of every
edge are real columns on real tables, and to promote only the edges that pass.

This module does the second. An edge whose upstream or downstream column does not exist is
not evidence of anything -- it is a mapping that would silently mis-scope a repair.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from .lineage_graph import LineageGraph

_URN = re.compile(r"urn:li:dataset:\(urn:li:dataPlatform:([^,]+),([^,]+),([^)]+)\)")


def parse_dataset_urn(urn: str) -> tuple[str, str, str]:
    """(platform, database.table, fabric). Raises on anything that is not a dataset urn."""
    m = _URN.match(urn)
    if not m:
        raise ValueError(f"not a dataset urn: {urn!r}")
    return m.group(1), m.group(2), m.group(3)


@dataclass(frozen=True)
class EdgeVerdict:
    upstream: str
    upstream_column: str
    downstream: str
    downstream_column: str
    ok: bool
    reason: str = ""

    def payload(self) -> dict:
        return {"upstream": self.upstream, "upstream_column": self.upstream_column,
                "downstream": self.downstream, "downstream_column": self.downstream_column,
                "validated": self.ok, "reason": self.reason}


class PayloadJsonReader:
    """Reads the KEYS inside a `payload_after` JSON column, via Athena.

    CDC layers store the source row as JSON in `payload_after string`, so the business
    columns are invisible to a Glue schema lookup -- `CUSTOMER_ID` is not a column on the
    EOD table, it is a key inside a blob. Declaring such an edge unvalidatable would be
    wrong: the column is real and present in every row. It is just that the only way to
    confirm it is to look at the DATA rather than at the metadata.

    One query per table, not per column: the key set is read once and cached.
    """

    def __init__(self, athena_query=None, sample_rows: int = 200):
        self._q = athena_query
        self._rows = sample_rows
        self._cache: dict[str, set[str] | None] = {}

    def keys(self, db_table: str) -> set[str] | None:
        if db_table in self._cache:
            return self._cache[db_table]
        if self._q is None:
            self._cache[db_table] = None
            return None
        sql = (f"SELECT DISTINCT k FROM {db_table} "
               f"CROSS JOIN UNNEST(map_keys(CAST(json_parse(payload_after) AS "
               f"MAP<VARCHAR, JSON>))) AS t(k) LIMIT 500")
        try:
            rows = self._q(sql)
            self._cache[db_table] = {r[0].lower() for r in rows if r and r[0]} or None
        except Exception:                        # noqa: BLE001 - absence is a verdict
            self._cache[db_table] = None
        return self._cache[db_table]


class GlueSchemaReader:
    """Reads and caches real column names per Glue table.

    A table that does not exist is NOT an error here: it is a verdict. An edge pointing at a
    table nobody created is exactly the drift this validation is for.
    """

    def __init__(self, client=None):
        self._client = client
        self._cache: dict[str, set[str] | None] = {}

    def _glue(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("glue")
        return self._client

    def columns(self, db_table: str) -> set[str] | None:
        if db_table in self._cache:
            return self._cache[db_table]
        try:
            db, table = db_table.split(".", 1)
        except ValueError:
            self._cache[db_table] = None
            return None
        try:
            t = self._glue().get_table(DatabaseName=db, Name=table)["Table"]
            sd = t.get("StorageDescriptor", {}) or {}
            cols = {c["Name"].lower() for c in sd.get("Columns", []) or []}
            cols |= {c["Name"].lower() for c in t.get("PartitionKeys", []) or []}
            self._cache[db_table] = cols or None
        except Exception:                       # noqa: BLE001 - absence is a verdict
            self._cache[db_table] = None
        return self._cache[db_table]


def validate_column_edges(graph: LineageGraph, reader: GlueSchemaReader | None = None,
                          payload: "PayloadJsonReader | None" = None
                          ) -> tuple[EdgeVerdict, ...]:
    """One verdict per column edge. Nothing is mutated here; promotion is a separate step.

    A column is confirmed if it is a top-level Glue column OR a key inside that table's
    `payload_after` JSON. Both are "the column is really there"; only the place differs.
    """
    reader = reader or GlueSchemaReader()
    out: list[EdgeVerdict] = []
    for e in graph.column_edges():
        try:
            _, up_tbl, _ = parse_dataset_urn(e.upstream_dataset)
            _, down_tbl, _ = parse_dataset_urn(e.downstream_dataset)
        except ValueError as err:
            out.append(EdgeVerdict(e.upstream_dataset, e.upstream_column,
                                   e.downstream_dataset, e.downstream_column,
                                   False, str(err)))
            continue
        up_cols, down_cols = reader.columns(up_tbl), reader.columns(down_tbl)

        def _has(table, cols, column):
            if cols is None:
                return False, f"table {table} does not exist in Glue"
            if column.lower() in cols:
                return True, ""
            if payload is not None and "payload_after" in cols:
                keys = payload.keys(table)
                if keys and column.lower() in keys:
                    return True, ""
                return False, (f"{table}.payload_after has no key {column}"
                               if keys else f"{table} payload_after unreadable")
            return False, f"{table} has no column {column}"

        ok_up, reason = _has(up_tbl, up_cols, e.upstream_column)
        if ok_up:
            ok_down, reason = _has(down_tbl, down_cols, e.downstream_column)
        out.append(EdgeVerdict(up_tbl, e.upstream_column, down_tbl, e.downstream_column,
                               not reason, reason))
    return tuple(out)


def summarise(verdicts: tuple[EdgeVerdict, ...]) -> dict:
    ok = [v for v in verdicts if v.ok]
    bad = [v for v in verdicts if not v.ok]
    reasons: dict[str, int] = {}
    for v in bad:
        key = re.sub(r"kafka_dev_lab_dev_\w+", "<table>", v.reason)
        reasons[key] = reasons.get(key, 0) + 1
    return {"total": len(verdicts), "validated": len(ok), "failed": len(bad),
            "validated_pct": round(100.0 * len(ok) / len(verdicts), 1) if verdicts else 0.0,
            "failure_reasons": reasons,
            "validated_pairs": sorted(f"{v.upstream}.{v.upstream_column}"
                                      f" -> {v.downstream}.{v.downstream_column}" for v in ok)}
