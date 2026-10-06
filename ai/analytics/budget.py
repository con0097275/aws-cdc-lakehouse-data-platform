"""Per-request analytical budget, with a result cache.

Without this an agent asked "why did deposits fall" happily issues one query per dimension
per period and bills twenty scans for one question. The budget is enforced by the OBJECT
that owns the queries, not by asking the planner to behave: a limit that depends on
cooperation is not a limit.

Caching is by SQL hash WITHIN one request. Not across requests: a cached value that outlives
the question is how a "live" dashboard shows yesterday's number.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field


class BudgetExceeded(RuntimeError):
    pass


@dataclass
class QueryBudget:
    max_queries: int = 6
    max_dimensions: int = 3
    max_history_days: int = 400
    max_rows: int = 1000
    max_bytes_scanned: int = 10 * 1024 ** 3          # matches the workgroup cutoff

    queries_run: int = 0
    dimensions_analysed: int = 0
    bytes_scanned: int = 0
    cache_hits: int = 0
    _cache: dict = field(default_factory=dict, repr=False)

    def _key(self, sql: str) -> str:
        return hashlib.sha256(sql.encode()).hexdigest()[:16]

    def check_dimension(self) -> None:
        if self.dimensions_analysed + 1 > self.max_dimensions:
            raise BudgetExceeded(
                f"analysing another dimension exceeds max_dimensions="
                f"{self.max_dimensions}. Dimensions are evaluated independently and ranked; "
                "widening the sweep costs a scan each and yields cells too small to read.")
        self.dimensions_analysed += 1

    def check_history(self, days: int) -> None:
        if days > self.max_history_days:
            raise BudgetExceeded(f"{days} days exceeds max_history_days="
                                 f"{self.max_history_days}")

    def run(self, sql: str, runner) -> dict:
        key = self._key(sql)
        if key in self._cache:
            self.cache_hits += 1
            return self._cache[key]
        if self.queries_run + 1 > self.max_queries:
            raise BudgetExceeded(
                f"this request has already run {self.queries_run} queries; "
                f"max_queries={self.max_queries}. Refusing rather than continuing to bill.")
        out = runner(sql)
        self.queries_run += 1
        scanned = out.get("bytes_scanned") or 0
        self.bytes_scanned += scanned
        if self.bytes_scanned > self.max_bytes_scanned:
            raise BudgetExceeded(
                f"{self.bytes_scanned:,} bytes scanned exceeds "
                f"{self.max_bytes_scanned:,} for this request")
        if out.get("row_count", 0) > self.max_rows:
            raise BudgetExceeded(f"{out['row_count']} rows exceeds max_rows={self.max_rows}")
        self._cache[key] = out
        return out

    def to_dict(self) -> dict:
        return {"queries_run": self.queries_run, "max_queries": self.max_queries,
                "dimensions_analysed": self.dimensions_analysed,
                "max_dimensions": self.max_dimensions,
                "bytes_scanned": self.bytes_scanned,
                "max_bytes_scanned": self.max_bytes_scanned,
                "cache_hits": self.cache_hits, "max_rows": self.max_rows}
