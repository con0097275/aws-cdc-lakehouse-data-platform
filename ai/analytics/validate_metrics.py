"""Execute every compiled metric query and check the number against an INDEPENDENT count.

BAI-P1 §10 asks for actual-value validation. Athena cannot be reached right now: the
platform is destroyed and the lake CMK is pending deletion. So this proves the strongest
thing still available, and says plainly what it does not prove.

  PROVEN HERE   the compiled SQL parses, executes, and returns the value an independent
                Python computation over the same rows produces. That catches a wrong
                aggregation, a wrong GROUP BY, a wrong window, and a wrong measure column.
  NOT PROVEN    that the live mart contains these values, or that Athena accepts this
                dialect. Both need the platform up.

SQLite is the executor because it is stdlib. `DATE 'x'` is rewritten to a bare literal --
the only dialect difference in the emitted statement -- and that rewrite is stated rather
than hidden, because a validation that quietly edits the thing under test proves nothing.
"""
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field

from .compiler import compile_metric_query
from .semantic import Registry, load_registry
from .timespec import TimeWindow

FIXTURE_COLUMNS = ("account_sk", "customer_sk", "business_date", "closing_balance",
                   "debit_amount", "credit_amount", "txn_count", "processing_status")


def default_fixture() -> list[tuple]:
    """Mirrors mart_account_balance_daily, including its real partition dates."""
    rows = []
    for i, d in enumerate(("2026-08-20", "2026-08-21", "2026-08-22")):
        for a in range(1, 5):
            rows.append((a, 100 + a, d, 1000.0 + a * 10 + i * 5, 20.0 + a, 35.0 + a,
                         2 + a, "CERTIFIED" if d != "2026-08-22" else "RECONCILED"))
    return rows


@dataclass
class MetricCheck:
    metric_id: str
    sql_hash: str
    executed: bool
    sql_value: float | None
    expected_value: float | None
    row_count: int | None
    weakest_status: str | None
    match: bool
    error: str | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return self.__dict__ | {"notes": list(self.notes)}


def _sqlite(rows: list[tuple]) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute(f"CREATE TABLE mart_account_balance_daily ({','.join(FIXTURE_COLUMNS)})")
    con.executemany(
        f"INSERT INTO mart_account_balance_daily VALUES ({','.join('?' * len(FIXTURE_COLUMNS))})",
        rows)
    return con


def _to_sqlite(sql: str) -> str:
    sql = sql.replace("kafka_dev_lab_dev_mart.mart_account_balance_daily", "mart_account_balance_daily")
    return re.sub(r"DATE '(\d{4}-\d{2}-\d{2})'", r"'\1'", sql)


def _expected(metric_id: str, rows: list[tuple], window: TimeWindow) -> float | None:
    """Independent Python computation. Deliberately NOT sharing code with the compiler."""
    idx = {c: i for i, c in enumerate(FIXTURE_COLUMNS)}
    sel = [r for r in rows if window.start <= r[idx["business_date"]] <= window.end]
    if not sel:
        return None
    if metric_id == "total_closing_balance":
        return sum(r[idx["closing_balance"]] for r in sel)
    if metric_id == "avg_balance_per_account":
        return sum(r[idx["closing_balance"]] for r in sel) / len(sel)
    if metric_id == "total_debit_amount":
        return sum(r[idx["debit_amount"]] for r in sel)
    if metric_id == "total_credit_amount":
        return sum(r[idx["credit_amount"]] for r in sel)
    if metric_id == "net_cash_flow":
        return sum(r[idx["credit_amount"]] - r[idx["debit_amount"]] for r in sel)
    if metric_id == "total_txn_count":
        return sum(r[idx["txn_count"]] for r in sel)
    if metric_id == "active_account_count":
        return float(len({r[idx["account_sk"]] for r in sel}))
    return None


def validate_all(registry: Registry | None = None, rows: list[tuple] | None = None,
                 business_date: str = "2026-08-21") -> list[MetricCheck]:
    reg = registry or load_registry()
    data = rows if rows is not None else default_fixture()
    win = TimeWindow(business_date, business_date, business_date)
    con = _sqlite(data)
    out: list[MetricCheck] = []
    for mid in reg.metrics:
        q = compile_metric_query(mid, win, registry=reg)
        try:
            cur = con.execute(_to_sqlite(q.sql))
            fetched = cur.fetchall()
            if not fetched:
                out.append(MetricCheck(mid, q.sql_hash(), True, None, None, 0, None, False,
                                       "query returned no rows"))
                continue
            _, value, rc, weakest = fetched[0]
            exp = _expected(mid, data, win)
            ok = exp is not None and abs(float(value) - float(exp)) < 1e-6
            out.append(MetricCheck(mid, q.sql_hash(), True, float(value), exp, int(rc),
                                   weakest, ok, None, list(q.notes)))
        except Exception as e:                                   # noqa: BLE001
            out.append(MetricCheck(mid, q.sql_hash(), False, None, None, None, None, False,
                                   f"{type(e).__name__}: {e}"))
    con.close()
    return out


if __name__ == "__main__":
    import json
    from pathlib import Path
    checks = validate_all()
    root = Path(__file__).resolve().parents[2]
    outdir = root / "artifacts" / "validation" / "bai-p1"
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "metric_value_checks.json").write_text(
        json.dumps([c.to_dict() for c in checks], indent=2, default=str))
    for c in checks:
        flag = "MATCH" if c.match else "FAIL "
        print(f"  {flag} {c.metric_id:24s} sql={c.sql_value!s:>12s} "
              f"expected={c.expected_value!s:>12s} rows={c.row_count} "
              f"status={c.weakest_status} {c.error or ''}")
    print(f"\n  {sum(c.match for c in checks)}/{len(checks)} metrics match an independent "
          "computation")
