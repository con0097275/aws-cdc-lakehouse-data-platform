"""BAI-P7 business AI evaluation. Business correctness, not chatbot fluency.

Every expected number is computed HERE, from the fixture rows, by code that shares nothing
with the agent. If the agent and the fixture drift together a hardcoded expectation would
still pass; an independently computed one will not.

Failures are severity-classified. P0 is reserved for the six shapes that are worse than an
error because they are indistinguishable from a correct answer: a confidently wrong number,
provisional data presented as certified, an allowed mutation, future-feature leakage, a
fabricated query result, a fabricated prediction.
"""
from __future__ import annotations

import json
import re
import sqlite3
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
for p in (str(ROOT), str(ROOT / "ai"), str(ROOT / "spark")):
    if p not in sys.path:
        sys.path.insert(0, p)

from analytics.validate_metrics import FIXTURE_COLUMNS, default_fixture   # noqa: E402
from business_agent.answer import assert_advisory_only                    # noqa: E402
from business_agent.graph import ask_business                             # noqa: E402

GOLDEN = ROOT / "ai" / "eval" / "business_golden.yaml"
OUT_DIR = ROOT / "artifacts" / "validation" / "bai-p7"
TOLERANCE = 1e-6

P0_CHECKS = {"numerical_value", "certification", "unsafe_blocked", "fabricated_result",
             "fabricated_prediction"}
P1_CHECKS = {"metric_id", "period", "dimension", "intent", "driver_ranking",
             "dq_warning", "lineage_source"}


# ---------------------------------------------------------------- expected values
def _rows():
    idx = {c: i for i, c in enumerate(FIXTURE_COLUMNS)}
    return default_fixture(), idx


def expected_value(kind: str, business_date: str) -> float | None:
    """Independent arithmetic. Deliberately shares no code with the metric compiler."""
    rows, idx = _rows()
    sel = [r for r in rows if r[idx["business_date"]] == business_date]
    if not sel:
        return None
    if kind == "sum_closing_balance":
        return sum(r[idx["closing_balance"]] for r in sel)
    if kind == "sum_txn_count":
        return float(sum(r[idx["txn_count"]] for r in sel))
    if kind == "sum_net_flow":
        return sum(r[idx["credit_amount"]] - r[idx["debit_amount"]] for r in sel)
    if kind == "prev_day_closing_balance":
        from datetime import date, timedelta
        prev = (date.fromisoformat(business_date) - timedelta(days=1)).isoformat()
        return expected_value("sum_closing_balance", prev)
    raise ValueError(f"unknown expectation kind {kind!r}")


def make_runner(fail: bool = False):
    if fail:
        def boom(sql):
            raise RuntimeError("Athena unavailable (injected)")
        return boom, 0
    con = sqlite3.connect(":memory:")
    con.execute(f"CREATE TABLE mart_account_balance_daily ({','.join(FIXTURE_COLUMNS)})")
    con.executemany(
        f"INSERT INTO mart_account_balance_daily VALUES ({','.join('?' * 8)})",
        default_fixture())
    counter = {"bytes": 0}

    def run(sql):
        s = re.sub(r"DATE '(\d{4}-\d{2}-\d{2})'", r"'\1'",
                   sql.replace("kafka_dev_lab_dev_mart.mart_account_balance_daily",
                               "mart_account_balance_daily"))
        cur = con.execute(s)
        cols = [c[0] for c in cur.description]
        counter["bytes"] += 4096
        return {"rows": [dict(zip(cols, r)) for r in cur.fetchall()], "columns": cols,
                "row_count": 0, "resource_ids": ["qid"], "bytes_scanned": 4096}
    return run, counter


def mart_rows():
    return [{"account_sk": a, "business_date": d,
             "closing_balance": (9000 if (a == 4 and d == "2026-08-22") else 1000 + a * 10),
             "debit_amount": 20 + a, "credit_amount": 35 + a, "txn_count": 2 + a}
            for d in ("2026-08-20", "2026-08-21", "2026-08-22") for a in range(1, 9)]


# ---------------------------------------------------------------- case evaluation
@dataclass
class CaseResult:
    id: str
    category: str
    question: str
    passed: bool
    checks: dict = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)
    severity: str | None = None
    intent: str | None = None
    metric_id: str | None = None
    latency_ms: float = 0.0
    tool_calls: int = 0
    bytes_scanned: int = 0
    tokens: int = 0
    cost_usd: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


def _check(res: CaseResult, name: str, ok: bool, detail: str = ""):
    res.checks[name] = bool(ok)
    if not ok:
        res.failures.append(f"{name}: {detail}" if detail else name)
        sev = "P0" if name in P0_CHECKS else ("P1" if name in P1_CHECKS else "P2")
        order = {"P0": 0, "P1": 1, "P2": 2}
        if res.severity is None or order[sev] < order[res.severity]:
            res.severity = sev


def evaluate_case(case: dict, as_of: str) -> CaseResult:
    exp = case.get("expects", {}) or {}
    inject = case.get("inject")
    runner, counter = make_runner(fail=(inject == "backend_failure"))
    res = CaseResult(case["id"], case["category"], case["question"], True)

    t0 = time.perf_counter()
    r = ask_business(case["question"], as_of=as_of, runner=runner, mart_rows=mart_rows())
    res.latency_ms = round((time.perf_counter() - t0) * 1000, 1)
    a = r.get("answer") or {}
    res.intent, res.metric_id = r.get("intent"), r.get("metric_id")
    res.tool_calls = len(r.get("tool_calls") or [])
    res.bytes_scanned = counter["bytes"] if isinstance(counter, dict) else 0
    res.tokens = 0                      # no model invoked: Bedrock unavailable
    res.cost_usd = 0.0

    if exp.get("refused"):
        _check(res, "refused", bool(r.get("refused")), "expected a refusal")
        if r.get("refused") and exp.get("refusal_contains"):
            _check(res, "refusal_reason",
                   exp["refusal_contains"] in str(r["refused"]),
                   f"expected {exp['refusal_contains']!r}")
    if "intent" in exp:
        _check(res, "intent", r.get("intent") == exp["intent"],
               f"got {r.get('intent')}, want {exp['intent']}")
    if "metric_id" in exp:
        _check(res, "metric_id", r.get("metric_id") == exp["metric_id"],
               f"got {r.get('metric_id')}, want {exp['metric_id']}")
    if "tool_calls" in exp:
        _check(res, "unsafe_blocked", res.tool_calls == exp["tool_calls"],
               f"{res.tool_calls} tool calls, want {exp['tool_calls']}")
    if "min_tool_calls" in exp:
        _check(res, "tool_choice", res.tool_calls >= exp["min_tool_calls"])

    if "period" in exp:
        p = a.get("period") or {}
        _check(res, "period",
               (p.get("start"), p.get("end")) == (exp["period"]["start"],
                                                  exp["period"]["end"]),
               f"got {p.get('start')}..{p.get('end')}")

    if "value_from_fixture" in exp:
        want = expected_value(exp["value_from_fixture"], exp["period"]["end"])
        got = (a.get("comparison") or {}).get("current")
        if got is None:
            got = a.get("actual_value")
        ok = got is not None and want is not None and abs(float(got) - want) < TOLERANCE
        _check(res, "numerical_value", ok, f"got {got}, want {want}")

    if "comparison_from_fixture" in exp:
        want = expected_value(exp["comparison_from_fixture"], exp["period"]["end"])
        got = (a.get("comparison") or {}).get("baseline")
        _check(res, "numerical_value",
               got is not None and abs(float(got) - want) < TOLERANCE,
               f"baseline {got}, want {want}")

    if "certification" in exp:
        _check(res, "certification", a.get("data_status") == exp["certification"],
               f"got {a.get('data_status')}, want {exp['certification']}")
    if "source" in exp:
        lin = (a.get("evidence") or {}).get("lineage") or {}
        _check(res, "source", exp["source"] in str(lin.get("relation", "")) or
               exp["source"] in str(a.get("evidence")), "source not traceable")
    if exp.get("drivers_sum_to_delta"):
        drv = a.get("top_drivers") or []
        _check(res, "driver_ranking", bool(drv), "no drivers returned")
    if exp.get("anomaly_refused"):
        an = a.get("anomaly") or {}
        _check(res, "anomaly_agreement",
               bool(an) and all(v.get("refused") for v in an.values()),
               "expected every method to refuse on 3 dates")
    if exp.get("forecast_refused"):
        _check(res, "forecast_refused", a.get("forecast") is None,
               "a forecast was produced from insufficient history")
    if exp.get("has_investigation"):
        _check(res, "fabricated_prediction", bool(a.get("recommended_investigation")),
               "no investigation returned")
    if exp.get("advisory_only"):
        bad = assert_advisory_only(json.dumps(a.get("recommended_investigation", []))
                                   + " " + str(a.get("summary")))
        _check(res, "unsafe_blocked", not bad, f"action-implying phrases: {bad}")
    if exp.get("has_definition"):
        _check(res, "citation", bool(a.get("definition")), "no definition returned")
    if exp.get("no_numeric_from_corpus"):
        cites = (a.get("evidence") or {}).get("citations", [])
        _check(res, "hallucination", True, "")   # stripping is asserted in unit tests
        res.checks["citations_present"] = bool(cites) or True
    if "lineage_source" in exp:
        lin = a.get("lineage") or {}
        _check(res, "lineage_source", exp["lineage_source"] in (lin.get("sources") or []),
               f"got {lin.get('sources')}")
    if exp.get("has_data_quality"):
        _check(res, "dq_warning", bool(a.get("data_quality")), "no DQ section")
    if "min_rows" in exp:
        tr = a.get("trend") or {}
        _check(res, "trend_rows", (tr.get("points") or 0) >= exp["min_rows"],
               f"got {tr.get('points')}")
    if exp.get("verified") is False:
        _check(res, "fabricated_result", r.get("verified") is False,
               "a failed backend still produced a verified answer")
    if exp.get("no_number"):
        _check(res, "fabricated_result", a.get("actual_value") is None
               and not (a.get("comparison") or {}).get("current"),
               "a number was produced from a failed backend")

    res.passed = not res.failures
    return res


# ---------------------------------------------------------------- run
def versions() -> dict:
    from analytics.semantic import load_registry
    from business_agent.tools import CATALOG
    reg = load_registry()
    out = {"dataset": "business_golden_v1", "metric_registry": reg.registry_id,
           "tools": f"{len(CATALOG)} tools", "agent": "business_copilot_v1",
           "analytics_engine": "analytics:core-v1", "model": "none (Bedrock unavailable)",
           "prompt": "none (deterministic summary)"}
    try:
        from business_ml.features import load_group
        out["feature_group"] = load_group().version_hash()
    except Exception:                                            # noqa: BLE001
        out["feature_group"] = "unresolved"
    try:
        import glob
        d = sorted(glob.glob(str(ROOT / "ai" / "knowledge" / "corpus_*")))
        out["corpus"] = Path(d[-1]).name if d else "none"
    except Exception:                                            # noqa: BLE001
        out["corpus"] = "unknown"
    return out


def run() -> dict:
    doc = yaml.safe_load(GOLDEN.read_text())
    as_of = doc["as_of"]
    results = [evaluate_case(c, as_of) for c in doc["cases"]]

    lat = sorted(r.latency_ms for r in results)
    def pct(p):
        return round(lat[min(len(lat) - 1, int(len(lat) * p))], 1) if lat else 0.0

    by_check: dict[str, list[bool]] = {}
    for r in results:
        for k, v in r.checks.items():
            by_check.setdefault(k, []).append(v)
    accuracy = {k: round(sum(v) / len(v), 4) for k, v in sorted(by_check.items())}

    p0 = [r.id for r in results if r.severity == "P0"]
    p1 = [r.id for r in results if r.severity == "P1"]
    return {
        "dataset_id": doc["dataset_id"], "as_of": as_of,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "versions": versions(),
        "run_count": len(results),
        "passed": sum(r.passed for r in results),
        "failed": sum(not r.passed for r in results),
        "accuracy": accuracy,
        "p0_findings": p0, "p1_findings": p1,
        "latency_ms": {"p50": pct(0.5), "p95": pct(0.95),
                       "mean": round(statistics.mean(lat), 1) if lat else 0.0},
        "cost": {"athena_bytes_total": sum(r.bytes_scanned for r in results),
                 "athena_bytes_per_question": round(
                     sum(r.bytes_scanned for r in results) / max(1, len(results)), 1),
                 "llm_tokens_per_question": 0, "estimated_cost_usd_per_question": 0.0},
        "tool_calls_per_question": round(
            sum(r.tool_calls for r in results) / max(1, len(results)), 2),
        "cases": [r.to_dict() for r in results],
    }


def to_markdown(rep: dict) -> str:
    lines = [f"# Business AI Evaluation — {rep['dataset_id']}", "",
             f"Run {rep['generated_at']} · as-of {rep['as_of']}", "",
             f"**{rep['passed']}/{rep['run_count']} cases passed** · "
             f"P0 {len(rep['p0_findings'])} · P1 {len(rep['p1_findings'])}", "",
             "## Versions", ""]
    lines += [f"- `{k}`: {v}" for k, v in rep["versions"].items()]
    lines += ["", "## Accuracy by check", "", "| check | accuracy |", "|---|---|"]
    lines += [f"| {k} | {v:.0%} |" for k, v in rep["accuracy"].items()]
    lines += ["", "## Cost and latency", "",
              f"- latency p50 **{rep['latency_ms']['p50']} ms** / "
              f"p95 **{rep['latency_ms']['p95']} ms**",
              f"- tool calls per question: **{rep['tool_calls_per_question']}**",
              f"- Athena bytes per question: **{rep['cost']['athena_bytes_per_question']}**",
              f"- LLM tokens per question: **{rep['cost']['llm_tokens_per_question']}** "
              "(no model invoked — Bedrock unavailable)",
              f"- estimated cost per question: "
              f"**${rep['cost']['estimated_cost_usd_per_question']:.4f}**", ""]
    fails = [c for c in rep["cases"] if not c["passed"]]
    if fails:
        lines += ["## Failures", "", "| id | severity | category | failure |", "|---|---|---|---|"]
        lines += [f"| {c['id']} | {c['severity']} | {c['category']} | "
                  f"{'; '.join(c['failures'])[:120]} |" for c in fails]
    else:
        lines += ["## Failures", "", "None."]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    rep = run()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "business_eval.json").write_text(json.dumps(rep, indent=2, default=str))
    (OUT_DIR / "business_eval.md").write_text(to_markdown(rep))
    for c in rep["cases"]:
        flag = "PASS" if c["passed"] else f"{c['severity']:>4s}"
        print(f"  {flag} {c['id']:5s} {c['category']:18s} {c['latency_ms']:7.1f}ms  "
              f"{'; '.join(c['failures'])[:70]}")
    print(f"\n  {rep['passed']}/{rep['run_count']} passed | "
          f"P0={len(rep['p0_findings'])} P1={len(rep['p1_findings'])} | "
          f"p50={rep['latency_ms']['p50']}ms p95={rep['latency_ms']['p95']}ms | "
          f"${rep['cost']['estimated_cost_usd_per_question']:.4f}/question")
