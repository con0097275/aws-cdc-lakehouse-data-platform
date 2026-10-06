"""Groundedness evaluation over the golden question set.

WHAT IS MEASURED, AND WHY NOT ACCURACY
--------------------------------------
Not string similarity to a reference answer. A paraphrase citing the right section is
correct; a fluent answer citing the wrong document is not, however well it reads.

So three things are checked:

  routing      did the question go to the intended tool, or to retrieval?
  groundedness did the evidence come from the expected source document?
  content      does the answer contain the facts it must contain?

Routing is checked FIRST and counted separately: a question that should be a free lookup
silently becoming a retrieval is a COST regression, and no accuracy metric would notice it.
"""

from __future__ import annotations

import os
import sys

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from assistant import Assistant  # noqa: E402

GOLDEN = os.path.join(os.path.dirname(__file__), "golden_questions.yml")


def run(verbose: bool = True) -> dict:
    with open(GOLDEN) as fh:
        golden = yaml.safe_load(fh)["questions"]

    a = Assistant()
    results, failures = [], []

    for case in golden:
        r = a.answer(case["q"])
        checks = {}

        checks["routing"] = r["mode"] == case["expect_mode"]
        if case.get("expect_tool"):
            checks["tool"] = r.get("tool") == case["expect_tool"]

        if case.get("expect_source"):
            cited = " ".join(r["citations"]) + r["answer"]
            checks["groundedness"] = case["expect_source"] in cited

        blob = r["answer"]
        checks["content"] = all(s.lower() in blob.lower()
                                for s in case.get("expect_contains", []))

        ok = all(checks.values())
        results.append({"q": case["q"], "ok": ok, "checks": checks,
                        "mode": r["mode"], "cost": r["cost_usd"]})
        if not ok:
            failures.append((case["q"], checks, r["mode"]))

    total = len(results)
    passed = sum(1 for r in results if r["ok"])
    summary = {
        "total": total,
        "passed": passed,
        "score": round(passed / total, 3) if total else 0.0,
        "routing_correct": sum(1 for r in results if r["checks"]["routing"]),
        "total_cost_usd": round(sum(r["cost"] for r in results), 6),
        "free_answers": sum(1 for r in results if r["cost"] == 0.0),
    }

    if verbose:
        for r in results:
            mark = "PASS" if r["ok"] else "FAIL"
            failed = [k for k, v in r["checks"].items() if not v]
            print(f"  [{mark}] {r['mode']:10s} {r['q'][:58]:60s}"
                  + (f" <- {failed}" if failed else ""))
        print(f"\n  {summary}")
        if failures:
            print("\n  failures:")
            for q, c, mode in failures:
                print(f"    {q}\n      got mode={mode}, checks={c}")
    return summary


if __name__ == "__main__":
    s = run()
    raise SystemExit(0 if s["score"] >= 0.85 else 1)
