"""Evidence verification. The node that decides whether an answer is allowed at all.

BAI-P6 section 6: if the evidence is insufficient, SAY SO. Never let the model fill the gap —
a filled gap is indistinguishable from a fact in the sentence that comes out, and it is the
one failure a reader cannot detect.
"""
from __future__ import annotations

from .tools import CATALOG


def verify_evidence(state) -> tuple[bool, list[str]]:
    checks: list[str] = []
    ok = True

    def fail(msg: str):
        nonlocal ok
        ok = False
        checks.append(f"FAIL {msg}")

    if not state.tool_calls:
        fail("no tool was called; there is no evidence to answer from")
        return ok, checks
    checks.append(f"PASS {len(state.tool_calls)} tool call(s): {state.tool_calls}")

    if state.errors:
        fail(f"tool errors present: {state.errors}")

    packs = [v for k, v in state.tool_results.items()
             if isinstance(v, dict) and "results" in v]
    for p in packs:
        results = p.get("results") or []
        if not results:
            fail("query returned no result object")
            continue
        r0 = results[0]
        if r0.get("error"):
            fail(f"query failed: {r0['error']}")
            continue
        if r0.get("row_count", 0) == 0:
            fail("query succeeded but returned no rows for the requested period")
        for col in ("metric_value",):
            if r0.get("rows") and col not in r0["rows"][0]:
                fail(f"expected column {col!r} missing from the result")
        w = r0.get("window") or {}
        tc = (state.time_context or {}).get("primary") or {}
        if tc and w and (w.get("start"), w.get("end")) != (tc.get("start"), tc.get("end")):
            fail(f"queried window {w.get('start')}..{w.get('end')} does not match the "
                 f"requested {tc.get('start')}..{tc.get('end')}")
        else:
            checks.append("PASS time period matches the request")

        if p.get("data_status") in (None, "UNKNOWN"):
            fail("no certification status returned; the number is UNVERIFIED")
        elif not p.get("certification_ok"):
            fail(f"certification {p['data_status']} is below the metric minimum")
        else:
            checks.append(f"PASS certification {p['data_status']}")

        md = p.get("metric_definition") or {}
        if md.get("grain"):
            checks.append(f"PASS grain {md['grain']}")

    d = state.metric_definition or {}
    if d and not d.get("unit") and d.get("metric_id"):
        checks.append("WARN unit not declared for this metric")
    if state.contract_notes:
        checks += [f"WARN {n}" for n in state.contract_notes]

    for t in state.tool_calls:
        if t in CATALOG and not CATALOG[t].read_only:
            fail(f"tool {t} is not read-only")
    return ok, checks
