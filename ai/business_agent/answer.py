"""The business answer contract.

Concise for a reader, fully traceable underneath. Confidence classes are kept SEPARATE
(section 8): a measured value, a statistical observation and a model score are three
different kinds of claim, and merging them into one confident paragraph is how a z-score
becomes a fact.

Recommendations are INVESTIGATIVE ONLY (section 7). "Investigate these accounts, they
account for 63% of the movement" is allowed. "Close these accounts" is not, and is blocked
in code rather than discouraged in a prompt.
"""
from __future__ import annotations

from .graph import FORBIDDEN_RECOMMENDATIONS


def _fmt(v, unit="", currency=""):
    if v is None:
        return "n/a"
    s = f"{v:,.2f}" if abs(v) < 1000 else f"{v:,.0f}"
    return f"{s} {currency}".strip() if unit == "currency" else s


def assert_advisory_only(text: str) -> list[str]:
    low = (text or "").lower()
    return [p for p in FORBIDDEN_RECOMMENDATIONS if p in low]


def _baseline_missing(pack) -> str:
    """Label of a baseline window that WAS queried and came back empty, else "".

    `compare_metric_periods` returns two results -- the current window and the baseline --
    and builds `comparison` only when both carry a value. When the baseline is empty it
    returns `comparison: None`, and the summary then fell through to the bare-value branch:
    a user who asked "compare with the previous day" was told "Total closing balance was
    2,031,880,160 VND", badged VERIFIED and CERTIFIED, with `limitations: []`.

    Both halves of that are true and the answer is still wrong, because the question was a
    comparison and nothing said the comparison had not happened. On a mart holding a single
    business date every comparison question answers this way. A missing baseline is a
    finding, not a silence.
    """
    if not pack or pack.get("comparison"):
        return ""
    results = pack.get("results") or []
    if len(results) < 2:
        return ""
    baseline = results[1]
    if baseline.get("total_value") is not None:
        return ""
    return (baseline.get("window") or {}).get("label") or "baseline period"


def build_answer(state, *, model=None) -> dict:
    d = state.metric_definition or {}
    unit, cur = d.get("unit", ""), d.get("currency", "")
    tr = state.tool_results
    ans: dict = {
        "summary": "", "metric": d.get("business_name") or state.metric_id,
        "actual_value": None, "period": None, "comparison": None, "trend": None,
        "top_drivers": [], "anomaly": None, "forecast": None, "prediction": None,
        "recommended_investigation": [], "data_status": "UNKNOWN",
        "confidence": {}, "limitations": [], "evidence": {}, "verified": state.verified,
    }

    pack = next((v for v in tr.values() if isinstance(v, dict) and "results" in v), None)
    if pack:
        ans["data_status"] = pack.get("data_status", "UNKNOWN")
        r0 = (pack.get("results") or [{}])[0]
        ans["period"] = r0.get("window")
        ans["actual_value"] = r0.get("total_value")
        ans["comparison"] = pack.get("comparison")
        ans["trend"] = pack.get("trend")
        ans["evidence"] = {"query_ids": [x.get("query_id") for x in pack.get("results", [])],
                           "sql_hashes": [x.get("sql_hash") for x in pack.get("results", [])],
                           "metric_version": pack.get("metric_version"),
                           "lineage": pack.get("lineage")}

    drv = tr.get("analyze_metric_drivers", {}).get("drivers") or {}
    for dim, v in list(drv.items())[:1]:
        ans["top_drivers"] = [{"segment": c["segment"], "share_of_delta": c["share_of_delta"],
                               "phrase": c["phrase"]} for c in v["contributions"][:3]]
        ans["confidence"]["top_drivers"] = "STATISTICAL_OBSERVATION"

    tnd = tr.get("detect_metric_anomaly", {}).get("trend")
    if tnd and not ans["trend"]:
        ans["trend"] = tnd
        ans["confidence"]["trend"] = "STATISTICAL_OBSERVATION"

    an = tr.get("detect_metric_anomaly", {}).get("anomaly")
    if an:
        ans["anomaly"] = an
        ans["confidence"]["anomaly"] = "STATISTICAL_OBSERVATION"
        if all(v.get("refused") for v in an.values()):
            ans["limitations"].append(
                "Anomaly detection was inconclusive: " +
                next(v["refused"] for v in an.values() if v.get("refused")))

    fc = tr.get("forecast_metric", {})
    if fc:
        ans["forecast"] = fc.get("forecast")
        ans["confidence"]["forecast"] = "STATISTICAL_OBSERVATION"
        if fc.get("forecast") is None:
            ans["limitations"] += fc.get("notes", ["forecast not produced"])

    pr = tr.get("predict_business_risk")
    if pr:
        ans["prediction"] = pr
        ans["confidence"]["prediction"] = "MODEL_PREDICTION"
        top = pr.get("predictions", [])[:3]
        ans["recommended_investigation"] = [
            {"entity": p["entity"], "score": p["score"], "severity": p["prediction"],
             # Investigative wording only.
             "why": f"account {p['entity']} deviates from its peers "
                    f"({p['prediction'].lower()}); worth a look"}
            for p in top]

    kn = tr.get("retrieve_business_knowledge")
    if kn:
        ans["definition"] = (kn.get("definition") or {}).get("definition")
        ans["evidence"]["citations"] = [c["citation"] for c in kn.get("chunks", [])]
        ans["confidence"]["definition"] = "DATA_FACT"

    lin = tr.get("get_lineage")
    if lin:
        ans["lineage"] = lin
        ans["confidence"]["lineage"] = "DATA_FACT"

    gov = tr.get("get_data_quality")
    if gov:
        ans["data_quality"] = {"verdict": gov.get("verdict"),
                               "findings": [{"check": f["check"], "severity": f["severity"],
                                             "summary": f["summary"]}
                                            for f in gov.get("findings", [])]}
        ans["confidence"]["data_quality"] = "STATISTICAL_OBSERVATION"

    if pack:
        ans["confidence"]["actual_value"] = "DATA_FACT"

    # ---- summary ------------------------------------------------------------------
    if not state.verified:
        ans["summary"] = ("I can't answer that from the evidence available. "
                          + "; ".join(c for c in state.verification if c.startswith("FAIL")))
        ans["limitations"] += [c for c in state.verification if c.startswith(("FAIL", "WARN"))]
    else:
        bits = []
        c = ans["comparison"]
        if c and c.get("delta") is not None:
            pct = f" ({c['delta_pct']:+.2%})" if c.get("delta_pct") is not None else ""
            bits.append(f"{ans['metric']} was {_fmt(c['current'], unit, cur)} versus "
                        f"{_fmt(c['baseline'], unit, cur)} ({c['baseline_label']}), "
                        f"a change of {_fmt(c['delta'], unit, cur)}{pct}.")
        elif ans["actual_value"] is not None:
            bits.append(f"{ans['metric']} was {_fmt(ans['actual_value'], unit, cur)}.")
            missing = _baseline_missing(pack)
            if missing:
                bits.append(f"No comparison against the {missing}: that period returned no "
                            f"rows, so the change cannot be computed.")
                ans["limitations"].append(
                    f"comparison unavailable — the {missing} has no data; this is a single "
                    f"value, not a change")
                ans["confidence"]["comparison"] = "NOT_AVAILABLE"
        seg_pack = tr.get("breakdown_metric")
        if seg_pack:
            rows = ((seg_pack.get("results") or [{}])[0].get("rows") or [])
            segs = sorted(({"segment": r.get("segment"),
                            "value": float(r.get("metric_value") or 0)} for r in rows),
                          key=lambda x: x["value"], reverse=True)
            ans["segments"] = segs[:10]
            if segs:
                top = "; ".join(f"{s['segment']}: {_fmt(s['value'], unit, cur)}"
                                for s in segs[:5])
                bits.append(f"Top segments by {ans['metric']} — {top}.")
        if ans["top_drivers"]:
            bits.append("By segment: "
                        + "; ".join(t["phrase"] for t in ans["top_drivers"][:2]) + ".")
        if ans["recommended_investigation"]:
            bits.append(f"{len(ans['recommended_investigation'])} account(s) are worth "
                        "investigating first.")
        cert = tr.get("get_certification_status")
        if cert:
            ans["data_status"] = cert.get("data_status", ans["data_status"])
            bits.append(f"{ans['metric']} is {cert['data_status']}; the metric requires at "
                        f"least {cert['minimum_certification']} "
                        f"({'meets' if cert['certification_ok'] else 'DOES NOT MEET'} it).")
        dq = ans.get("data_quality")
        if dq:
            worst = dq["findings"][0] if dq["findings"] else None
            if worst:
                bits.append(f"Data quality verdict {dq['verdict']}; worst check "
                            f"{worst['check']} = {worst['severity']}.")
        if ans.get("lineage"):
            lin = ans["lineage"]
            bits.append(f"It is built from {lin.get('relation')} "
                        f"(sources: {', '.join(lin.get('sources', [])) or 'unresolved'}).")
        # ANOMALY and FORECAST return no metric pack, so without these branches the
        # summary was EMPTY on a verified answer -- worse than a wrong one, because the
        # page looked broken while the evidence underneath was fine.
        if ans.get("anomaly"):
            usable = {k: v for k, v in ans["anomaly"].items() if not v.get("refused")}
            if usable:
                k = max(usable, key=lambda x: abs(usable[x].get("score") or 0))
                v = usable[k]
                bits.append(
                    f"{ans['metric']} was {_fmt(v.get('actual'), unit, cur)} against a "
                    f"baseline of {_fmt(v.get('baseline'), unit, cur)} — "
                    f"{v.get('severity')} (score {v.get('score'):.2f} via {k}). "
                    "This is a statistical observation, not a diagnosis.")
            else:
                bits.append("Anomaly detection was inconclusive: " + str(
                    next(iter(ans["anomaly"].values())).get("refused", "no baseline")))
        if ans.get("forecast"):
            f = ans["forecast"]
            if f.get("point") is not None:
                bits.append(
                    f"Forecast for {f.get('target_date')}: "
                    f"{_fmt(f['point'], unit, cur)} (range {_fmt(f.get('lower'), unit, cur)} "
                    f"to {_fmt(f.get('upper'), unit, cur)}), method {f.get('method')} with "
                    f"a backtested error of {f.get('backtest_mape', 0):.1%}.")
        if ans.get("trend") and not ans.get("anomaly"):
            t = ans["trend"]
            if t.get("change_pct") is not None:
                bits.append(f"Over {t.get('points')} dates the metric moved "
                            f"{t['change_pct']:+.2%}.")
        if ans.get("definition"):
            bits.append(str(ans["definition"])[:400])
        elif pack is None and (ans.get("evidence") or {}).get("citations"):
            # A knowledge answer with retrieved chunks but no registry definition still has
            # something to say; an empty summary reads as a failure.
            bits.append("Answered from the knowledge corpus — see the cited sources below.")
        # A KNOWLEDGE answer ran no query, so "Data status: UNKNOWN" is noise that reads
        # like a failure. Only report a status when a data query actually produced one.
        if ans["data_status"] != "UNKNOWN":
            bits.append(f"Data status: {ans['data_status']}.")
        ans["summary"] = " ".join(bits)

    if model is not None:
        ans["confidence"]["summary"] = "LLM_INTERPRETATION"
    else:
        ans["confidence"]["summary"] = "DATA_FACT"

    violations = assert_advisory_only(ans["summary"])
    if violations:
        # Blocked in CODE, not discouraged in a prompt.
        ans["summary"] = ("A recommendation implying an action on a customer or account was "
                          "removed. This copilot is advisory: it can say what to "
                          "investigate, never what to do.")
        ans["limitations"].append(f"blocked action-implying phrases: {violations}")
    ans["limitations"] += state.contract_notes
    return ans
