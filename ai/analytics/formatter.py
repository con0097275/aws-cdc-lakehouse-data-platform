"""Structured answer formatting. No LLM.

BAI-P2 §9: numerical correctness is proven here WITHOUT generation. If the number is only
right when a model narrates it, it was never right. Narration is added later and is only
allowed to rephrase what this already contains.
"""
from __future__ import annotations

from .plan import EvidencePack


def _fmt(v: float | None, unit: str, currency: str) -> str:
    if v is None:
        return "n/a"
    s = f"{v:,.2f}" if abs(v) < 1000 else f"{v:,.0f}"
    return f"{s} {currency}".strip() if unit == "currency" else s


def format_answer(pack: EvidencePack) -> str:
    d = pack.metric_definition
    out = [f"Metric:      {d['business_name']}  ({pack.metric_id})",
           f"Intent:      {pack.intent}"]

    primary = pack.results[0] if pack.results else None
    if primary and primary.error:
        out += [f"Actual:      UNAVAILABLE",
                f"Error:       {primary.error}",
                "",
                "The backend failed. No number is given rather than a stale or partial one."]
        return "\n".join(out)

    if primary and primary.row_count and pack.intent in ("BREAKDOWN", "TOP_N", "BOTTOM_N"):
        out.append(f"Period:      {primary.window['start']} .. {primary.window['end']}")
        out.append("Segments:")
        for r in primary.rows:
            seg = r.get("segment", "?")
            out.append(f"  {str(seg):>16s}  "
                       f"{_fmt(float(r['metric_value']), pack.unit, pack.currency)}")
    elif primary and primary.row_count and pack.intent == "TREND":
        out.append(f"Period:      {primary.window['start']} .. {primary.window['end']}"
                   f"  ({primary.row_count} dates)")
        for r in primary.rows:
            out.append(f"  {r.get('business_date')}  "
                       f"{_fmt(float(r['metric_value']), pack.unit, pack.currency)}")
    elif primary:
        out.append(f"Actual:      {_fmt(primary.total_value, pack.unit, pack.currency)}")
        out.append(f"Period:      {primary.window['start']} .. {primary.window['end']}")

    if pack.comparison:
        c = pack.comparison
        pct = f"{c['delta_pct']:+.2%}" if c["delta_pct"] is not None else "n/a"
        out.append(f"Comparison:  {_fmt(c['baseline'], pack.unit, pack.currency)} "
                   f"({c['baseline_label']}) -> delta "
                   f"{_fmt(c['delta'], pack.unit, pack.currency)}  {pct}")

    flag = "" if pack.certification_ok else "   ** BELOW REQUIRED TIER **"
    out += [f"Data status: {pack.data_status}{flag}"
            f"   (requires >= {d['minimum_certification']})",
            f"Source:      {primary.relation if primary else 'n/a'}",
            f"Query ID:    {primary.query_id if primary else 'n/a'}",
            f"Query hash:  {primary.sql_hash if primary else 'n/a'}",
            f"Metric ver:  {pack.metric_version}",
            f"Bytes:       {primary.bytes_scanned if primary else 'n/a'}"]
    if pack.notes:
        out.append("Notes:")
        out += [f"  - {n}" for n in pack.notes]
    return "\n".join(out)
