"""Business summary from a verified EvidencePack.

The model is handed NUMBERS THAT ARE ALREADY TRUE and asked only to phrase them. It never
computes, never chooses a metric, and never sees raw table access.

The deterministic path is not a degraded mode — it is the reference. It runs when Bedrock is
unavailable (as it is on this account), and it also defines exactly what the model is
permitted to say. If the two ever disagree on a number, the deterministic one is right.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

PROMPT_VERSION = "prompt:insight-summary-v1"

SYSTEM_PROMPT = """You write one short business summary of a metric movement.

RULES, in order of importance:
1. Use ONLY the numbers in the evidence. Never compute, adjust, round differently, or infer
   a figure that is not present.
2. Say "contributed to the observed change". NEVER say "caused", "because of", "due to",
   "driven by" or "as a result of". Contribution analysis cannot establish causation, and a
   reader who is told a branch CAUSED a decline will act on it.
3. If the evidence marks the data below the required certification tier, say so first.
4. If a forecast or anomaly is absent, do not speculate about it. Absent means not computed,
   not "nothing found".
5. Two or three sentences. No preamble, no bullet points, no recommendation unless the
   evidence contains one.
"""

_FORBIDDEN = ("caused", "because of", "due to", "driven by", "as a result of",
              "resulted in", "led to")


@dataclass
class Summary:
    text: str
    source: str                    # deterministic | llm
    model_version: str | None
    prompt_version: str
    tokens_in: int | None
    tokens_out: int | None
    duration_ms: float
    notes: list[str]


def _fmt(v, unit="", currency=""):
    if v is None:
        return "n/a"
    s = f"{v:,.2f}" if abs(v) < 1000 else f"{v:,.0f}"
    return f"{s} {currency}".strip() if unit == "currency" else s


def deterministic_summary(pack, spec, severity: str) -> str:
    m = pack.metric_definition["business_name"]
    parts: list[str] = []
    if not pack.certification_ok:
        parts.append(f"Data for this date is {pack.data_status}, below the required "
                     f"{spec.required_certification}; treat the figure as provisional.")
    c = pack.comparison
    if c and c["delta"] is not None:
        pct = f" ({c['delta_pct']:+.2%})" if c["delta_pct"] is not None else ""
        parts.append(f"{m} was {_fmt(c['current'], pack.unit, pack.currency)} versus "
                     f"{_fmt(c['baseline'], pack.unit, pack.currency)} "
                     f"({c['baseline_label']}), a change of "
                     f"{_fmt(c['delta'], pack.unit, pack.currency)}{pct}.")
    elif pack.results and pack.results[0].total_value is not None:
        parts.append(f"{m} was {_fmt(pack.results[0].total_value, pack.unit, pack.currency)}.")
    else:
        parts.append(f"{m} could not be measured for this date.")

    drivers = (pack.drivers or {})
    for dim, d in list(drivers.items())[:1]:
        top = d["contributions"][:2]
        if top:
            phrases = "; ".join(t["phrase"] for t in top)
            parts.append(f"By {dim}: {phrases}.")

    if pack.anomaly:
        refused = [v for v in pack.anomaly.values() if v.get("refused")]
        if refused and len(refused) == len(pack.anomaly):
            parts.append("Anomaly detection was inconclusive: "
                         f"{refused[0]['refused']}.")
        else:
            parts.append(f"Anomaly severity: {severity}.")
    if pack.forecast is None:
        parts.append("No forecast was produced for this metric.")
    return " ".join(parts)


def check_no_causal_claims(text: str) -> list[str]:
    low = (text or "").lower()
    return [w for w in _FORBIDDEN if w in low]


def summarise(pack, spec, severity: str, *, model=None) -> Summary:
    """Deterministic first; the model only rephrases, and only if it stays truthful."""
    t0 = time.perf_counter()
    baseline = deterministic_summary(pack, spec, severity)
    notes: list[str] = []

    if model is None:
        return Summary(baseline, "deterministic", None, PROMPT_VERSION, 0, 0,
                       round((time.perf_counter() - t0) * 1000, 1),
                       ["no model supplied; deterministic summary is authoritative"])
    try:
        out = model.generate(system=SYSTEM_PROMPT,
                             user=f"EVIDENCE:\n{pack.to_dict()}\n\nBASELINE:\n{baseline}")
        text = (out.get("text") or "").strip()
        bad = check_no_causal_claims(text)
        if bad or not text:
            notes.append(
                f"model output rejected ({'causal language: ' + ', '.join(bad) if bad else 'empty'}); "
                "deterministic summary used instead")
            return Summary(baseline, "deterministic", out.get("model_version"),
                           PROMPT_VERSION, out.get("tokens_in"), out.get("tokens_out"),
                           round((time.perf_counter() - t0) * 1000, 1), notes)
        return Summary(text, "llm", out.get("model_version"), PROMPT_VERSION,
                       out.get("tokens_in"), out.get("tokens_out"),
                       round((time.perf_counter() - t0) * 1000, 1), notes)
    except Exception as e:                                       # noqa: BLE001
        notes.append(f"generation unavailable ({type(e).__name__}); deterministic summary "
                     "used — the numbers are unaffected")
        return Summary(baseline, "deterministic", None, PROMPT_VERSION, 0, 0,
                       round((time.perf_counter() - t0) * 1000, 1), notes)
