"""Business phrase -> ONE governed metric.

Deterministic, in three ordered stages. There is no model call and no fuzzy scoring that
could drift between releases:

  1. exact match on a normalised alias  ("total deposits" -> total_closing_balance)
  2. all-token containment              ("total deposit balance" -> the same metric)
  3. otherwise: AMBIGUOUS or UNKNOWN, never a guess

Stage 3 is the point. Guessing between `total_closing_balance` and `avg_balance_per_account`
because both contain "balance" produces a confident answer to a question nobody asked.
"""
from __future__ import annotations

from dataclasses import dataclass

from .semantic import AmbiguousMetric, Registry, UnknownMetric, _norm, load_registry

#: Words that carry no discriminating meaning in a metric phrase.
_STOP = {"the", "a", "an", "of", "for", "in", "on", "at", "to", "my", "our", "please",
         "show", "me", "what", "is", "was", "are", "were", "how", "much", "many",
         "give", "get", "value", "number", "amount", "total"}


@dataclass(frozen=True)
class Resolution:
    metric_id: str
    matched_on: str
    stage: str
    confidence: str        # exact | token

    def to_dict(self) -> dict:
        return {"metric_id": self.metric_id, "matched_on": self.matched_on,
                "stage": self.stage, "confidence": self.confidence}


def _tokens(s: str) -> set[str]:
    return {t for t in _norm(s).split() if t not in _STOP}


def resolve_metric(phrase: str, registry: Registry | None = None) -> Resolution:
    reg = registry or load_registry()
    text = _norm(phrase)
    if not text:
        raise UnknownMetric("empty phrase")

    # 1 ---------------------------------------------------------------- exact
    if text in reg.alias_index:
        return Resolution(reg.alias_index[text], text, "exact_alias", "exact")

    # 2 ------------------------------------------------- alias inside phrase
    # Longest alias first: "total deposit balance" must prefer the 3-word alias over a
    # 1-word one it also contains.
    contained = sorted((a for a in reg.alias_index if a and a in text),
                       key=len, reverse=True)
    if contained:
        best = contained[0]
        winners = {reg.alias_index[a] for a in contained if len(a) == len(best)}
        if len(winners) == 1:
            return Resolution(reg.alias_index[best], best, "alias_in_phrase", "exact")
        raise AmbiguousMetric(phrase, sorted(winners))

    # 3 --------------------------------------------------- token containment
    want = _tokens(phrase)
    if want:
        hits = {mid for alias, mid in reg.alias_index.items()
                if (at := _tokens(alias)) and at <= want}
        if len(hits) == 1:
            return Resolution(next(iter(hits)), phrase, "token_subset", "token")
        if len(hits) > 1:
            raise AmbiguousMetric(phrase, sorted(hits))

    raise UnknownMetric(
        f"no governed metric matches {phrase!r}. Declared metrics: "
        f"{sorted(reg.metrics)}. Add it to aiplatform/metrics/business_metrics.yaml with an "
        "owner and a grain rather than answering from an undefined definition.")


def suggest(phrase: str, registry: Registry | None = None, k: int = 3) -> list[str]:
    """Best-effort candidates for a clarification prompt. Never used to auto-pick."""
    reg = registry or load_registry()
    want = _tokens(phrase)
    scored = []
    for alias, mid in reg.alias_index.items():
        overlap = len(_tokens(alias) & want)
        if overlap:
            scored.append((overlap, mid))
    out: list[str] = []
    for _, mid in sorted(scored, key=lambda t: -t[0]):
        if mid not in out:
            out.append(mid)
    return out[:k]
