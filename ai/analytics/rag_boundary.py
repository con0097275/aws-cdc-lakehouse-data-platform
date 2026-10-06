"""Where RAG is allowed, and where it is not.

RAG explains what a metric MEANS. It never supplies what a metric IS RIGHT NOW.

    "What does active account mean?"        -> RAG only
    "How many active accounts yesterday?"   -> metric layer + Athena (RAG optional)
    "Why did active accounts fall?"         -> both, and the number still comes from SQL

The failure this prevents is subtle and expensive: a retrieved document containing a number
from a past report reads exactly like a current answer. Corpus text is prose about
definitions; it has no as-of date, no certification tier and no query id, so a number taken
from it cannot be checked and cannot be right except by accident.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

#: Digits that look like a business quantity. Deliberately broad: a false positive costs a
#: dropped sentence, a false negative costs a fabricated number presented as fact.
# NOTE the boundary handling on the unit group. An earlier version ended the alternation
# with a single `\b`, which never matched "12.5%" followed by a space: `%` and ` ` are both
# non-word characters, so there is no word boundary between them and the percentage sailed
# through untouched. Word-suffixes carry their own `\b`; `%` needs none.
_NUMERIC = re.compile(r"(?<![\w.])\d{1,3}(?:[,\d]{3,})(?:\.\d+)?(?![\w])|"
                      r"(?<![\w.])\d+(?:\.\d+)?\s*(?:%|percent\b|bn\b|mn\b|k\b)", re.I)


@dataclass
class KnowledgeContext:
    metric_id: str
    definition: str
    citations: list[str] = field(default_factory=list)
    stripped_numeric_claims: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return self.__dict__ | {"citations": list(self.citations),
                                "notes": list(self.notes)}


def strip_numeric_claims(text: str) -> tuple[str, int]:
    """Remove quantity-shaped tokens from retrieved prose before it reaches an answer."""
    n = len(_NUMERIC.findall(text or ""))
    return _NUMERIC.sub("[value-redacted]", text or ""), n


def knowledge_for_metric(metric, retriever=None, top_k: int = 3) -> KnowledgeContext:
    """Definition context for a metric. NEVER a value.

    The registry definition is authoritative; retrieval only adds surrounding explanation,
    with quantities stripped so nothing numeric can leak from prose into a business answer.
    """
    ctx = KnowledgeContext(metric_id=metric.metric_id, definition=metric.description.strip())
    if retriever is None:
        ctx.notes.append("no retriever supplied; registry definition only")
        return ctx
    try:
        hits = retriever(f"{metric.business_name} definition {metric.metric_id}", top_k)
    except Exception as e:                                        # noqa: BLE001
        ctx.notes.append(f"retrieval unavailable ({type(e).__name__}); "
                         "registry definition still authoritative")
        return ctx
    total = 0
    for h in hits[:top_k]:
        cleaned, n = strip_numeric_claims(h.get("text", ""))
        total += n
        ctx.citations.append(h.get("citation", "?"))
        if cleaned:
            ctx.definition += "\n\n" + cleaned[:600]
    ctx.stripped_numeric_claims = total
    if total:
        ctx.notes.append(f"{total} quantity-shaped token(s) removed from retrieved prose: "
                         "numbers come from the governed query, never from the corpus")
    return ctx
