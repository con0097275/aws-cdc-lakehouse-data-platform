"""BM25 retrieval over the documentation corpus. No embeddings, no external dependency.

WHY BM25 AND NOT EMBEDDINGS
---------------------------
The corpus is ~700 chunks of technical prose written by one team with a consistent
vocabulary. The questions use that same vocabulary — "what is the retention of L2",
"which runbook covers consumer lag". Lexical overlap is high, which is exactly the regime
BM25 is strongest in and semantic search is least needed.

Embeddings would cost money twice (indexing and every query), require a model version to
pin, and add a vector store to operate. For 700 chunks of jargon-dense text that is spend
without a corresponding gain. If the corpus grew to include user-written free text — support
tickets, incident write-ups in varied phrasing — that calculus would change, and
docs/AI_USE_CASE.md records the trigger.

BM25 is ~60 lines and has no dependencies at all.
"""

from __future__ import annotations

import json
import math
import os
import re
from collections import Counter

CORPUS_PATH = os.path.join(os.path.dirname(__file__), "knowledge", "corpus.json")

# Domain vocabulary that a generic stopword list would keep but which carries no signal
# here: every chunk mentions "data" and "table".
_STOP = {
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "of", "to", "in", "on",
    "for", "and", "or", "not", "it", "this", "that", "with", "as", "by", "at", "from",
    "what", "which", "how", "why", "when", "where", "does", "do", "can", "i", "we",
}

K1 = 1.5   # term-frequency saturation
B = 0.75   # length normalisation


def tokenize(text: str) -> list:
    """Lowercase word tokens, keeping dotted identifiers intact.

    `mart.dim_customer` must survive as one token: splitting it on the dot makes every
    dataset name retrieve every other dataset name, because they all share `mart`.
    """
    text = text.lower()
    tokens = re.findall(r"[a-z_][a-z0-9_]*(?:\.[a-z0-9_]+)*", text)
    return [_fold(t) for t in tokens if t not in _STOP and len(t) > 1]


def _fold(token: str) -> str:
    """Fold a trailing plural so `conflict` and `conflicts` are the same term.

    Without it the coverage rule rejected the chunk that answers the question best: the
    query `iceberg commit conflict` missed `docs/RUNBOOK.md#iceberg-commit-conflictS`,
    scored it as covering only two thirds of the question, and promoted a weaker chunk that
    happened to use the singular. A retrieval rule strict enough to reject noise has to stop
    treating a plural as a different word.

    Deliberately not a stemmer: no suffix stripping beyond this, because `certified` and
    `certification` are genuinely different terms in this corpus and collapsing them loses
    the distinction the certification ladder rests on. Identifiers with `_` or `.` are left
    alone -- `dv_pk_hash` is a name, not a plural.
    """
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss") \
            and "_" not in token and "." not in token:
        return token[:-1]
    return token


#: How much of the QUESTION a chunk has to account for, weighted by how rare each word is.
#:
#: An absolute score floor cannot do this job: measured on this corpus, the answerable query
#: "watermark" scores 7.19 and "full_cdc" scores 4.63, while the unanswerable "what does
#: total deposits mean?" scores 5.45. Magnitude does not separate them -- COVERAGE does.
#:
#: 0.30 is the value MEASURED against `ai/eval/evaluate_rag.py` for the sibling
#: implementation in `ai/retrieval/service.py`, which is the one the copilot actually calls
#: and the only one the quality gate exercises. This module serves `ai/assistant.py` and has
#: no gate of its own, so it takes the measured number rather than a plausible one.
MIN_QUERY_COVERAGE = 0.30

#: Keep only hits within this fraction of the best one. A long tail of weak matches under a
#: strong first hit is noise presented as corroboration.
REL_FLOOR = 0.45


class Retriever:
    def __init__(self, corpus_path: str = CORPUS_PATH):
        with open(corpus_path) as fh:
            self.chunks = json.load(fh)["chunks"]
        self._index()

    def _index(self):
        self.docs = []
        df = Counter()
        for c in self.chunks:
            # The heading is weighted by repeating it: a section titled "kafka-consumer-lag"
            # should beat one that mentions the phrase once in passing.
            toks = tokenize(c["heading"]) * 3 + tokenize(c["text"])
            self.docs.append(Counter(toks))
            df.update(set(toks))

        n = len(self.docs)
        self.idf = {t: math.log(1 + (n - c + 0.5) / (c + 0.5)) for t, c in df.items()}
        self.lengths = [sum(d.values()) for d in self.docs]
        self.avg_len = (sum(self.lengths) / n) if n else 0.0

    def search(self, query: str, k: int = 5, source_filter: str | None = None) -> list:
        """Top-k chunks with scores. Returns [] rather than guessing when nothing matches."""
        q = tokenize(query)
        if not q:
            return []

        scored = []
        for i, doc in enumerate(self.docs):
            if source_filter and source_filter not in self.chunks[i]["source"]:
                continue
            score = 0.0
            for term in q:
                if term not in doc:
                    continue
                tf = doc[term]
                denom = tf + K1 * (1 - B + B * self.lengths[i] / (self.avg_len or 1))
                score += self.idf.get(term, 0.0) * (tf * (K1 + 1)) / denom
            if score > 0:
                scored.append((score, i))

        scored.sort(reverse=True)
        if not scored:
            return []

        # RELEVANCE FLOOR. `score > 0` is not a match: one shared common word is enough to
        # clear it. "What does total deposits mean?" scored 5.45 against
        # docs/RUNBOOK.md#iceberg-commit-conflicts and rendered it as a CITATION under the
        # answer -- the word "deposits" appears in 0 of 708 chunks, so the subject of the
        # question was absent and the hit came from "total" and "mean" alone.
        #
        # A citation that does not support the answer is worse than no citation: it invites
        # the reader to check it, and checking it disproves nothing because it was never a
        # claim. The docstring above has always promised [] rather than a guess.
        #
        # Calibrated on this corpus, and asserted by a test with a known-answerable and a
        # known-unanswerable query, so re-indexing that breaks the separation fails CI
        # rather than quietly degrading.
        # A term the corpus has NEVER seen is maximally informative, not minimally. The BM25
        # loop above reaches it through `self.idf.get(term, 0.0)`, which scores an unknown
        # word like a ubiquitous one -- so a question about something absent looks like a
        # question about something everywhere, and any chunk can answer it.
        unknown_idf = max(self.idf.values(), default=0.0)
        weight = {t: self.idf.get(t, unknown_idf) for t in set(q)}
        demanded = sum(weight.values())
        if demanded <= 0:
            return []

        covered = []
        for s_, i in scored:
            got = sum(w for t, w in weight.items() if t in self.docs[i])
            if got / demanded >= MIN_QUERY_COVERAGE:
                covered.append((s_, i))
        if not covered:
            return []

        best = covered[0][0]
        keep = [(s_, i) for s_, i in covered if s_ >= best * REL_FLOOR]
        return [{**self.chunks[i], "score": round(s_, 3)} for s_, i in keep[:k]]


def format_citations(results: list) -> str:
    """Render results with their source. Every answer must be traceable to a chunk.

    Acceptance criterion: answers cite source chunks. A citation that names only the file is
    not much of a citation for a 900-line document, so the heading and anchor go too.
    """
    if not results:
        return "No matching documentation found."
    lines = []
    for r in results:
        lines.append(f"[{r['source']}#{r['anchor']}] ({r['heading']}, score {r['score']})")
    return "\n".join(lines)
