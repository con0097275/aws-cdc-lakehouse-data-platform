"""Deterministic version identifiers.

THE RULE: a version is a CONTENT HASH, not a counter and never a random id.

`reporting/compile.py` already established this for the reporting plan (`plan_hash`,
`source_sha256`). The same reasoning applies to every AI object:

  - a random/uuid version cannot answer "is this the same corpus as yesterday?"
  - a hand-incremented counter is a field someone forgets to bump, and a stale version is
    worse than no version because downstream trusts it
  - a content hash re-derives itself and CANNOT disagree with the content

Consequence worth stating: re-embedding an unchanged corpus is detectable as a no-op, which
is what makes "embedding is idempotent per corpus_version" enforceable rather than aspirational.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

VERSION_PREFIX_LEN = 16


def canonical(payload: Any) -> str:
    """Canonical JSON: sorted keys, no insignificant whitespace, stable separators.

    Two payloads that differ only in key ORDER must hash identically, or every dict
    rebuild produces a spurious new version.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      default=str)


def content_hash(payload: Any) -> str:
    """Full sha256 of the canonical form."""
    return hashlib.sha256(canonical(payload).encode("utf-8")).hexdigest()


def version_id(kind: str, payload: Any) -> str:
    """A human-readable, content-addressed version id, e.g. `corpus:9f2a1c...`.

    The prefix names WHAT is versioned. Truncated to 16 hex chars: enough that a collision
    is not a practical concern at this scale, short enough to appear in a log line and a
    table column without wrapping.
    """
    if not kind or not kind.replace("_", "").isalnum():
        raise ValueError(f"version kind must be alphanumeric/underscore, got {kind!r}")
    return f"{kind}:{content_hash(payload)[:VERSION_PREFIX_LEN]}"


# The version kinds this platform recognises. Named centrally so a typo becomes an error
# instead of a second, parallel namespace.
VERSION_KINDS = (
    "corpus",       # the published knowledge corpus
    "chunking",     # the chunking strategy
    "embedding",    # embedding model + config
    "feature",      # a feature definition
    "feature_group",
    "dataset",      # a training dataset
    "model",        # a trained model
    "agent",
    "prompt",
    "tool",
    "evaluation",   # an evaluation dataset
)


def typed_version(kind: str, payload: Any) -> str:
    if kind not in VERSION_KINDS:
        raise ValueError(f"unknown version kind {kind!r}; known: {', '.join(VERSION_KINDS)}")
    return version_id(kind, payload)
