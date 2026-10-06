"""RAG knowledge contracts: source -> document -> chunk.

RAG SERVES DOCUMENTS, NOT ROWS. A knowledge source is documentation, ADRs, runbooks,
contracts, lineage and metadata. It is never a business table, and `KnowledgeSource`
rejects one at compile time so the boundary cannot erode by accident (ADR-048/050).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any

from .classification import Classification
from .versioning import content_hash, typed_version

# Paths whose contents must never enter the corpus. Exclusion is the PRIMARY secret
# control; redaction is the backstop. A redactor catches what it recognises; an exclusion
# catches what it never read. Mirrors ai/knowledge/build_index.py.
EXCLUDED_ROOTS = ("terraform", "artifacts", ".git", "target", "metastore_db")
EXCLUDED_PATTERNS = tuple(re.compile(p) for p in (
    r"\.tfvars", r"\.tfstate", r"terraform\.lock", r"\.env$", r"credentials",
    r"\.pem$", r"\.key$", r"id_rsa",
))

DOCUMENT_TYPES = ("architecture", "adr", "runbook", "contract", "lineage",
                  "dq_rule", "glossary", "table_doc", "column_doc", "decision_log",
                  "incident", "procedure")


class ContractViolation(ValueError):
    """A config that would break an AI platform invariant. Fatal, never a warning."""


def is_excluded(source_path: str) -> bool:
    p = source_path.replace("\\", "/").lstrip("./")
    if p.split("/")[0] in EXCLUDED_ROOTS:
        return True
    return any(rx.search(p) for rx in EXCLUDED_PATTERNS)


@dataclass(frozen=True)
class KnowledgeSource:
    """One declared origin of organizational knowledge."""
    source_id: str
    document_type: str
    include: tuple[str, ...]                 # glob patterns, repo-relative
    owner: str
    domain: str
    classification: Classification = Classification.INTERNAL
    description: str = ""
    exclude: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{2,63}", self.source_id):
            raise ContractViolation(
                f"source_id {self.source_id!r} must be lower_snake_case, 3-64 chars")
        if self.document_type not in DOCUMENT_TYPES:
            raise ContractViolation(
                f"unknown document_type {self.document_type!r}; "
                f"known: {', '.join(DOCUMENT_TYPES)}")
        if not self.include:
            raise ContractViolation(f"{self.source_id}: at least one include pattern required")
        for pat in self.include:
            if is_excluded(pat):
                raise ContractViolation(
                    f"{self.source_id}: include pattern {pat!r} reaches an excluded path. "
                    "Corpus exclusion is the primary secret control and is not negotiable "
                    "per-source.")
        if self.classification is Classification.RESTRICTED:
            raise ContractViolation(
                f"{self.source_id}: RESTRICTED content must not be indexed for retrieval. "
                "The AI plane is denied restricted data at the IAM layer too (ADR-060).")

    @property
    def version(self) -> str:
        return typed_version("corpus", asdict(self))


@dataclass(frozen=True)
class KnowledgeDocument:
    """One document as published to the corpus. `content_hash` makes staleness detectable."""
    document_id: str
    document_type: str
    source_path: str
    source_uri: str
    git_commit: str
    content_hash: str
    owner: str
    domain: str
    environment: str
    classification: Classification
    architecture_version: str
    knowledge_version: str
    created_at: datetime
    updated_at: datetime
    table_name: str | None = None
    database_name: str | None = None

    def __post_init__(self) -> None:
        if is_excluded(self.source_path):
            raise ContractViolation(
                f"{self.document_id}: source_path {self.source_path!r} is excluded from the corpus")
        if self.classification is Classification.RESTRICTED:
            raise ContractViolation(
                f"{self.document_id}: RESTRICTED documents are never published to the corpus")
        if not re.fullmatch(r"[0-9a-f]{7,64}", self.git_commit):
            raise ContractViolation(
                f"{self.document_id}: git_commit must be a hex sha, got {self.git_commit!r} — "
                "a document stamped with a commit its content does not match is worse than "
                "one with no commit at all")


@dataclass(frozen=True)
class Chunk:
    """A retrievable unit. Chunked by SECTION, never by fixed character count.

    A fixed window splits a runbook procedure across two chunks, and half a recovery
    procedure is worse than none — the reasoning already recorded in docs/AI_USE_CASE.md.
    """
    chunk_id: str
    document_id: str
    chunk_index: int
    content_hash: str
    chunking_version: str
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    embedding_version: str | None = None      # None until AI-P3 embeds it

    def __post_init__(self) -> None:
        if self.chunk_index < 0:
            raise ContractViolation(f"{self.chunk_id}: chunk_index must be >= 0")
        if not self.text.strip():
            raise ContractViolation(f"{self.chunk_id}: empty chunk")
        cls = Classification.parse(self.metadata.get("classification"))
        if not cls.retrievable_by_ai():
            raise ContractViolation(
                f"{self.chunk_id}: classification {cls.value} is not retrievable by the AI "
                "plane. Note an ABSENT classification lands here too, by design.")

    @staticmethod
    def make_id(document_id: str, chunk_index: int, text: str) -> str:
        """Deterministic: the same document and text always yield the same chunk_id."""
        return f"{document_id}#{chunk_index:04d}:{content_hash(text)[:12]}"
