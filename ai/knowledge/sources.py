"""Allow-listed source registry, and the exclusions that are the primary secret control.

WHY AN ALLOW-LIST OF SOURCES, NOT A WALK OF THE REPO
---------------------------------------------------
A walk-and-exclude design fails open: a new directory is indexed until someone remembers to
exclude it. An allow-list fails closed — a new directory is invisible until someone decides
it should be knowledge. For a corpus that a language model reads back to operators, failing
closed is the only defensible default (ADR-048).

The exclusions below are the second control, applied to every candidate even inside an
allow-listed root, because a glob can still reach somewhere it should not.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: Directories never traversed, whatever an include pattern says.
EXCLUDED_ROOTS = frozenset({
    ".git", "target", "dbt_packages", "artifacts", "metastore_db", "spark-warehouse",
    "node_modules", "__pycache__", ".terraform", "logs", ".venv", "venv",
})

#: Path patterns that disqualify a file anywhere.
EXCLUDED_PATTERNS = tuple(re.compile(p, re.I) for p in (
    r"\.tfvars", r"\.tfstate", r"terraform\.lock", r"\.env$", r"(^|/)\.env\.",
    r"credentials", r"\.pem$", r"\.key$", r"id_rsa", r"\.p12$", r"\.pfx$",
    r"\.jks$", r"\.log$", r"\.parquet$", r"\.avro$", r"\.jar$",
    # SECRET-BEARING FILES, not documents that discuss secrets. A bare `secret` pattern
    # excluded docs/adr/ADR-031-secret-store.md -- an ADR operators need in the corpus --
    # while excluding nothing that actually held a credential. Match the file, not the word.
    r"(^|/)secrets?/", r"(^|/)secrets?\.(ya?ml|json|properties|txt|env)$",
    r"[._-]secrets?\.(ya?ml|json|properties|txt|env)$",
    r"\.zip$", r"\.gz$", r"\.png$", r"\.jpg$", r"\.pyc$",
))

#: Anything larger is a dataset, not a document. 512 KiB of prose is ~130k tokens.
MAX_DOCUMENT_BYTES = 512 * 1024


@dataclass(frozen=True)
class Source:
    source_id: str
    document_type: str
    globs: tuple[str, ...]
    owner: str
    domain: str
    classification: str = "internal"


#: The registry. Every entry is a deliberate decision that this content is knowledge.
#: Runbooks, DQ rules and dbt descriptions are here because the approved architecture
#: identified them as the three highest-value sources the previous builder never reached.
REGISTRY: tuple[Source, ...] = (
    Source("architecture", "architecture", ("docs/*.md",), "data-platform", "ops"),
    Source("adr", "adr", ("docs/adr/*.md",), "data-platform", "ops"),
    Source("runbooks", "runbook", ("docs/runbooks/*.md",), "data-platform", "ops"),
    Source("decision_log", "decision_log", ("DECISION_LOG.md", "DECISIONS.md"),
           "data-platform", "ops"),
    Source("operating_contract", "contract", ("CLAUDE.md",), "data-platform", "ops"),
    Source("dataset_registry", "contract", ("governance/catalog/domains.yml",),
           "data-governance", "ops"),
    Source("lineage", "lineage", ("governance/lineage/openlineage.yml",),
           "data-governance", "ops"),
    Source("dq_rules", "dq_rule", ("governance/dq/rules.yml",), "data-governance", "ops"),
    Source("reporting_config", "contract",
           ("reporting/layers.yaml", "reporting/jobs/*.yaml"), "data-platform", "ops"),
    Source("ai_contracts", "contract", ("aiplatform/*.yaml", "aiplatform/**/*.yaml"),
           "data-platform", "ops"),
    # dbt model/column documentation is extracted from the manifest, not globbed -- see
    # ai/knowledge/dbt_meta.py. Indexing the manifest as a file would produce one 688 KB
    # chunk that matches everything and answers nothing.
)


def is_excluded(rel_path: str) -> str | None:
    """Reason this path must not be indexed, or None."""
    # removeprefix, NOT lstrip: `lstrip("./")` strips a CHARACTER SET, so ".git/config"
    # became "git/config" and slipped past the ".git" root exclusion entirely. Same for
    # ".terraform" and ".env". Found by test 2026-08-26.
    p = rel_path.replace("\\", "/")
    while p.startswith("./"):
        p = p[2:]
    for part in p.split("/"):
        if part in EXCLUDED_ROOTS:
            return f"excluded directory {part!r}"
    for rx in EXCLUDED_PATTERNS:
        if rx.search(p):
            return f"excluded pattern {rx.pattern!r}"
    return None
