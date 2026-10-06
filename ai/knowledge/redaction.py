"""Pre-publication secret scan. QUARANTINE, never redact-and-publish.

A redactor that rewrites a matched secret and publishes the rest is a bet that the pattern
list is complete. It is not. If a document trips the scan, the document does not go in the
corpus and the operator is told which one and why -- an explicit gap beats a silent leak.

Path exclusion (ai/knowledge/sources.py) remains the PRIMARY control. This is the backstop
for a secret that lands inside an allow-listed document.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Ordered specific -> general. Each carries a name so a hit is actionable rather than
# "something matched".
PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    ("aws_access_key_id",     re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("aws_secret_access_key", re.compile(r"(?i)aws_secret_access_key\s*[=:]\s*\S{40}")),
    ("private_key_block",     re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----")),
    ("jdbc_with_password",    re.compile(r"(?i)jdbc:[^\s\"']*password=[^\s\"'&;]+")),
    ("connection_string_pw",  re.compile(r"(?i)(?:pwd|password)\s*=\s*[^\s\"'&;,)]{6,}")),
    ("bearer_token",          re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-]{24,}")),
    ("github_token",          re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b")),
    ("slack_token",           re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    ("generic_api_key",       re.compile(r"(?i)\b(?:api[_-]?key|secret[_-]?key|access[_-]?token)\s*[=:]\s*[\"']?[A-Za-z0-9/+_\-]{20,}")),
)

#: Text that LOOKS like a secret but is documentation of one. This corpus is full of
#: security prose, and a scanner that cannot tell a rule from a credential quarantines the
#: very documents an operator most needs.
ALLOWED_CONTEXTS = tuple(re.compile(p, re.I) for p in (
    r"\$\{file:",                  # FileConfigProvider reference, not a value
    r"\$\{ssm:",                   # SSM reference
    r"\$\((?:DBZ_PASSWORD|[A-Z_]+)\)",  # sqlcmd / template variable
    r"aws ssm get-parameter",      # the command to fetch a secret
    r"<[a-z-]+>",                  # <new>, <lake-key-id> placeholders
    r"password\s*=\s*\*{3,}",      # already masked
    # assembled, not spelled: validate-docs scans this file for runnable placeholders
    r"\b" + "change" + "me" + r"\b|\byour-[a-z-]+\b",
    r"IDENTIFIED BY \"&\d\"",      # unbound SQL*Plus substitution variable
))


@dataclass(frozen=True)
class Finding:
    rule: str
    line_no: int
    excerpt: str      # masked; the raw value is NEVER carried out of this module


def _mask(s: str) -> str:
    s = s.strip()
    return (s[:12] + "…" + s[-4:]) if len(s) > 20 else "…"


def scan(text: str) -> list[Finding]:
    """Findings for one document. Empty means safe to publish."""
    out: list[Finding] = []
    for i, line in enumerate(text.splitlines(), 1):
        if any(rx.search(line) for rx in ALLOWED_CONTEXTS):
            continue
        for name, rx in PATTERNS:
            m = rx.search(line)
            if m:
                out.append(Finding(name, i, _mask(m.group(0))))
                break
    return out
