"""Heading-aware, deterministic chunking.

CHUNKING_VERSION is part of every chunk id. Changing the strategy therefore changes every
chunk id, which is what makes "re-embed only what changed" decidable rather than a guess.

WHY STRUCTURE, NOT A FIXED WINDOW
---------------------------------
A fixed character window splits a runbook procedure across two chunks, and half a recovery
procedure is worse than none — retrieval returns step 4 of 7 and the reader acts on it.
Markdown already carries the author's structure; using it costs nothing and preserves the
unit a human wrote.

Fenced code blocks are never split: half a command is not a command.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

CHUNKING_VERSION = "chunking:v2-heading-aware"

MAX_CHARS = 4000          # ~1k tokens; large enough for a whole procedure
MIN_CHARS = 80            # below this a chunk is a heading with no content
OVERLAP_CHARS = 200       # carry context across a forced split

_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.M)
_FENCE = re.compile(r"^```")


@dataclass(frozen=True)
class Piece:
    heading_path: tuple[str, ...]
    text: str
    index: int


def _split_oversized(text: str) -> list[str]:
    """Split a too-large section on paragraph/line boundaries, never inside a code fence."""
    lines, out, buf, in_fence = text.splitlines(keepends=True), [], [], False
    size = 0
    for ln in lines:
        if _FENCE.match(ln):
            in_fence = not in_fence
        if size + len(ln) > MAX_CHARS and not in_fence and buf:
            out.append("".join(buf))
            tail = "".join(buf)[-OVERLAP_CHARS:]
            buf, size = [tail, ln], len(tail) + len(ln)
        else:
            buf.append(ln)
            size += len(ln)
    if buf:
        out.append("".join(buf))
    return out


def chunk_markdown(text: str) -> list[Piece]:
    """Sections delimited by headings, preserving the heading path for context."""
    matches = list(_HEADING.finditer(text))
    if not matches:
        return [Piece((), t, i) for i, t in enumerate(_split_oversized(text))
                if len(t.strip()) >= MIN_CHARS]

    # Ignore headings inside fenced code -- `# comment` in bash is not a section.
    fences, in_fence = set(), False
    for i, ln in enumerate(text.splitlines()):
        if _FENCE.match(ln):
            in_fence = not in_fence
        elif in_fence:
            fences.add(i)
    line_of = lambda pos: text.count("\n", 0, pos)
    matches = [m for m in matches if line_of(m.start()) not in fences]
    if not matches:
        return [Piece((), t, i) for i, t in enumerate(_split_oversized(text))
                if len(t.strip()) >= MIN_CHARS]

    pieces, path, idx = [], [], 0
    for i, m in enumerate(matches):
        level, title = len(m.group(1)), m.group(2).strip()
        path = path[: level - 1] + [title]
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[m.start():end].rstrip()
        if len(body.strip()) < MIN_CHARS:
            continue
        for part in _split_oversized(body):
            if len(part.strip()) >= MIN_CHARS:
                pieces.append(Piece(tuple(path), part, idx))
                idx += 1
    return pieces


def chunk_structured(text: str, path: str) -> list[Piece]:
    """YAML/JSON: split on top-level keys so one dataset's entry stays whole."""
    blocks, cur, key = [], [], None
    for ln in text.splitlines(keepends=True):
        if ln and not ln[0].isspace() and not ln.lstrip().startswith("#") and ":" in ln:
            if cur:
                blocks.append((key, "".join(cur)))
            key, cur = ln.split(":", 1)[0].strip(), [ln]
        else:
            cur.append(ln)
    if cur:
        blocks.append((key, "".join(cur)))
    out, idx = [], 0
    for k, body in blocks:
        if len(body.strip()) < MIN_CHARS:
            continue
        for part in _split_oversized(body):
            if len(part.strip()) >= MIN_CHARS:
                out.append(Piece((k,) if k else (), part, idx))
                idx += 1
    return out
