#!/usr/bin/env python3
"""Relative-link and anchor checker for the repository's Markdown.

Why anchors and not just files: a renamed heading leaves the link resolving to a real file
and landing in the wrong place, which is the failure a reader actually hits. The GitHub
anchor algorithm is reimplemented here (lowercase, strip punctuation, spaces to hyphens)
rather than guessed.

Standard library only. No network. Read-only. Exit 0 = clean, 1 = at least one broken link.
Usage: python3 scripts/check-links.py [--repo-root PATH] [-v]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

SKIP_DIRS = {".git", ".venv", "__pycache__", ".pytest_cache", "node_modules", "target"}
MD_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
MD_IMAGE = re.compile(r"!\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HTML_SRC = re.compile(r"<img[^>]+src=\"([^\"]+)\"", re.IGNORECASE)
HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*$")
FENCE = re.compile(r"^\s*```")


def md_files(root: Path):
    for p in sorted(root.rglob("*.md")):
        if not any(part in SKIP_DIRS for part in p.parts):
            yield p


def anchors_of(path: Path) -> set[str]:
    """GitHub's slugger: lowercase, drop anything but word chars/spaces/hyphens, spaces
    to hyphens. Duplicate headings get -1, -2 ... suffixes."""
    out: set[str] = set()
    seen: dict[str, int] = {}
    in_fence = False
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return out
    for line in text.splitlines():
        if FENCE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        m = HEADING.match(line)
        if not m:
            continue
        title = re.sub(r"`([^`]*)`", r"\1", m.group(2))
        title = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", title)
        title = re.sub(r"[*_]", "", title)
        slug = re.sub(r"[^\w\- ]", "", title.lower(), flags=re.UNICODE).replace(" ", "-")
        n = seen.get(slug, 0)
        seen[slug] = n + 1
        out.add(slug if n == 0 else f"{slug}-{n}")
        out.add(slug)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    root = Path(args.repo_root).resolve()

    anchor_cache: dict[Path, set[str]] = {}
    broken: list[str] = []
    checked = 0

    for p in md_files(root):
        rel = p.relative_to(root).as_posix()
        text = p.read_text(encoding="utf-8", errors="replace")
        targets = (MD_LINK.findall(text) + MD_IMAGE.findall(text) + HTML_SRC.findall(text))
        for raw in targets:
            if raw.startswith(("http://", "https://", "mailto:", "tel:", "data:", "#!")):
                continue
            checked += 1
            frag = ""
            target = raw
            if "#" in raw:
                target, frag = raw.split("#", 1)
            if target == "":
                dest = p
            else:
                dest = (p.parent / target).resolve()
                if not dest.exists():
                    broken.append(f"{rel}: missing target {raw}")
                    continue
            if frag and dest.suffix == ".md":
                if dest not in anchor_cache:
                    anchor_cache[dest] = anchors_of(dest)
                if frag.lower() not in anchor_cache[dest]:
                    broken.append(f"{rel}: anchor #{frag} not found in {target or rel}")
        if args.verbose:
            print(f"  {rel}: {len(targets)} links")

    print(f"check-links.py — {checked} relative links across "
          f"{sum(1 for _ in md_files(root))} markdown files")
    if broken:
        print(f"\nFAIL — {len(broken)} broken:")
        for b in broken:
            print(f"  {b}")
        return 1
    print("PASS — 0 broken")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
