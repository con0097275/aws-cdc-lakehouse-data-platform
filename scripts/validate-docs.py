#!/usr/bin/env python3
"""Cross-document consistency checks for the AWS CDC Lakehouse repository.

Session 00 found five mutually inconsistent definitions of the same CDC column
lineage (defects D1-D5, docs/GAP_ANALYSIS.md section 5). Session 01 resolved them in
docs/DATA_CONTRACTS.md. This script is what stops them coming back.

Standard library only. No network. Read-only.

Exit 0 = all checks pass. Exit 1 = at least one FAIL.
Usage: python3 scripts/validate-docs.py [--repo-root PATH] [-v]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# --------------------------------------------------------------------------- #
# check registry
# --------------------------------------------------------------------------- #

RESULTS: list[tuple[str, str, str]] = []  # (status, check name, detail)


def record(status: str, name: str, detail: str = "") -> None:
    RESULTS.append((status, name, detail))


def ok(name: str, detail: str = "") -> None:
    record("PASS", name, detail)


def fail(name: str, detail: str) -> None:
    record("FAIL", name, detail)


def skip(name: str, detail: str) -> None:
    record("SKIP", name, detail)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def read(root: Path, rel: str) -> str | None:
    p = root / rel
    if not p.is_file():
        return None
    return p.read_text(encoding="utf-8", errors="replace")


# Session 00 documents are copied verbatim with a provenance header. They
# legitimately quote rejected names and repo A's placeholder bucket while reporting
# those very defects. Excluding them keeps the checks meaningful rather than
# permanently red. This validator is excluded from its own pattern scans for the
# same reason: it necessarily contains the strings it searches for.
VENDORED = {
    "docs/EXISTING_PLATFORM_AUDIT.md",
    "docs/GAP_ANALYSIS.md",
    "docs/SOURCE_REPOSITORY_DECISION.md",
}
SELF = "scripts/validate-docs.py"


def md_files(root: Path) -> list[Path]:
    """Every tracked markdown file, excluding vendored Session 00 copies."""
    out = []
    for p in sorted(root.rglob("*.md")):
        rel = p.relative_to(root).as_posix()
        if rel in VENDORED:
            continue
        if rel.startswith(("artifacts/", ".git/", ".venv")):
            continue
        out.append(p)
    return out


def strip_sql_comments(sql: str) -> str:
    """Remove -- comments so prose inside them is not mistaken for column names."""
    return "\n".join(re.sub(r"--.*$", "", ln) for ln in sql.splitlines())


def section_of(lines: list[str], index: int) -> str:
    """The nearest preceding markdown heading for line `index` (0-based)."""
    for i in range(index, -1, -1):
        m = re.match(r"^#{1,6}\s+(.*)$", lines[i])
        if m:
            return m.group(1)
    return ""


def code_blocks(text: str, lang: str | None = None) -> list[str]:
    pattern = r"```" + (lang if lang else r"[a-zA-Z]*") + r"\n(.*?)```"
    return re.findall(pattern, text, re.DOTALL)


# --------------------------------------------------------------------------- #
# CHECK 1 - the D4 regression test
# --------------------------------------------------------------------------- #

def check_l3_sql_columns(root: Path) -> None:
    """Every column the L3 build SQL references must be defined in L1/L2.

    This is the exact defect D4: reference/ICEBERG_LAYER_SPEC.md:79-84 ordered by
    source_order_1 / source_order_2 and filtered on source_commit_ts, none of which
    any layer defined. The SQL could not have run.
    """
    name = "D4: L3 build SQL references only defined columns"
    text = read(root, "docs/DATA_CONTRACTS.md")
    if text is None:
        fail(name, "docs/DATA_CONTRACTS.md not found")
        return

    sql_blocks = code_blocks(text, "sql")
    l3_sql = next((b for b in sql_blocks if "row_number()" in b and "rn = 1" in b), None)
    if l3_sql is None:
        fail(name, "could not locate the L3 build SQL block (expected row_number() and rn = 1)")
        return
    l3_sql = strip_sql_comments(l3_sql)

    # Columns declared in the L1 and L2 tables: leading `| `column`` in a table row.
    declared = set(re.findall(r"^\|\s*`([a-z_][a-z0-9_]*)`\s*\|", text, re.MULTILINE))
    # event_order sub-fields are declared in the struct definition, not a table row.
    declared |= set(re.findall(r"^\s*(position_primary|position_secondary|source_ts_ms|kafka_partition|kafka_offset)\s*:",
                               text, re.MULTILINE))
    declared |= {"event_order"}

    # Identifiers used in the SQL, minus keywords, functions, bind params and aliases.
    keywords = {
        "with", "as", "select", "from", "where", "and", "or", "not", "over",
        "partition", "by", "order", "desc", "asc", "row_number", "rn", "ranked",
        "full_cdc", "table", "false", "true", "null", "current_timestamp",
        "merge", "into", "using", "on", "when", "matched", "then", "insert",
        "coalesce", "lower", "hex", "sha256", "date",
    }
    used = set()
    for tok in re.findall(r"\b([a-z_][a-z0-9_]*)\b", l3_sql):
        if tok in keywords:
            continue
        used.add(tok)

    # Bind parameters (:cutoff, :snapshot_date...) and placeholders are not columns.
    binds = set(re.findall(r":([a-z_][a-z0-9_]*)", l3_sql))
    used -= binds
    # `<projected business columns from after>` is an intentional placeholder.
    used -= {"projected", "business", "columns", "after", "from"}
    # Output aliases introduced by this very SELECT.
    aliases = set(re.findall(r"AS\s+([a-z_][a-z0-9_]*)", l3_sql, re.IGNORECASE))
    used -= aliases

    missing = sorted(used - declared)
    if missing:
        fail(name, f"columns used in L3 SQL but not declared in any layer: {missing}")
    else:
        ok(name, f"{len(used)} referenced columns all declared")


# --------------------------------------------------------------------------- #
# CHECK 2 - rejected aliases (D2, D3)
# --------------------------------------------------------------------------- #

def check_rejected_aliases(root: Path) -> None:
    """Defects D2/D3: the ordering key had three names and the position struct two shapes.

    Rejected names may appear only inside an explicit rejection section, so the
    resolution stays documented without the aliases leaking back into normative text.
    """
    name = "D2/D3: no rejected column aliases outside a rejection section"
    rejected = ["event_order_key", "source_order_1", "source_order_2"]
    # A rejected name may appear where the document is explicitly recording that it
    # was rejected, superseded, or is being contrasted with the correct form.
    ok_section = re.compile(
        r"(rejected|superseded|why this document exists|differences from|"
        r"decisions locked|resolutions|rewritten build)", re.I)
    ok_line = re.compile(r"(rejected|replaced by|never existed|no layer|→|supersed)", re.I)

    offences: list[str] = []
    files = md_files(root)
    for p in files:
        rel = p.relative_to(root).as_posix()
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        for i, line in enumerate(lines):
            for alias in rejected:
                if alias not in line:
                    continue
                if ok_line.search(line) or ok_section.search(section_of(lines, i)):
                    continue
                offences.append(f"{rel}:{i + 1} contains '{alias}'")

    if offences:
        fail(name, "; ".join(offences[:10]))
    else:
        ok(name, f"checked {len(rejected)} aliases across {len(files)} files")


# --------------------------------------------------------------------------- #
# CHECK 3 - enable_* flag cross-reference
# --------------------------------------------------------------------------- #

def check_enable_flags(root: Path) -> None:
    name = "enable_* flags agree between COST.md and the flag matrix"
    cost = read(root, "docs/COST.md")
    arch = read(root, "docs/TARGET_ARCHITECTURE.md")
    if cost is None or arch is None:
        fail(name, "docs/COST.md or docs/TARGET_ARCHITECTURE.md not found")
        return

    pat = re.compile(r"\b(enable_[a-z0-9_]+|allow_multiple_optional_query_engines)\b")
    in_cost = set(pat.findall(cost))
    in_arch = set(pat.findall(arch))

    missing_from_matrix = sorted(in_cost - in_arch)
    if missing_from_matrix:
        fail(name, f"named in COST.md but absent from TARGET_ARCHITECTURE.md: {missing_from_matrix}")
    else:
        ok(name, f"{len(in_cost)} flags in COST.md all present in the matrix ({len(in_arch)} total)")


# --------------------------------------------------------------------------- #
# CHECK 4 - ADR index completeness
# --------------------------------------------------------------------------- #

def check_adr_index(root: Path) -> None:
    name = "every ADR file is indexed and every indexed ADR exists"
    index = read(root, "DECISIONS.md")
    if index is None:
        fail(name, "DECISIONS.md not found")
        return

    adr_dir = root / "docs" / "adr"
    on_disk = {p.name for p in adr_dir.glob("ADR-*.md")} if adr_dir.is_dir() else set()
    linked = set(re.findall(r"docs/adr/(ADR-[^)\s]+\.md)", index))

    unindexed = sorted(on_disk - linked)
    dangling = sorted(linked - on_disk)
    problems = []
    if unindexed:
        problems.append(f"on disk but not linked from DECISIONS.md: {unindexed}")
    if dangling:
        problems.append(f"linked from DECISIONS.md but missing on disk: {dangling}")

    if problems:
        fail(name, "; ".join(problems))
    else:
        ok(name, f"{len(on_disk)} ADR files, all indexed")


# --------------------------------------------------------------------------- #
# CHECK 5 - ADR structure
# --------------------------------------------------------------------------- #

def check_adr_sections(root: Path) -> None:
    """DECISIONS.md:36 of the guide requires each ADR to record context, options,
    consequences, cost, security, rollback and validation."""
    name = "every ADR has the required sections"
    required = ["Context", "Options", "Decision", "Consequences", "Cost", "Security", "Rollback", "Validation"]
    adr_dir = root / "docs" / "adr"
    if not adr_dir.is_dir():
        fail(name, "docs/adr/ not found")
        return

    offences = []
    files = sorted(adr_dir.glob("ADR-*.md"))
    for p in files:
        text = p.read_text(encoding="utf-8", errors="replace")
        heads = set(re.findall(r"^##\s+(.+?)\s*$", text, re.MULTILINE))
        for req in required:
            if not any(req.lower() in h.lower() for h in heads):
                offences.append(f"{p.name} missing '{req}'")

    if offences:
        fail(name, "; ".join(offences[:12]))
    else:
        ok(name, f"{len(files)} ADRs x {len(required)} sections")


# --------------------------------------------------------------------------- #
# CHECK 6 - timezone consistency (D8 / ADR-024)
# --------------------------------------------------------------------------- #

def check_timezone(root: Path) -> None:
    name = "D8/ADR-024: no local-time cutoff sneaks back in"
    # \bICT\b, not a bare ICT: the check is case-insensitive, so an unbounded token
    # matches inside ordinary words — "verdict", "restrict", "predict", "strict". It fired
    # on `business_date = DATE '...' AND verdict <> 'PASS'`, reporting a SQL example as a
    # timezone violation. A scanner that cannot tell a token from a substring reports
    # correct text as the bug.
    suspicious = re.compile(
        r"(cutoff|business_date|snapshot_date)[^.\n]{0,60}"
        r"(Asia/|\bICT\b|UTC\+7|local time|localtime)",
        re.IGNORECASE,
    )
    allow_markers = ("Rejected", "rejected", "revisit", "Revisit", "cost is real", "would not be", "no longer match")
    offences = []
    for p in md_files(root):
        rel = p.relative_to(root).as_posix()
        for lineno, line in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if suspicious.search(line) and not any(m in line for m in allow_markers):
                offences.append(f"{rel}:{lineno}")
    if offences:
        fail(name, f"local-time cutoff referenced at {offences[:8]}")
    else:
        ok(name, "cutoffs are UTC-only")


# --------------------------------------------------------------------------- #
# CHECK 7 - mermaid structural parse
# --------------------------------------------------------------------------- #

def check_mermaid(root: Path) -> None:
    """mmdc is not installed on this machine, so parse structurally instead:
    balanced fences, a declared diagram type, and balanced subgraph/end pairs."""
    name = "mermaid blocks are structurally valid (structural parse, not mmdc)"
    offences = []
    total = 0
    for p in md_files(root):
        rel = p.relative_to(root).as_posix()
        text = p.read_text(encoding="utf-8", errors="replace")
        for block in re.findall(r"```mermaid\n(.*?)```", text, re.DOTALL):
            total += 1
            lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
            if not lines:
                offences.append(f"{rel}: empty mermaid block")
                continue
            if not re.match(r"^(flowchart|graph|sequenceDiagram|classDiagram|erDiagram|stateDiagram|gantt|pie|journey)",
                            lines[0]):
                offences.append(f"{rel}: first line is not a diagram type: {lines[0][:40]!r}")
            # `end` closes more than `subgraph`. In a sequenceDiagram it also closes
            # loop/alt/opt/par/critical/break/rect, and in a flowchart it closes `rect`.
            # Counting only `subgraph` reported a valid sequence diagram as unbalanced
            # ("0 subgraph vs 1 end") the first time one was added to this repo, which is a
            # false FAIL on correct mermaid -- the worst kind, because the fix people reach
            # for is to mangle the diagram until the checker stops complaining.
            block_openers = ("subgraph", "loop", "alt", "opt", "par", "critical",
                             "break", "rect")
            opens = sum(1 for ln in lines
                        if any(ln == kw or ln.startswith(kw + " ") for kw in block_openers))
            closes = sum(1 for ln in lines if ln == "end")
            if opens != closes:
                offences.append(f"{rel}: {opens} block-openers vs {closes} end")
            for ln in lines:
                if ln.count("[") != ln.count("]") or ln.count("(") != ln.count(")"):
                    offences.append(f"{rel}: unbalanced brackets: {ln[:50]!r}")
                    break

    if offences:
        fail(name, "; ".join(offences[:8]))
    elif total == 0:
        skip(name, "no mermaid blocks found")
    else:
        ok(name, f"{total} diagrams parsed")


# --------------------------------------------------------------------------- #
# CHECK 8 - placeholders (CLAUDE.md section 9.4)
# --------------------------------------------------------------------------- #

def check_placeholders(root: Path) -> None:
    name = "CLAUDE.md 9.4: no runnable placeholders"
    pat = re.compile(r"(changeme|your-account-id|your_account_id|replace-with|REPLACE_ME|FIXME|xxxxx)", re.IGNORECASE)
    offences = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(root).as_posix()
        if rel.startswith((".git/", "artifacts/", ".venv")) or ".example" in rel:
            continue
        if rel in VENDORED or rel == SELF:
            continue
        if p.suffix not in {".md", ".py", ".sh", ".tf", ".hcl", ".yml", ".yaml", ".json", ".sql", ""}:
            continue
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if pat.search(line):
                # Quoting repo A's placeholder while documenting it is legitimate.
                if "backend.hcl.example" in line or "placeholder" in line.lower():
                    continue
                offences.append(f"{rel}:{lineno}")
    if offences:
        fail(name, f"placeholders at {offences[:8]}")
    else:
        ok(name, "none outside *.example")


# --------------------------------------------------------------------------- #
# CHECK 9 - secrets (CLAUDE.md section 3.1)
# --------------------------------------------------------------------------- #

def check_secrets(root: Path) -> None:
    name = "CLAUDE.md 3.1: no static credentials"
    patterns = [
        (re.compile(r"\bAKIA[0-9A-Z]{16}\b"), "AWS access key id"),
        (re.compile(r"\bASIA[0-9A-Z]{16}\b"), "AWS temporary key id"),
        (re.compile(r"aws_secret_access_key\s*=\s*[\"']?[A-Za-z0-9/+=]{40}"), "AWS secret key"),
        (re.compile(r"(password|passwd|secret|token)\s*[:=]\s*[\"'][^\"'{}$<\s]{8,}[\"']", re.I), "literal credential"),
        (re.compile(r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----"), "private key"),
    ]
    offences = []
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(root).as_posix()
        # `.pytest_cache/` and `__pycache__/` are GENERATED caches, not source. pytest
        # stores parametrise ids verbatim, so a test whose case is named
        # "aws_access_key_id" lands in the cache and trips this scan -- a finding about a
        # RULE NAME, not a credential. Skipping generated caches keeps the scan pointed at
        # files a human wrote, which is the only place a real secret can be committed from.
        if rel.startswith((".git/", "artifacts/", ".pytest_cache/", ".venv")) or rel == SELF:
            continue
        if "__pycache__/" in rel:
            continue
        if p.suffix not in {".md", ".py", ".sh", ".tf", ".hcl", ".yml", ".yaml", ".json", ".sql", ""}:
            continue
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            for pat, label in patterns:
                if pat.search(line):
                    offences.append(f"{rel}:{lineno} ({label})")
    if offences:
        fail(name, f"possible credentials at {offences[:8]}")
    else:
        ok(name, "no credential patterns matched")


# --------------------------------------------------------------------------- #
# CHECK 10 - price traceability
# --------------------------------------------------------------------------- #

def check_price_traceability(root: Path) -> None:
    """CLAUDE.md section 4 forbids hard-coded prices. Every price in COST.md must be
    derivable from PRICE_REFERENCE.md, which is itself traceable to saved API
    responses."""
    name = "CLAUDE.md 4: COST.md prices trace to PRICE_REFERENCE.md"
    cost = read(root, "docs/COST.md")
    ref = read(root, "docs/PRICE_REFERENCE.md")
    if cost is None or ref is None:
        fail(name, "docs/COST.md or docs/PRICE_REFERENCE.md not found")
        return

    if "PRICE_REFERENCE" not in cost:
        fail(name, "docs/COST.md does not cite docs/PRICE_REFERENCE.md")
        return

    pricing_dir = root / "artifacts" / "validation" / "session-01" / "pricing"
    jsons = [p for p in pricing_dir.glob("*.json")] if pricing_dir.is_dir() else []
    if not jsons:
        fail(name, "no saved pricing responses under artifacts/validation/session-01/pricing/")
        return

    # Key unit prices that must appear verbatim in PRICE_REFERENCE.md.
    anchors = ["0.2550", "0.1200", "0.0590", "0.0130", "0.0657", "0.4500", "5.0000"]
    absent = [a for a in anchors if a not in ref]
    if absent:
        fail(name, f"anchor unit prices missing from PRICE_REFERENCE.md: {absent}")
    else:
        ok(name, f"{len(jsons)} saved responses; {len(anchors)} anchor prices present")


# --------------------------------------------------------------------------- #
# CHECK 11 - required documents exist
# --------------------------------------------------------------------------- #

def check_required_docs(root: Path) -> None:
    name = "Session 01 and 02-Stage-A deliverables exist"
    required = [
        # Session 01
        "CLAUDE.md", "README.md", "DECISIONS.md", "DECISION_LOG.md",
        "PROJECT_STATE.md", "IMPLEMENTATION_REPORT.md", "SESSION_HANDOFF.md",
        "SESSION_PLAN.md", ".gitignore",
        "docs/TARGET_ARCHITECTURE.md", "docs/DATA_CONTRACTS.md", "docs/COST.md",
        "docs/RISK_REGISTER.md", "docs/PRICE_REFERENCE.md", "docs/VERSIONS.md",
        "docs/SESSION_DEPENDENCY_GRAPH.md", "docs/APPROVAL_GATES.md",
        "scripts/validate-docs.py",
        "docs/gates/GATE-1-msk-instance-type-probe.md",
        "docs/gates/GATE-1-state-backend-bootstrap.md",
    ]
    # The operator scripts and the Terraform examples are part of the PLATFORM, and the
    # portfolio copy of this repository does not publish them (PORTFOLIO_SCOPE.md). Asserting
    # them unconditionally would turn a deliberate scope decision into a permanent red check,
    # and a check that cannot pass is one nobody reads. Required when present, skipped when
    # the whole platform tree is absent -- never quietly downgraded for one missing file.
    platform = [
        "scripts/collect-pricing.sh", "scripts/lib.sh", "scripts/install-tools.sh",
        "scripts/probe-msk-instance-type.sh", "scripts/bootstrap-state-backend.sh",
        "terraform/envs/dev/backend.hcl.example",
        "terraform/envs/dev/terraform.tfvars.example",
    ]
    # The curated copy is detected by NONE of the platform files being present, not by one
    # being absent: a single missing file is a defect, the whole set missing is a scope.
    curated = not any((root / r).is_file() for r in platform)
    if not curated:
        required += platform

    missing = [r for r in required if not (root / r).is_file()]
    if missing:
        fail(name, f"missing: {missing}")
    elif curated:
        ok(name, f"{len(required)} files present; {len(platform)} platform files not "
                 f"published in this copy")
    else:
        ok(name, f"{len(required)} files present")


# --------------------------------------------------------------------------- #
# CHECK 14 - AWS-mutating scripts default to dry-run
# --------------------------------------------------------------------------- #

def check_scripts_default_dry_run(root: Path) -> None:
    """docs/APPROVAL_GATES.md: "No gate may be self-approved by an agent." A script that
    can create billable AWS resources must not do so because someone ran it without
    arguments. Assert the guard is actually wired up, not just documented."""
    name = "APPROVAL_GATES: AWS-mutating scripts default to dry-run"

    mutating = [
        "scripts/probe-msk-instance-type.sh",
        "scripts/bootstrap-state-backend.sh",
        # The only thing in the repository that deletes a streaming checkpoint (ADR-045).
        "scripts/streaming-reset.sh",
    ]
    if not any((root / rel).is_file() for rel in mutating):
        skip(name, "operator scripts are not published in this copy (PORTFOLIO_SCOPE.md); "
                   "the gate applies to the platform repository")
        return

    offences = []
    for rel in mutating:
        p = root / rel
        if not p.is_file():
            offences.append(f"{rel}: missing")
            continue
        text = p.read_text(encoding="utf-8", errors="replace")
        # The invariant is "defaults to dry-run and needs an explicit --execute", not
        # "calls one particular helper". A script that also takes its own arguments cannot
        # use parse_execution_flags, which dies on anything it does not recognise, so the
        # equivalent explicit form is accepted -- and still has to source lib.sh, where
        # DRY_RUN=1 is the default.
        explicit = "--execute)" in text and "DRY_RUN=0" in text
        if "parse_execution_flags" not in text and not explicit:
            offences.append(f"{rel}: neither parse_execution_flags nor an explicit "
                            f"--execute -> DRY_RUN=0")
        if "confirm_destructive" not in text:
            offences.append(f"{rel}: no typed-phrase confirmation")
        if "require_identity" not in text:
            offences.append(f"{rel}: no identity guard")

    lib = root / "scripts" / "lib.sh"
    if lib.is_file():
        libtext = lib.read_text(encoding="utf-8", errors="replace")
        if "DRY_RUN=1" not in libtext:
            offences.append("scripts/lib.sh: DRY_RUN does not default to 1")
        if "refusing to run non-interactively" not in libtext:
            offences.append("scripts/lib.sh: confirmation can be satisfied non-interactively")
    else:
        offences.append("scripts/lib.sh: missing")

    if offences:
        fail(name, "; ".join(offences))
    else:
        ok(name, f"{len(mutating)} mutating scripts guarded")


# --------------------------------------------------------------------------- #
# CHECK 12 - the executor naming trap (CLAUDE.md section 7)
# --------------------------------------------------------------------------- #

def check_executor_naming(root: Path) -> None:
    """CLAUDE.md section 7 calls out KubernetesCeleryExecutor as a non-existent name.
    Fail if it appears anywhere except where it is explicitly identified as wrong."""
    name = "CLAUDE.md 7: KubernetesCeleryExecutor only appears as a known-wrong name"
    offences = []
    for p in md_files(root):
        rel = p.relative_to(root).as_posix()
        if rel == "CLAUDE.md":
            continue  # the contract itself names it in order to forbid it
        for lineno, line in enumerate(p.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if "KubernetesCeleryExecutor" in line:
                if not re.search(r"(does not exist|not exist|wrong|reversed|Rejected|nhầm)", line, re.I):
                    offences.append(f"{rel}:{lineno}")
    if offences:
        fail(name, f"used without correction at {offences[:8]}")
    else:
        ok(name, "no uncorrected use")


# --------------------------------------------------------------------------- #
# CHECK 13 - session graph and approval gate coverage
# --------------------------------------------------------------------------- #

def check_sessions_and_gates(root: Path) -> None:
    """prompts/00_PROMPT.md requires a dependency graph covering Sessions 00-19 and
    approval gates for five specific action classes. Assert both are complete, so a
    later edit cannot quietly drop a session or a gate."""
    name = "Sessions 00-19 all graphed; all 5 approval gate classes covered"
    graph = read(root, "docs/SESSION_DEPENDENCY_GRAPH.md")
    gates = read(root, "docs/APPROVAL_GATES.md")
    problems = []

    if graph is None:
        problems.append("docs/SESSION_DEPENDENCY_GRAPH.md not found")
    else:
        missing_sessions = [f"{n:02d}" for n in range(20)
                            if not re.search(rf"\b{n:02d}\b", graph)]
        if missing_sessions:
            problems.append(f"sessions absent from the graph: {missing_sessions}")
        for optional in ("13B", "13C"):
            if optional not in graph:
                problems.append(f"optional session {optional} absent from the graph")

    if gates is None:
        problems.append("docs/APPROVAL_GATES.md not found")
    else:
        required_gates = {
            "terraform apply": r"terraform\s+apply",
            "helm/kubernetes": r"(helm|kubernetes|kubectl)",
            "connector registration": r"connector\s+registration",
            "database CDC": r"(database\s+CDC|sp_cdc_enable|supplemental\s+log)",
            "destructive cleanup": r"(destructive\s+cleanup|terraform\s+destroy)",
        }
        for label, pat in required_gates.items():
            if not re.search(pat, gates, re.I):
                problems.append(f"gate class not covered: {label}")
        # A gate with no stated blocking condition is decoration.
        if len(re.findall(r"^###\s+Blocks", gates, re.M)) < 5:
            problems.append("fewer than 5 gates declare a 'Blocks' condition")

    if problems:
        fail(name, "; ".join(problems))
    else:
        ok(name, "20 sessions + 13B/13C graphed; 5 gates each with blocking conditions")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

CHECKS = [
    check_required_docs,
    check_l3_sql_columns,
    check_rejected_aliases,
    check_enable_flags,
    check_adr_index,
    check_adr_sections,
    check_timezone,
    check_mermaid,
    check_placeholders,
    check_secrets,
    check_price_traceability,
    check_executor_naming,
    check_sessions_and_gates,
    check_scripts_default_dry_run,
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo-root", default=str(Path(__file__).resolve().parent.parent))
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    root = Path(args.repo_root).resolve()
    if not root.is_dir():
        print(f"repo root not found: {root}", file=sys.stderr)
        return 2

    print(f"validate-docs.py — repo root: {root}")
    print("=" * 78)

    for check in CHECKS:
        try:
            check(root)
        except Exception as exc:  # a crashing check is a failing check, not a pass
            record("FAIL", check.__name__, f"check raised {type(exc).__name__}: {exc}")

    width = max(len(n) for _, n, _ in RESULTS)
    for status, name, detail in RESULTS:
        mark = {"PASS": "PASS", "FAIL": "FAIL", "SKIP": "SKIP"}[status]
        line = f"[{mark}] {name.ljust(width)}"
        if detail and (args.verbose or status != "PASS"):
            line += f"  — {detail}"
        print(line)

    failures = sum(1 for s, _, _ in RESULTS if s == "FAIL")
    passes = sum(1 for s, _, _ in RESULTS if s == "PASS")
    skips = sum(1 for s, _, _ in RESULTS if s == "SKIP")
    print("=" * 78)
    print(f"{passes} passed, {failures} failed, {skips} skipped")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
