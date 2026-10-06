# ADR-025 — Implementation repository location

- Status: **ACCEPTED** (Session 01)
- Closes: defect D13, Gap 17
- Related: ADR-001

## Context

`LOCAL_PROJECT_CONTEXT.md:6` sets
`TARGET_PROJECT=/path/to/aws-cdc-lakehouse-claude-guide-v2`
— which is **the guide package itself**. Directing implementation code there has three
concrete problems:

1. The guide's 73 files are checksummed by `MANIFEST.json` and `SHA256SUMS`. Adding
   `terraform/`, `spark/` and `airflow/` invalidates the manifest and makes tampering
   undetectable.
2. The guide's own `CLAUDE.md` and `README.md` are *guide* documents, not the
   implementation-repo root files that `reference/REPO_TREE_TARGET.md` expects at
   those paths.
3. `SESSION_WORKFLOW.md:29` requires a Git commit per handoff, but the guide package
   is not a Git repository (Gap 17) and should not become one — it is an input.

## Options

| Option | Verdict |
|---|---|
| **New sibling repo, guide vendored read-only** | **CHOSEN** |
| Implement in place, `git init` the guide package | Rejected |
| Defer the decision to Session 02 | Rejected |

## Decision

`/path/to/aws-cdc-lakehouse/` is the implementation
repository, `git init` on `main`, laid out per `reference/REPO_TREE_TARGET.md`.

The guide package stays a **read-only input**. Its `CLAUDE.md` is copied verbatim to
the new repo root — it is the operating contract, so it belongs at the root of the
repo it governs — and Session 00's three `docs/` outputs are copied with provenance
headers naming the origin and stating that corrections are made in the new repo, not
upstream.

Deferring was rejected because Session 01's own deliverables need somewhere to live.
A decision that cannot be applied to the artifacts of the session that made it is not
a decision.

## Consequences

- `LOCAL_PROJECT_CONTEXT.md` in the guide package is amended: `TARGET_PROJECT` points
  at the new repo and `REFERENCE_GUIDE_PACKAGE` is added. **This is the only write to
  the guide package**, and it is the file whose purpose is to define paths. Without
  it, a future session reading only the guide writes code into the wrong tree again.
- The guide's `MANIFEST.json` / `SHA256SUMS` stay meaningful for the 73 original
  files. `docs/` already diverged in Session 00; that is noted, not compounded.
- Two locations for Session 00's documents. The new repo's copies are authoritative;
  the provenance header says so on every file.
- Git history starts at Session 01 with Session 00 reconstructed in
  `IMPLEMENTATION_REPORT.md`.

## Cost

Zero.

## Security

Positive: `.gitignore` excludes `*.tfstate*`, `tfplan`, `*.tfvars`, `backend.hcl`,
`*.pem` and `.env` from the first commit, so state and secrets cannot be committed
even accidentally. The guide package has no `.gitignore` because it was never a repo.

## Rollback

Moving the tree elsewhere is a `git mv` plus a path update. Nothing depends on the
absolute path except `LOCAL_PROJECT_CONTEXT.md`.

## Validation

- The new repo exists, is a Git repository, and matches `REPO_TREE_TARGET.md`.
- `CLAUDE.md` at the new root is byte-identical to the guide's (verified by
  `sha256sum`).
- The guide package's only modification is `LOCAL_PROJECT_CONTEXT.md`.
- `.gitignore` blocks state, plans, tfvars and keys — tested with `git check-ignore`.
