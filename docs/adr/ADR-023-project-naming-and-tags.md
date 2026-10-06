# ADR-023 — Project naming and tag schema

- Status: **ACCEPTED** (Session 01)
- Closes: defect D14
- Related: ADR-001

## Context

`CLAUDE.md:12` describes the existing project as **`kafka-dev-lab`**. Repo A actually
builds **`kafka-prod-lab-lab`** from `project_name = "kafka-prod-lab"` and
`environment = "lab"` (`terraform.tfvars:1-2`). No resource named `kafka-dev-lab` has
ever existed in account `111122223333`, and `LOCAL_PROJECT_CONTEXT.md:24` sets
`ENVIRONMENT=dev` while repo A uses `lab`.

This is not cosmetic. Repo A's AWS Budget filters on `user:Project$kafka-prod-lab`
(`budget.tf`), so the `Project` tag value is load-bearing for cost attribution — and
cost attribution is how this project stays inside $80/month.

## Options

| Option | Migration cost | Verdict |
|---|---|---|
| **Rename to `kafka-dev-lab` / `dev`** | **zero — nothing is deployed** | **CHOSEN** |
| Amend `CLAUDE.md` to `kafka-prod-lab` / `lab` | zero | Rejected |
| New name, e.g. `cdc-lakehouse` / `dev` | zero | Rejected, narrowly |

## Decision

`Project = kafka-dev-lab`, `Environment = dev`.

**This is the cheapest possible moment to fix it.** Tag changes on live resources force
replacement for some resource types and always force a budget-filter change; with
zero resources deployed, the rename is a text edit. Once MSK exists, it is not.

Aligning with `CLAUDE.md:12` rather than amending it was preferred because
`CLAUDE.md` is the operating contract every future session reads first — a contract
that disagrees with reality trains sessions to distrust it. And an environment named
`lab` while every other document says `dev` is exactly the ambiguity that leads to
someone running the wrong workspace.

`cdc-lakehouse` was genuinely tempting — the project is no longer only a Kafka lab —
but it would put the guide's own `CLAUDE.md` out of date, which reintroduces the
problem being solved. Keep the name the contract already specifies.

## Consequences

- The budget cost filter changes from `user:Project$kafka-prod-lab` to
  `user:Project$kafka-dev-lab`. If this is missed, **the budget silently monitors a
  project that does not exist** — the alarm would never fire (risk R7).
- All six required tags on every resource (`CLAUDE.md` §4.11), applied via provider
  `default_tags`, which repo A already does correctly.
- `auto_destroy_after` changes from `"manual"` to a real ISO-8601 timestamp. Repo A's
  value satisfies the tag requirement while encoding no deadline, which makes it
  unusable by any cleanup sweep.
- Resource names become `kafka-dev-lab-dev-*`. The doubled suffix is inherited from
  repo A's `${project}-${environment}` convention; it is ugly but consistent, and
  changing the convention is a wider edit than this ADR justifies.

## Cost

Zero. Prevents a mis-scoped budget filter, which is worth the entire budget.

## Security

Neutral. Correct tagging improves auditability and makes orphan detection possible.

## Rollback

Trivial before apply; a replacement after. Which is the argument for doing it now.

## Validation

- `grep -r "kafka-prod-lab"` returns nothing outside `PROVENANCE.md` and this ADR.
- The plan shows all six tags on every taggable resource.
- The budget's cost filter matches the `Project` tag value exactly.
- `auto_destroy_after` parses as a timestamp.
