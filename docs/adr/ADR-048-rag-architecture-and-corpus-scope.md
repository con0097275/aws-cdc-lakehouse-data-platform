# ADR-048 — RAG corpus scope — exclusion beats redaction

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

Retrieval reaches whatever it indexes. A corpus that can see tfvars or state files leaks secrets no matter how good the redactor is.

## Options

Index everything and redact on the way out; index everything and filter by classification at query time.

## Decision

Exclude by PATH at build time — `terraform/`, `artifacts/`, `spark/`, `dbt/`, `airflow/`, `.git`, plus tfvars/tfstate/credential patterns. Redaction stays as a backstop, never the boundary.

## Consequences

Some genuinely useful content (dbt model descriptions) must be reached through a named file rather than by un-excluding a directory.

## Cost

$0 — exclusion is a filter in the builder.

## Security

A redactor catches what it recognises; an exclusion catches what it never read. Classification defaults to RESTRICTED when it cannot be derived, so an unlabelled chunk is withheld rather than served.

## Rollback

Rebuild the corpus from the previous commit.

## Validation

Tests assert every excluded pattern is rejected by `KnowledgeSource`, and that a chunk with no classification is refused.
