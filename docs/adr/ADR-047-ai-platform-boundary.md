# ADR-047 — AI platform boundary — four planes, never a source of truth

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

The AI work touches every layer. Without a stated boundary it drifts into becoming a second system of record, and the first symptom is a number quoted from a vector store.

## Options

One undifferentiated 'AI layer'; RAG as a general query engine; a new AI-owned copy of curated data.

## Decision

Four separate planes — RAG/Knowledge, Feature/ML, Agent, Governance. FULL_CDC stays canonical; REALTIME and EOD stay siblings; the AI platform consumes contracts and never alters the semantics of FULL_CDC, REALTIME, EOD, CURATED, MART or OPS.

## Consequences

Each plane has its own owner, flag and kill switch. A plane can be deleted without touching the others or the lakehouse.

## Cost

$0 by itself; each plane is costed in its own ADR. No plane may be always-on.

## Security

Blast radius is bounded per plane; a compromise of the agent cannot reach raw CDC because that is denied at the IAM layer.

## Rollback

Delete the plane's flag and module; the lakehouse is untouched because nothing in it depends on the AI platform.

## Validation

`spark/tests/test_ai_contracts.py` asserts `aiplatform` never imports `ai/`; `make validate-docs` asserts the ADR set is indexed.
