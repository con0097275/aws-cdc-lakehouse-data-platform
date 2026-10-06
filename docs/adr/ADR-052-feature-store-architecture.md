# ADR-052 — Feature store — Iceberg offline, lakehouse stays authoritative

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

ML features need history and reproducibility. The lakehouse already provides both.

## Options

A separate feature database; a managed feature store as the system of record.

## Decision

Offline features live in Iceberg under a new Glue database, partitioned by `source_cob_date`, never by the entity key. The lakehouse remains authoritative; the feature store is a derived consumer.

## Consequences

Feature tables are rebuildable from the lakehouse. They are not a source of truth and nothing may treat them as one.

## Cost

One Glue database is $0 idle; storage is megabytes at this volume; materialisation is EMR seconds on the existing auto-stop application.

## Security

Features inherit classification from their sources; `entity_id` is the surrogate key, never a raw identifier.

## Rollback

Drop the tables and delete the warehouse prefix. No other layer is touched, because the feature store only ever reads.

## Validation

Contract tests reject partitioning on an entity key and reject a group with no features.
