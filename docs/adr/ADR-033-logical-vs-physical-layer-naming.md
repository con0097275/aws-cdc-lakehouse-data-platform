# ADR-033 — Logical vs physical layer naming

- Status: **ACCEPTED** (Session 22)
- Related: `docs/REPORTING_REFERENCE_EVIDENCE.md` §12 F10, `docs/DATA_CONTRACTS.md` §8,
  `aws-cdc-lakehouse-claude-guide-v2/prompts/architect kafka event to s3 3layer.md`

## Context

The reporting framework must name the layers it reads. Two vocabularies exist and they
disagree on the meaning of one word:

| Term | This repository | Guide-v2 architecture prompt |
|---|---|---|
| `stream` | L1, raw CDC landing | — |
| `full_cdc` | **L2**, immutable full history | **L1**, the source of truth |
| `snapshot` | L3, dedup as-of T-1 | — |
| `EOD` | a flow that stamps `CERTIFIED` | **L2**, the dedup layer |
| `REALTIME` | does not exist | L3, bounded rolling window |

Three Glue databases already exist in AWS with the repository's names
(`terraform/modules/glue_catalog/main.tf:10`, applied 2026-08-15). `full_cdc` therefore
means L2 in every deployed object, every DDL file under `spark/`, `dbt/models/sources.yml`
and forty documents — and L1 in the prompt the reporting work is derived from.

Left unresolved, `source_layer_policy: FULL_CDC` in a job config is ambiguous, and the
ambiguity is silent: both readings return rows.

## Options

| Option | Verdict |
|---|---|
| **Logical names in config, physical names unchanged** | **CHOSEN** |
| Rename the Glue databases to match the prompt | Rejected |
| Use physical names in job config | Rejected |

Renaming means destroying and recreating three deployed Glue databases, rewriting every
DDL, `sources.yml`, macro and doc, and re-pointing S3 warehouse prefixes — for a naming
preference, before a single mart exists. Using physical names in config re-imports the
ambiguity into the one file a mart author writes.

## Decision

Job configuration references **logical layer names only**. One file,
`reporting/layers.yaml`, maps logical to physical. Business SQL never writes a physical
Glue database name; it calls `resolve_source_layer(table, flow_mode)`.

| Logical | Physical (unchanged) | Meaning |
|---|---|---|
| `FULL_CDC` | `<prefix>_full_cdc.*` | L2, immutable full CDC history — the canonical truth |
| `FULL_CDC_RAW` | `<prefix>_stream.*` | L1 landing. **Not a permitted reporting source** |
| `EOD` | `<prefix>_snapshot.*` | L3, dedup state as of COB T-1 |
| `REALTIME` | `<prefix>_realtime.*` | bounded rolling window T-N → T |
| `CURATED` | `<prefix>_curated.*` | Kimball dimensions and facts |
| `MART` | `<prefix>_mart.*` | reporting targets |
| `OPS` | `<prefix>_ops.*` | framework ledgers |

`FULL_CDC_RAW` exists so the prohibition is expressible rather than merely conventional:
config validation rejects it as a `source_layer_policy` value.

**REALTIME does not exist yet.** Building it is upstream layer work, not reporting work.
Until it does, `layers.yaml` maps `REALTIME` to the `full_cdc` database with a mandatory
bounded date predicate, and the mapping carries `status: SUBSTITUTED`. Every execution
records which mapping it used, so no run is silently reading a substitute.

## Consequences

- `docs/DATA_CONTRACTS.md` §8 gains the logical-name table and the `realtime` layer.
- The guide-v2 3-layer prompt is marked `CONFLICTING` and gains a normative mapping
  section rather than being deleted (`docs/REPORTING_ARCHITECTURE_DISCOVERY.md` §16).
- `modules/glue_catalog/variables.tf:24-33` validates a **subset**, so adding a `realtime`
  key needs no change to the validation.
- Any future physical rename is one file, not forty.

## Cost

Zero. One `realtime` Glue database when it is built (Glue databases are free; the S3
prefix and its data are not, and are costed when that work is scoped).

## Security

None. Logical names carry no credential or ARN.

## Rollback

Edit `reporting/layers.yaml` and recompile. No data movement.

## Validation

- Config validation rejects a physical database name in `source_layer_policy`.
- Config validation rejects `FULL_CDC_RAW` as a reporting source.
- A test asserts every logical name in `layers.yaml` resolves to a database present in
  `terraform output glue_database_names`.
- A test asserts no file under `dbt/models/marts/` contains a literal `_full_cdc.`,
  `_snapshot.` or `_stream.` string.
- Every `job_master_execution_hist` row records `resolved_source_layer` and whether the
  mapping was `SUBSTITUTED`.
