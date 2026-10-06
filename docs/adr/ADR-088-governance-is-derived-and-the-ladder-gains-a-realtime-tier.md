# ADR-088 — Governance metadata is derived from the platform's own config, and the certification ladder gains a REALTIME tier

* **Status:** Accepted
* **Date:** 2026-09-30
* **Phase:** DRP1
* **Related:** ADR-033, ADR-040, ADR-067, ADR-080, ADR-086, ADR-087

## Context

DRP0 measured a governance plane describing 12 datasets of which 1 existed, while 72 of 73
live tables carried no owner, classification, retention or SLA. ADR-087 decided the direction
— derive, do not hand-maintain — and DRP1 had to make it concrete for 84 assets.

Two problems surfaced during implementation that the brief did not anticipate.

**One: the fifth certification tier collided with a sentinel.** The brief asks for a ladder
of `CERTIFIED > RECONCILED > PROVISIONAL_CORRECTED > PROVISIONAL_NRT > REALTIME`.
`spark/common/flows.py` held four of those at ranks 1–4 and ranked an **unrecognised** status
`0`, mirrored by `ELSE 0` in `dbt/macros/status_priority.sql`, so that garbage can never win
a comparison. Numbering `REALTIME` as 0 would have made an unknown status compare *equal* to
the real bottom tier.

**Two: three files claimed to own a PII column list** — `governance/catalog/domains.yml`
(drifted), `DimensionSpec.pii_columns` in `spark/dimensions/dim_builder.py` (live, in Python),
and the new registry block. Two claimed to own classification, and two claimed to own
freshness with different numbers (60 versus 1440, DRP0 open question 4).

## Decision

**1. The asset list is derived; only metadata is authored.**
`cdc/governance_plan.py` derives 84 assets from `cdc/registry/sources.yaml` (10 tables × 5
identities), `reporting/curated/entities.yaml`, `DIMENSIONS` **parsed** out of
`dim_builder.py` with `ast`, the write targets in `curated_build.py`, and the dbt manifest.
`governance/registry/derived_assets.yaml` may add metadata to a derived asset and **may not
invent one** — an overlay key that resolves to nothing is a compile error. That single rule is
what stops it becoming the file DRP0 measured at 1-of-12.

**2. A fact has exactly one home; the second place checks rather than restates.**
`classification` lives in the CDC registry and is *refused* inside a `governance:` block.
`pii_fields` for a Kimball dimension is derived from `DimensionSpec.pii_columns`, and an
overlay that restates it is refused. `retention_class` is a band checked against
`eod.retention_days`. `freshness_slo_minutes` is the published promise, checked against the
executable `dq.freshness_sla_minutes` — an SLO tighter than the SLA is refused, which resolves
DRP0 open question 4 by giving the two numbers different jobs instead of picking a winner.

**3. Inheritance accumulates for controls and overrides for scalars.**
Four levels, most specific wins — except `tags`, `glossary_terms` and `pii_fields`, which
union. A table declaring `tags: [reconciled]` must not thereby shed its domain's
`[regulated]`; a governance default that can be dropped by a local edit is not a control.
Every field carries provenance, so a deliberate assignment is distinguishable from an
unreviewed default.

**4. The ladder moves to `cdc/certification.py` and shifts to ranks 1–5.**
`spark/common/flows.py` imports it; `dbt/macros/status_priority.sql` mirrors it and is
drift-tested. `0` stays reserved for an unrecognised status. The relative order of the
original four is unchanged, which is the only property any comparison depends on.

## Options

**Re-author `domains.yml` with 73 entries.** Rejected: that is the experiment that has
already been run here, with the result measured. A hand-maintained list drifts because the
platform beneath it is config-driven and the list is not.

**Number `REALTIME` as 0 and leave the other four alone.** The smallest diff, and wrong. It
collides with the unrecognised-status sentinel in both the Python and the SQL, so a garbage
value would tie with a real tier in an anti-downgrade comparison.

**Keep two ladders — four tiers in `flows.py`, five in the new module.** Rejected by
`flows.py`'s own comment: a second hardcoded rank table is how an anti-downgrade rule stops
matching the ladder it enforces.

**Import `dim_builder.py` to read `DIMENSIONS`.** Rejected: it uses bare module imports that
only resolve with `spark/dimensions` on `sys.path`, and `cdc/` importing from `spark/` would
invert the dependency direction the package was restructured to protect. Parsed with `ast`
instead — the same technique `test_dbt_contract.py` already uses on the dbt macro.

**Collapse `NOT_EVALUATED` into `SKIPPED` to match the brief's five statuses.** Rejected: it
would make an unevaluated required check non-blocking, re-opening the exact defect
`dq_engine.py` was written to close. Six statuses, with the extension documented.

## Consequences

* Governance is compiled, deterministic and offline: 84 assets, `owned 84/84`, a stable
  `config_version`, no AWS and no Spark.
* The compile found 15 real defects — `account`, `transaction` and `digital_event` carried PII
  while classified `internal`, and the masking views read classification, not the PII list.
  Fixed in the registry; `artifacts/cdc/table-plan.json` regenerated.
* One finding is left open on purpose: `curated:dim_customer_bi` exists only in Python. It is
  governed and still reported, because a table nobody can find from config cannot be
  onboarded or decommissioned.
* Two existing tests changed. One asserted `classification = 'internal'` — which is also the
  global default, so it could not distinguish a registry value from a hardcoded one; it is
  stronger now. The other used `account` as its non-PII example.
* `governance/catalog/domains.yml` and `governance/dq/rules.yml` are **still drifted and still
  read at runtime** by `ai/guards.py` and `dq_engine.py`. Repointing them is DRP5's gate.

## Cost

$0. No AWS resource, no deployment, no new dependency — `ast`, `json`, `hashlib` and the
already-present `PyYAML`. The compile is a sub-second local function.

The only future cost this ADR creates is `governance/registry/*.yaml` needing to enter
`cdc-framework.zip` when a Spark job first reads it (DRP5). Omitting it fails *after*
acquiring EMR capacity — the same shape as the `per_table` and `full_cdc_job` defects.

## Security

* `PiiCategory` is a closed set, so masking, retention and export rules can differ by kind
  rather than keying off a boolean.
* The compile raised three tables holding PII from `internal` to `confidential`, which is the
  field the masking views and the AI deny list actually read.
* `sample_reference` in a DQ result is validated as a location, so failing rows cannot be
  copied into a table read by dashboards, alerts and the AI assistant.
* `RecoveryPolicy` refuses `allow_source_writes` unconditionally and defaults
  `allow_offset_reset` to false.
* No credential, endpoint or account identifier is introduced anywhere in this phase.

## Rollback

* The ladder: revert `cdc/certification.py`, `spark/common/flows.py` and
  `dbt/macros/status_priority.sql` together. The three existing drift tests fail loudly if
  only some are reverted, which is the property that made the change safe to make.
* The registry classifications: revert the three `classification: confidential` lines and run
  `make cdc-compile`. The compile then reports 15 findings again rather than failing.
* The new modules (`cdc/assets.py`, `governance.py`, `contracts.py`, `quality.py`,
  `certification.py`, `incidents.py`, `governance_plan.py`) have no runtime consumer outside
  their own tests and `flows.py`'s ladder import; deleting them affects nothing else.
* The DDL creates nothing — it is a declaration.

## Validation

| Check | Result |
|---|---|
| DRP1 test suites | **118 passed** (`test_drp1_metadata.py` 52, `test_drp1_contracts.py` 66) |
| Scale, offline | 100 / 500 / 1000 assets resolved, deterministic across two runs |
| Compiled inventory | 84 assets, `owned 84/84 (100.0%)`, 1 finding (the known registration gap) |
| Determinism | two compiles produce byte-identical payloads and the same `config_version` |
| Ladder drift | `test_dbt_macro_matches_python_status_rank`, `test_every_rank_maps_back_to_a_name`, `test_unknown_statuses_rank_below_every_real_tier` all pass against the 5-tier ladder |
| CDC regression | `test_cdc_operations.py`, `test_cdc_router.py`, `test_cdc_registry.py` — 199 passed |
| Doc validator | 14 passed, 0 failed |

Not claimed: no contract is executed, no DQ result is produced, no certification is written,
no incident is raised, and none of the five declared tables exists. Those are DRP5, DRP8 and
DRP9.
