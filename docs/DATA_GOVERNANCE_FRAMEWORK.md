# DATA GOVERNANCE FRAMEWORK

- Phase: DRP7 · The eleven capabilities, what each one is, and where it actually lives
- Companions: `DATA_OWNERSHIP.md`, `DATA_CLASSIFICATION.md`, `BUSINESS_GLOSSARY.md`,
  `DATA_RETENTION_POLICY.md`, `GOVERNANCE_METADATA_CONTRACT.md`

---

## 1. The organising principle

> **Governance metadata is derived from the files the platform already obeys. Nothing is
> hand-maintained twice.**

The DRP0 audit measured what the alternative produces: a careful, well-argued, hand-written
registry describing 12 datasets of which **1** existed, while **72 of 73** live tables
carried no owner, classification, retention or SLA. The file had a header arguing against
exactly that failure. It drifted anyway, because the platform beneath it became
config-driven and the YAML could not follow.

## 2. The eleven capabilities

| # | Capability | Lives in | State |
|---|---|---|---|
| 1 | **Catalog** | `compile_inventory()` → 84 assets, derived | built |
| 2 | **Glossary** | `governance/registry/domains.yaml`, Git-controlled, compile-checked | built |
| 3 | **Ownership** | the CDC registry, 4-level inheritance with provenance | built — 84/84 |
| 4 | **Domains** | 4 real domains, names taken from the platform | built |
| 5 | **Classification / PII** | the registry's `classification:` + categorised `pii_fields` | built — 54 confidential, 35 with PII |
| 6 | **Contracts** | `cdc/contracts.py` — producer-oriented, executable | modelled; none executed |
| 7 | **DQ** | `cdc/dq_catalog.py` + `spark/ops/dq_engine.py` | catalogue built; engine still reads the drifted rules file |
| 8 | **Lineage** | `cdc/lineage_graph.py` — 81 nodes, 80 edges, 0 cycles | built offline; nothing emitted |
| 9 | **Retention** | band + executable number, cross-checked | built for lake data; DQ/lineage retention unset |
| 10 | **Access / security metadata** | classification drives IAM scope, masking views, the AI deny list | catalogued, **not enforcing** |
| 11 | **Certification / trust** | `cdc/certification.py` — 5 tiers, 8 gates | built; not wired into the flows |

## 3. Capability 10 deserves its own sentence

> **Metadata tags do not enforce access. IAM, Lake Formation and the serving views do.**

The catalogue *records* the access posture so a reviewer can see it and an auditor can query
it. If the tag and the IAM policy disagree, **the IAM policy is what happens**. A platform
that lets a tag stand in for a grant has an access-control system whose control plane is a
web UI anyone with edit rights can change.

## 4. Certification requires a configurable set

An asset may reach `CERTIFIED` only with: an owner, a description, a contract, a freshness
target, a DQ verdict, lineage, and a reconciliation. Two of those are enforced today by the
governance compile (`certified_requires_dq`, `certified_requires_contract`); the rest are
enforced by the certification gates (`CERTIFICATION_MODEL.md` §3).

The set is configurable because different tiers warrant different bars — but the direction
is fixed: adding a requirement is a config change, removing one is an ADR.

## 5. Every rule names the failure it prevents

| Rule | The failure |
|---|---|
| `owner_required` | an unowned asset has nobody to route an incident to |
| `critical_requires_named_owners` | at `tier_1`, "the data team" is not a routable owner |
| `critical_requires_description` | a consumer cannot judge fitness for use from a name |
| `certified_requires_dq` | certification without a verdict means "the job finished" |
| `certified_requires_contract` | no stated shape for the check to be a check *of* |
| `pii_requires_classification` | masking reads classification, not the PII list |
| `retention_band_mismatch` | the band is read, the number deletes |
| `slo_tighter_than_sla` | a promise nothing measures |
| `undeclared_asset` | a table nobody can find from config cannot be onboarded or decommissioned |

A rule whose failure nobody can describe is a rule that gets overridden the first time it
fires.

## 6. Where governance is published

`airflow/dags/metadata_ingestion.py` → DataHub: `datasetProperties`, `ownership`, `domains`,
`globalTags`, `glossaryTerms`, and per-check `assertionInfo`.

Deliberately **separate from every business DAG**: a stale catalogue must never turn an EOD
close red. And deliberately push-only for ownership — `extract_owners: false` on Glue,
`write_semantics: PATCH` on dbt — so ingestion cannot overwrite a governed owner.

## 7. The honest scoreboard

| | DRP0 | now |
|---|---|---|
| assets with governance | 1 | **84** |
| declared datasets that do not exist | 11 | **0** |
| open governance findings | 16 | **1** (`dim_customer_bi`, declared only in Python) |
| governance *executed* against live data | none | **still none** |

The last row is the one to keep in view. Everything above is compiled, tested and offline.
No DQ result has been produced, no certification written, no metadata ingested. The phases
that change that are DRP5's engine rewire and DRP11's live run.
