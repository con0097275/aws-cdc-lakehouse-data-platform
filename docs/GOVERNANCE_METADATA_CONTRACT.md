# GOVERNANCE METADATA CONTRACT

- Phase: **DRP1** — implemented, offline, no AWS, no DataHub
- Date: **2026-09-30**
- Code: `cdc/assets.py`, `cdc/governance.py`, `cdc/governance_plan.py`
- Config: `cdc/registry/sources.yaml`, `governance/registry/domains.yaml`,
  `governance/registry/derived_assets.yaml`
- Related: ADR-087, `docs/GOVERNANCE_CURRENT_STATE_AUDIT.md`

---

## 1. What DRP1 changed, in one number

| | DRP0 (measured) | DRP1 (compiled) |
|---|---|---|
| assets carrying governance metadata | **1** | **84** |
| live tables with an owner | 1 of 73 | every asset in the inventory |
| declared datasets that do not exist | 11 | **0** — an overlay that names one is a compile error |
| places a PII column list is maintained | 3, disagreeing | 2, and neither may restate the other |

The compiled inventory is **84 assets**: 50 CDC-side (10 captured tables × 5 identities),
20 CURATED, 13 MART, and 1 that exists only in Python. `coverage()` reports
`owned 84/84 (100.0%)`.

## 2. Canonical identity

Every governed thing has one id, `kind:name`:

| Kind | Example | Derived from |
|---|---|---|
| `src` | `src:oracle.coredb.corebank.account` | `naming.table_id` |
| `topic` | `topic:cdc.oracle.COREBANK.ACCOUNT` | `naming.topic_for` — engine-aware casing |
| `full_cdc` | `full_cdc:oracle_coredb_corebank_account` | `naming.logical_name` |
| `realtime` | `realtime:oracle_coredb_corebank_account` | same |
| `eod` | `eod:oracle_coredb_corebank_account` | same |
| `curated` | `curated:dim_customer` | dbt sources, `entities.yaml`, `DIMENSIONS` |
| `mart` | `mart:mart_customer_360_daily` | dbt models |
| `serving` | `serving:<view>` | — (layer unbound; see §7) |
| `bi_dataset` / `bi_report` | `bi_report:balances` | — (no Power BI exists) |

Two properties do real work:

- **`lineage_key`** — the three lake layers of one table share one name, so impact analysis
  can move sideways from `eod:X` to `realtime:X`.
- **The kind is part of the identity.** `dim_customer` is a CURATED table and
  `mart_customer_360_daily` is a MART one; `customer` is a source table, a FULL_CDC table, a
  REALTIME table and an EOD table. The DRP0 audit found that ambiguity resolved *differently*
  in different files — `mart.dim_customer` in the governance registry against
  `curated.dim_customer` in the catalogue.

`assets_for_table()` cross-checks its output against `TableConfig`'s own precomputed names
and refuses to compile on a mismatch. Two spellings of one table is the drift defect, so it
is an error rather than a warning.

## 3. Inheritance — four levels, and the accumulate rule

```
global      cdc/registry/sources.yaml       defaults.governance
domain      governance/registry/domains.yaml  domains.<name>.defaults
source      cdc/registry/sources.yaml       sources[].defaults.governance
asset       the table's own governance:, or the derived-asset overlay
```

Most specific wins **for scalars**. `tags`, `glossary_terms` and `pii_fields`
**accumulate**.

That asymmetry is the part worth arguing for. If tags overrode, a table declaring
`tags: [reconciled]` would silently shed its domain's `[regulated]` — the asset would become
*less* governed for having said something about itself. Accumulation means a domain-level
control cannot be dropped by a local edit. `pii_fields` accumulates per column, so a more
specific level may *refine* a column's category but cannot un-declare one.

Every resolved field carries **provenance** (`global`, `domain:<d>`, `source`, `asset`).
"Who owns this table" and "who owns everything in this domain" are different answers, and an
audit that cannot tell them apart cannot distinguish a deliberate assignment from an
unreviewed default.

## 4. The fields

| Field | Notes |
|---|---|
| `owner`, `technical_owner`, `business_owner` | the latter two fall back to `owner` |
| `domain`, `subdomain` | checked against the vocabulary; unknown = compile error |
| `description` | required at `tier_1` |
| `classification` | **not settable in a governance block** — see §5 |
| `pii_fields` | `{column, category}`; a bare column name is refused |
| `retention_class` | governance band; checked against the executable number |
| `freshness_slo_minutes` | the published promise; checked against the executable SLA |
| `criticality` | `tier_1..tier_3`; drives the ownership and certification rules |
| `tags`, `glossary_terms` | accumulate; terms must exist in the glossary |
| `contract_policy`, `dq_policy`, `reconciliation_policy`, `certification_policy` | names checked against the vocabulary |

`PiiCategory` is a closed set — `direct_identifier`, `contact`, `financial`,
`behavioural`, `pseudonymous`. `pii: true` was never enough: a name and a national id need
different masking, different retention and different approval to export, and a boolean
cannot express that.

## 5. Three places a number could have been duplicated, and what was done instead

**Classification.** It has exactly one home: the registry's own `classification:` key, which
the CDC compiler already inherits into every layer. A `classification` inside a `governance:`
block is a **compile error** naming the real home. Two spellings of one classification is the
drift DRP1 exists to end.

**Retention.** `retention_class` is the governance band; `eod.retention_days` stays the
executable number. The compiler checks the number falls inside the band
(`retention_band_mismatch`) rather than storing it twice.

**Freshness.** DRP0 open question 4 found `60` in the CDC registry and `1440` in the
governance registry, with no way to prefer one. Resolved by giving them different jobs:

- `dq.freshness_sla_minutes` — the **executable** threshold a check evaluates at
- `freshness_slo_minutes` — the **published** promise a consumer may rely on

and then refusing the combination that is actually wrong: an SLO *tighter* than the SLA.
Promising 60 minutes while only checking at 1440 is a promise nothing tests. An SLA tighter
than the SLO is fine — measuring harder than you promised.

## 6. Findings from the real registry

The compiler found 16 on first run. Fifteen were one rule:

> **`pii_requires_classification`** — `account`, `transaction` and `digital_event` declared
> PII columns while classified `internal` (the global default). The masking views and the AI
> deny list read **classification**, not the PII list, so those columns were marked and
> unprotected.

Fixed in `cdc/registry/sources.yaml`: the three tables are now `confidential`. That change
also improved an existing test — `test_the_ddl_carries_the_registrys_values_not_a_hardcoded_default`
asserted `classification = 'internal'`, which is *also* the global default, so it could not
actually tell a registry value from a hardcoded one.

One finding remains, deliberately:

> **`undeclared_asset`** — `curated:dim_customer_bi` exists but is declared only in Python.

It is governed (it appears in the inventory with an owner and a description) and still
**reported**, because a table nobody can find from config cannot be onboarded, decommissioned
or reasoned about. Closing that by fiat would hide the gap; DRP4 reconciles the list against
the live Glue catalogue.

**Not fixed, recorded:** `owner: my-aws-profile` on both sources is the AWS principal this lab
runs as — a person, not a rota. It is live config and changing it is the owner's decision,
not a side effect of a metadata phase.

## 7. Known limits

| # | Limit | Owner |
|---|---|---|
| 1 | `SERVING` has no binding in `reporting/layers.yaml`, so a `serving:` asset has an identity but no database. ADR-033 makes that a config decision, not an assumption this module may make. | DRP4 |
| 2 | No Power BI exists, so `bi_dataset` / `bi_report` are vocabulary only. | out of scope |
| 3 | `governance/registry/*.yaml` is **not** in the `cdc-framework.zip` built by `scripts/cdc-deploy-code.sh`. Nothing reads it from Spark yet; the first job that does must add it, or it fails with a missing file *after* acquiring capacity — the same shape as the `per_table` and `full_cdc_job` defects. | DRP5 |
| 4 | `governance/catalog/domains.yml` and `governance/dq/rules.yml` are **still drifted**. DRP1 did not delete them: `ai/guards.py` and `spark/ops/dq_engine.py` read them at runtime, and repointing those is DRP5's gate, not a drive-by edit. | DRP5 |

## 8. How to run it

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.governance_plan import compile_inventory; \
  inv = compile_inventory(); \
  print(inv.config_version(), inv.coverage()); \
  [print(' ', f) for f in inv.findings]"
```

No AWS, no Spark, no network. `config_version()` is a stable hash of the governance payload
with **findings excluded** — fixing an unrelated asset must not invalidate every DQ result
already stamped with that version.
