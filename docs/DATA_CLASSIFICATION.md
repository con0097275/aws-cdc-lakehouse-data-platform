# DATA CLASSIFICATION

- Phase: DRP7 · Code: `cdc/governance.py` (`Classification`, `PiiCategory`, `PiiField`)
- Single home: `cdc/registry/sources.yaml` → `classification:` — refused anywhere else

---

## 1. The scheme

| Class | Meaning | BI |
|---|---|---|
| `public` | no restriction | allowed |
| `internal` | no direct identifiers | allowed |
| `confidential` | controlled personal or financial data | via masked/serving views |
| `restricted` | raw PII | **denied** |

Current state of the compiled inventory:

| | Assets |
|---|---|
| `confidential` | **54** |
| `internal` | 30 |
| `restricted` / `public` | 0 |

## 2. PII is categorised, not a boolean

`pii: true` was never enough: a name and a national id need different masking, different
retention and different approval to export, and a boolean cannot express that.

| Category | Columns in the inventory |
|---|---|
| `financial` | 27 |
| `pseudonymous` | 16 |
| `direct_identifier` | 14 |
| `behavioural` | 5 |

**35 of 84 assets carry declared PII.** A bare column name is refused — every entry needs
`{column, category}`.

## 3. The rule that fired on real config

> **`pii_requires_classification`** — an asset declaring PII columns while classified
> `public` or `internal` is a finding.

It fired **15 times** on first compile: `account`, `transaction` and `digital_event` across
all five of their identities. The masking views and the AI deny list read **classification**,
not the PII list, so those columns were marked and unprotected. All three were raised to
`confidential` in the registry.

That is the whole argument for the rule: the PII list is documentation; the classification is
the control.

## 4. One home per fact

`classification` lives in `cdc/registry/sources.yaml` and is **refused inside a `governance:`
block** with a message naming the real home. `pii_fields` for a Kimball dimension is derived
from `DimensionSpec.pii_columns` in `spark/dimensions/dim_builder.py`, and an overlay that
restates it is refused.

The DRP0 audit found three files claiming to own a PII column list, disagreeing. Two remain,
and neither may restate the other.

## 5. Values never leave the boundary

| Control | State |
|---|---|
| ingestion profiling disabled on every source | enforced in the recipes, tested |
| DQ `sample_reference` must be a **location**, never a row | enforced, tested |
| lineage facets: closed allowlist, deny list guards the allowlist at load | enforced, tested |
| `spark.openlineage.facets.custom_environment_variables=[]` | enforced |
| incident `business_keys` are ids, not personal values | by contract |

Column **names** and **categories** are published to the catalogue. Column **values** are not,
anywhere, by any path.

## 6. Classification drives real controls

- the AI assistant's BI-readability guard (`ai/guards.py`)
- the masking / serving views
- `certified_upstream` on the SERVING layer
- the IAM scope per workload

Which is why it is a closed enum: a typo in `confidental` must not silently become a new,
unenforced class.

## 7. Open

| # | Item |
|---|---|
| 1 | no asset is `restricted` — FULL_CDC carries raw before/after payloads and arguably should be |
| 2 | `ai/guards.py` still reads the drifted `governance/catalog/domains.yml`, not the compile (DRP5 gate) |
| 3 | Lake Formation column policies are not derived from these classifications |
