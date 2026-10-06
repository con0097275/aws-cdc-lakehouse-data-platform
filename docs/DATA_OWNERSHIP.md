# DATA OWNERSHIP

- Phase: DRP7 · Derived from `compile_inventory()` — regenerate, do not hand-edit
- Config: `cdc/registry/sources.yaml`, `governance/registry/domains.yaml`,
  `governance/registry/derived_assets.yaml`

---

## 1. Coverage

**84 of 84 assets have an owner (100%).** That number is the point of DRP1: the DRP0 audit
measured 1 of 73.

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.governance_plan import compile_inventory; i=compile_inventory(); \
  print(i.config_version(), i.coverage()); [print(' ',f) for f in i.findings]"
```

| Owner | Assets | What |
|---|---|---|
| `my-aws-profile` | 50 | the CDC layers — **the AWS principal this lab runs as** |
| `banking-data` | 15 | conformed banking entities, dimensions and facts |
| `data-platform` | 10 | shared/conformed reference and calendar dimensions |
| `digital-data` | 9 | conformed digital entities |

## 2. The open finding, stated plainly

> **`owner: my-aws-profile` on 50 assets is a PERSON (an IAM principal), not a rota.**

It is live config in `cdc/registry/sources.yaml`, and reassigning ownership is the owner's
decision — not a side effect of a metadata phase. It is recorded here and in
`PROJECT_STATE.md` rather than silently rewritten.

Why it matters operationally: an incident routes to an owner. A principal that is one
laptop cannot take a page.

## 3. Three owner roles

| Field | Answers | Fallback |
|---|---|---|
| `owner` | who is accountable | required — a finding if absent |
| `technical_owner` | who fixes the pipeline | falls back to `owner` |
| `business_owner` | who defines what the number means | falls back to `owner` |

The fallbacks are deliberate. A blank technical owner on an asset that *has* an owner is not
missing information — it is the same team wearing one hat. Reporting it as absent would make
the coverage number lie in the pessimistic direction, which erodes trust as fast as lying
optimistically.

**At `tier_1` the fallback is not enough**: `critical_requires_named_owners` fires unless
both are resolvable, and `critical_requires_description` unless the asset says what it is.
At that tier a defect is externally visible and "the data team" is not a routable owner.

## 4. Inheritance and provenance

```
global (registry defaults) < domain defaults < source defaults < the asset's own block
```

Every resolved field records **which level supplied it**. "Who owns this table" and "who owns
everything in this domain" are different answers, and an audit that cannot tell them apart
cannot tell a deliberate assignment from an unreviewed default.

```python
inv.by_id()["eod:oracle_coredb_corebank_account"].governance.provenance
# {'owner': 'source', 'business_owner': 'domain:core_banking', ...}
```

## 5. Domains

| Domain | Assets | Owner default |
|---|---|---|
| `core_banking` | 40 | `banking-data` |
| `digital_channel` | 34 | `digital-data` |
| `shared` | 10 | `data-platform` |
| `ops` | — | `data-platform` (ops tables are not yet in the inventory) |

The names are the ones the platform already uses. Renaming them to something tidier in the
governance file, while the registry kept the old spelling, would have created drift on day
one of the phase built to end it.

`shared` exists because "who owns `dim_date`" has a different answer from either source
system, and attaching it to one of them makes the other a second-class consumer of its own
dimension.

## 6. Ownership is published, not ingested

`extract_owners: false` on the Glue recipe and `write_semantics: PATCH` on the dbt recipe.
Ownership is **derived** (ADR-088) and pushed by `metadata_ingestion`; letting ingestion
supply it would overwrite the governed owner with whatever the Glue table happens to carry.

## 7. Open

| # | Item |
|---|---|
| 1 | `my-aws-profile` → a team rota on 50 assets |
| 2 | `curated:dim_customer_bi` is governed but declared only in Python (`undeclared_asset`) |
| 3 | the `ops.*` tables are not yet in the compiled inventory |
