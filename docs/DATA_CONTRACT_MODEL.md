# DATA CONTRACT MODEL

- Phase: **DRP1** — model implemented; no contract is executed yet (DRP5)
- Code: `cdc/contracts.py` · Tests: `spark/tests/test_drp1_contracts.py`
- Related: ADR-087, `docs/GOVERNANCE_METADATA_CONTRACT.md`, `cdc/schema_guard.py`

---

## 1. The boundary

A contract is **producer-oriented**: it states what the job writing a table guarantees, so a
consumer can depend on it and a check can verify it.

| Governance metadata | Executable contract |
|---|---|
| owner, technical/business owner | schema and its evolution policy |
| domain, subdomain, description | business key |
| classification, PII, criticality | nullability, uniqueness, accepted values |
| tags, glossary terms | referential integrity |
| retention class, freshness SLO | freshness, volume |
| | custom predicates, reconciliation requirements |

`DataContract` has **no** `owner`, `tags`, `description` or `classification` field, and
`test_a_contract_carries_no_governance_field` asserts it against the dataclass itself.

The reason is one line:

> **An owner is not falsifiable. A uniqueness rule is.**

Mixing them produces a contract whose "violations" include things no query can detect, which
trains everyone to ignore the ones that matter. It also produces the inverse failure DRP0
measured: governance attributes living in a file nothing executes, drifting for 38 sessions
while looking maintained.

## 2. What a contract declares

| Element | Type | Notes |
|---|---|---|
| `schema` | `FieldSpec(name, type, nullable, description)` | `description` is documentation; it never renders into a check |
| `business_key` | columns | required — without a grain there is nothing to state uniqueness *about* |
| `schema_evolution` | `additive_only` \| `full` \| `frozen` | reuses `cdc/models.py`; `cdc/schema_guard.py` judges actual changes |
| `freshness` | timestamp column + max lag | |
| `volume` | min / max rows, max % change | `min_rows` guards the empty-result-looks-like-a-pass failure; `max_pct_change` guards a join that fanned out |
| `nullability` | per column, max null % | |
| `uniqueness` | column tuples | |
| `accepted_values` | column + closed set | an empty set is refused: it would fail every row |
| `referential` | column → `parent_asset.parent_column` | parent is an `AssetId`, parsed and checked |
| `custom_checks` | name + boolean SQL predicate | |
| `reconciliation` | counterpart asset, metric, tolerance | |

Reconciliation is declared **in the contract**, not only in a DQ config, so that "this table
must be reconciled" survives someone deleting the reconciliation job's config. The
requirement is part of the promise, not of the tooling that happens to check it.

## 3. Check identity is derived, never typed

Every rule renders to a `ContractCheck` whose `check_id` is a hash of the rule's own content:

```
uniqueness.21be5f7f93f9   (customer_id,cob_date) is unique
freshness.1004bc8150a7    max(cob_date) within 1440m
```

DQ results are keyed on `check_id` across runs. A hand-typed id that someone renames silently
starts a **new** check with no history — the old one stops reporting and nothing says so. A
derived id cannot be renamed without changing the rule it names, and changing the rule
*should* start a new history.

## 4. Validate before rendering, and report everything

`checks()` refuses to render an invalid contract. Rendering first would hand the runtime a
uniqueness rule on a column that does not exist — which evaluates to `NOT_EVALUATED`, and
therefore **blocks**, looking like a data defect when it is a config one.

`validate()` returns every problem rather than the first. A validator that raises on problem
one turns an 84-asset backlog into 84 edit-and-rerun cycles, and the person doing it stops
after a few.

Refused at validation:

- a **nullable business key** — a NULL key silently collapses rows in every downstream join
  and cannot be deduplicated
- a rule on a column not in the schema
- an empty `accepted_values` set
- a duplicate `check_id` — two rules sharing one identity overwrite each other's results
- `min_rows > max_rows`, a non-positive freshness lag, an empty custom predicate

## 5. Policies

Contracts are attached to assets by name, through `contract_policy` in the governance
vocabulary:

| Policy | Applies to |
|---|---|
| `cdc_source_contract` | a captured source table: schema, PK, evolution |
| `curated_conformed_contract` | a conformed dimension or fact |
| `mart_serving_contract` | a mart read by BI |
| `telemetry_contract` | ops ledgers |

An asset whose `certification_policy` can reach `CERTIFIED` **must** declare a
`contract_policy` — `certified_requires_contract`. Without a contract there is no stated
shape for the check to be a check *of*.

## 6. What DRP1 did not do

No contract instance is authored for a real asset yet, and nothing executes one. DRP5 binds
the contracts to the DQ engine and moves its rule source off the drifted
`governance/dq/rules.yml`. That is a behaviour change to a module that currently blocks
publishes, so it belongs behind its own gate rather than arriving as a side effect of a
modelling phase.
