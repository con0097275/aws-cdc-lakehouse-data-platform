# Data governance

- Session: 14
- Date: 2026-08-15
- Status: **registry and access controls static-validated; nothing deployed**

---

## 1. One registry

`governance/catalog/domains.yml` — ownership, domain, grain, PK, classification, PII
columns, retention and freshness SLA for **12 datasets** across every layer.

ADR-028 *chose* this path and `docs/DATA_CONTRACTS.md` §2 referenced it **twice**, but the
file never existed — a dangling contract for six sessions. It is created here at the path
those documents already point at rather than at a new one.

**Everything derives from it**: the DQ rules, the IAM deny list, the masked tables, the
retention policy. A second list of PII columns somewhere else is exactly how masking and
policy drift apart while both files look maintained. `test_governance.py` asserts the
derivations actually hold — a registry nothing checks against is documentation, not
governance.

## 2. Classification

| Class | Layers | BI |
|---|---|---|
| `restricted` | L1, L2 — raw PII payloads | **denied** |
| `confidential` | L3, dimensions — controlled PII | **denied**, masked copy instead |
| `internal` | facts, marts — no direct identifiers | read |
| `operational` | `ops.*` telemetry | read |

## 3. Defect D14-1 — a masked view is not a masked table

Session 13 denied BI the L1/L2 prefixes. That was necessary and **not sufficient**.

`snapshot.banking_customer` and `mart.dim_customer` hold unmasked `full_name` and `dob`, and
they live under `warehouse/snapshot/*` and `warehouse/mart/*` — prefixes the BI policy
**allows in full**. The masked view `v_dim_customer_bi` did not close it:

> **An Athena view requires the caller to have read access to the underlying table.** A
> principal that can query the view can always query the base table instead.

Session 13 stated exactly this principle — *"a view is not a security boundary if the caller
can read the base table"* — and then relied on a view over a readable table anyway. The
principle was right; its application was incomplete.

### The IAM-only fix

Scope item 5 requires the IAM baseline to stay functional with Lake Formation off, so
column-level security is not available. Two changes:

1. **Explicit Deny** on the PII-bearing curated tables — S3 objects *and* Glue catalog
   entries — driven by `var.pii_table_prefixes`.
2. **A materialized masked table**, `mart.dim_customer_bi`, written by the Kimball build
   with `full_name`/`dob` already tokenised. BI reads real data that never contained the raw
   values.

`test_the_iam_deny_list_matches_the_registry` fails if the Terraform list and the registry
drift.

**Lake Formation** would allow column-level masking on a single table and is the natural
answer at scale — it stays an optional flag (`CLAUDE.md` §4.10) because it adds a governance
plane to operate for a lab with two PII tables.

## 4. Access matrix

| Principal | L1 | L2 | L3 | dim_customer | dim_customer_bi | facts/marts | ops |
|---|---|---|---|---|---|---|---|
| `spark_stream` | RW | – | – | – | – | – | W |
| `spark_eod` | R | RW | RW | RW | RW | RW | W |
| `airflow` | R | R | R | R | R | R | R |
| **`athena_bi`** | **DENY** | **DENY** | **DENY** | **DENY** | **R** | R | R |
| `governance` | R | R | R | R | R | R | RW |

The **governance** role is what runs cross-layer reconciliation, and it is deliberately not
the role Power BI uses: reconciliation needs L1/L2 by definition, so the ability to do it is
separated from the ability to build a dashboard.

## 5. Naming standards

```
<layer>.<source_system>_<domain>_<entity>     stream.oracle_corebank_customer
<layer>.<domain>_<entity>                     snapshot.banking_customer
mart.dim_<entity> / mart.fact_<grain>         mart.dim_customer, mart.fact_transaction
mart.mart_<subject>_<grain>                   mart.mart_customer_360_daily
ops.<function>                                ops.dq_result
<table>_bi                                    a materialized, PII-tokenised copy
```

Glue database names cannot contain hyphens (already handled by `local.glue_prefix`).

## 6. Retention, and the erasure/audit tension

| Dataset | Retention | Why |
|---|---|---|
| L1 | 90 days | replay source; expires once L2 has absorbed it |
| L2 | **7 years** | the audit history |
| L3 | 3 years | certified snapshots |
| dimensions/facts | 7 years | matches L2 |
| `ops.*` | 1 year | telemetry |

**L1 deletion is blocked until L2 is validated** (`deletion_blocked_until: l2_validated`).
Deleting L1 first removes the only way to rebuild — this is a correctness setting, not a
storage one.

### Right-to-delete vs immutable audit

These genuinely conflict, and the project resolves it rather than pretending otherwise:

- **L2 keeps everything for 7 years.** It is the audit record; erasing rows from it destroys
  the ability to explain a historical number.
- **L3 `_history` is opt-in per table** (`enable_snapshot_history`). `snapshot.banking_customer`
  has it **false** — a PII-bearing dataset must not default to retaining deleted records.
  `snapshot.banking_account` has it true; it carries no PII.
- **Erasure is executed at the serving layer**: the masked copy is rebuilt without the
  subject, and the active snapshot already excludes deleted PKs (S08).

That is a documented position, not a solved problem. A jurisdiction requiring erasure from
the audit layer itself would need L2 rewriting, which Iceberg can do and this project has
deliberately not automated.

## 7. Schema contracts

`ops.contract_change` records every schema change with its compatibility class and approval
reference. BACKWARD-compatible changes may be auto-approved; **drop, rename and narrowing
require a recorded human approval**, because a change that is *technically* compatible can
still break a report.

Registry-enforced BACKWARD compatibility (Session 05) rejects incompatible changes upstream;
this table is the evidence trail for the ones that got through.

## 8. OPEN-20 — the 39 orphan Glue tables

Glue database `vannk-dev-oracle-db` holds **39 tables**, has no crawler, and predates this
project's Terraform. Session 10 found it; Session 14 owns the decision.

**Decision: ISOLATE. Do not adopt, do not delete.**

- **Not adopt** — unknown provenance, no owner, no classification. Adopting it into the
  registry would assert governance metadata nobody has verified, which is worse than
  leaving it visibly ungoverned.
- **Not delete** — it is not ours to assume. Destroying 39 catalogued tables of unknown
  origin on the theory that they look unused is exactly the action that cannot be undone.
- **Isolate** — the BI and workload roles are scoped to *this project's* Glue databases by
  name (`glue_database_arns`), so the orphan database is already outside every grant. It
  costs nothing (Glue Data Catalog is free below 1M objects).

**Action for the owner:** identify it, then adopt or delete deliberately. Tracked as OPEN-20
with an explicit owner decision required — not silently closed.

## 9. Evidence

```
spark/tests/test_governance.py   27 passed
spark/tests/test_dq_engine.py    24 passed
terraform fmt/validate           Success
```

**Not tested:** the deny in practice. The policy is correct as written and rendered; that it
produces `AccessDeniedException` in Athena is `NOT_TESTED` — nothing is deployed.
