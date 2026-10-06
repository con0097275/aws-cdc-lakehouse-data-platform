# ADR-028 — Domain taxonomy

- Status: **ACCEPTED** (Session 01)
- Closes: defect D9
- Related: `docs/DATA_CONTRACTS.md` §2

## Context

`ARCHITECTURE.md:71-76` builds S3 paths as
`s3://<lake>/warehouse/<layer>/<domain>/<table>/` and
`reference/ICEBERG_LAYER_SPEC.md:66` names L3 tables `snapshot.<domain>_<entity>`.
**Nothing in the guide package ever defines what a domain is** or how a source table
maps to one.

Left unresolved, Session 06 improvises — and because the domain appears in the S3
prefix, an improvised or drifting mapping physically relocates data.

## Options

| Option | Verdict |
|---|---|
| **Explicit registry in `governance/catalog/domains.yml`** | **CHOSEN** |
| Derive from `source_schema` | Rejected — a schema rename silently relocates data; and one schema can hold several domains |
| Derive from `source_system` | Rejected — collapses to one domain per source, which is not a business grouping |
| Free text per table | Rejected — unenforceable |

## Decision

```text
domain = f(source_system, source_database, source_schema)
```

Registered in **one** place, `governance/catalog/domains.yml`, resolved at **write**
time and materialized as a `domain` column at L1 — never inferred at read time.

| `source_system` | `source_database` | `source_schema` | `domain` |
|---|---|---|---|
| `oracle_core` | `COREDB` | `BANK` | `core_banking` |
| `oracle_core` | `COREDB` | `PARTY` | `party` |
| `sqlserver_ops` | `OPSDB` | `dbo` | `operations` |
| `sqlserver_ops` | `OPSDB` | `sales` | `sales` |

Rules:

1. A domain is a **business** grouping, never a technical one. No domain-per-source.
2. `entity` may differ from the physical table name (`CUSTOMER` → `customer`).
3. **An unregistered triple is a hard failure at job start**, not a fallback to
   `unknown`. A silent fallback puts production data in the wrong prefix and nobody
   notices until a grant fails or a reconciliation disagrees.
4. Adding a domain is a reviewed change to `domains.yml` plus an S3 prefix and a Glue
   grant. It is not a code change.

Resolving at write time and materializing the column is the load-bearing part: it
makes the mapping at the moment of writing an immutable property of the row. A
read-time lookup would mean that editing `domains.yml` retroactively changes where
historical data appears to belong, while the bytes stay where they were.

## Consequences

- `domain` is a column at L1, L2 and L3 (`docs/DATA_CONTRACTS.md` §5–7).
- Glue grants and S3 prefix policies can be written per domain, enabling real
  least-privilege for the governance and BI roles.
- Renaming a domain is a data migration, not a config edit. That is the correct
  weight for a change that moves S3 prefixes.
- The lab's four domains are illustrative; the mechanism is what generalises.

## Cost

Zero.

## Security

Positive and material: domain-scoped S3 prefixes and Glue databases are what make
`CLAUDE.md` §3.7's per-workload IAM roles enforceable at data granularity rather than
bucket granularity.

## Rollback

Changing the function means rewriting S3 prefixes for affected tables. Adding a
domain is cheap; re-partitioning the mapping is not.

## Validation

- `governance/catalog/domains.yml` exists and covers every source table in the lab.
- An unregistered triple fails the job at start, with a message naming the triple.
- `domain` is populated on every L1 row.
- S3 prefixes match the registry.
