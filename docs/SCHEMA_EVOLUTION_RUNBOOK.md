# Schema Evolution Runbook

**Contract**: ADR-068. **Code**: `cdc/schema_guard.py`.

The source changes shape and the platform has to decide, per column, whether that is safe.
The answer is one of three verdicts, and the middle one is the useful one.

---

## The three verdicts

| verdict | means | who acts |
|---|---|---|
| `ALLOW` | apply automatically; recoverable | nobody |
| `PLAN` | safe, but wide enough to want a person to see it first | a reviewer |
| `BLOCK` | never automatic; needs a migration | an engineer |

`PLAN` exists because the two-verdict version of this is useless: everything that is not
provably safe becomes `BLOCK`, an engineer overrides `BLOCK` routinely, and the override
stops being read.

```bash
python3 scripts/cdc-maintenance.py schema --table <id>       # live diff against the catalog
```

## Type-name spelling is not a type change

Athena says `varchar`, Iceberg says `string`; they are the same type. `TYPE_ALIASES` folds
those spellings so a catalog round-trip does not read as nine columns changing type.

**The subtlety, and it was a real defect**: folding the *name* must not fold the
*parameters*. The first version of the alias table made `decimal(18,2)` → `decimal(18,4)`
read as "unchanged" — permitting the single most destructive change in the set, a silent
precision shift on money. `normalise_type_name` folds the name and **keeps the parameters**:

```
varchar(50)    -> string(50)     # name folded
decimal(18,2)  -> decimal(18,2)  # parameters preserved, so a change to (18,4) is VISIBLE
```

## What each change classifies as

| change | verdict | why |
|---|---|---|
| new nullable column | `ALLOW` | additive; old rows read NULL |
| new required column | `BLOCK` | no value exists for history |
| widen `int` → `bigint`, `decimal(10,2)` → `decimal(18,2)` | `ALLOW` | no value is lost |
| narrow anything | `BLOCK` | silently truncates |
| change decimal **scale** | `BLOCK` | changes what a monetary number means |
| rename a column | `BLOCK` | indistinguishable from drop + add |
| drop a column | `PLAN` | recoverable from history, but downstream may read it |
| reorder columns | `ALLOW` | Iceberg tracks field ids, not positions |
| change the primary key | `BLOCK` | changes the grain of every derived layer |

`schema_evolution: additive_only` (the shipped default) narrows this further: anything not
purely additive is refused regardless of the table above.

---

## Applying a change

1. **See it**: `cdc-maintenance.py schema --table <id>`.
2. **Declare it** in `cdc/registry/sources.yaml` — the payload column list, with an
   `encoding` if it is temporal (see below).
3. **Plan it**: `cdc-table-plan.py --table <id>`, and read the `*** BUSINESS SEMANTICS ***`
   marks.
4. **Apply**: the provisioning job alters the Iceberg table. FULL_CDC keeps history; the
   derived layers rebuild.

### Temporal columns always need an `encoding`

Debezium temporals are **custom Connect logical types** and arrive as plain `int64`/`int32`.
Cast one to `timestamp` and you get **year 58609**, with no error anywhere. The compiler
refuses a temporal column with no declared encoding — see `cdc/rowspec.py::SOURCE_ENCODINGS`.

### A BLOCK you believe is wrong

Do not override it. A `BLOCK` verdict names the reason; if the reason does not apply to your
case, the fix is either a migration (write the new column, backfill it, then drop the old) or
a correction to the classifier with a test. An override that lives in a runbook is an
override nobody reviews.

## Rebuilding after a change

* **FULL_CDC** — never rebuilt. It is the history; a schema change adds to it.
* **REALTIME** — `full_refresh` at the cost of one window read.
* **EOD** — re-close the affected dates. A COB partition is replaced, not appended, so this
  is convergent and repeatable (`EOD_SNAPSHOT_RUNBOOK.md`).

## Rollback

Revert the registry entry and re-compile. Iceberg schema changes are metadata operations and
adding a column back restores reads of the history that was never deleted. The exception is
anything classified `BLOCK` that was forced through: those are not reversible by config,
which is what the verdict was saying.
