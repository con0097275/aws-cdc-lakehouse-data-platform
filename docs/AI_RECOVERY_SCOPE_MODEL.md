# AI RECOVERY — SCOPE MODEL

Source: `ai/reliability/model.py` · phase **AIGR3/AIGR4**

## 1. The distinction the whole model rests on

```
column lineage      ->  WHICH jobs are affected     (dependency selection)
RecoveryCapability  ->  WHAT a job can recompute    (execution granularity)
```

Knowing that only `BALANCE` is wrong does **not** mean Spark can rewrite one column. It
means fewer jobs need to run — each at whatever granularity it actually declares.

## 2. `RecoveryScope`

Typed, never a predicate. Fields beyond dates and keys: `root_column`, `key_set_ref`,
`watermark_lower/upper`, `source_snapshot_start/end`, `affected_partitions`, `flow_mode`,
`reason`, `scope_confidence`, `scope_source`, `requested_by`, `request_id`.

Refused at construction:

| condition | why |
|---|---|
| no root asset | there is nothing to repair |
| >500 inline business keys with no `key_set_ref` | a million keys in a tool argument is a prompt, a cost and a truncation risk at once |
| `from_date > to_date` | |
| a column-narrowed scope whose lineage is not `VALIDATED` | it implies precision the graph cannot support, and the descendants it omits never get rebuilt |

`bounded` is the property everything else checks. **Unbounded is not a scope; it is the
absence of one**, and it is how a small defect becomes an outage.

## 3. `LineageConfidence`

`VALIDATED` > `DERIVED` > `DECLARED` > `ABSENT`. Only `VALIDATED` may narrow to columns.
`DECLARED` — believed but never observed — forces approval even when everything else is
small.

## 4. `RecoveryCapability`

Declared per job, never inferred: a planner that *guesses* a job supports
`BUSINESS_KEY_SET` produces a plan that silently rebuilds the wrong thing.

Narrowest first: `BUSINESS_KEY_SET` < `SOURCE_SNAPSHOT_RANGE` < `WATERMARK_RANGE` <
`PARTITION` < `COB_DATE` < `DATE_RANGE` < `FULL_TABLE`.

`smallest_supported()` falls back to `FULL_TABLE` rather than failing — **a wider rebuild
the approver can see is safer than a refusal that leaves the data wrong** — and the widened
scope carries a `reason` saying so.

## 5. Root cause decides what is permitted

14 categories, each with a `Disposition`. 8 are recoverable; the six that block recovery
entirely (`BAD_SOURCE_VALUE`, `MISSING_SOURCE_EVENT`, `SCHEMA_DRIFT`,
`TRANSFORM_LOGIC_DEFECT`, `DQ_RULE_DEFECT`, `UNKNOWN`):

| category | disposition | why a rerun is wrong |
|---|---|---|
| `BAD_SOURCE_VALUE` | `WAITING_SOURCE_CORRECTION` | rebuilding reproduces the bad value; the platform must not write the source |
| `MISSING_SOURCE_EVENT` | `WAITING_SOURCE_CORRECTION` | there is nothing to rebuild from |
| `TRANSFORM_LOGIC_DEFECT` | `CODE_FIX_REQUIRED` | the same code produces the same wrong answer, more expensively |
| `DQ_RULE_DEFECT` | `RULE_FIX_REQUIRED` | the data may be correct; repairing it repairs the wrong thing |
| `UNKNOWN` | `NO_AUTOMATIC_RECOVERY` | |

`SCHEMA_DRIFT` requires contract review first — rebuilding before reconciliation bakes the
drift in.

A `RootCause` other than `UNKNOWN` **must** carry evidence references. A cause with nothing
behind it is a guess wearing a category name.
