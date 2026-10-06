# ADR-093 — A column reference matches a name or a whole segment, never a fragment

* **Status:** Accepted
* **Date:** 2026-09-30
* **Phase:** AIGR2 / AIGR12
* **Related:** ADR-090, ADR-091, ADR-092

## Context

`ai/reliability/context.get_column_lineage()` decided whether a user's word named a real
column by testing it against the graph's column edges:

```python
edges = [e for e in graph.column_edges()
         if column.lower() in (getattr(e, "upstream_column", "") or "").lower()
         or column.lower() in (getattr(e, "downstream_column", "") or "").lower()]
```

A substring test. `"in" in "closing_balance"` is `True` — the fragment sits inside
`clos*in*g_balance`.

Separately, `ai/reliability/ask.resolve()` extracted column candidates with a pattern that
reads the word **after** the keyword:

```python
re.findall(r"(?:column|field)\s+([A-Za-z_][A-Za-z0-9_]*)", question, re.I)
```

English puts the name before the keyword as often as after. In *"What does the BALANCE
column in EOD ACCOUNT feed?"* the word after `column` is the preposition **in**.

The two defects compounded. `in` was extracted, matched `closing_balance` by substring, and
returned confidence `DERIVED` — **not** `ABSENT`. The resolver accepts the first non-`ABSENT`
candidate and stops:

```python
for c in candidates:
    cl = get_column_lineage(r.asset_id, c)
    if cl["confidence"] != LineageConfidence.ABSENT.value:
        r.column, r.column_confidence = c, cl["confidence"]
        break
```

`BALANCE` was sitting in the same candidate list, one position later, and was never tried.

## What it actually cost

The visible symptom was a wrong label: the copilot answered `column in`.

The expensive part was silent. `BALANCE` is **VALIDATED** — confirmed against real data by
`cdc/column_validation.py`. `DERIVED` is below the bar that permits narrowing (ADR-090), so
the planner correctly fell back to table level and rebuilt whole dates. The flagship
scenario — *"repair **only** the affected downstream data"* — had the evidence to narrow to
one column and three keys, and did not use it. Descendant actions ran at `COB_DATE` where
they were entitled to run at `BUSINESS_KEY_SET`.

Nothing was wrong. Everything was wider than it needed to be, and the answer that said so
was the one nobody reads twice.

`docs/AI_COPILOT_SAMPLE_QUESTIONS.md` had recorded the expectation as
`confidence VALIDATED` since AIGR2. The documentation was right and the implementation was
wrong, and no test compared them.

## Decision

1. **A column reference matches by name, or by one whole `_`-delimited segment of a name.**
   `BALANCE` → `closing_balance` (segment). `in` → `closing_balance` (nothing). `bal` →
   `closing_balance` (nothing: a prefix is not a name).

   ```python
   def _names_column(edge_column: str, wanted: str) -> bool:
       ec, w = (edge_column or "").lower(), wanted.lower()
       return bool(ec) and (ec == w or w in ec.split("_"))
   ```

2. **Filler is dropped before the lineage lookup, not after.** Candidates are filtered
   against the same `_FILLER` stoplist that asset scoring already used, plus a length floor,
   so a preposition never reaches `get_column_lineage()` and never consumes the `break`.

3. **Both word orders are accepted for both keywords.** `column X`, `field X`, `X column`
   and `X field`. Only the keyword-first pattern had accepted `field`; fixing the filler bug
   alone turned *"the BALANCE field in EOD ACCOUNT is wrong"* from a wrong answer into no
   answer — which moves a defect rather than removing it. That gap was found by the
   regression test written for defect 1, not by running the fix.

## Options

1. **Relabel the 39 unvalidated edges as `VALIDATED`.** Rejected: it makes the symptom
   disappear by lowering the evidence bar, which is the opposite of the fix.
2. **Fuzzy-match column names (edit distance, prefix).** Rejected: it widens exactly the
   failure being repaired. `bal` would match `closing_balance`, and the next fragment to
   collide would be found by an operator, not a test.
3. **Require an exact column name and nothing else.** Rejected: *"the BALANCE column"* is how
   people refer to `closing_balance`, and forcing the physical name makes the copilot
   unusable for the person most likely to be asking during an incident.
4. **Name or whole `_`-delimited segment, with filler dropped before the lookup.** Chosen:
   accepts the way people speak, refuses fragments, and cannot silently widen a recovery.

## Consequences

* The flagship scenario narrows: descendants plan at `BUSINESS_KEY_SET`, not `COB_DATE`.
* Tightening the match can only *remove* column hits, never invent one. A question whose
  column no longer resolves falls back to table level — wider, and correct — so the change
  cannot make a recovery too narrow.
* `LineageConfidence.ABSENT` regains its meaning. It had become unreachable for any short
  word, and a confidence that cannot be `ABSENT` is not a confidence.

## Cost

**$0.** Pure-Python resolution; no AWS call, no model call, no additional query. Recoveries
planned after this change are *cheaper*: descendants that previously rebuilt a whole
`COB_DATE` now rebuild a `BUSINESS_KEY_SET` when the question supports it.

## Security

No change to the authorization surface. `MUTATING_TOOLS` is untouched (ADR-092), and column
resolution is a read path that reaches no mutating tool. The change can only narrow or drop a
column hit, never invent one, so it cannot broaden what an approved plan is permitted to
touch. A question whose column no longer resolves falls back to table-level scope — wider,
slower, and correct.

## Rollback

Revert `_names_column()` to the substring test in `ai/reliability/context.py` and the
candidate filter in `ai/reliability/ask.py`. Both are self-contained and neither has a
migration or a persisted artifact. `TestColumnNameResolution` in
`spark/tests/test_aigr_intent_resolution.py` fails immediately on revert, which is the
intended alarm.

## Validation

```bash
python3 -m pytest spark/tests/test_aigr_intent_resolution.py -q     # 37 passed
python3 -m pytest spark/tests/ airflow/tests/ -q                    # 3,497 passed, 0 failed

scripts/reliability-ask.py "What does the BALANCE column in EOD ACCOUNT feed?"
#   column   BALANCE  confidence=VALIDATED   <- may narrow the recovery

scripts/reliability-ask.py "the BALANCE field in EOD ACCOUNT is wrong"
#   column   BALANCE  confidence=VALIDATED

scripts/reliability-ask.py "Plan a recovery for EOD ACCOUNT on 2026-09-28"
#   no column named -> no column resolved; 22 actions, all COB_DATE (unchanged)
```

Live-tested 2026-09-30. The flagship scenario plans descendants at `BUSINESS_KEY_SET` where
it previously used `COB_DATE`.

## The general rule

**A near-miss that lands on a lower confidence tier is more dangerous than an error.** An
`ABSENT` would have been visible: no column, table-level scope, question over. `DERIVED` is
a plausible state — a real column whose lineage merely has not been validated yet — so it
propagated through the planner, the policy gate and the printed plan without once looking
wrong. Matching loosely to be helpful produced an answer that was confidently, defensibly,
and invisibly too wide.
