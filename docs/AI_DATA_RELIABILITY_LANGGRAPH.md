# AI DATA RELIABILITY — THE LANGGRAPH COPILOT

Source: `ai/reliability/copilot.py` · phase **AIGR7**

## 1. Fixed sequence, explicit exits

```
authenticate → classify_intent → resolve_scope → classify_root_cause
   → build_plan → policy_gate → execute → build_evidence → final_answer
```

Every node may short-circuit to `build_evidence`: a run that stops early still owes an
account of why. No ReAct loop, no agent calling another agent, no step count that depends on
what a model decided.

## 2. Where the model is allowed to be

Two places only:

- `classify_intent` — and a model returning nonsense resolves to `AMBIGUOUS`, which cannot mutate.
- `final_answer` — prose composed **from a completed evidence pack**. It is appended beside
  the labelled lines the deterministic nodes already wrote; it never replaces them.

## 3. Early terminations, each with a name

`waiting_source_correction` · `code_fix_required` · `rule_fix_required` ·
`unknown_root_cause` · `blocked` · `awaiting_approval` · `refused`

## 4. Budgets

`max_steps` 24 · `max_tool_calls` 40 · `max_recovery_attempts` 1 · timeout 300s · tokens.
Checked between nodes; exceeding one terminates the graph.

**`build_evidence` and `final_answer` are deliberately unbudgeted.** The evidence pack is
the account of what happened — including the account of running out of budget. Charging for
it would mean a run that exceeds its ceiling produces no explanation, which is the one
output always owed to the user.

## 5. Answer labels

`FACT · DIAGNOSIS · PLAN · ACTION_EXECUTED · VALIDATION_RESULT · LIMITATION`

A reader can tell a measurement from an inference from a thing that has not happened yet.
Never *"Fixed"* without a `VALIDATION_RESULT` behind it.

## 6. Intent gates mutation

`MUTATING_INTENTS = {EXECUTE}`. `PLAN` builds and stops even when policy would allow
execution. `AMBIGUOUS` can never mutate — *"fix it"* without a resolved asset is a question.
