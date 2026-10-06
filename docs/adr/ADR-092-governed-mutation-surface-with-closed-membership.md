# ADR-092 — The agent gets one mutation surface, and its membership is closed

* **Status:** Accepted
* **Date:** 2026-09-30
* **Phase:** AIGR1
* **Supersedes:** ADR-057 (Agent V1 is read-only, enforced in code)
* **Related:** ADR-051, ADR-055, ADR-056, ADR-061, ADR-090

## Context

ADR-057 made the agent read-only and enforced it in `ToolSpec.__post_init__`:

```python
if not self.read_only:
    raise ValueError(f"{self.name}: V1 tools are read-only (ADR-057)")
```

That was right for V1 and it held. The AIGR programme needs the agent to be able to *start a
recovery* — and a recovery writes business data. So the question is not whether to relax the
rule but what to replace it with that is still enforceable.

The obvious design is "allow mutating tools, review them carefully". That fails slowly: a
surface whose *behaviour* is reviewed grows one reasonable tool at a time —
`retry_job`, `clear_task`, `refresh_partition` — until it is no longer a surface. Every
addition is defensible on its own and the aggregate is an agent that can do anything.

## Decision

**Keep read-only as the default. Add exactly one mutating authorization class, and close its
membership by name.**

1. `read_only=True` remains the default and the overwhelming majority of tools.
2. Two new authorization classes:
   - `plan_write` — may write **immutable planning and audit** objects (incidents, plans,
     scopes, simulations). May not touch business data. Still `read_only=False`? **No** —
     `plan_write` tools remain `read_only=True` with respect to *business data*, and the
     flag keeps that meaning. Planning artefacts are append-only audit records, not data.
   - `recovery_submit` — the only class permitted to carry `read_only=False`.
3. `MUTATING_TOOLS` is a **closed frozenset of two names**:
   - `submit_recovery_plan`
   - `request_recovery_cancel`
   A tool declaring `read_only=False` whose name is not in that set fails construction.
4. A `recovery_submit` tool must declare `requires_approval_ref=True` and
   `requires_idempotency_key=True`, or construction fails.

`request_recovery_cancel` is included deliberately even though it mutates: it can only
*reduce* activity. A cancel that needs an approval round-trip is a cancel that arrives after
the damage, which is the wrong safety trade.

## Options

| | Option | Why not |
|---|---|---|
| A | Keep ADR-057 unchanged | The copilot cannot start a recovery; the entire AIGR programme is a read-only reporting tool. |
| B | Allow any `read_only=False` tool, review each | The failure is gradual and invisible: every addition is individually defensible, the aggregate is unbounded. |
| C | Closed membership by name (**chosen**) | Adding a mutation requires editing this ADR and the frozenset — a deliberate, reviewable act, not a tool registration. |
| D | Separate mutating agent with its own IAM | Two agents to keep in sync, two prompt surfaces to defend, and the same question one layer down. |

## Consequences

- Adding a new mutating capability is a **code change plus an ADR amendment**, which is the
  point. Registering a tool is not enough.
- `submit_recovery_plan` takes a `plan_id` and an approval reference — never a scope, a job
  id or SQL. The agent cannot describe *what* to run; it can only submit something the
  deterministic planner already built and a policy already approved.
- The LLM never holds a credential. Authorization comes from the authenticated runtime
  context, never from anything the model produced or read.

## Cost

$0. No AWS resource. The classes and the frozenset are code.

## Security

The threat this addresses is not a malicious model; it is a **confused** one — prompt
injection from a DataHub description, a dbt comment, a DQ error message or a RAG document.
Those inputs are untrusted by design. Closed membership means the worst an injected
instruction can achieve is to submit a plan that a deterministic planner built and a policy
gate approved, under an identity the injection does not control.

Explicitly still forbidden and not reachable by any tool: shell, arbitrary SQL, arbitrary
DAG trigger, source-database write, Kafka offset reset, checkpoint deletion, S3 delete,
Terraform, IAM change.

## Rollback

Revert this ADR and restore the ADR-057 check. The copilot degrades to read-only, which is
its behaviour today and is a safe resting state — no migration, no persisted state to unwind.

## Validation

| | Result |
|---|---|
| a mutating tool not in `MUTATING_TOOLS` | construction raises |
| a `recovery_submit` tool without an approval ref | construction raises |
| a `recovery_submit` tool without an idempotency key | construction raises |
| every existing V1 tool | unchanged, still `read_only=True` |
| `MUTATING_TOOLS` membership | asserted against this ADR by test |

## The sentence this ADR exists for

> A mutation surface whose **behaviour** is reviewed will grow. A mutation surface whose
> **membership** is fixed cannot.
