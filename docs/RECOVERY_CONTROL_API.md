# RECOVERY CONTROL API

Source: `ai/reliability/control.py` · phase **AIGR5**

## 1. Why a service rather than a direct Airflow call

If the agent could call Airflow, the safety properties would live in the agent's prompt.
Here they live in code that runs **after the agent has stopped talking**.

## 2. Surface

| operation | revalidates |
|---|---|
| `create_plan` | every job id against a **closed registry**; environment match |
| `get_plan` | |
| `approve` | approver role; **refuses an agent identity** |
| `execute` | plan exists · hash unchanged · approval valid, unexpired, same environment · job ids still registered · no duplicate in flight · idempotency |
| `status` | |
| `request_cancel` | not already terminal |

## 3. What `execute` re-checks, and why each is there

- **plan hash** — an approval is a statement about one plan. The usual way this breaks is an
  innocent edit between approval and submit; the hash makes that a different plan.
- **approval expiry** — 4 hours. An approval from yesterday is not consent today.
- **environment** — a dev approval must not transfer to prod.
- **closed job registry** — checked at create *and* at execute. A registry that changed in
  between is a real scenario.
- **duplicate in flight** — two executions of one plan is the fastest way to double-apply.
- **idempotency key** — the same approved retry converges on the same execution instead of
  running the work twice.

## 4. An agent may not approve its own plan

`approve()` refuses `agent` and any `llm:` identity. The model may **relay** an approval; it
may not **be** one.

## 5. No executor means nothing ran, and the record says so

With no executor injected, `execute` records `PENDING` and a detail reading *"recorded; no
executor injected, so nothing ran. This is a dry surface, not a silent success."*

A control service that returns `SUCCEEDED` having run nothing is the exact failure this
platform keeps finding.

## 6. Action types

`REALTIME_REBUILD · EOD_REBUILD · AUTO_CORRECT · FULFILL · MART_RERUN ·
STREAM_BATCH_REPLAY · DQ_RECHECK · RECONCILE · CERTIFY`. An enum, not a string.
