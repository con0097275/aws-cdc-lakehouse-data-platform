# AI RECOVERY — OBSERVABILITY

Phase **AIGR11**

## 1. One correlation chain

```
request_id → thread_id → tool audit records → incident_id
          → plan_id / plan_hash → policy decision → approval_id
          → execution_id → Airflow run → Spark app / dbt invocation
          → DQ run → reconciliation run → certification tier
```

`plan_hash` is the strongest link: it is content-derived, so it identifies *what was
approved* rather than *what was called*.

## 2. The evidence pack is the record

`node_build_evidence` always runs — including after a refusal or a budget stop. It carries
request, resolved entities, root cause **with evidence refs**, impact with **excluded assets
and reasons**, plan, policy decision, approval, execution, verification, safety events,
budget consumption, and agent version.

## 3. Every tool call is audited

`contract.invoke()` records tool name, version, authorization class, actor, input hash,
outcome and duration — for reads as well as writes. An input *hash*, not the input: a key
list or a sample must not be duplicated into an audit sink.

## 4. What to alert on

| signal | why |
|---|---|
| any `safety_events` entry | a refusal fired |
| `terminated = blocked` | policy stopped something |
| repeated `awaiting_approval` for one plan | an approver is stuck |
| budget exceeded | the graph hit a ceiling |
| execution `PENDING` with `nothing ran` | a dry surface mistaken for success |
| a plan built but never submitted | may be correct; a spike is not |
