# ADR-027 — Ephemeral lifecycle: destroy is the cost control

- Status: **ACCEPTED** (Session 01)
- Related: ADR-001, ADR-022, risks R7, R10, R12

## Context

`docs/COST.md` §1: the Kafka platform as currently configured costs **~$594/month at
24/7** against a **$80** budget — roughly 7×. That is not a tuning problem; it is a
constraint that determines the shape of the architecture.

`reference/COST_PROFILES.md` treats cost control as a set of guardrails. That is
necessary but not sufficient: guardrails bound the rate, they do not bound the
duration. With MSK at $0.8143/hr, duration is what matters.

## Options

| Option | Monthly | Verdict |
|---|---|---|
| Run continuously, shrink everything | ~$163 at 3 × t3.small + minimal storage | Rejected — still 2× budget |
| **Metered windows, destroy between them** | **~$80 for ~8 six-hour windows** | **CHOSEN** |
| Stop instances between windows | ~$19/month floor for idle disks and keys | Rejected as the primary control |

## Decision

The lab exists in **metered windows**. Outside a window, the only surviving resources
are the state bucket, the S3 lake, the CMKs, secrets, alarms and the budget — a floor
of ~$4.60/month. Everything else is created and destroyed per window.

> **Amended 2026-08-12 (Session 02 Stage A).** The floor is **~$5.61/month**, not
> ~$4.60: the Terraform state backend (ADR-021) adds a third always-on CMK plus a
> negligible amount of versioned state (`docs/COST.md` §3.2). The state backend is also
> the one resource **exempt** from this ADR's lifecycle — it outlives every window by
> design, so `verify-destroy` must assert that it still *exists* rather than that it is
> gone. The decision itself is unchanged.

**`destroy`, not `stop`.** This is the part that is easy to get wrong, so it is stated
as a rule: stopping leaves EBS volumes, CMKs, secrets, log groups and alarms billing —
about **$19/month**, a quarter of the budget for a lab doing nothing. EBS in
particular bills while the volume exists, including on a stopped instance
(`docs/PRICE_REFERENCE.md` §2). So `make stop-ephemeral` is a convenience for
*within* a window, never the between-window control.

Envelope: **~8 six-hour windows per month** (`docs/COST.md` §3.3) — enough for eight
end-to-end demo sessions.

## Consequences

- **This is what makes ADR-001's absorption correct.** One `terraform destroy` is both
  the dependency teardown and the cost control. Under a split-repo topology the
  destroy would break the downstream stack, and the predictable outcome would be a
  platform left running — the expensive failure.
- Every module needs a working destroy path and a verify-destroy assertion. A module
  that cannot be destroyed cleanly is a permanent cost.
- Pipeline state must survive teardown. `ops.layer_watermark`
  (`docs/DATA_CONTRACTS.md` §6.2) exists for this: after MSK is recreated, the
  pipeline resumes from S3 state rather than from Kafka.
- **RPO is 24 h** and follows from Kafka's retention (risk R6). Between windows, no
  CDC is captured at all — which is fine for synthetic lab data and would not be for
  anything real.
- `auto_destroy_after` carries a real timestamp per window, not `"manual"`.
- Endpoints, being VPC-lifetime rather than workload-lifetime, are bundled into
  workload flags so they cannot survive (risk R10).
- MSK storage autoscaling is **off** and `broker_ebs_gib = 20`: storage is a one-way
  ratchet and cannot be reduced after create (risk R12).

## Cost

Turns a $594/month platform into ~$80/month of deliberate usage. It is the single
highest-value decision in this session.

## Security

Positive, incidentally: a lab that does not exist most of the time has no attack
surface most of the time. Short-lived credentials and rotated generated passwords per
window follow naturally.

## Rollback

`enable_*` flags allow a longer-lived deployment for a specific demonstration,
provided the cost is computed first and the budget consequence recorded.

## Validation

- `make verify-destroy` asserts empty for MSK, EC2, EBS, VPC endpoints, NAT, EMR
  Serverless applications and Redshift namespaces **and** workgroups.
- Post-destroy cost is within the ~$5.61/month floor, checked against Cost Explorer
  rather than modelled.
- `show-cost-resources` lists everything billable with its stop/destroy command.
- After a destroy/recreate cycle, the pipeline resumes from `ops.layer_watermark`
  without duplicating or losing L2 rows.
