# ADR-056 — Agent runtime — Lambda now, AgentCore deferred

- Status: **ACCEPTED** (AI-P1, 2026-08-26)
- Related: `docs/AI_TARGET_ARCHITECTURE.md`, `docs/AI_DATA_CONTRACTS.md`

## Context

The agent needs somewhere to run. The options differ by an order of magnitude in idle cost.

## Options

ECS/Fargate — pays while idle. The existing k3s node — couples the copilot to orchestration uptime. Bedrock Agents Classic — not permitted for a new architecture.

## Decision

Local CLI for V1; AWS Lambda when hosting is needed. Bedrock AgentCore Runtime is DEFERRED, with a stated revisit trigger: multiple concurrent human users needing isolated sessions with managed identity.

## Consequences

The same graph runs locally and hosted, so the local path stays testable without AWS.

## Cost

Lambda is $0 idle. AgentCore was not costed here because it could not be verified — see Validation.

## Security

The runtime assumes the dedicated AI role. No existing workload role is broadened. No public endpoint, no inbound 0.0.0.0/0.

## Rollback

Set `enable_ai_agent_runtime=false`; the local CLI is unaffected.

## Validation

Verified 2026-08-25: `aws bedrock-agentcore-control` is absent from the installed botocore, and the AWS provider is pinned exactly at 6.56.0. AgentCore cannot be expressed or tested today; AI-P9 re-verifies before deciding.

---

## AI-P9 re-evaluation — outcome: **DEFERRED_LAMBDA** (unchanged)

Re-checked against the live account and the pinned provider on 2026-08-26.

### The capability objection is withdrawn

ADR-056 originally deferred AgentCore partly because it "could not be expressed or tested
today". **That was wrong**, and AI-P3 corrected it: provider 6.56.0 ships
`aws_bedrockagentcore_agent_runtime` and `aws_bedrockagentcore_agent_runtime_endpoint`. The
CLI still cannot address them; Terraform always could.

So the deferral now rests on requirements alone, which is a stronger footing.

### What AgentCore would actually require

| | |
|---|---|
| artifact | `container_configuration.container_uri` (an ECR image to build, push and store) **or** `code_configuration` |
| network | `network_configuration.network_mode` required; VPC mode needs subnets + security groups |
| identity | `authorizer_configuration.custom_jwt_authorizer` — an identity provider this lab does not have |
| endpoint | a separate `agent_runtime_endpoint` resource |

### The trigger has not fired

ADR-056's revisit trigger is **multiple concurrent human users needing isolated sessions
with managed identity**. This is a single-operator lab with no identity provider. AgentCore's
distinguishing features — managed sessions, managed identity, per-session isolation — have
no consumer here. Adopting it would add a second packaging path and an ECR image to maintain
in exchange for capabilities nothing uses.

### A second, decisive fact

**Bedrock is not invokable from this account right now.** Anthropic models return
`ResourceNotFoundException: Model use case details have not been submitted`, and Cohere
embeddings return `AccessDeniedException: INVALID_PAYMENT_INSTRUMENT` — the latter having
worked during AI-P3.

Deploying a managed agent runtime whose entire purpose is to host an agent that calls a
model, while no model can be called, would provision cost and surface area for something
that cannot function. That is true of Lambda too, which is why **neither is applied** in
this phase.

### Decision

**Lambda remains the selected runtime and is implemented as Terraform, flag-off, plan-only.**

- `terraform/modules/ai_runtime/` — Lambda + dedicated `ai` role + log group, `default = false`
- `$0 idle`; `reserved_concurrent_executions = 2` as a cost ceiling, not a performance knob
- No VPC config, therefore no NAT Gateway: the function needs Bedrock, Athena, Glue and S3,
  all public AWS endpoints. The **role is the boundary, not the network**.
- No function URL, no public endpoint, no inbound rule

**Re-open this decision when there are concurrent users needing isolated managed sessions —
not merely when AgentCore becomes convenient to reach.**

