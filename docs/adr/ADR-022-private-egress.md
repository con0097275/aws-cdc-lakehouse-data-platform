# ADR-022 — Private-subnet egress: per-workload paths, not one uniform answer

- Status: **ACCEPTED** (Session 01)
- Closes: Gap 8 — the architecture-breaking gap
- Related: ADR-008, ADR-027, risks R4, R10, R13

## Context

`network.tf:71` declares three private route tables with **no `route` blocks at all**.
That was correct for repo A, whose only client was a toolbox in a *public* subnet. It
cannot stand here: every new compute workload needs AWS API access, and
`CLAUDE.md` §4.2 forbids NAT Gateway by default.

`docs/GAP_ANALYSIS.md` §3 framed this as endpoints-versus-NAT. The live price list
shows that framing is incomplete.

## Options — priced from `docs/PRICE_REFERENCE.md` §3

| Option | Configuration | Hourly | 48 h/month | Complies |
|---|---|---:|---:|---|
| A | 12 interface endpoints, 1 AZ | 0.1560 | 7.49 | ✅ |
| A-min | 8 interface endpoints, 1 AZ | 0.1040 | 4.99 | ✅ |
| **A-lean** | **1 interface (Glue) + S3/DynamoDB gateway** | **0.0130** | **0.62** | ✅ |
| B | 1 NAT Gateway, ephemeral | 0.0590 | 2.83 + $0.059/GB | ❌ needs amendment |
| C | 12 interface endpoints, 3 AZ | 0.4680 | 22.46 | ✅ |
| **D** | **IGW from a public subnet, zero-inbound SG** | **0.0000** | **0.00** | ✅ |

Two findings overturn the intuitive answer:

1. **A NAT Gateway is cheaper per hour than five interface endpoints** (0.0590 vs
   0.0650). "Endpoints are always cheaper than NAT" is only true at low endpoint
   counts. Option C — the cautious full set — is $341.64/month, over 4× the entire
   budget, and it keeps billing after MSK is destroyed because endpoints live with
   the VPC.
2. **Free egress already exists in this account's audited design.** Repo A's toolbox
   sits in a public subnet with a public IP and a security group with **zero inbound
   rules** (`network.tf:29`). Egress through the Internet Gateway is free.
   `CLAUDE.md` §3.3 forbids inbound `0.0.0.0/0`; it says nothing against a public IP
   with no inbound path.

## Decision

**Per-workload egress. A-lean for EMR Serverless, D for everything else.**

| Workload | Placement | Egress | Cost |
|---|---|---|---|
| Toolbox | public, public IP, zero-inbound SG | IGW | 0.0000 |
| Source lab | public, public IP, zero-inbound SG | IGW (image pulls, SSM) | 0.0000 |
| CDC runtime | public, public IP, zero-inbound SG | IGW; MSK in-VPC SG-to-SG on 9098 | 0.0000 |
| k3s / Airflow | public, public IP, zero-inbound SG | IGW; EMR Serverless API | 0.0000 |
| **EMR Serverless** | **private subnets** | **S3 gateway (free) + Glue interface** | **0.0130/hr** |

EMR Serverless is the sole workload that genuinely cannot use option D: its ENIs
never receive public IPs, so an IGW route does not reach them. It needs S3 (gateway,
free), Glue (interface), and MSK (in-VPC). Routing its logs to **S3 rather than
CloudWatch** removes the `logs` endpoint from the set.

No NAT Gateway. `CLAUDE.md` §4.2 stands unamended — which matters, because option B
would have required amending a stated invariant to save $1.66/month against A-lean.
That trade was not worth making.

## Consequences

- Endpoints are bundled into `enable_emr_serverless`, so they cannot outlive the
  workload (risk R10). A forgotten endpoint is $9.49/month.
- Single-AZ endpoint: an AZ failure stops Spark for the window. Accepted for a lab;
  3 AZ costs 3× (`docs/RISK_REGISTER.md` §3).
- Four workloads gain public IPs. This widens the network surface beyond repo A's
  single toolbox and must be documented in `docs/SECURITY.md` explicitly rather than
  inherited silently (risk R13).
- **The lean endpoint set is the least-proven part of this design.** Its failure mode
  is a hang, not an error (risk R4). The fallback is costed and one variable away.

## Cost

$0.078 per 6-hour window, versus $2.81 for option C. Over eight windows: $0.62/month
versus $22.46. See `docs/COST.md` §2.

## Security

Materially **better** than NAT for the private workload: an interface endpoint reaches
exactly one service and can carry an endpoint policy, whereas a NAT Gateway grants
general outbound internet access — an unaudited egress path and a data-exfiltration
route. For the public-subnet workloads the posture is weaker than private+endpoints
and stronger than NAT+broad egress; it is repo A's accepted pattern, and it is
recorded as a decision rather than an accident.

## Rollback

`enable_private_only = true` switches the public workloads to private subnets with
the A-min endpoint set at +$0.1040/hr, changing only `modules/vpc_endpoints` and
subnet assignments. The design is deliberately structured so this is one variable.

## Validation

- **R4 test first:** a trivial Iceberg read job from EMR Serverless with only the S3
  gateway and Glue interface endpoints and no NAT.
- `describe-nat-gateways` returns empty — always, in every environment.
- Every SG has zero inbound rules or SG-to-SG references only.
- `describe-vpc-endpoints` is empty when `enable_emr_serverless = false`.
- Aggressive AWS SDK timeouts set, so a missing endpoint fails fast instead of hanging.
