# ADR-011 — Kubernetes runtime: k3s default, EKS behind a flag

- Status: **ACCEPTED** (Session 01)
- Related: ADR-010

## Context

Airflow 3 with `KubernetesExecutor` needs a Kubernetes cluster. `CLAUDE.md` §4.4
forbids EKS by default if k3s on EC2 meets the session's needs.

## Options

| Option | Control-plane cost | Verdict |
|---|---|---|
| **k3s single-node on EC2** | $0 — control plane is the node | **CHOSEN (default)** |
| EKS | ~$0.10/hr control plane, always-on, plus nodes | **Optional, `enable_eks`** |
| MWAA (managed Airflow) | environment charged continuously, no stop | Rejected |

## Decision

k3s single-node on an ephemeral `t3.large`, default. EKS behind `enable_eks = false`
with a verified destroy path.

The EKS control plane bills **continuously and independently of any workload** — it
cannot be stopped, only deleted. On a lab used ~48 hours a month, that is roughly
$73/month for an idle control plane: 91 % of the entire budget, for nothing. That
single fact settles it.

MWAA is rejected for the same structural reason: the environment is billed
continuously with no stop.

## Consequences

- No HA. Losing the node costs a window, not data: DAGs are code in Git, task state
  is in the metadata DB (backed up to S3), and pipeline state is in S3/Iceberg.
- No IRSA. Pods inherit the node instance profile (ADR-010) — coarser than
  per-service-account roles, accepted for the lab.
- k3s bundles Traefik and ServiceLB; both are disabled, since nothing is exposed.
- Airflow, and optionally Trino (ADR-014), share the node. Running Trino and Airflow
  together on one `t3.large` will be tight — the federation demo uses its own sizing
  (`docs/COST.md` §5).

## Cost

$0.1056/hr while running. EKS would add ~$0.10/hr control plane **plus** node cost,
and would keep billing between windows.

## Security

Zero-inbound SG; API server reachable only via SSM port forwarding. IMDSv2 required
with a hop limit that permits pod access while blocking external retrieval. No
`kubeconfig` written outside the node.

## Rollback

`enable_eks = true` migrates the same Helm release to EKS, since the Airflow chart is
distribution-agnostic. Requires a destroy path verified before it is ever enabled —
an orphaned EKS control plane is the most expensive single mistake available in this
project.

## Validation

- `kubectl get nodes` shows one `Ready` node.
- Airflow Helm release deploys and the scheduler reaches the metadata DB.
- `enable_eks = false` produces zero EKS resources in the plan.
- Destroy leaves no EKS cluster: `aws eks list-clusters` is empty.
