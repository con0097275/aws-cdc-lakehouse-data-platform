# ADR-008 — Spark runtime: EMR Serverless on ARM64

- Status: **ACCEPTED** (Session 01)
- Related: ADR-009, ADR-022, risk R4

## Context

Spark writes every CDC layer transition and every Kimball mart. Options: EMR
Serverless, EMR on EC2, EMR on EKS, Glue ETL, or self-managed Spark.

## Options

| Option | Idle cost | Verdict |
|---|---|---|
| **EMR Serverless** | zero when idle, auto-stop | **CHOSEN** |
| EMR on EC2 | cluster runs continuously | Rejected — `CLAUDE.md` §4 |
| EMR on EKS | requires EKS control plane, always-on | Rejected — `CLAUDE.md` §4.4 |
| Glue ETL | $0.44/DPU-hour, ~6.7× EMR Serverless per equivalent unit | Rejected |
| Self-managed Spark on k3s | cheap but hand-operated | Rejected |

## Decision

EMR Serverless with `architecture = ARM64`, no pre-initialized capacity, auto-stop
at 15 minutes idle, and a 120-minute job timeout.

**ARM64 is the cheapest uncontroversial saving in the whole design.** From
`docs/PRICE_REFERENCE.md` §5:

| | x86 | ARM | Saving |
|---|---:|---:|---:|
| vCPU-hour | 0.065728 | 0.052585 | 20 % |
| GB-hour | 0.007189 | 0.005746 | 20 % |

Spark, Iceberg and the AWS SDK are JVM workloads with published arm64 builds; no
source change is required. A 4 vCPU / 16 GB configuration costs $0.302276/hr on ARM
versus $0.377936/hr on x86. Note this does **not** generalise to the source lab,
which is x86-only (ADR-005).

`CLAUDE.md` §4.5 forbids pre-initialized capacity, §4.6 requires auto-stop, max
capacity and job timeout. `max_emr_vcpu = 16` and `max_emr_memory_gb = 64` cap a
runaway job at roughly $1.21/hr.

## Consequences

- Cold start of tens of seconds per job. Acceptable for 5–10 minute NRT batches
  (ADR-009), not for sub-second latency — which this architecture never promises.
- EMR Serverless needs VPC configuration to reach MSK, which puts ENIs in private
  subnets and creates the Gap 8 egress problem for this workload alone (ADR-022).
- The 120-minute timeout sets the floor for Iceberg orphan-file retention:
  120 min × 3 retries + margin = 72 h (risk R14, `docs/DATA_CONTRACTS.md` §12).
- Job logs go to **S3, not CloudWatch Logs**, which removes one interface endpoint
  from the required set (`docs/COST.md` §2.1).

## Cost

$0.302276/hr at 4 vCPU / 16 GB (ARM). A 6-hour NRT window plus EOD is ~$2.01
(`docs/COST.md` §3.1). Zero when no job runs — the property that makes this the
right choice for a lab used ~48 hours a month.

## Security

Dedicated `spark` job role, least-privilege to the lake bucket prefixes, the lake
CMK and Glue. No static credentials — EMR Serverless supplies role credentials
through the container credentials endpoint.

## Rollback

Switching to EMR on EC2 or Glue ETL is a job-submission change, not a code change,
because Spark/Iceberg code is runtime-agnostic. Revisit if a genuinely continuous
stream is required (ADR-009).

## Validation

- A trivial Iceberg read job succeeds **with only the S3 gateway and Glue interface
  endpoints present and no NAT** — this is the R4 test and must run before any real
  job is built.
- Auto-stop observed after 15 idle minutes.
- A job exceeding 120 minutes is killed.
- ARM64 job completes with no library incompatibility.
