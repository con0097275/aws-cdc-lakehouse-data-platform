# GATE 1 PACKAGE — `kafka.t3.small` availability probe (OPEN-03)

- Session: 02 Stage A
- Status: **PREPARED — awaiting human approval. Nothing has been executed.**
- Gate: `docs/APPROVAL_GATES.md` §1 (`terraform apply` / AWS mutation)
- Script: `scripts/probe-msk-instance-type.sh`
- Closes: OPEN-03, risk R11

---

## 1. The question and what it is worth

Is `kafka.t3.small` accepted for **new** MSK Provisioned clusters in `ap-southeast-1`?

| Broker type | USD/hr per broker | 3 brokers, 6 h window | Windows within $80/mo |
|---|---:|---:|---:|
| `kafka.m7g.large` (current pin) | 0.2550 | 4.59 | **~8** |
| `kafka.t3.small` (if available) | 0.0578 | 1.04 | **~12.6** |

Source: `docs/PRICE_REFERENCE.md` §3, `docs/COST.md` §3.3. The delta is **$432/month** —
larger than the entire budget. This is the highest-value open question in the project.

There is **no read-only API that answers it**. MSK publishes no "list supported broker
instance types" call, and `aws kafka list-kafka-versions` returns versions, not
instance types. The only evidence is a `CreateCluster` attempt.

## 2. Prior evidence — read before approving any spend

The reference repository already recorded a live answer from **this same account**,
17 days before today:

| Source | Statement |
|---|---|
| `kafka-kraft-aws/kafka-aws-production-lab/docs/VERSIONS.md:16` | "`kafka.t3.small` is no longer accepted for new clusters — a live `CreateCluster` call (2026-07-26) returned `Unsupported InstanceType`; valid values are `kafka.m5.*`, `kafka.m7g.*` and `express.m7g.*`" |
| `.../terraform/variables.tf:71` | "`kafka.t3.small` was retired for new clusters (live `CreateCluster` error, 2026-07-26)" |
| `.../terraform/terraform.tfvars:12` | "`kafka.t3.small` is no longer offered for new clusters (verified live 2026-07-26)" |

Session 01 declined to close OPEN-03 on this basis for a stated reason: `DECISION_LOG.md`
S00-4 established that repo A's own status documents had already been wrong once —
they claimed no apply had ever run while state files and CloudTrail proved an apply
followed by a destroy. A second-hand claim worth $432/month deserves one confirming call.

**Stage 1 below confirms or refutes it for $0.00.**

## 3. What will be executed

### Stage 1 — zero-resource probe · cost **$0.00**

`CreateCluster` with `kafka.t3.small` **and deliberately non-existent subnet IDs**
(`subnet-00000000000000001`, `subnet-00000000000000002`).

The request is guaranteed to be rejected, so **no resource can be created and nothing
can bill**. The information is in *which* validation error comes back:

| Response | Meaning | Outcome |
|---|---|---|
| error names the **instance type** | validation reached it and refused it | **OPEN-03 answered — rejected.** Keep `kafka.m7g.large` |
| error names the **subnets** | instance-type validation was never reached | inconclusive → stage 2 |
| request **succeeds** | should be impossible | script raises a loud warning and tells you to check `list-clusters-v2` immediately |

Repo A's recorded error was `Unsupported InstanceType`, so stage 1 has a good chance of
settling this at zero cost.

Request body:

```json
{
  "ClusterName": "kafka-dev-lab-dev-t3probe-stage1",
  "KafkaVersion": "3.9.x.kraft",
  "NumberOfBrokerNodes": 2,
  "BrokerNodeGroupInfo": {
    "InstanceType": "kafka.t3.small",
    "ClientSubnets": ["subnet-00000000000000001", "subnet-00000000000000002"],
    "StorageInfo": { "EbsStorageInfo": { "VolumeSize": 1 } }
  }
}
```

### Stage 2 — billable probe · cost **~$0.06**, only if stage 1 is inconclusive

A real minimal cluster in the **default VPC** (`vpc-08e1cd261da4fb890`, verified
read-only on 2026-08-12), deleted immediately.

| | |
|---|---|
| Brokers | 2 × `kafka.t3.small` — MSK requires the broker count to be a multiple of the subnet count, so 2 is the floor |
| Storage | 2 × 1 GiB gp3 (MSK minimum) |
| Marginal rate | **0.1156 USD/hr** |
| Expected duration | 15–30 min provisioning, then delete |
| Expected total | **~$0.06** |

Stage 2 requires `--stage2` **in addition to** `--execute`, plus its own typed
confirmation phrase.

## 4. Gate 1 checklist (`docs/APPROVAL_GATES.md` §1)

| # | Requirement | Status |
|---|---|---|
| 0.1–0.4 | Identity, profile, region, environment | `require_identity` in `scripts/lib.sh` refuses to continue on any mismatch. Verified: account `111122223333`, profile `my-aws-profile`, region `ap-southeast-1` |
| 0.5 | Working tree committed | branch `session-02-prerequisites`; commit before executing |
| 0.6 | Static checks pass | `make validate-docs`; `checkov` 3.3.10 installed. **`tflint` absent — OPEN-06** |
| **0.7** | **Budget notifies someone** | **PASSES** — an account-wide $30 budget already notifies `owner@example.com`. Verified live; see §6 |
| 1 | Saved plan | n/a — this is a raw API call, not Terraform. The exact request body is above and in the script |
| 2 | Resource action counts | stage 1: **creates nothing**. stage 2: creates 1 MSK cluster, deletes 1 MSK cluster |
| 3 | Cost delta | stage 1 $0.00; stage 2 ~$0.06 against a month-to-date of $0.00 |
| 4 | Security assertions | no security group, no ingress rule, no key pair, no secret in the request. The probe cluster has default (IAM-less) auth and is deleted immediately |
| 5 | Destroy plan | `aws kafka delete-cluster --cluster-arn <arn>`; full Gate 5 verification in §5 |
| 6 | `auto_destroy_after` | n/a — the resource is deleted within the same operation, not left for a sweep |

## 5. Teardown and verification

Stage 1 creates nothing, so only stage 2 needs this.

```bash
# delete
aws kafka delete-cluster --cluster-arn "<arn>" \
  --profile my-aws-profile --region ap-southeast-1

# verify — the Gate 5 block from docs/APPROVAL_GATES.md
aws kafka list-clusters-v2 --profile my-aws-profile --region ap-southeast-1 \
  --query 'length(ClusterInfoList)'                                    # 0
aws ec2 describe-volumes --profile my-aws-profile --region ap-southeast-1 \
  --query 'Volumes[].VolumeId'                                         # []  <- the forgotten one
```

`describe-volumes` matters: EBS bills while a volume exists even after the thing that
created it is gone.

## 6. Gate 0.7 — resolved, and it is not what Session 01 recorded

Session 01 marked precondition 0.7 **FAILING** on the basis of repo A's
`budget_email = ""`. That reasoning inspected *code*, not the account. Verified
read-only on 2026-08-12 (`artifacts/validation/session-02/aws/gate-0.7-budgets.txt`):

| Budget | Limit | Filter | Notifications | Subscriber |
|---|---:|---|---|---|
| `My Monthly Cost Budget` | $30.00 | **none — account-wide** | ACTUAL >85 %, ACTUAL >100 %, FORECASTED >100 % | **owner@example.com** |
| `My Zero-Spend Budget` | $1.00 | none — account-wide | ACTUAL > $0.01 (currently in `ALARM`) | — |

**Gate 0.7 passes.** The account is not unmonitored, and the existing subscriber is the
same address chosen for `budget_email`. Being account-wide and unfiltered, these budgets
cover this project's spend automatically — broader coverage than the tag-filtered budget
Stage B will add, which by construction misses anything untagged.

Gate 1 is therefore **not blocked** for either stage of this probe.

One consequence to carry forward, tracked as **OPEN-09**: the live budget is **$30**,
while `docs/COST.md` sizes the entire envelope against **$80**. At $30 the affordable
window count is ~2.6, not ~7.9. See `DECISION_LOG.md` OPEN-09 — this needs an operator
decision before Stage B applies anything hourly. It does not block this probe: stage 1
costs $0.00 and stage 2 costs ~$0.06 against either figure.

## 7. Approval

```bash
cd /path/to/aws-cdc-lakehouse

# review first — prints everything, contacts nothing
bash scripts/probe-msk-instance-type.sh

# stage 1 only, $0.00
bash scripts/probe-msk-instance-type.sh --execute

# stage 2 as well, ~$0.06 — only if stage 1 was inconclusive
bash scripts/probe-msk-instance-type.sh --execute --stage2
```

Both stages require a typed confirmation phrase and refuse to run non-interactively.

## 8. Recording the result

Either way, record it in `DECISION_LOG.md` under OPEN-03 with the date and the raw API
response (saved to `artifacts/validation/session-02/open-03/probe-result.txt`).

- **Rejected** → close OPEN-03; `docs/COST.md` and `docs/VERSIONS.md` already assume
  `kafka.m7g.large`; nothing else changes.
- **Accepted** → close OPEN-03; re-derive `docs/COST.md` §3.3 (~8 windows/month becomes
  ~12.6); update `docs/VERSIONS.md`; revisit the window batching in
  `docs/SESSION_DEPENDENCY_GRAPH.md` §5, which was sized against the expensive broker.
