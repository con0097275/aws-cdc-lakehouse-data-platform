# Session 00 — Architect Review

- Session: 00 — Audit existing repository (`sessions/00_audit_existing_repo.md`)
- Review date: **2026-08-13**
- Reviewer: `/senior-architect` pass
- Reviewed artefacts: `docs/SOURCE_REPOSITORY_DECISION.md`, `docs/EXISTING_PLATFORM_AUDIT.md`, `docs/GAP_ANALYSIS.md`
- Identity at review time: account `111122223333`, region `ap-southeast-1`, profile `my-aws-profile`
- **Verdict: `ARCHITECTURE_APPROVED_FOR_IMPLEMENTATION`** — see §14

---

## 1. Scope and method

Session 00 was executed on 2026-08-07 and its three deliverables already exist. This
pass is therefore a **verification-grade review**, not a greenfield design pass: every
load-bearing claim was re-derived independently from the filesystem and from read-only
AWS API calls rather than accepted from the documents.

**Method.** Each factual assertion that a later session depends on was re-tested at its
source. Where a document cited a file and line, that file and line were read. Where it
cited an AWS fact, the AWS API was called read-only. Nothing was accepted on the
document's own authority.

**Mutations performed: none.** No `terraform` command of any kind, no AWS write, no
change to either reference repository, no change to the read-only guide package. Every
AWS call was a `describe`/`list`/`get`.

---

## 2. Verification results

### 2.1 Repo A is authoritative — CONFIRMED

The decisive evidence is state lineage, and it reproduces exactly:

```
terraform.tfstate         lineage=bd2a46c8-9ae6-abd3-d27b-1c18076b9d75  serial=106  resources=0   outputs=0   tf=1.15.8
terraform.tfstate.backup  lineage=bd2a46c8-9ae6-abd3-d27b-1c18076b9d75  serial=52   resources=43  outputs=10  tf=1.15.8
```

Same lineage, serial 52 → 106, 43 resources → 0. That is an apply followed by a destroy
on one workspace. The interpretation in `SOURCE_REPOSITORY_DECISION.md` §3.1 is correct
and the alternative readings (fresh init, state loss, second workspace) are all excluded
by the shared lineage.

### 2.2 Evidence table

| # | Claim | Source | Result |
|---|---|---|---|
| 1 | State lineage 52 → 106, 43 → 0 resources | `SRD` §3.1 | ✅ reproduced byte-exact |
| 2 | Repo A has **no `backend` block** — local state only | `GAP` Gap 1 | ✅ `versions.tf`/`providers.tf` contain none |
| 3 | Version pinning exact | `SRD` §3.4 | ✅ `~> 1.15.0`, aws `6.56.0`, random `3.9.0` |
| 4 | Private route tables have **zero routes** | `GAP` Gap 8 | ✅ `network.tf:71-79` — no `route` block at all |
| 5 | Toolbox is public-subnet with public IP | `GAP` §3 | ✅ `network.tf:29`, `toolbox.tf:9` |
| 6 | `auto.create.topics.enable=false` | `GAP` Gap 12 | ✅ `msk.tf:12` |
| 7 | Kafka retention 24 h | `GAP` Gap 13 | ✅ `variables.tf:145` default `24` |
| 8 | 6 Kafka-only CloudWatch alarms | `GAP` Gap 11 | ✅ `monitoring.tf` — exactly 6 |
| 9 | Repo B opens 9 ports to `0.0.0.0/0` | `SRD` §5.1 | ✅ `main.tf:10,24` |
| 10 | Repo B has **no** version pinning | `SRD` §5.4 | ✅ grep returns nothing |
| 11 | Repo B pins an upstream author's macOS `.pem` path | `SRD` §5.5 | ✅ `terraform.tfvars:5` |
| 12 | Repo B is `us-east-1` | `SRD` §5.6 | ✅ `terraform.tfvars:1` |
| 13 | Repo B `null_resource` + `timestamp()` | `SRD` §5.7 | ✅ 4 such resources |
| 14 | Account is empty | `GAP` §1 | ✅ MSK 0, non-default VPC `[]`, EC2 `[]`, NAT `[]`, VPCE `[]` |
| 15 | Neither repo is a Git repository | `SRD` §3.5 | ✅ `fatal: not a git repository` for both |

**15 of 15 confirmed.** No claim was found to be overstated.

### 2.3 Credit where due — §3.5 is the mark of an honest audit

`sessions/00_audit_existing_repo.md` and the session prompt both name Git history as a
required evidence source. It does not exist here. Session 00 **stated the absence
explicitly and recorded that no conclusion depends on it** rather than quietly
substituting file mtimes and calling it history. That is the correct handling, and it is
why the rest of the document can be trusted: the one place it could have bluffed, it
declared instead.

The same discipline appears in §4, which contradicts Repo A's own
`IMPLEMENTATION_REPORT.md` and `VALIDATION_REPORT.md` — both claim no apply ever ran —
and correctly ranks state files and CloudTrail above a repository's self-description.

---

## 3. New findings

Four items the Session 00 audit did not record. **None invalidates its conclusions;**
one changes the project's cost shape and one is a standing security exposure.

### F1 — The operator identity is an IAM user with static keys *(P1, security)*

```
Arn: arn:aws:iam::111122223333:user/my-aws-profile
```

Session 00 audited the security posture of both *repositories* thoroughly, but never
audited the **identity that executes the project**. Every `plan`, `apply` and `destroy`
across all 19 sessions runs as a long-lived IAM user with static access keys resident on
a WSL2 workstation.

`CLAUDE.md` §3.2 forbids *creating* IAM users or access keys. This identity predates the
project, so it is not a violation by creation — but it is the single most privileged
credential in the system and it sits outside the security model the same document spends
ten invariants constructing. A stolen laptop is a total account compromise, with no
session expiry to limit the window.

This is **not a Session 00 blocker** — the audit's conclusions stand regardless. It is a
gap in the audit's *coverage* that should be closed before the account holds real
infrastructure.

**Recommendation:** an ADR in Session 02 covering (a) MFA enforcement on the user,
(b) access-key age and rotation policy, (c) whether to move to short-lived credentials
via an assumed role, and (d) whether `CLAUDE.md` §3.2 should be amended to say
"no *new* IAM users, and the bootstrap identity is `my-aws-profile` with these controls" —
so the invariant matches reality instead of being silently excepted.

### F2 — OPEN-09 is now settled, and its provenance explains it *(P0, cost)*

The `$80` figure that `docs/COST.md` sizes the entire project against is **inherited,
not decided**:

```hcl
# kafka-kraft-aws/kafka-aws-production-lab/terraform/terraform.tfvars:26
monthly_budget_usd = 80
```

It is a variable value in a repository whose `Project` tag is `kafka-prod-lab`, carried
forward into planning without anyone choosing it. The live account says:

| Budget | Limit | Scope |
|---|---:|---|
| `My Monthly Cost Budget` | **$30.00** | account-wide |
| `My Zero-Spend Budget` | $1.00 | account-wide |

**The operator confirmed in this session that $30 is authoritative.** OPEN-09 is
therefore resolved: **the budget is $30/month.**

Consequence — and it is not small. Against the ~$9.45 per 6-hour metered window and the
~$5.61/month always-on floor already established in `docs/COST.md`:

| | at $80 (as planned) | **at $30 (real)** |
|---|---:|---:|
| Affordable windows/month | ~7.9 | **~2.6** |
| Approx. lab hours/month | ~47 | **~16** |

Every batch in `docs/SESSION_DEPENDENCY_GRAPH.md` §5 was sequenced against ~7.9 windows.
At ~2.6 the sequencing does not merely tighten — the "core release ≈ $48.52, fits one
month" conclusion **exceeds the budget outright** and must be re-planned across months
or re-scoped. This is Session 02's first task, ahead of any Terraform.

### F3 — OPEN-03's prior evidence is weighed in the probe but not in the framing *(P2)*

Repo A's tfvars carries a live verification result, dated:

```hcl
# kafka.t3.small is no longer offered for new clusters (verified live 2026-07-26).
broker_instance_type = "kafka.m7g.large"
```

Session 02 Stage A **did** weigh this — `DECISION_LOG.md` S02A-4 cites repo A's recorded
`Unsupported InstanceType` error as the reason stage 1 of the probe is likely sufficient.
The probe design is sound and building it remains correct: stage 1 costs $0.00, a code
comment is not a captured API response, and instance offerings do change.

The gap is in **framing, not analysis**. `PROJECT_STATE.md` lists OPEN-03 as P0 with
"favourable answer raises lab time ~60 %", and `SESSION_HANDOFF.md` leads with the same
upside. Both read as though the favourable branch is the expected case, when the project's
own evidence says the base case is **no change**. Record the expected outcome as negative
so a favourable result is treated as a surprise worth re-verifying.

At $30/month (F2) this matters more, not less: the plan cannot bank on a `t3.small`
reprieve that a prior live check already refused.

### F4 — `GAP_ANALYSIS.md` contradicts itself on the output score *(P3, documentation)*

Gap 2 says "**4 of ~12** required keys exported". §4's contract table says
"**Score: 5 of 16** available". §4 is correct — `outputs.tf` declares 10 outputs, of
which exactly 5 are contract keys (`aws_account_id`, `aws_region`, `msk_cluster_arn`,
`bootstrap_brokers_sasl_iam`, `toolbox_instance_id`); the other 5 are operator
conveniences (port-forward command strings, the Grafana SSM parameter name, sample policy
ARNs). Correct Gap 2 to `5 of 16` so the two statements stop disagreeing.

---

## 4. Scope boundaries

**In scope for Session 00 — all delivered:**

- Read-only inventory of both reference repositories and the target.
- Authoritative-source determination with evidence.
- Platform audit, gap matrix, integration-output contract.
- Read-only AWS identity and resource inventory.

**Explicitly out of scope, and correctly not done:**

- Any `terraform apply` / `destroy` / `init` / state operation.
- Any modification to either reference repository.
- Any connector, database, or Kubernetes mutation.
- Choosing the repository topology — correctly deferred to Session 01 (ADR-001).
- Resolving the private-egress design — correctly deferred to Session 01 (ADR-022).

**Boundary observation.** Session 00 deferred both architecture-changing gaps (1 and 8)
rather than resolving them inside an audit session. That is the right call: an audit that
also decides is an audit that rationalises its decisions. Session 01 took both up and
produced ADR-021 and ADR-022.

---

## 5. Prerequisites

| # | Prerequisite | Status |
|---|---|---|
| P1 | Both reference repositories checked out locally | ✅ present |
| P2 | AWS credentials valid for read-only calls | ✅ `sts get-caller-identity` succeeds |
| P3 | Region `ap-southeast-1`, profile `my-aws-profile` | ✅ confirmed, never switched |
| P4 | Git history available | ❌ **unavailable** — declared in `SRD` §3.5; no conclusion depends on it |
| P5 | Target repository is a Git repository | ✅ *now* — resolved by ADR-025 after Session 00 raised it as Gap 17 |

P4 is a permanent condition, not a defect to fix. It is recorded so no later session
re-opens the question expecting a different answer.

---

## 6. ADRs

### 6.1 Session 00 decisions — confirmed, no change

| ID | Decision | Review verdict |
|---|---|---|
| S00-1 | Repo A is authoritative, but not deployed | **Upheld** — state lineage is decisive |
| S00-2 | Repo B rejected | **Upheld** — 8 disqualifiers, 5 independently re-verified |
| S00-3 | Repos are unrelated lineages | **Upheld** — no shared module, resource, or naming convention |
| S00-4 | Repo A's own status docs are stale and superseded | **Upheld** — and this is the finding most likely to be re-derived wrongly later; it is correctly recorded |

### 6.2 Downstream ADRs resting on Session 00 — spot-checked

ADR-001 (absorb Repo A) cites Session 00's lineage evidence and the 5-of-16 output score
as its decisive premises. **Both premises verify.** The reasoning — that under the
ephemeral cost model a destroyed platform is the *normal* state, so tag-lookup (C) and
remote-state (B) both fail on the common path while absorption (A) makes the dependency
graph and the cost control the same mechanism — is sound and survives the $30 budget in
F2 unchanged. If anything, F2 strengthens it: a tighter budget means more destroy cycles,
and A is the option that makes destroy cheap.

### 6.3 ADRs this review proposes

| Proposed | Subject | Session | Driver |
|---|---|---|---|
| **ADR-029** | Operator identity, credential lifetime and MFA | 02 | F1 |
| **ADR-030** | Budget of record = **$30/month**; supersede the inherited $80 throughout `docs/COST.md` and `docs/SESSION_DEPENDENCY_GRAPH.md` §5 | 02 | F2 |

---

## 7. Integration contract

Session 00's §4 contract is **accepted as the normative platform→lakehouse interface**,
with two amendments already carried by Session 01 and one correction from F4.

| Contract key | Available in Repo A today | Disposition |
|---|---|---|
| `aws_account_id` | ✅ | carry through the module |
| `aws_region` | ✅ | carry through |
| `msk_cluster_arn` | ✅ | carry through |
| `msk_bootstrap_brokers_sasl_iam` | ✅ as `bootstrap_brokers_sasl_iam` | **rename** (S01-12) |
| `toolbox_instance_id` | ✅ | carry through |
| `msk_cluster_name` | ❌ | add in Session 02 Stage B |
| `vpc_id` | ❌ | add |
| `vpc_cidr` | ❌ | add |
| `private_subnet_ids` | ❌ | add |
| `public_subnet_ids` | ❌ | add |
| `availability_zones` | ❌ | add |
| `msk_security_group_id` | ❌ | add |
| `toolbox_security_group_id` | ❌ | add |
| `kms_key_arn` | ❌ | add — **platform CMK only**; the lake gets a separate CMK (S01-11) |
| `toolbox_role_arn` | ❌ | add |
| `private_route_table_ids` | ❌ | add — required to attach S3/DynamoDB **gateway** endpoints |

**Score: 5 of 16 available; 11 to add.** Correct Gap 2's "4 of ~12" to match (F4).

**Contract stability requirement.** These 16 keys are consumed by Sessions 02, 03, 04,
06 and 12. Once Stage B publishes them, renames become breaking changes across five
sessions — which is exactly why S01-12's rename is being taken now, while it is free.

---

## 8. Security controls

### 8.1 Inherited from Repo A — reusable as-is

Verified present and invariant-compliant; these are the template for every new instance:

- MSK security-group ingress on 9098/11001/11002 sourced **from the toolbox SG only**; the toolbox SG itself has zero inbound rules.
- No `key_name` on any instance — SSM Session Manager only.
- CMK encryption at rest, TLS in transit, `public_access { type = "DISABLED" }`.
- IMDSv2 enforced.
- Secrets generated at apply → SSM SecureString → never surfaced in outputs.
- All six required tags via provider `default_tags`.

### 8.2 Controls this review requires

| # | Control | Rationale | Owner |
|---|---|---|---|
| C1 | **Audit the operator identity** — MFA, key age, rotation, or move to an assumed role | F1 — the most privileged credential is outside the security model | 02 (ADR-029) |
| C2 | Repo B's `resources_00_tmp/config/kafka_kraft.yml` is the **only** file permitted to be referenced from Repo B, read-only, and no file may be copied without a provenance note and security review | Repo B fails four invariants; a partial copy could import them | any session touching JMX rules |
| C3 | Reference repositories stay read-only — no edit, apply, destroy, re-init, state migration, or backend change without separate written approval | `LOCAL_PROJECT_CONTEXT.md` | all |
| C4 | Repo A's `terraform.tfstate.backup` (serial 52, 153 KB) contains **43 real resource records** including ARNs, CMK IDs and broker endpoints — treat as sensitive; never commit to the target repo during absorption | `CLAUDE.md` §3.8 makes state sensitive | 02 Stage B |

C4 deserves emphasis: ADR-001 copies Repo A's `terraform/` directory into
`terraform/modules/kafka_platform/`. A naive `cp -r` brings both state files with it.
**The absorption must copy `.tf` sources only** and the target `.gitignore` must already
exclude `*.tfstate*` before the copy happens.

---

## 9. Data-correctness invariants

Session 00 produced no data pipeline, so it introduces no correctness surface of its own.
Its contribution is **defect discovery**: D1–D5 identify that the CDC event → L1 → L2 →
L3 column lineage is specified three mutually inconsistent ways across `CLAUDE.md` §5,
`reference/CDC_EVENT_CONTRACT.md` and `reference/ICEBERG_LAYER_SPEC.md`.

That cluster is the highest-value finding in the whole audit, because `CLAUDE.md` §5
makes ordering correctness a hard invariant while the documents that define the ordering
key **cannot all be right**. The guide gave it three incompatible spellings; `event_order`
is normative and the alternatives are **rejected** — `event_order_key` is rejected and
replaced by `event_order`, and the split pair `source_order_1` / `source_order_2` is
likewise rejected and superseded by the single `event_order` struct (ADR / S01-17).
`source_commit_position` was nested in one document and flat in another, leaving SQL
Server's event-serial-number tie-breaker with nowhere to live (D3).

Session 00's recommendation — that Session 01 produce a single normative column contract
superseding all three, rather than letting Session 06 improvise one under deadline — is
correct and was carried out (`docs/DATA_CONTRACTS.md`, closing D1–D5 and D7–D9).
**Confirmed as the right sequencing:** improvising a column contract inside the session
that writes the streaming job is how ordering bugs become table migrations.

---

## 10. Failure and recovery paths

For Session 00 itself the failure modes are epistemic, not operational:

| Failure mode | Mitigation in place |
|---|---|
| Concluding "never deployed" from Repo A's stale markdown | `SRD` §4 explicitly names and supersedes those documents |
| Re-deriving authority from Git history | `SRD` §3.5 declares Git unavailable |
| Assuming the platform can be reused at runtime | `SRD` §1 and Gap 0 state it is destroyed |
| Copying Repo B code for expedience | `SRD` §5 records 8 disqualifiers permanently |
| **Assuming the $80 budget** | ← **was NOT mitigated**; F2 closes it |
| **Assuming `kafka.t3.small` may be available** | ← **partially mitigated**; F3 supplies the prior evidence |

The two unmitigated rows are precisely the two findings this review adds. Both are cost
assumptions rather than architecture errors, which is consistent with an audit that
scrutinised security and topology closely and treated inherited numbers as given.

---

## 11. Resources and cost drivers

**Incremental cost of Session 00: $0.00.** It created nothing. Re-verified in this
review — MSK 0, non-default VPC `[]`, EC2 `[]`, NAT `[]`, VPC endpoints `[]`.

**Existing always-on cost from prior work: $0.00 for lab resources.** There is no running
MSK, EC2, endpoint or NAT to bill. The 44 S3 buckets and 1 Glue database in the account
are unrelated prior work, outside this project's tags, and not this project's cost.

**The always-on floor is prospective, not current:** ~$5.61/month once Stage B applies
(state bucket + three CMKs). Nothing is billing for this project today.

Cost drivers this review flags for Session 02, both from F2:

1. The budget of record is **$30**, not $80 — ~2.6 windows/month, not ~7.9.
2. The "core release ≈ $48.52 fits one month" conclusion **does not hold at $30** and
   must be re-planned or re-scoped before Stage B writes Terraform.

---

## 12. Test matrix

All tests are read-only. Every row was executed during this review.

| # | Test | Command | Result |
|---|---|---|---|
| T1 | Identity guard | `aws sts get-caller-identity` | ✅ account `111122223333`, user `my-aws-profile` |
| T2 | Platform absent — MSK | `aws kafka list-clusters-v2 --query 'length(ClusterInfoList)'` | ✅ `0` |
| T3 | Platform absent — VPC | `aws ec2 describe-vpcs --query 'Vpcs[?IsDefault==\`false\`].VpcId'` | ✅ `[]` |
| T4 | Platform absent — EC2 | `aws ec2 describe-instances ... State.Name!='terminated'` | ✅ `[]` |
| T5 | No NAT | `aws ec2 describe-nat-gateways` | ✅ `[]` |
| T6 | No endpoints | `aws ec2 describe-vpc-endpoints` | ✅ `[]` |
| T7 | Budget reality | `aws budgets describe-budgets --account-id 111122223333` | ✅ $30.00 + $1.00 |
| T8 | State lineage | `python3 -c "import json; ..."` on both state files | ✅ 52/43 → 106/0, same lineage |
| T9 | No backend block | `grep -rn backend versions.tf providers.tf` | ✅ no match |
| T10 | Output surface | `grep -c '^output' outputs.tf` | ✅ 10 declared, 5 are contract keys |
| T11 | Private routes absent | `grep -A8 'aws_route_table" "private"' network.tf` | ✅ no `route` block |
| T12 | Repo B unpinned | `grep -rn 'required_version\|required_providers' *.tf` | ✅ exit 1, no match |
| T13 | Repo B open ingress | `grep -n '0.0.0.0/0' main.tf` | ✅ 9 ports |
| T14 | Git unavailable | `git log` in both repos | ✅ `fatal: not a git repository` |

**14 executed, 14 passed.** Status of every claim in this document: `static-validated`
for filesystem facts, `live-tested` for the read-only AWS calls T1–T7. **Nothing here is
`deployed`.**

---

## 13. Rollback and cleanup

**Nothing to roll back.** Session 00 created no AWS resource and modified no reference
repository. This review created exactly one file:

```
docs/reviews/SESSION-00-ARCHITECT-REVIEW.md
```

To revert this review:

```bash
cd /path/to/aws-cdc-lakehouse
rm docs/reviews/SESSION-00-ARCHITECT-REVIEW.md
```

Standing cleanup verification, unchanged:

```bash
aws kafka list-clusters-v2 --profile my-aws-profile --region ap-southeast-1 --query 'length(ClusterInfoList)'
aws ec2 describe-instances --profile my-aws-profile --region ap-southeast-1 \
  --query 'Reservations[].Instances[?State.Name!=`terminated`].InstanceId'
```

---

## 14. Verdict and implementation checklist

### `ARCHITECTURE_APPROVED_FOR_IMPLEMENTATION`

Session 00's deliverables are **accepted without amendment to their conclusions**. All 15
load-bearing claims reproduce from source. The authoritative-source determination is
sound, the rejection of Repo B is permanently and adequately evidenced, the integration
contract is the right abstraction, and the two architecture-changing gaps were correctly
deferred to Session 01 rather than decided inside an audit.

The four new findings are **additive**. F1 and F4 do not touch any Session 00 conclusion.
F3 sharpens an expectation. F2 resolves an open question in the project's favour of
accuracy — and while it materially reshapes downstream cost planning, it does not
invalidate a single Session 00 finding, because Session 00 never asserted the $80 figure;
it inherited it silently, which is exactly what F2 now records.

Approval is **not** contingent on the checklist below. Those items belong to Session 02.

### Exact implementation checklist

Ordered. Items 1–3 are corrections to existing documents; 4–6 are Session 02 entry work.

- [ ] **1.** Correct `docs/GAP_ANALYSIS.md` Gap 2: "4 of ~12" → **"5 of 16"**, matching §4 *(F4)*
- [ ] **2.** Record **OPEN-09 as RESOLVED = $30/month** in `PROJECT_STATE.md` and `DECISION_LOG.md`; cite `terraform.tfvars:26` as the provenance of the inherited $80 *(F2)*
- [ ] **3.** Re-derive `docs/COST.md` window math at $30 (~2.6 windows, ~16 h/month) and re-sequence `docs/SESSION_DEPENDENCY_GRAPH.md` §5 — the "core release ≈ $48.52 fits one month" conclusion **fails at $30** and must be re-planned across months or re-scoped *(F2)*
- [ ] **4.** Reframe OPEN-03's **expected outcome as negative** in `PROJECT_STATE.md` and `SESSION_HANDOFF.md`, citing `terraform.tfvars:11` and S02A-4; the "+60 % lab time" upside is a low-probability branch, not the base case. The probe itself needs no change *(F3)*
- [ ] **5.** Raise **ADR-029 — operator identity, credential lifetime and MFA**; decide whether `CLAUDE.md` §3.2 is amended to name the bootstrap identity explicitly rather than excepting it silently *(F1)*
- [ ] **6.** Before absorbing Repo A under ADR-001: confirm `.gitignore` excludes `*.tfstate*`, and copy **`.tf` sources only** — `terraform.tfstate.backup` holds 43 live resource records and must not enter the target repository *(C4)*

### Carried forward unchanged

Gap 0 (platform not deployed) remains the governing constraint: Sessions 04, 05, 06, 12
and 15 hard-depend on a running MSK that does not exist and, under the ephemeral cost
model, will not exist most of the time. That is by design, and ADR-001 is what makes it
survivable.
