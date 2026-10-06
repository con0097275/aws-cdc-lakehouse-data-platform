# ADR-001 — Absorb the Kafka platform as a module

- Status: **ACCEPTED** (Session 01, 2026-08-12)
- Supersedes: the `DECISIONS.md` summary row "Reuse existing MSK Provisioned KRaft"
- Related: ADR-021, ADR-023, ADR-027

## Context

`DECISIONS.md` recorded ADR-001 as "reuse existing MSK — don't rebuild". Session 00
proved that premise false: repo A was applied and destroyed on 2026-07-26 (state
lineage `bd2a46c8…` serial 52 → 106, CloudTrail `DeleteCluster`), and the account was
re-verified empty on 2026-08-12. There is nothing to reuse at runtime.

Worse, the integration surface is unusable even if it were running:
`docs/GAP_ANALYSIS.md` §4 scored **5 of 16** required contract keys as exported,
and repo A has **no `backend` block** — state is local files, so
`terraform_remote_state` is impossible.

## Options

| Option | Mechanism | Verdict |
|---|---|---|
| **A. Absorb** | Copy repo A into `terraform/modules/kafka_platform/` with provenance | **CHOSEN** |
| B. Keep separate + remote state | Add 11 outputs and an S3 backend to repo A; read via `terraform_remote_state` | Rejected |
| C. Leave untouched + data sources | Resolve by tag lookup on `Project` | Rejected |

## Decision

Absorb. One repository, one state, one `apply`, one `destroy`.

The decisive argument is **the cost model, not code aesthetics**. `docs/COST.md` §1
shows the platform at ~$594/month against an $80 budget, so the lab must be
destroyed between metered windows — that is its normal state, not an exception.

- Under **C**, every destroyed window breaks the lakehouse plan, because tag-based
  data sources fail hard when the platform is absent.
- Under **B**, the remote-state read breaks the same way unless every output is
  made optional, which defeats the point of a contract.
- Under **A**, one `terraform destroy` tears down platform and lakehouse together —
  **the dependency graph and the cost control become the same mechanism.**

B was genuinely attractive for the portfolio narrative (it mirrors a real
platform/application org boundary) and would have been the right answer for a
persistent platform. It loses here only because of ephemerality.

## Consequences

- No read-only exception to `LOCAL_PROJECT_CONTEXT.md:15` is needed; repo A is
  **copied**, never edited.
- The 11 missing outputs are fixed directly at absorption (`docs/TARGET_ARCHITECTURE.md` §5).
- Forked from upstream: fixes merge by hand — risk R9, mitigated by
  `modules/kafka_platform/PROVENANCE.md` recording source path, copy date and every
  local modification with its reason.
- Apply ordering becomes trivial: Terraform's own graph handles it.

## Cost

No direct cost. Enables the ephemeral lifecycle that makes ~8 metered windows/month
affordable (`docs/COST.md` §3.3). Under B or C the practical outcome is a platform
left running because tearing it down breaks the downstream stack — the expensive
failure mode.

## Security

Neutral to positive. One state file to encrypt, lock and IAM-restrict instead of
two (ADR-021). Repo A's security posture — private brokers, IAM SASL, TLS, CMK,
zero-inbound SGs, SSM-only, IMDSv2 — is inherited verbatim and re-audited at
absorption rather than trusted.

## Rollback

The absorbed module is self-contained. Extracting it back to a separate repository
later is a copy plus an S3 backend plus `terraform state mv` — mechanical, though
it requires a maintenance window. Nothing here is one-way.

## Validation

- `modules/kafka_platform/PROVENANCE.md` exists and lists every local modification.
- All 17 contract keys in `docs/TARGET_ARCHITECTURE.md` §5 are declared outputs.
- `terraform validate` passes on the absorbed module.
- A destroy plan shows platform and lakehouse resources in one graph.
