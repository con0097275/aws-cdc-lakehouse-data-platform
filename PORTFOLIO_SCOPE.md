# What this repository is, and what it is not

## What is here, and what is not

This repository publishes the **architecture, the AI/ML platform, the governance plane and
the CDC correctness code** — 194 of the 425 Python modules — with the full decision record
and all live-run evidence. It deliberately does **not** publish the parts that would let someone
stand the platform up without having understood any of it.

### Published in full

| | |
|---|---|
| **The AI platform** (`ai/`, 91 files) | The reliability copilot — LangGraph graph, planner, the deterministic policy gate, the typed tool contract, the root-cause taxonomy — plus the business agent, the evaluation harness, retrieval, analytics, insights and the governance tooling |
| **The AI/ML platform definitions** (`aiplatform/`, 18 files) | The declarative layer: feature definitions, the business metric registry, agent configuration, model specifications and the knowledge registry, with the compiler, versioning, evaluation and classification modules that read them |
| **Feature platform and ML pilot** (`spark/features/`, `spark/ml/`) | Point-in-time feature engineering and the training pilot — the part of an ML platform that is actually hard to get right |
| **The governance and metadata plane** (`cdc/`, 48 files) | Asset identity and URNs, the executable contract model, DQ and reconciliation result contracts, the five-tier certification ladder, incidents and immutable recovery plans, the lineage graph with its quality rules, impact analysis and the recovery planner, the DataHub client, and the config compiler |
| **Governance configuration** (`governance/`) | Vocabulary, derived-asset overlay, DQ rules, lineage and plane config |
| **CDC correctness code** (`spark/`, 34 files) | The SCN/LSN ordering contract, the Debezium envelope contract, the Kafka → FULL_CDC jobs, the REALTIME materialization, the full STREAMING_RT resident layer, deterministic snapshots and the EOD cutoff window |
| **Tests** | 18 files, **652 tests, all passing** against exactly what is published |
| **Documentation** | 160+ documents, **83 ADRs**, 11 validation reports, the engineering journal, screenshots |
| **Evidence** | EMR job ids, Athena query ids, row counts, Iceberg snapshot ids, the capability matrix |

### Not published

The **Spark reporting and flow framework** (the five processing flows, dimensions, facts,
features, the ops layer), the **Airflow DAGs**, the **dbt models**, the **Terraform modules**,
the container definitions, and the **operator tooling** — the scripts that open a metered AWS
window, drive a recovery, or deploy anything.

That is the line: **the thinking is published, the plumbing is not.** Everything needed to
judge the engineering is here — the contracts, the invariants, the failure analysis, the
decisions with their rejected alternatives, and the real code at every point where a wrong
choice would be invisible. What is absent is the deployment machinery, which is the part that
is valuable to copy and worthless to read.

The runbooks still describe the withheld components in full, because understanding how a
platform is operated is part of judging whether it was designed well.

If you are evaluating this for a role and want to see a specific area in more depth, ask.

## What was removed to make it public

Identifiers, and runtime state. Nothing was removed to hide a defect — the engineering
record, including every defect found and how it was fixed, is intact in
[`docs/ENGINEERING_JOURNAL.md`](docs/ENGINEERING_JOURNAL.md) and
[`DECISION_LOG.md`](DECISION_LOG.md).

| Removed | Why |
|---|---|
| The AWS account id, CLI profile, owner email, home paths, unix username | An account id plus a bucket name is a target list |
| `terraform.tfvars`, `*.tfstate*`, `tfplan*.json` | State and plans are sensitive by policy; a plan JSON carries a `prior_state` |
| `dbt/logs/`, `*.log`, the AI UI session log | Real run logs, full of absolute paths and a username |
| `.venv*`, `.terraform/`, caches, jars, the Iceberg warehouse | Build and runtime artefacts — 3.7 GB down to 26 MB |
| Git history | Filtering history is error-prone and one missed blob undoes the exercise, so it was dropped rather than rewritten |
| The two sanitiser scripts | They carry the private identifiers in their rewrite table, and the verifier's self-test fixtures are indistinguishable from real credentials to any scanner |

Full detail, and the eight-pattern scan that proves it:
[`SANITIZATION.md`](SANITIZATION.md) · [`PUBLIC_RELEASE_REPORT.md`](PUBLIC_RELEASE_REPORT.md).

**The identity guard still works, and will refuse to run for you.** `scripts/lib.sh` asserts
the expected account and profile before any AWS call, and those now hold placeholders. Every
script stops rather than touching an account it was not built for. Set your own values first.

## It is a lab, not an operated production system

The platform was applied, exercised and destroyed inside metered windows. It has never been
left standing, there is no on-call rotation, and nothing here has served a real user.

- **Claims are labelled** `LIVE TESTED`, `IMPLEMENTED`, `DESIGN ONLY` or `OPTIONAL`, and the
  label is a grade of *evidence*, not of quality —
  [`docs/CAPABILITY_MATRIX.md`](docs/CAPABILITY_MATRIX.md).
- **No throughput, events-per-second, concurrency or uptime figure appears anywhere**,
  because none was measured. The only durations recorded are wall-clock times of single runs,
  stated as that.
- **The lab and the production target are separated explicitly** —
  [`docs/LAB_VS_PRODUCTION.md`](docs/LAB_VS_PRODUCTION.md), including the five things the lab
  cannot tell you.

## It is not a product you can `terraform apply`

You could, with your own account and your own values — the modules are real and every one is
`enable_*` gated with a destroy path. But that is not what this repository is for, and it
will spend money if you do. The guardrails are deliberate: no NAT gateway, auto-stop on EMR
Serverless, a bytes-scanned cutoff on Athena, a budget module, an `AutoDestroyAfter` tag on
every resource, and an approval gate in front of anything that mutates AWS
([`docs/APPROVAL_GATES.md`](docs/APPROVAL_GATES.md)).

If you want to see it work without spending anything, the AI layer, the governance compile,
the lineage graph and the full 3,529-test suite all run **offline, at $0** — see the
Quickstart in [`README.md`](README.md).

## Licensing

No licence file is present yet, which under default copyright means **all rights reserved**:
you may read this code, and you should not assume permission to reuse it. If you want to use
something here, ask. A licence will be added deliberately rather than by accident, because
"portfolio piece" and "reusable library" are different intentions and the file is what says
which one this is.

## If you are reviewing this for a role

Start with [`docs/PORTFOLIO_OVERVIEW.md`](docs/PORTFOLIO_OVERVIEW.md) (five minutes), then
[`docs/CAPABILITY_MATRIX.md`](docs/CAPABILITY_MATRIX.md) to see what is actually proven, then
[`docs/ENGINEERING_JOURNAL.md`](docs/ENGINEERING_JOURNAL.md) if you want to judge the
engineering rather than the architecture diagram. The interview-facing material is in
[`docs/INTERVIEW_GUIDE.md`](docs/INTERVIEW_GUIDE.md).
