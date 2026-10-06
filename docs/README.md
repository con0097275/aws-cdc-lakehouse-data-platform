# Documentation index

162 documents, 83 ADRs and 11 validation reports. This page is the map. Nothing here is
required reading in order — pick the row that matches the question you arrived with.

**Shortest useful path:** [`PORTFOLIO_OVERVIEW.md`](PORTFOLIO_OVERVIEW.md) (five minutes) →
[`CAPABILITY_MATRIX.md`](CAPABILITY_MATRIX.md) (what is real, and how real) →
[`ENGINEERING_JOURNAL.md`](ENGINEERING_JOURNAL.md) (every defect found, and what it changed).

---

## If you arrived with a question

| Question | Document |
|---|---|
| What is this project, in five minutes? | [`PORTFOLIO_OVERVIEW.md`](PORTFOLIO_OVERVIEW.md) |
| What is actually proven, and what is only designed? | [`CAPABILITY_MATRIX.md`](CAPABILITY_MATRIX.md) |
| Is the engineering any good? | [`ENGINEERING_JOURNAL.md`](ENGINEERING_JOURNAL.md) |
| Show me it working. | [`DEMO.md`](DEMO.md) · [`USE_CASES.md`](USE_CASES.md) · [`AI_COPILOT_TRANSCRIPTS.md`](AI_COPILOT_TRANSCRIPTS.md) |
| How do I run it and check the claims myself? | [`HOW_TO_USE_AND_VERIFY.md`](HOW_TO_USE_AND_VERIFY.md) · [`TEST_EVERY_FEATURE.md`](TEST_EVERY_FEATURE.md) |
| The pipeline is green and the number is wrong. | [`RERUN_ONLY_WHAT_BROKE.md`](RERUN_ONLY_WHAT_BROKE.md) |
| What would this cost, and what did it cost? | [`COST.md`](COST.md) · [`FINOPS.md`](FINOPS.md) · [`PRICE_REFERENCE.md`](PRICE_REFERENCE.md) |
| What is deliberately not done? | [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md) · [`RISK_REGISTER.md`](RISK_REGISTER.md) |
| I am interviewing the author. | [`INTERVIEW_GUIDE.md`](INTERVIEW_GUIDE.md) · [`CV_BULLETS.md`](CV_BULLETS.md) |

---

## Platform architecture

| Document | What it covers |
|---|---|
| [`TARGET_ARCHITECTURE.md`](TARGET_ARCHITECTURE.md) | The whole platform as designed — the reference every other doc defers to |
| [`ARCHITECTURE_AND_TEST_GUIDE.md`](ARCHITECTURE_AND_TEST_GUIDE.md) | Architecture paired with the test that holds each part honest |
| [`LAB_VS_PRODUCTION.md`](LAB_VS_PRODUCTION.md) | **The cost-optimized lab and the production target, concern by concern** — and what does *not* change between them |
| [`PLATFORM_RESOURCE_INVENTORY.md`](PLATFORM_RESOURCE_INVENTORY.md) | Every AWS resource, the full run-through, the costs, and three post-rebuild traps |
| [`DATA_LAKE_FOUNDATION.md`](DATA_LAKE_FOUNDATION.md) | S3 layout, Iceberg warehouse, Glue catalog, KMS |
| [`AIRFLOW_K8S.md`](AIRFLOW_K8S.md) | Airflow 3 on k3s, `KubernetesExecutor`, the Helm pin |
| [`VERSIONS.md`](VERSIONS.md) | Every pinned version, and why `latest` is forbidden |
| [`SLO.md`](SLO.md) · [`DR.md`](DR.md) | Service objectives, and the disaster-recovery posture with its cost |

## CDC and the lakehouse layers

| Document | What it covers |
|---|---|
| [`CDC_CONTRACT_IMPLEMENTATION.md`](CDC_CONTRACT_IMPLEMENTATION.md) | **The ordering contract** — SCN, LSN, `event_order`, and why arrival order is never used |
| [`DATA_CONTRACTS.md`](DATA_CONTRACTS.md) | The column-level contract every layer is built against |
| [`SOURCE_CDC_SETUP.md`](SOURCE_CDC_SETUP.md) · [`CONNECT_REGISTRY.md`](CONNECT_REGISTRY.md) | Oracle/SQL Server capture, Debezium, Kafka Connect, Avro and the schema registry |
| [`L1_FULL_CDC.md`](L1_FULL_CDC.md) · [`FULL_CDC_PER_TABLE.md`](FULL_CDC_PER_TABLE.md) | The canonical append-only history, and the per-table physical layout |
| [`L2_REALTIME_STREAM.md`](L2_REALTIME_STREAM.md) · [`REALTIME_LAYER.md`](REALTIME_LAYER.md) | The REALTIME overlay: shapes, write strategies, the incremental cursor |
| [`L3_SNAPSHOT.md`](L3_SNAPSHOT.md) · [`EOD_LAYER.md`](EOD_LAYER.md) | The certified close: cutoff, dedup, delete policy, determinism |
| [`KIMBALL_MODEL.md`](KIMBALL_MODEL.md) | SCD2 dimensions, facts, deterministic surrogate keys, the additivity registry |
| [`SCHEMA_EVOLUTION_RUNBOOK.md`](SCHEMA_EVOLUTION_RUNBOOK.md) | What happens when the source changes shape |

## Streaming

| Document | What it covers |
|---|---|
| [`FULL_CDC_STREAMING.md`](FULL_CDC_STREAMING.md) · [`FULL_CDC_STREAMING_RUNBOOK.md`](FULL_CDC_STREAMING_RUNBOOK.md) | Kafka → FULL_CDC: execution modes, checkpoints, restarting without deleting one |
| [`REALTIME_STREAMING_RT.md`](REALTIME_STREAMING_RT.md) | **The resident four-app serving layer** — late dimensions published flagged, pointer-before-row, the watermarked merge, two live windows |
| [`REALTIME_WINDOW_RUNBOOK.md`](REALTIME_WINDOW_RUNBOOK.md) | The overlay window, and why it is not STREAM_BATCH |
| [`REALTIME_CURRENT_STATE_CONTRACT.md`](REALTIME_CURRENT_STATE_CONTRACT.md) | The one governed way to read "now", tombstone rule included |
| [`REALTIME_EOD_REBASE_RUNBOOK.md`](REALTIME_EOD_REBASE_RUNBOOK.md) · [`REALTIME_FAILURE_RECOVERY.md`](REALTIME_FAILURE_RECOVERY.md) | Rebasing on a certified close; and why every failure here is silent |
| [`REALTIME_PERFORMANCE_BENCHMARK.md`](REALTIME_PERFORMANCE_BENCHMARK.md) | The cost model (config) versus the benchmark (measured) |

## Reporting and marts

| Document | What it covers |
|---|---|
| [`REPORTING_FRAMEWORK.md`](REPORTING_FRAMEWORK.md) · [`REPORTING_ARCHITECTURE_DISCOVERY.md`](REPORTING_ARCHITECTURE_DISCOVERY.md) | The five processing flows and the model behind them |
| [`REPORTING_ORCHESTRATION.md`](REPORTING_ORCHESTRATION.md) | One DAG per cadence, topological turns, gates, Airflow wiring |
| [`DEPENDENCY_ORCHESTRATION.md`](DEPENDENCY_ORCHESTRATION.md) · [`EXECUTION_ORDER.md`](EXECUTION_ORDER.md) | dbt manifest as dependency truth; same-turn parallelism; attempt ≠ turn |
| [`FOUR_FLOWS.md`](FOUR_FLOWS.md) · [`AUTO_CORRECT_RUNBOOK.md`](AUTO_CORRECT_RUNBOOK.md) | EOD, AUTO_CORRECT, FULFILL, STREAM_BATCH — and the accuracy ladder |
| [`DBT_SPARK.md`](DBT_SPARK.md) · [`NEW_DATAMART.md`](NEW_DATAMART.md) · [`ADD_NEW_TABLE_AND_MODEL.md`](ADD_NEW_TABLE_AND_MODEL.md) | dbt on Spark, and adding a mart without writing a DAG |
| [`BUSINESS_METRIC_LAYER.md`](BUSINESS_METRIC_LAYER.md) | The governed metric definitions the business AI resolves against |
| [`EOD_CONTROL_PLANE.md`](EOD_CONTROL_PLANE.md) · [`EOD_SNAPSHOT_RUNBOOK.md`](EOD_SNAPSHOT_RUNBOOK.md) | `eod_info` vs run history, readiness, `WAITING_SOURCE` |

## Governance, quality and reliability

| Document | What it covers |
|---|---|
| [`DATA_RELIABILITY_OVERVIEW.md`](DATA_RELIABILITY_OVERVIEW.md) | **Start here** — the data plane, the reliability plane, and the four questions they answer |
| [`DATA_GOVERNANCE_FRAMEWORK.md`](DATA_GOVERNANCE_FRAMEWORK.md) | The eleven governance capabilities and where each actually lives |
| [`GOVERNANCE_METADATA_CONTRACT.md`](GOVERNANCE_METADATA_CONTRACT.md) | Canonical asset identity, four-level inheritance, and the compile that took coverage from 1 asset to 84 |
| [`DATAHUB_ARCHITECTURE.md`](DATAHUB_ARCHITECTURE.md) · [`DATAHUB_SECURITY.md`](DATAHUB_SECURITY.md) | Three modes, one URN per table, environment fabrics; metadata as untrusted input |
| [`END_TO_END_LINEAGE.md`](END_TO_END_LINEAGE.md) · [`LINEAGE_SOURCE_MATRIX.md`](LINEAGE_SOURCE_MATRIX.md) | The graph (81 nodes, 8 hops), and where each edge comes from |
| [`COLUMN_LINEAGE_STRATEGY.md`](COLUMN_LINEAGE_STRATEGY.md) | Why a fuzzy mapping on a critical field is worse than none |
| [`DATA_QUALITY_MODEL.md`](DATA_QUALITY_MODEL.md) · [`DATA_CONTRACT_MODEL.md`](DATA_CONTRACT_MODEL.md) | Six DQ statuses; the executable producer contract, and why an owner is not one of its fields |
| [`CERTIFICATION_MODEL.md`](CERTIFICATION_MODEL.md) | The five-tier ladder and its gates — a Spark job exiting 0 is not certification |
| [`DATA_INCIDENT_RECOVERY_MODEL.md`](DATA_INCIDENT_RECOVERY_MODEL.md) · [`LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md`](LINEAGE_DRIVEN_RECOVERY_RUNBOOK.md) | Incidents, immutable plans, and rerunning only what is affected |
| [`DATA_OWNERSHIP.md`](DATA_OWNERSHIP.md) · [`DATA_CLASSIFICATION.md`](DATA_CLASSIFICATION.md) · [`DATA_RETENTION_POLICY.md`](DATA_RETENTION_POLICY.md) · [`BUSINESS_GLOSSARY.md`](BUSINESS_GLOSSARY.md) | Ownership, PII categories, six retention domains, Git-controlled terms |
| [`DATA_RELIABILITY_OBSERVABILITY.md`](DATA_RELIABILITY_OBSERVABILITY.md) · [`METADATA_COST_MODEL.md`](METADATA_COST_MODEL.md) | 18 metrics and why a proposed SLO may not page; the streaming-lineage volume decision |

**OpenLineage is not DataHub.** OpenLineage is the runtime event standard — what a job emits
while it runs. DataHub is the metadata graph those events land in, and the thing you query
for impact. Mixing them up is the most common error in this area:
[`LINEAGE.md`](LINEAGE.md) draws the line.

## AI agents

Two separate agents. They share a tool-safety discipline and nothing else.

| Document | What it covers |
|---|---|
| [`AI_DATA_RELIABILITY_LANGGRAPH.md`](AI_DATA_RELIABILITY_LANGGRAPH.md) | **The reliability copilot** — state, nodes, conditional routes, interrupts, durable state |
| [`AI_AGENT_ACTION_BOUNDARY.md`](AI_AGENT_ACTION_BOUNDARY.md) · [`AI_AGENT_TOOL_CATALOG.md`](AI_AGENT_TOOL_CATALOG.md) | What the agent cannot do, and the 27 typed tools of which 2 may mutate |
| [`AI_RECOVERY_PLANNER.md`](AI_RECOVERY_PLANNER.md) · [`AI_RECOVERY_SCOPE_MODEL.md`](AI_RECOVERY_SCOPE_MODEL.md) · [`AI_RECOVERY_APPROVAL_POLICY.md`](AI_RECOVERY_APPROVAL_POLICY.md) | Bounded plans, scope granularity, and the deterministic approval gate |
| [`RECOVERY_CONTROL_API.md`](RECOVERY_CONTROL_API.md) | The only service that may execute a plan, and what it refuses |
| [`AI_RECOVERY_SECURITY.md`](AI_RECOVERY_SECURITY.md) · [`AI_FAILURE_MATRIX.md`](AI_FAILURE_MATRIX.md) · [`AI_DATA_RELIABILITY_EVAL.md`](AI_DATA_RELIABILITY_EVAL.md) | Prompt-injection posture, failure modes, and what is measured versus deliberately not |
| [`BUSINESS_METRIC_LAYER.md`](BUSINESS_METRIC_LAYER.md) · [`BAI_P0_DISCOVERY.md`](BAI_P0_DISCOVERY.md) | **The business/datamart agent** — metric resolver, safe Athena, EvidencePack |
| [`AI_COPILOT_USER_GUIDE.md`](AI_COPILOT_USER_GUIDE.md) · [`AI_COPILOT_SAMPLE_QUESTIONS.md`](AI_COPILOT_SAMPLE_QUESTIONS.md) · [`AI_COPILOT_TRANSCRIPTS.md`](AI_COPILOT_TRANSCRIPTS.md) | How to drive both, what to ask, and real annotated sessions including four refusals |
| [`AI_GOVERNANCE.md`](AI_GOVERNANCE.md) · [`AI_GOVERNANCE_USE_CASES.md`](AI_GOVERNANCE_USE_CASES.md) | 19 use cases, each labelled tested / modelled / not built |

## Operations

| Document | What it covers |
|---|---|
| [`OPERATIONS_RUNBOOK.md`](OPERATIONS_RUNBOOK.md) | Bring-up, and the index to every developer how-to |
| [`runbooks/`](runbooks/) | 16 task runbooks: Airflow access, checkpoint recovery, auto-correct repair, dependency investigation, AI platform operations and more |
| [`ICEBERG_MAINTENANCE_RUNBOOK.md`](ICEBERG_MAINTENANCE_RUNBOOK.md) · [`DATA_LAYOUT_BENCHMARK.md`](DATA_LAYOUT_BENCHMARK.md) | Threshold-driven compaction and retention; measured partition layouts |
| [`DATAHUB_OPERATIONS_RUNBOOK.md`](DATAHUB_OPERATIONS_RUNBOOK.md) | Start/stop the metadata plane, the smoke test, upgrades |
| [`APPROVAL_GATES.md`](APPROVAL_GATES.md) | Every gate that stops a script spending money without a human |
| [`GITHUB_METADATA.md`](GITHUB_METADATA.md) | The repository description, topics and social-preview spec, kept with the code so they cannot drift |
| [`LAKE_CMK_LIFECYCLE.md`](LAKE_CMK_LIFECYCLE.md) · [`SECURITY_SCAN_BASELINE.md`](SECURITY_SCAN_BASELINE.md) | CMK lifecycle; the security baseline this repo is scanned against |
| [`ATHENA.md`](ATHENA.md) · [`POWERBI.md`](POWERBI.md) | The serving layer, and the BI connection path |

## Guides — onboarding a table

| Document | What it covers |
|---|---|
| [`CDC_TABLE_QUICKSTART.md`](CDC_TABLE_QUICKSTART.md) | The short version |
| [`ADD_A_CDC_TABLE.md`](ADD_A_CDC_TABLE.md) | **The whole path**, plus how to verify each hop in S3, Glue and Athena |
| [`CDC_TABLE_ONBOARDING_RUNBOOK.md`](CDC_TABLE_ONBOARDING_RUNBOOK.md) · [`CDC_TABLE_DEVELOPER_WORKFLOW.md`](CDC_TABLE_DEVELOPER_WORKFLOW.md) | The gated workflow, and the day-to-day developer loop |
| [`CDC_TABLE_CONFIG_REFERENCE.md`](CDC_TABLE_CONFIG_REFERENCE.md) | Every field in the registry YAML, and what it controls |
| [`CDC_CUTOVER.md`](CDC_CUTOVER.md) · [`CDC_TABLE_MIGRATION.md`](CDC_TABLE_MIGRATION.md) | Moving a table from the legacy shared layer to per-table |
| [`CDC_TABLE_DECOMMISSION_RUNBOOK.md`](CDC_TABLE_DECOMMISSION_RUNBOOK.md) | Turning a table off, and what is deliberately *not* deleted |

## Validation and evidence

| Document | What it covers |
|---|---|
| [`CAPABILITY_MATRIX.md`](CAPABILITY_MATRIX.md) | Four evidence grades, applied to every capability |
| [`validation/`](validation/) | 11 reports, including the two scored acceptance reviews (`DRP12_FINAL_REVIEW.md`, `AIGR12_FINAL_REVIEW.md`) |
| [`VERIFY_EVERY_FEATURE.md`](VERIFY_EVERY_FEATURE.md) · [`VERIFY_END_TO_END.md`](VERIFY_END_TO_END.md) | Read-only verification of every layer, with expected counts |
| [`LINEAGE_AND_RECOVERY_TEST_GUIDE.md`](LINEAGE_AND_RECOVERY_TEST_GUIDE.md) | Hands-on: test lineage in AWS, then rerun only the affected table / date / key / column |
| [`E2E_LIVE_TEST_PLAN.md`](E2E_LIVE_TEST_PLAN.md) · [`END_TO_END_WALKTHROUGH.md`](END_TO_END_WALKTHROUGH.md) | The live test plan, and the walkthrough it produced |
| [`DEMO_CAPTURE_GUIDE.md`](DEMO_CAPTURE_GUIDE.md) · [`SEEDED_DEMO_DATA.md`](SEEDED_DEMO_DATA.md) · [`LIVE_MODE_SETUP.md`](LIVE_MODE_SETUP.md) | 24 questions with verified output; what is seeded and why it lives in its own relation |

## Design decisions

| Document | What it covers |
|---|---|
| [`../DECISIONS.md`](../DECISIONS.md) | The ADR index — 83 decisions, one line each |
| [`adr/`](adr/) | The ADR bodies: context, decision, alternatives, consequences |
| [`../DECISION_LOG.md`](../DECISION_LOG.md) | The chronological log, **including the open items and the things that are decisions but not architecture** |
| [`CODE_WALKTHROUGH.md`](CODE_WALKTHROUGH.md) | **The decisions as code** — the files worth reading, and the invariant each one protects |
| [`GAP_ANALYSIS.md`](GAP_ANALYSIS.md) · [`EXISTING_PLATFORM_AUDIT.md`](EXISTING_PLATFORM_AUDIT.md) | What was wrong at the start, which is where most of the decisions came from |

---

## Reading the evidence labels

Everything in this repository carries one of four labels, and they are grades of *evidence*,
not of quality:

| Label | Means |
|---|---|
| **LIVE TESTED** | It was executed and the result was observed |
| **IMPLEMENTED** | Code exists and passes tests; the deployed behaviour is unverified |
| **DESIGN ONLY** | Written and validated, never executed |
| **OPTIONAL** | Feature-flagged, default off |

If a document states a number, the command that produced it is nearby. If it could not be
produced, the document says so — see [`KNOWN_LIMITATIONS.md`](KNOWN_LIMITATIONS.md).
