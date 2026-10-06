# Final Governance / Security / Observability / Reconciliation Acceptance

- **Date:** 2026-09-03, 11:38–11:50 UTC
- **Platform:** live, 292 Terraform resources, MSK ACTIVE, ~$1.2340/hr
- **Evidence:** `artifacts/validation/final-e2e/governance/`
- **Verdict:** **PARTIAL PASS.** Governance, security and reconciliation **PASS**.
  **Observability FAILS** on three connected findings (G1–G3).

## 1. Reconciliation — PASS, and this is the headline

The same value survives every hop from the source database to the AI answer, with **zero
drift**:

| Stage | Rows / events | Sum(closing_balance) |
|---|---|---|
| Oracle `corebank.account` (live, via SSM) | **321 accounts** | **2,031,880,937.77** |
| Kafka `cdc.oracle.COREBANK.ACCOUNT` | 326 events + 2 tombstones | — |
| FULL_CDC `cdc_events` | 326 ACCOUNT rows (5,744 total) | — |
| REALTIME `cdc_events_realtime` | 5,738 (point-in-time) | — |
| EOD `curated.fact_account_daily_snapshot` | **321** | **2,031,880,937.77** |
| MART `mart_account_balance_daily` | **321** | **2,031,880,937.77** |
| Business AI `ask_business()` | — | **2,031,880,937.77** |

321 = 320 seeded + account 990002 (deleted, then recreated); account 990001 was deleted and
is correctly **absent** at every layer downstream of FULL_CDC. The AI figure is not an
independent re-derivation — it is the mart value reached through the governed metric and
the guarded Athena tool, which is what makes the chain meaningful.

## 2. Data governance — PASS

`governance/catalog/domains.yml`: **12 datasets, 3 domains, zero missing required fields**
(owner, classification, retention_days, freshness_sla_minutes, grain, primary_key —
counting file-level defaults as present).

- **Classification:** `restricted`, `confidential`, `internal`, `operational`
- **PII:** 6 datasets carry `pii_columns`; `bi_access` is `denied` on the raw layers
- **Retention:** per-dataset overrides where PII is held; 2 datasets carry
  `deletion_blocked_until`, making retention a correctness setting rather than a storage one
- **Contracts / lineage / DQ:** `docs/DATA_CONTRACTS.md`, `governance/lineage/openlineage.yml`
  and `governance/dq/rules.yml` all present and parse
- **Certification:** every mart row carries `processing_status=CERTIFIED`, `certified_at`,
  `config_version`, `execution_id`, `coordinator_run_id`, `source_flow_mode`
- **Schema evolution:** exercised live — FULL_CDC widened itself with `dv_event_id`,
  `dv_src_event_id`, `loaded_at` and logged each addition
- **Lineage:** resolved from the **dbt manifest**, not a parallel hand-maintained list

## 3. Security — PASS

| Control | Result |
|---|---|
| Static credentials | none — `validate-docs` credential scan passes (14/14) |
| SSH / key pairs | **0 key pairs** on all 4 instances; **no port-22 ingress anywhere** |
| Public inbound | **no security group permits 0.0.0.0/0** |
| MSK | `PublicAccess: DISABLED`, in-transit **TLS**, in-cluster encryption on, at-rest CMK `b43ae6ae…` |
| NAT | **0** (CLAUDE.md §4.2) |
| Secrets | 8 SSM SecureString parameters; CDC passwords hash-verified identical across SSM → `.env` → Connect secrets; secrets file `0600` uid 1000 |
| S3 | public access blocked, SSE-KMS default, versioning on |
| Athena | workgroup cutoff 10 GiB with `EnforceWorkGroupConfiguration: true` |
| AI source checks | `make ai-security` **23/23 PASS**, 12 components costed, 0 always-on |
| Agent mutation path | **none** — 4/4 DROP/DELETE/terraform+Kafka-reset/prompt-injection refused, `tool_calls: []`, no Athena query issued |
| Defense in depth | IAM denies the raw databases; the tool allow-list is the *second* control, and its unqualified-reference bypass was closed earlier today |

## 4. AI governance — PASS

Knowledge corpus is built only from documentation and governance metadata, never from
tfvars, state or the data itself; redaction runs in both directions. Metric ownership is
explicit (`total_closing_balance`, owner `risk-data`, `metric:0d1b5e7ea203f9de`,
`minimum_certification: RECONCILED`). Feature contracts enforce **event time**
(`feature_event_time`) and reject processing-time join keys; PIT correctness and the
horizon-leakage guard pass in 73 tests. Every answered request carries `request_id`,
metric/feature/config versions, dbt lineage, Athena query ids, sql hashes, bytes scanned,
tool calls, and `tokens 0/0 · $0.000000`. Write tools are asserted empty (`WRITE_TOOLS = {}`,
ADR-057), so any future write action is a deliberate code change, not a runtime toggle.

Live gates: `e2e_p14` **10 PASS / 0 FAIL**, RAG gate **PASSED**, agent gate **PASSED**.

## 5. Observability — **FAIL**

Nine alarms exist and six are `OK`. Three are not connected to a live signal.

### G1 (P1) — an alarm that can never clear
`kafka-dev-lab-dev-active-controller-missing` watches `AWS/Kafka ActiveControllerCount`.
The cluster is **KRaft** (`3.9.x.kraft`); that metric returned **0 datapoints** over the
preceding hour, and the alarm treats missing data as breaching. It has been in ALARM for
2.5 h against a demonstrably healthy cluster. An alarm that cannot clear is worse than no
alarm — it trains operators to ignore the whole set.

### G2 (P1) — reporting observability is structurally blind
Namespace `kafka-dev-lab-dev/Reporting` is referenced by two alarms and contains **0
metrics**. Consequently `reporting-no-success` is ALARM even though EOD and STREAM_BATCH
both **SUCCEEDED** in this window, and `reporting-watermark-lag` is `INSUFFICIENT_DATA`
even though a watermark was written. **A real reporting failure would be indistinguishable
from the current healthy state.**

### G3 (P1) — one root cause under G2 and the earlier EMR failure
VPC endpoints are S3 (Gateway), DynamoDB (Gateway), Glue (Interface). There is **no `logs`
endpoint, no `monitoring` endpoint, and no NAT**. Nothing in the private subnets can reach
CloudWatch Logs or `PutMetricData`. That single gap explains all three symptoms seen today:
the EMR job that died on `Connect timeout … logs.ap-southeast-1.amazonaws.com`, the empty
metric namespace, and both stuck alarms.

**Fix:** add `logs` and `monitoring` interface endpoints (~$0.013/hr per AZ each), or route
reporting telemetry to S3 as the EMR jobs already do.

### G4 (P2) — workload hosts are publicly addressable
All four EC2 instances run in the `*-public-*` subnets with public IPs. No inbound rule
permits anything and no key pair exists, so this is not an exposure today — but defense
rests entirely on security-group rules, which is weaker than the private-subnet posture the
documents describe. It is the deliberate consequence of operating without NAT.

## 6. SLOs and alerts — PARTIAL

`docs/SLO.md` defines the objective set (NRT freshness, EOD completion, CDC completeness,
streaming liveness, DQ failure, reporting completion, AI error/latency/cost). MSK-side
alarms (broker CPU, disk, offline partitions, under-replicated partitions, multiple active
controllers) are wired and `OK`. **Everything that depends on a custom metric is
unmeasured** because of G2/G3: CDC freshness, canonical-layer freshness, realtime freshness,
EOD completion, reporting completion, streaming liveness, DQ failures and the AI
error/latency/cost SLOs have no live signal behind them.

## Verdict

Governance, security, AI governance and reconciliation are **acceptance-grade**, and the
reconciliation chain is the strongest evidence produced in this engagement. Observability is
**not** acceptance-grade: three of nine alarms carry no live signal, and the SLO set is
largely unmeasured. G3 is a single, cheap, well-understood fix that resolves most of it.
