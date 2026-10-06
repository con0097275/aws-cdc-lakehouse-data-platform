# artifacts/validation/final-e2e/governance

Governance / security / observability / reconciliation acceptance, 2026-09-03 11:38-11:50 UTC.
Document: `docs/FINAL_GOVERNANCE_SECURITY_ACCEPTANCE.md`.

## THE HEADLINE — end-to-end numerical reconciliation, zero drift

The same number survives every hop from the source database to the AI answer:

```
Oracle corebank.account      321 accounts   2,031,880,937.77   (live, via SSM)
Kafka ACCOUNT topic          326 events + 2 tombstones
FULL_CDC  cdc_events         326 ACCOUNT rows (5,744 total)
EOD curated fact             321 rows       2,031,880,937.77
MART  mart_account_balance_daily  321 rows  2,031,880,937.77
Business AI ask_business()                  2,031,880,937.77
```

321 = 320 seeded + account 990002 (deleted then recreated); account 990001 was deleted and
is correctly absent. The AI number is not a re-derivation — it is the mart value, reached
through the governed metric and the guarded Athena tool.

## Verified PASS

| Area | Evidence |
|---|---|
| No static credentials | `validate-docs` 14/14 incl. the credential scan |
| No SSH | 0 key pairs on all 4 instances; no port-22 ingress anywhere |
| No public inbound | no SG permits 0.0.0.0/0 |
| MSK | `PublicAccess: DISABLED`; in-transit TLS; at-rest CMK `b43ae6ae…`; in-cluster encryption on |
| NAT | 0 (CLAUDE.md 4.2) |
| AI security | `make ai-security` 23/23 PASS, 12 components costed, 0 always-on |
| Agent read-only | 4/4 mutation + injection attempts refused, `tool_calls: []`, no Athena issued |
| Athena guard | workgroup cutoff 10 GiB, `EnforceWorkGroupConfiguration: true` |
| Governance registry | 12 datasets, 3 domains, 0 missing required fields, 4 classifications, 6 PII datasets, 2 deletion-blocked |
| DQ + lineage config | `governance/dq/rules.yml`, `governance/lineage/openlineage.yml` present and parsed |
| Certification | mart rows carry `processing_status=CERTIFIED`, `config_version`, `execution_id`, `certified_at` |
| AI gates (live) | e2e_p14 **10/10**, RAG gate PASSED, agent gate PASSED |

## Findings

**G1 (P1) — `ActiveControllerCount` cannot clear on a KRaft cluster.**
`kafka-dev-lab-dev-active-controller-missing` watches `AWS/Kafka ActiveControllerCount`.
The cluster is `3.9.x.kraft`; that metric returned **0 datapoints** over the last hour, and
the alarm treats missing data as breaching. It has been ALARM for 2.5 h on a healthy
cluster and can never clear. An alarm that cannot clear trains operators to ignore the set.

**G2 (P1) — the reporting framework publishes no CloudWatch metrics at all.**
Namespace `kafka-dev-lab-dev/Reporting` is referenced by two alarms but holds **0 metrics**.
So `reporting-no-success` sits ALARM despite EOD and STREAM_BATCH both SUCCEEDING, and
`reporting-watermark-lag` is INSUFFICIENT_DATA despite a watermark being written.
**Reporting observability is structurally blind: a real failure is indistinguishable from
today's healthy state.**

**G3 (P1) — one root cause under G2 and the earlier EMR log failure.**
VPC endpoints are S3 (Gateway), DynamoDB (Gateway), Glue (Interface). There is **no `logs`
endpoint, no `monitoring` endpoint and no NAT**. Nothing in the private subnets can reach
CloudWatch Logs or `PutMetricData`. This single gap explains the EMR job that failed on
`Connect timeout ... logs.ap-southeast-1.amazonaws.com`, the empty metric namespace, and
both stuck alarms. Fix: add `logs` + `monitoring` interface endpoints (~$0.013/hr per AZ
each), or route reporting telemetry to S3.

**G4 (P2) — workload hosts are publicly addressable.**
All 4 EC2 instances sit in the `*-public-*` subnets with public IPs. No inbound rule
permits anything and there is no key pair, so this is not an exposure today; but defense
rests entirely on SG rules, which is weaker than the private-subnet posture the docs
describe. It is the deliberate consequence of running without NAT.
