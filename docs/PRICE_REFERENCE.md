# Price Reference — `ap-southeast-1`, collected 2026-08-12

Every unit price below was fetched from the **AWS Pricing API** by
`scripts/collect-pricing.sh` and is traceable to a raw response saved under
`artifacts/validation/session-01/pricing/`. `CLAUDE.md` §4 and
`reference/COST_PROFILES.md:67` forbid hard-coded prices; this file exists so
`docs/COST.md` can do arithmetic against real numbers while remaining re-derivable.

- Location filter: `Asia Pacific (Singapore)` (`regionCode = ap-southeast-1`)
- Pricing API endpoint: `us-east-1` (the API is not served from `ap-southeast-1`; this is the endpoint, not the region being priced)
- Term: On-Demand, no reservations, no Savings Plans, no free tier applied
- Collected by: `my-aws-profile` in account `111122223333`, read-only

**Re-derive with:** `bash scripts/collect-pricing.sh artifacts/validation/session-01/pricing`

**Prices change.** Anything older than ~90 days should be re-collected before it
is used to justify a spend decision. Re-collection date is recorded in
`docs/VERSIONS.md`.

---

## 1. Amazon MSK — the dominant cost driver

Source: `pricing/msk-all-page1.json`

| Item | Usage type | Price | Unit |
|---|---|---:|---|
| Broker `kafka.m7g.large` (2 vCPU / 8 GiB) | `APS1-Kafka.m7g.large` | **0.2550** | USD/hour |
| Broker `kafka.m7g.xlarge` | `APS1-Kafka.m7g.xlarge` | 0.5100 | USD/hour |
| Broker `kafka.m5.large` | `APS1-Kafka.m5.large` | 0.2630 | USD/hour |
| Broker `kafka.t3.small` (2 vCPU / 2 GiB) | `APS1-Kafka.t3.small` | **0.0578** | USD/hour |
| Broker storage (provisioned, GP2-class) | `APS1-Kafka.Storage.GP2` | **0.1200** | USD/GB-month |
| Tiered storage | `APS1-Kafka.Storage.Tiered` | 0.0652 | USD/GB-month |
| Tiered-storage retrieval | `APS1-Kafka.DataRetrieval.Tiered` | 0.0018 | USD/GB |
| Express `express.m7g.large` | `APS1-Express.m7g.large` | 0.5100 | USD/hour |
| Express storage | `APS1-Express.Storage` | 0.1200 | USD/GB-month |
| MSK Serverless cluster | `APS1-KafkaServerless-ClusterHours` | 0.9375 | USD/hour |
| MSK Serverless partition | `APS1-KafkaServerless-PartitionHours` | 0.0018750 | USD/hour |
| Private connectivity (multi-VPC) | `APS1-Kafka.PrivateConnectivityHours` | 0.0252 | USD/hour |

> **`kafka.t3.small` is listed in the price list at 0.0578 USD/hour — 4.4× cheaper
> than `m7g.large`.** Repo A's `terraform.tfvars:11` comment claims t3 sizes are
> "no longer offered for new clusters (verified live 2026-07-26)". That claim is
> **not re-verifiable read-only**: there is no AWS API that lists supported broker
> types, and `scripts/preflight.sh` (which checks identity, AZ count and
> `list-kafka-versions`) does **not** test it. Presence in the price list is not
> proof of availability for new clusters. Because the difference is roughly the
> whole budget, this must be tested at the start of Session 02 — see
> `docs/RISK_REGISTER.md` R11 and `DECISION_LOG.md` OPEN-03.

## 2. EC2 and EBS

Source: `pricing/ec2-t3-*.json`, `pricing/ebs-gp3.json`

| Item | Usage type | Price | Unit |
|---|---|---:|---|
| `t3.small` Linux on-demand | `APS1-BoxUsage:t3.small` | 0.0264 | USD/hour |
| `t3.large` Linux on-demand | `APS1-BoxUsage:t3.large` | 0.1056 | USD/hour |
| `t3.xlarge` Linux on-demand | `APS1-BoxUsage:t3.xlarge` | 0.2112 | USD/hour |
| EBS `gp3` volume | `APS1-EBS:VolumeUsage.gp3` | **0.0960** | USD/GB-month |

> EBS bills while a volume exists, **including when its instance is stopped**.
> This is the single most common "I stopped everything, why am I billed" cause and
> is why `docs/COST.md` §5 tracks stopped-state residuals separately.

## 3. Network egress paths (Gap 8 inputs)

Source: `pricing/nat-gateway.json`, `pricing/vpc-endpoint.json`

| Item | Usage type | Price | Unit |
|---|---|---:|---|
| NAT Gateway hours | `APS1-NatGateway-Hours` | **0.0590** | USD/hour |
| NAT Gateway data processed | `APS1-NatGateway-Bytes` | **0.0590** | USD/GB |
| Interface endpoint (per endpoint per AZ) | `APS1-VpcEndpoint-Hours` | **0.0130** | USD/hour |
| Interface endpoint data processed (first tier) | `APS1-VpcEndpoint-Bytes` | 0.0100 | USD/GB |
| Gateway endpoint (`s3`, `dynamodb`) | — | **0.0000** | free |
| Gateway Load Balancer endpoint | `APS1-VpcEndpoint-GWLBE-Hours` | 0.0130 | USD/hour |

> One NAT hour (0.0590) costs the same as **4.5 interface-endpoint-AZ-hours**
> (0.0130 each). The comparison in `docs/COST.md` §4 turns on how many distinct
> interface endpoints the workload set actually needs — this is what makes the
> decision arithmetic rather than ideology.

## 4. Storage and catalog

Source: `pricing/s3-storage.json`, `pricing/s3-requests.json`, `pricing/glue.json`

| Item | Usage type | Price | Unit |
|---|---|---:|---|
| S3 Standard (first 50 TB) | `APS1-TimedStorage-ByteHrs` | **0.0250** | USD/GB-month |
| S3 Standard-IA | `APS1-TimedStorage-SIA-ByteHrs` | 0.0138 | USD/GB-month |
| S3 Glacier Instant Retrieval | `APS1-TimedStorage-GIR-ByteHrs` | 0.0050 | USD/GB-month |
| S3 PUT/COPY/POST/LIST | `APS1-Requests-Tier1` | 0.0000050 | USD/request (= $0.005 / 1 000) |
| S3 GET and other | `APS1-Requests-Tier2` | 0.0000004 | USD/request (= $0.004 / 10 000) |
| Glue Data Catalog storage | `APS1-Catalog-Storage` | 0.0000100 | USD/object-month |
| Glue Data Catalog requests | `APS1-Catalog-Request` | 0.0000010 | USD/request |
| Glue crawler / ETL DPU | `APS1-Crawler-DPU-Hour`, `APS1-ETL-DPU-Hour` | 0.4400 | USD/DPU-hour |

> Glue Data Catalog is effectively free at lab scale (first 1M objects and 1M
> requests/month are free tier; even unfree it is $0.00001/object-month). Glue
> **crawlers and ETL jobs** are not free — at $0.44/DPU-hour they are why this
> architecture has Spark write Iceberg metadata directly instead of crawling.

## 5. Compute engines

Source: `pricing/emr-serverless-units.json`, `pricing/athena.json`, `pricing/redshift-serverless.json`, `pricing/redshift-managed-storage.json`

| Item | Usage type | Price | Unit |
|---|---|---:|---|
| EMR Serverless x86 vCPU | `APS1-EMR-SERVERLESS-vCPUHours` | **0.0657280** | USD/vCPU-hour |
| EMR Serverless x86 memory | `APS1-EMR-SERVERLESS-MemoryGBHours` | **0.0071890** | USD/GB-hour |
| EMR Serverless ARM vCPU | `APS1-EMR-SERVERLESS-ARM-vCPUHours` | 0.0525850 | USD/vCPU-hour |
| EMR Serverless ARM memory | `APS1-EMR-SERVERLESS-ARM-MemoryGBHours` | 0.0057460 | USD/GB-hour |
| EMR Serverless storage (beyond free 20 GB/worker) | `APS1-EMR-SERVERLESS-StorageGBHours` | 0.0001330 | USD/GB-hour |
| Athena data scanned | `APS1-DataScannedInTB` | **5.0000** | USD/TB |
| Athena Spark DPU | `APS1-CodeExecutionInDPUHours` | 0.4500 | USD/DPU-hour |
| Redshift Serverless compute | `APS1-Redshift:ServerlessUsage` | **0.4500** | USD/RPU-hour |
| Redshift managed storage | `APS1-RMS:Serverless` | 0.0261 | USD/GB-month |

> **ARM (Graviton) EMR Serverless is 20 % cheaper on vCPU and 20 % cheaper on
> memory.** Spark/Iceberg/Java workloads run on ARM without source changes, so
> `ADR-008` selects `architecture = ARM64` as the default. This is the cheapest
> uncontroversial saving available in the whole design.

## 6. Supporting services

Source: `pricing/secretsmanager.json`, `pricing/kms.json`, `pricing/cloudwatch-logs.json`

| Item | Usage type | Price | Unit |
|---|---|---:|---|
| Secrets Manager secret | `APS1-AWSSecretsManager-Secret` | **0.4000** | USD/secret-month |
| Secrets Manager API request | `APS1-AWSSecretsManagerAPIRequest` | 0.0000050 | USD/request |
| KMS customer managed key | `ap-southeast-1-KMS-Keys` | **1.0000** | USD/key-month |
| KMS symmetric request | `ap-southeast-1-KMS-Requests` | 0.0000030 | USD/request |
| CloudWatch Logs ingestion (vended, first tier) | `APS1-VendedLog-Bytes` | 0.0700 | USD/GB |
| CloudWatch Logs storage | `APS1-TimedStorage-ByteHrs` (CW) | 0.0300 | USD/GB-month |
| CloudWatch Logs scanned (Insights) | `APS1-DataScanned-Bytes` | 0.0070 | USD/GB |
| CloudWatch alarm (standard) | `APS1-CW:AlarmMonitorUsage` | 0.1000 | USD/alarm-month |
| CloudWatch custom metric | `APS1-CW:MetricsUsage` | 0.0700 | USD/metric-month |

> KMS keys and Secrets Manager secrets bill **monthly regardless of use**, and
> survive a `terraform destroy` of the compute stack if they live in a different
> module. Each CMK is $1/month; each secret is $0.40/month. Small individually,
> but they are the "destroyed lab that still bills" tail — see `docs/COST.md` §5.

---

## Queries that returned no products

Recorded so a later session does not repeat the mistake:

| Query | Why it failed | Correct form |
|---|---|---|
| `AmazonMSK` filtered on `instanceType` | MSK has no `instanceType` attribute. Its attributes are `computeFamily`, `vcpu`, `memoryGib`, `usagetype`. | Filter on `location` only, then match `usagetype` / `computeFamily`. |
| `AmazonS3` filtered on `volumeType=Standard` | Not a valid S3 attribute pairing with `storageClass`. | Filter `productFamily=Storage`, then match `usagetype=APS1-TimedStorage-ByteHrs`. |
| `ElasticMapReduce` filtered on `location` alone | Returns ~2 000 EC2-instance products; EMR Serverless units are buried past the first page. | Add `productFamily=EMR Serverless` (exact string). |
| `AmazonRedshift` filtered on `productFamily=Redshift Serverless` | The family is named `Serverless`, not `Redshift Serverless`. | `productFamily=Serverless`; storage is `productFamily=Redshift Managed Storage`. |
