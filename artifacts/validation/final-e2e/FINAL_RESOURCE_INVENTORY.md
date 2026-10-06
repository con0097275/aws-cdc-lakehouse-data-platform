# FINAL RESOURCE INVENTORY — 2026-09-03T11:37Z

Account 111122223333 · ap-southeast-1 · dev · profile my-aws-profile · Terraform state **292 resources**

| Resource | State | Detail |
|---|---|---|
| VPC | RUNNING | vpc-01063d55c6d35f1b5, 10.42.0.0/16, 6 subnets (3 public / 3 private), 1 IGW |
| NAT gateways | **0** | CLAUDE.md 4.2 |
| VPC endpoints | RUNNING | S3 Gateway, DynamoDB Gateway, Glue Interface — **no logs, no monitoring** (finding G3) |
| MSK | RUNNING | kafka-dev-lab-dev, ACTIVE, 3 x kafka.m7g.large, 3.9.x.kraft, TLS + CMK, PublicAccess DISABLED |
| EC2 | RUNNING | 4: toolbox t3.small, source-lab t3a.xlarge, cdc-runtime t3.large, airflow t3.large |
| EMR Serverless | CREATED | 00g8g05ud5dj2u25, ARM64, emr-7.2.0, auto-stop 15 min, no pre-init |
| Airflow | RUNNING | k3s, 7 pods Running, UI HTTP 200 via SSM only |
| DynamoDB | RUNNING | 4 tables, PAY_PER_REQUEST, 27 execution records + 2 watermarks |
| S3 lake | RUNNING | kafka-dev-lab-dev-lake-111122223333, SSE-KMS, versioned, public access blocked |
| Glue | RUNNING | 7 databases; mart/curated/full_cdc/stream tables materialised |
| Athena | RUNNING | kafka-dev-lab-dev-wg, 10 GiB cutoff ENFORCED |
| CloudWatch | PARTIAL | 9 alarms; 3 carry no live signal (G1/G2) |
| KMS | RUNNING | lake CMK 2fd510a7 + MSK CMK b43ae6ae; 6 further lake CMKs from prior cycles |
| Redshift / Trino / EKS / OpenSearch / SageMaker / RDS / Secrets Manager / EIP / ALB | **NOT_CREATED** | all flags off |

## Drift
```
terraform plan: 0 add, 1 change, 0 destroy
the 1 change = module.data_lake.aws_s3_object.prefix["logs/airflow/"] (known non-converging, G-P3-3)
```
