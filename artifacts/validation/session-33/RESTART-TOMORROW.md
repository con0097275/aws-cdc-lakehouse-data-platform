# Restarting tomorrow — what survived, what did not

## State at shutdown 2026-08-20

| Resource | State | Consequence |
|---|---|---|
| MSK cluster | **DELETED** | the $0.7650/hr meter is off; topics, Connect internal state and the 24 Apicurio schemas are gone with it |
| source-lab `i-0108f96e91b11b717` | **stopped**, 50 GiB volume intact | Oracle + SQL Server, their data, ARCHIVELOG, CDC users and snapshot isolation all survive |
| cdc-runtime `i-0711caaff2c72aab0` | **TERMINATED** | every host-side fix is gone; it rebuilds from the repo |
| toolbox `i-07055404691dd87ba` | TERMINATED | rebuilds |
| S3 lake | intact | L1 8,932 rows, L2, curated 320, mart, all Phase 14 evidence |
| DynamoDB | intact, $0 idle | 19 execution records, 4 watermarks |
| Terraform state | **183 resources, partially destroyed** | the targeted destroy removed ~15 IAM resources before failing |

Overnight cost: ~$0.25 (one 50 GiB volume + two KMS keys).

## KMS — no action needed (corrected 2026-08-21)

An earlier note in this file called for cancelling a key deletion. That was wrong and has
been removed. The state is:

    8d20ebe1-39e8-4f10-be31-957cd4b8fad2  Enabled          <- the CURRENT lake key
    c449919b-f71a-485b-948e-9724d10d40d3  PendingDeletion  <- old lake key, dead stack
    a3f3f553-4c31-48b0-9c3d-9531414b6537  PendingDeletion  <- old MSK key, dead stack

The current lake key matches `terraform output lake_kms_key_arn` and has no deletion date.
Everything in S3 -- L1, L2, curated, the mart, the Phase 14 evidence -- is encrypted with
it. The two PendingDeletion keys belong to the stack destroyed on 2026-08-16; letting them
expire on 2026-08-23 is correct and saves $2/month.

## Fixes now durable in the repo (they rebuild automatically)

- `docker/cdc-runtime/connect-worker.properties` — `plugin.path=/opt/connect-plugins`, and
  the converter class `io.apicurio.registry.utils.converter.AvroConverter`
- `docker/cdc-runtime/docker-compose.yml` — MSK IAM jar mounted onto the worker classpath;
  Apicurio's `REGISTRY_KAFKA_COMMON_*` security passthrough, `JAVA_CLASSPATH` +
  `JAVA_MAIN_CLASS`, `REGISTRY_AUTH_ENABLED=false`, healthcheck at `/health/ready`
- `docker/cdc-runtime/versions.env` — the Apicurio CONVERTER artifact
- `terraform/modules/lake_iam` — `connect:consume`
- `terraform/modules/emr_serverless` — prefix-list egress for the S3/DynamoDB gateways
- `terraform/modules/reporting_ops` — Glue table ARN pattern, `artifacts/*` read scope
- `terraform/modules/data_lake` — reporting prefixes
- `scripts/register-connectors.sh` — JSON payload via file, curl reports failures

## NOT yet automated — expect these to bite again

1. **The Apicurio converter bundle is not downloaded by the bootstrap.** `versions.env` names
   it, but nothing fetches `apicurio-registry-distro-connect-converter-2.6.2.Final.tar.gz`
   into `/opt/connect-plugins/apicurio-converter/`. Do it by hand or wire it in.
2. **Two Oracle JDBC drivers ship together.** Remove `ojdbc8-21.11.0.0.jar` from
   `/opt/connect-plugins/debezium-oracle/` or you get ORA-01005 again.
3. **`create-topics.sh` still emits the wrong names.** Correct ones:
   `cdc.sqlserver.digital.dbo.<table>` and `cdc.oracle.COREBANK.<TABLE>`, plus
   `cdc.oracle`, `cdc.sqlserver`, both `*.schema-history`
   (**cleanup.policy=delete, retention.ms=-1 — NOT compact**) and the heartbeat topics.
4. **New MSK = new broker DNS.** Re-render `connect-worker.rendered.properties` and
   `client.properties`; the old bootstrap string is baked in.
5. **The MSK security-group flap.** `aws_security_group.msk` uses inline `ingress` while
   `emr_serverless` and `cdc_runtime` add separate rule resources. Any plan will want to
   revoke EMR and CDC access on port 9098. Read the plan before applying.
6. **Rotate the CDC passwords** — `/kafka-dev-lab/dev/source-lab/{oracle,sqlserver}-cdc-password`
   both appeared in terminal output and in the `connect-configs` topic.

## Sequence

    bash scripts/tf.sh plan                    # EXPECT a large plan: IAM was partly destroyed
    bash scripts/tf.sh show                    # read it — check the MSK SG diff (item 5)
    bash scripts/tf.sh apply --execute         # MSK takes 20-30 min to reach ACTIVE
    bash scripts/cdc-window-start.sh           # readiness gate
    # then items 1-3 by hand, then seed / topics / connectors

Budget: ~$8.50 spent of $30, ~$21.50 left. A full rebuild costs ~$1.12/hr once MSK is up.
