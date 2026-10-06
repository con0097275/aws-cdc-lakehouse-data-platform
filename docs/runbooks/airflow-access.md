# Runbook — Airflow: access the UI, and the three defects fixed to get there

## Access the UI

No public endpoint exists and none should (CLAUDE.md §3.3 forbids inbound `0.0.0.0/0`).
The UI is reached through an SSM tunnel:

```bash
AF=$(aws ec2 describe-instances --region ap-southeast-1 --profile my-aws-profile \
  --filters "Name=tag:Component,Values=airflow" Name=instance-state-name,Values=running \
  --query 'Reservations[].Instances[].InstanceId' --output text)

aws ssm start-session --target "$AF" \
  --region ap-southeast-1 --profile my-aws-profile \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'
```

Then open **http://localhost:8080**.

### If it fails

| Symptom | Cause | Check |
|---|---|---|
| `TargetNotConnected` | SSM agent not registered | `aws ssm describe-instance-information --filters Key=InstanceIds,Values=$AF` — needs `PingStatus: Online` |
| Tunnel opens, page dead | chart not installed | `k3s kubectl get pods -A` — need an `airflow` namespace |
| Pods `CrashLoopBackOff` | usually DNS | `k3s kubectl -n kube-system logs deploy/coredns --tail=20` |

Confirm readiness before tunnelling:

```bash
aws ssm send-command --instance-ids "$AF" --document-name AWS-RunShellScript \
  --region ap-southeast-1 --profile my-aws-profile --parameters 'commands=[
    "cloud-init status","k3s kubectl get pods -A"]'
```

`cloud-init status: done` and Airflow pods `Running` before the tunnel is worth opening.

## The three defects that made this necessary

All fixed in Terraform on 2026-08-23. A fresh `terraform apply` now produces a working node.

### 1. The node was in a PRIVATE subnet with no route out

`envs/dev/main.tf` wired `airflow_k3s` to `private_subnet_ids[0]` with
`associate_public_ip_address = false`. There is no NAT (CLAUDE.md §4.2 forbids one) and
there are no SSM/ECR interface endpoints, so the node could reach **nothing**: k3s never
installed, the chart was never pulled, and the SSM agent never registered — so even
`start-session` returned `TargetNotConnected`. The instance ran 30+ minutes doing nothing
while appearing "deployed".

**Fixed:** public subnet + public IP, matching `cdc_runtime`, `toolbox` and `source_lab`.
`TARGET_ARCHITECTURE.md` §2 states this pattern: compute that needs the internet lives in a
public subnet behind a **zero-inbound** SG and exits through the IGW for free. The security
posture is unchanged — no ingress from the internet, and the UI is still SSM-only.

After the fix, SSM registered in **10 seconds**.

### 2. k3s pod CIDR collided with the VPC CIDR

k3s defaults `--cluster-cidr` to `10.42.0.0/16`. **The VPC is also `10.42.0.0/16`.** Pod
traffic and VPC traffic occupied the same space, and CoreDNS forwarded to itself:

```
[FATAL] plugin/loop: Loop (10.42.0.2:39225 -> :53) detected for zone "."
```

CoreDNS crash-looped, so nothing in the cluster could resolve anything.

**Fixed:** `--cluster-cidr 10.244.0.0/16 --service-cidr 10.245.0.0/16` in
`templates/bootstrap.sh.tftpl`.

This one would have recurred on every rebuild and is invisible until you read CoreDNS logs
— the node looks healthy and k3s reports `active`.

### 3. The module referenced an S3 object it never uploaded

The bootstrap downloads `bootstrap/airflow/values.yaml`; nothing created it:

```
fatal error: 404 HeadObject: Key "bootstrap/airflow/values.yaml" does not exist
```

k3s installed, then the script died — leaving a running Kubernetes cluster with **no
Airflow on it**, which from the outside reads as a successful deployment.

**Fixed:** `aws_s3_object.helm_values` uploads `airflow/helm/values.yaml`, the same way
`source_lab` and `cdc_runtime` stage their assets. `airflow_k3s` was the only module that
referenced an asset it never published.

## Verifying the deployment, once up

```bash
# 4 flow DAGs + the streaming lifecycle, and NOTHING per-mart
k3s kubectl -n airflow exec deploy/airflow-scheduler -- airflow dags list
```

Adding a datamart must add **zero** DAG files (ADR-039). If you see a per-mart DAG,
something has regressed — `spark/tests/test_phase16_datamart_template.py` asserts this.
