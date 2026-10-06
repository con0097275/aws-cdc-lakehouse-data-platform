# VERSIONS

Pinned versions and the date each was verified. `CLAUDE.md` §3.9 forbids `latest`.

## Verified 2026-08-12 (Sessions 01 and 02 Stage A)

### Tooling present on the workstation

| Tool | Version | Source | Verified |
|---|---|---|---|
| `terraform` | **1.15.8** | `terraform --version` | Session 01 |
| `python3` | 3.10.8 | `python3 --version` | Session 01 |
| `aws` CLI | 1.36.20 (botocore 1.35.79) | `aws --version` | Session 01 |
| `git` | 2.34.1 | `git --version` | Session 01 |
| `jq` | 1.6 | `jq --version` | Session 01 |
| `npx` | 11.12.1 | `npx --version` | Session 01 |
| `checkov` | **3.3.10** | `checkov --version` | **Session 02 Stage A** |

`checkov` was installed with `pip install --user 'checkov==3.3.10'` and the resolved
version was then pinned in `scripts/install-tools.sh`. It is **not** pinned to a value
chosen in advance — the pin records what was actually installed. Smoke-tested against a
deliberately non-compliant `aws_s3_bucket` and it reported the expected findings.

> **Environment note.** `pip check` reports four dependency conflicts on this
> workstation — `aiobotocore`/`botocore`, `dbt-common`/`isodate`,
> `dbt-redshift`/`redshift-connector`, `opentelemetry-proto`/`protobuf`. None of the
> four packages appears in checkov's dependency list, so these are **pre-existing** in
> the Python user site and were merely reported by the install. If they later cause
> trouble, install checkov into a dedicated virtualenv instead of `--user`.

### Python libraries the reporting framework depends on (Session 22, Phase 3)

Verified 2026-08-19 by importing each in the workstation interpreter. **No new dependency
was introduced** — every one was already present, which is why Phase 3 needed no install
step and no network access.

| Library | Version | Used by | Already present because |
|---|---|---|---|
| `PyYAML` | **6.0.1** | `config_loader` reads the authored job YAML | dbt-core dependency |
| `jsonschema` | **4.23.0** | schema validation of `reporting/*.yaml` | pre-installed |
| `networkx` | **2.6.3** | `graph` — turns, closure, cycle detection | **direct dbt-core dependency** (`pip show dbt-core` → `Requires: … networkx …`) |
| `boto3` | **1.35.49** | `dynamodb_state` adapter (code only; no table exists) | AWS CLI / SDK already installed |
| `pytest` | **9.1.1** | the suite | pre-installed |

`moto` is **NOT** installed and is **NOT** required: the DynamoDB adapter is tested against
a hand-written fake (`spark/tests/fake_dynamodb.py`) that evaluates the condition-expression
subset the framework emits. That keeps the reporting tests at ~1 second and $0.

### Tooling required but ABSENT

| Tool | Needed for | Tracked as |
|---|---|---|
| `tflint` | `reference/ACCEPTANCE_CRITERIA.md:6` lint gate | **OPEN-06 — still open** |
| `shellcheck` | shell script lint for `scripts/*.sh` | OPEN-06 (added Session 02) |
| `mmdc` | mermaid render; structural parse used instead | — |

`tflint` was deliberately not installed in Session 02 Stage A: pinning it requires
choosing and verifying a specific release rather than accepting whatever `latest`
resolves to (`CLAUDE.md` §3.9). `bash scripts/install-tools.sh` prints the exact
resolve → pin → download → **checksum-verify** → install sequence. Record the version
here and close OPEN-06 once it is run.

`scripts/validate-docs.py` runs **14** checks as of Session 02 Stage A (the new one asserts
that AWS-mutating scripts default to dry-run and cannot be confirmed non-interactively).

### Terraform providers — inherited from repo A, verified against its `versions.tf`

| Provider | Pin | Note |
|---|---|---|
| `hashicorp/aws` | **`6.56.0`** | exact, no `~>` |
| `hashicorp/random` | **`3.9.0`** | exact |
| `required_version` | `~> 1.15.0` | state was written by 1.15.8 |

`.terraform.lock.hcl` in repo A carries hashes for `linux_amd64`, `darwin_amd64`,
`darwin_arm64`, `windows_amd64`. Carry it forward at absorption (ADR-001).

### AWS platform versions — from repo A `terraform.tfvars`, to be re-verified at apply

| Component | Pin | Note |
|---|---|---|
| MSK Kafka version | `3.9.x.kraft` | KRaft confirmed ACTIVE in `ap-southeast-1` on 2026-07-26; re-verify with `aws kafka list-kafka-versions` |
| MSK broker type | `kafka.m7g.large` | **OPEN-03 CLOSED, negative — 2026-08-13.** `kafka.t3.small` is **not offered** in `ap-southeast-1`. CloudTrail holds the `CreateCluster` rejection of 2026-07-26T13:01:14Z: `BadRequestException`, `invalidParameter: instanceType`, with the full valid list — smallest is `kafka.m5.large` / `kafka.m7g.large`. `m7g.large` is already the cheapest (Graviton undercuts m5). Evidence: `artifacts/validation/session-00/aws/open-03-settled.txt` |
| Prometheus | `3.13.1` | on the toolbox |
| Grafana | `13.1.1` | on the toolbox |
| Alertmanager | `0.33.1` | on the toolbox |

### To be pinned in later sessions

Each needs an exact version and a checksum before use. No `latest` tags.

| Component | Session | Note |
|---|---|---|
| Debezium Oracle / SQL Server connectors | 05 | pin the plugin archive and record its SHA256 |
| `aws-msk-iam-auth` | 04 | **risk R2** — pin and checksum; a version mismatch is a silent auth failure |
| Apicurio Registry image | 04 | KafkaSQL variant |
| Oracle Database Free image | 03 | **x86_64 only** (ADR-005) |
| SQL Server Developer image | 03 | **x86_64 only** |
| EMR Serverless release label | 06 | with the Iceberg runtime version it bundles |
| Apache Iceberg | 06 | must match the EMR release's bundled version |
| Airflow | 12 | **RESOLVED 2026-08-14: `3.2.2`** — the chart's own appVersion, NOT PyPI's newer 3.3.1. `KubernetesExecutor` (ADR-010) |
| Airflow Helm chart | 12 | **RESOLVED 2026-08-14: `1.22.0`**, published 2026-06-01, sha256 `1f7d1dfe3d58e2c54899950aba907a43625a18c8b6fb3c54760c21592129a5b6`. Digest verified by the bootstrap before `helm upgrade`; refuses on mismatch |
| PostgreSQL subchart | 12 | `13.2.24` (Airflow chart dependency) — lab metadata DB on a PVC, not RDS |
| k3s | 12 | **RESOLVED 2026-08-14: `v1.36.3+k3s1`** from the stable channel |
| dbt-core / dbt-spark | 11 | **RESOLVED 2026-09-21: `1.9.11` / `1.9.3`.** `dbt/dbt_project.yml` carries `require-dbt-version: [">=1.9.0", "<1.10.0"]`; this row said only "compatible pair", so the first wheelhouse was built at 1.8.9 and the EMR run failed on the version check (`00g8turrhf2ipg27`). The wheelhouse must be rebuilt whenever this pin moves |
| Trino | 13C | optional |
| GitHub Actions | 15/18 | `CLAUDE.md` §3.10 — pin third-party actions to a full commit SHA |

## Price list

Unit prices in `docs/PRICE_REFERENCE.md` were collected from the AWS Pricing API on
**2026-08-12** for `ap-southeast-1`. Prices change; re-collect with
`bash scripts/collect-pricing.sh <dir>` before any spend decision made more than ~90
days after that date, and update this line with the new collection date.

## Measured 2026-09-30 (DRP0 — audit only, nothing pinned or installed)

DRP0 needed the real runtime versions in order to constrain the future OpenLineage and
DataHub pins. **No version was changed and nothing was installed.**

| Component | Measured | How |
|---|---|---|
| Python (workstation) | 3.10.8 | `python3 --version` |
| Java | OpenJDK 17.0.20.1 | `java -version` |
| PySpark (workstation) | 3.5.0 | `pyspark.__version__` |
| EMR Serverless release | `emr-7.2.0` → Spark 3.5, Scala **2.12** | `terraform/envs/dev/variables.tf:418` |
| Apache Iceberg | **1.5.2** | `scripts/dbt-verify.sh:42`, `scripts/layout-benchmark.py:93` |
| `openlineage-*`, `acryl-datahub` | **not installed** | `pip list` |

### The workstation is not the deployment — a DRP3 trap, recorded before it is hit

| Component | Deployed | Workstation package |
|---|---|---|
| Airflow | **3.2.2** (Helm chart 1.22.0, `KubernetesExecutor`) | **2.9.3** |
| dbt-core | **1.9.11** (EMR wheelhouse pin) | **1.9.4** |

`apache-airflow-providers-openlineage` must be validated against Airflow **3.2.2**. An
Airflow-2-era provider will import cleanly on this workstation and fail to load on the
cluster.

> **Resolved live on 2026-09-30.** `apache-airflow-providers-openlineage==2.20.2` was
> installed into a pod running the deployed `apache/airflow:3.2.2` image and **emitted 7
> real OpenLineage events**. The version constraint is no longer a claim. The same run found
> that `AIRFLOW__OPENLINEAGE__TRANSPORT` must be a **JSON object** (`{"type":"console"}`),
> not the bare string the Spark listener takes — a bare word raises during plugin import,
> so the listener is silently never registered. See
> `artifacts/validation/data-reliability/drp3-airflow-runtime-lineage.json`. The same asymmetry already cost this project once: the row above at
`docs/VERSIONS.md` dbt entry records the first wheelhouse being built at 1.8.9 against a
`>=1.9.0` requirement, failing on EMR at run `00g8turrhf2ipg27`.

### Pins DRP2/DRP3 must resolve (deliberately absent here — CLAUDE.md §3.9)

| Artifact | Constraint |
|---|---|
| `io.openlineage:openlineage-spark_2.12` | Spark 3.5, Scala 2.12, verified against Iceberg 1.5.2; record the SHA256 |
| `apache-airflow-providers-openlineage` | must support Airflow 3.2.2 |
| `openlineage-dbt` | must match dbt-core 1.9.11; **rebuild the EMR wheelhouse** |
| DataHub server + `acryl-datahub` CLI | same release line; the release must expose the OpenLineage ingestion endpoint |

See `docs/adr/ADR-087-datahub-primary-metadata-plane-and-openlineage-runtime.md`.

## Resolved 2026-09-30 (DRP2/DRP3 — metadata plane and runtime lineage)

Read from the registries, not chosen. Nothing below is installed yet.

| Component | Pin | Source |
|---|---|---|
| DataHub server image | `v1.7.0.1` (2026-09-03) | Docker Hub; digests recorded in `governance/registry/metadata_plane.yaml` |
| DataHub CLI | `acryl-datahub==1.7.0.13` | PyPI, `requires_python >=3.10` |
| OpenLineage Spark | `io.openlineage:openlineage-spark_2.12:1.53.0`, sha1 `af24560ba5f93a6da925440eaf74277c7c399ca4` | `repo1.maven.org/.../maven-metadata.xml` |
| OpenLineage Python / dbt | `1.53.0` | PyPI |
| Airflow provider | `apache-airflow-providers-openlineage==2.20.2` | PyPI; requires `apache-airflow>=2.11.0` |

**The Airflow constraint from the DRP0 audit is now closed.** The provider requires
`apache-airflow>=2.11.0`, so it loads on the deployed **3.2.2**. The workstation's 2.9.3 is
still not a valid test bed for it.

**Read `maven-metadata.xml`, not the Maven search API.** The search index reported
`openlineage-spark_2.12` topping out at **1.34.0** while the metadata had **1.53.0** —
nineteen releases stale. Pinning from the index would have shipped a year-old jar with a
resolve to point at. Same class of defect as the dbt 1.8.9 wheelhouse above: a pin that came
from the wrong authority.

**Server and CLI are compared on a release LINE, not a string.** DataHub advances the two on
independent build cadences — on this date the newest image was `v1.7.0.1` and no `1.7.0.1`
existed on PyPI at all. `cdc/metadata_plane.py` compares the `major.minor.patch` prefix.

### Installation state

| | State |
|---|---|
| `apache-airflow-providers-openlineage` | the STOCK `apache/airflow:3.2.2` ships **2.17.0** with `openlineage-python 1.47.1`, and the plugin is registered on the running scheduler. **2.20.2** (with `openlineage-python 1.53.0`, matching the Spark jar) is **DEPLOYED** as `airflow-openlineage:3.2.2-ol2.20.2` — built with kaniko on the node, imported into containerd, rolled out as Helm revision 2, and confirmed emitting on a real task run. Chart 1.22.0 removed `extraPipPackages`, so the newer provider could not come from values. |
| `openlineage-python` 1.53.0 | pulled in by the provider; confirmed at runtime |
| `io.openlineage:openlineage-spark_2.12:1.53.0` | exercised locally — 12 real events from Spark 3.5.0 |
| `acryl-datahub` | used against a local DataHub `v1.7.0.1`; absent from the EMR wheelhouse |

The pins above are what any install must use. None of them is resident in a 24/7 deployment,
which is deliberate: lineage and the metadata plane are both flag-gated and default off.
