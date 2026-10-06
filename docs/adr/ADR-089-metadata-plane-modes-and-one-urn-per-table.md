# ADR-089 — The metadata plane has three modes, and one table has exactly one URN

* **Status:** Accepted
* **Date:** 2026-09-30
* **Phase:** DRP2
* **Related:** ADR-033, ADR-067, ADR-087, ADR-088

## Context

ADR-087 chose DataHub as the primary metadata plane and set four constraints: derive the
catalogue, never let DataHub execute a job, carry an evidence class on every edge, and keep
the cost posture (`enable_datahub` default false, offline-first). DRP2 had to turn that into
something runnable without spending money and without making the data plane depend on a
catalogue.

Three problems had to be settled before any code.

**One: what happens when DataHub is down.** A metadata publish sits at the end of a job that
has already committed correct business rows. If the publish raises, a catalogue outage becomes
a data outage — the table is right, the mart is right, and the DAG goes red because a web
service was restarting. If it silently swallows everything, a stale catalogue looks fresh.

**Two: one table, many ingestors.** A single FULL_CDC table is visible to at least three
sources — the DataHub Glue source sees `database.table`, an S3/Iceberg source would see a
bucket path, and the Spark OpenLineage listener sees the job's write target. Three names
produce three "datasets", and the lineage graph splits into fragments that each look complete.
This is the failure DRP4's brief names explicitly, and it has to be prevented before any
ingestor is enabled, not after.

**Three: environment bleed.** DataHub URNs carry a fabric (`DEV`/`QA`/`PROD`), but nothing
forces a process to use its own. A dev job that emits a `PROD` URN writes an edge onto a
production dataset, and the production graph becomes confidently wrong in a way that looks
like more coverage.

## Decision

**1. Three modes, and `disabled` is a first-class one.**
`metadata_plane.mode ∈ {disabled, local, production}`, defaulting to `disabled` under
`lab_low_cost`. `disabled` is not a broken `local`: publishing is a no-op that records what
it would have sent, and every test run therefore exercises the off case. A code path that only
works when DataHub is running is a code path that has taken a dependency on a catalogue.

**2. Outage behaviour is declared per flow, and a data flow may not be `REQUIRED`.**
`BEST_EFFORT` records, counts and continues; `REQUIRED` fails the task. The config *refuses*
`REQUIRED` as the default and refuses it on any flow outside `METADATA_ONLY_FLOWS`
(`metadata_ingestion`, `lineage_impact`) — the two whose entire purpose is metadata. With the
plane `disabled`, a `REQUIRED` flow refuses to **start**, which is a different and more honest
outcome than failing.

**3. Retry is bounded and the worst case is stated.** 3 attempts, 10 s timeout,
0.5 s + 1.0 s backoff → `worst_case_seconds = 31.5`. A number nobody can state is a number
nobody has bounded, and unbounded retry inside a Spark task is a hang wearing a resilience
costume.

**4. One URN per table, minted in one function.** `UrnMinter.dataset_urn()` is the only
sanctioned way to name a dataset. All five lakehouse kinds — FULL_CDC, REALTIME, EOD, CURATED,
MART — map to `urn:li:dataPlatform:glue` with the name `<database>.<table>`, because the Glue
Data Catalog is this platform's catalogue of record (CLAUDE.md §6). An Iceberg table registered
in Glue is one dataset that happens to be stored in Iceberg. A test asserts that all 84
compiled assets mint 84 distinct URNs.

**5. The fabric is part of the identity and is enforced on every emit.** Two environments may
not share a fabric (refused at load); `assert_fabric()` raises on any foreign URN; a client and
its minter may not disagree about which environment they speak for. Domains are name-scoped
(`core_banking-dev`) because DataHub domains have no fabric component, and without that a dev
asset would appear inside the production domain's asset list.

**6. Versions are resolved, not chosen.** Server `v1.7.0.1` with recorded image digests, CLI
`acryl-datahub==1.7.0.13`, both read from the registries on 2026-09-30. A floating tag is
refused. The server and the CLI are checked for a shared `major.minor.patch` line rather than
string equality, because DataHub publishes them on independent build cadences — on that date
the newest server image was `v1.7.0.1` and no `1.7.0.1` existed on PyPI at all.

## Options

**Fail the job when a metadata publish fails.** Rejected as the default: it chooses a data
outage over a metadata one. Kept as `REQUIRED` for exactly the two flows that produce nothing
but metadata.

**Swallow every publish failure silently.** Rejected: a stale catalogue that reports success
is the same defect class as a DQ check passing on an empty table. Hence `result.degraded`, the
`degraded` counter, and a recorded error string.

**Let each ingestor name datasets in its own native way and reconcile later.** This is the
path of least resistance and the one the DRP4 brief warns about. Reconciling duplicate URNs
after the fact means rewriting lineage edges that already point at the wrong entity — and
until it is done, every impact query returns a partial answer that looks complete.

**Model an Iceberg table as both a `glue` dataset and an `iceberg`/`s3` dataset.** Technically
defensible — they are different physical facts. Rejected because every consumer of the graph
(impact analysis, recovery planning, certification) cares about *the table*, and a design that
makes them join two entities to find one table will be got wrong somewhere.

**Hand-maintain a docker-compose for local mode.** Rejected: DataHub's compose topology changes
between releases, so a copy here would be correct the day it was written and silently wrong at
the next upgrade — and a stale compose file fails as a half-initialised metadata store, which
looks like a DataHub bug and is not one. The pinned CLI fetches the topology belonging to the
pinned version.

**Use the `acryl-datahub` SDK or `requests` in the client.** Rejected: this module ships to EMR
inside `cdc-framework.zip`. Adding an HTTP client to publish metadata would put a new
transitive dependency tree into every Spark job, turning a catalogue feature into a deployment
risk. `urllib` only.

**Deploy DataHub now.** Rejected: DRP0 open question 3 — whether the k3s node can host
GMS + frontend + Elasticsearch + metadata store + Kafka under `lab_low_cost` — is unmeasured,
and CLAUDE.md §4.10 already keeps Marquez off for a strictly smaller footprint.

## Consequences

* The data plane is unchanged and takes no new dependency. With the plane `disabled` every
  publish path still runs, and that is the default.
* `FileTransport` gives DRP4–DRP7 a $0 path: valid metadata change proposals on disk, loadable
  when a server exists.
* `RestTransport` was written unexercised and is now **live-tested** (2026-09-30, DataHub
  v1.7.0.1): 9/9 smoke aspects, 373 governance aspects, 87 lineage aspects, all accepted on
  the first attempt. That run found three defects the 48 offline tests could not — a
  recording transport accepts any dict, and the aspect shape is only validated server-side.
  Recorded as D-LIVE-1: an offline test of a wire format proves internal consistency and
  nothing about the far end.
* DRP3 and DRP4 must derive every dataset identifier from `UrnMinter`, including the
  OpenLineage facets. An integration that formats its own name re-opens the duplicate-URN
  problem this ADR exists to close.
* The production design is written and nothing is provisioned. Mode `production` refuses to
  load without `enable_datahub: true`, `networking: private_only`, `tls: required`,
  `authentication: required` and `secret_source: ssm_securestring`.

## Cost

**$0.** No AWS resource, no deployment, no new Python dependency.

| Mode | Cost |
|---|---|
| `disabled` (default) | $0 — nothing runs |
| `local` | $0 in AWS. ~2.7 GB of images, ~10 GB RAM while running, stopped by hand |
| `production` | not provisioned; flag-gated and blocked on DRP0 open question 3 |

The measured workstation headroom is 19.4 GB RAM and 113 GB disk, so `local` is feasible here —
which is what makes a $0 smoke path real rather than theoretical.

The one future cost this ADR creates: when a Spark job first publishes metadata,
`governance/registry/*.yaml` must enter `cdc-framework.zip`, or the run fails with a missing
file *after* acquiring EMR capacity.

## Security

* **No credential in Git — enforced at load.** A key whose name looks like a credential and
  whose value is a literal is refused; keys naming *where* a secret lives (`_env`,
  `_ssm_parameter`, `_source`, `_arn`, …) are allowed. Tested.
* The token is read from `DATAHUB_GMS_TOKEN` at call time and never stored on the config
  object, so it cannot leak through a `repr`, a log line or a pytest failure dump. Tested.
* `local` binds `127.0.0.1` only — no published port reaches a network interface and no
  inbound rule exists. `production` is private-only with SSM port-forward ingress, and the
  mode refuses to load otherwise (CLAUDE.md §3.3, §3.4, §3.6).
* No IAM user and no access key: ingestion will use IRSA / instance profile (CLAUDE.md §3.2).
* Environment isolation is a security control, not only a correctness one — without
  `assert_fabric`, anyone with dev access could write ownership, tags and lineage onto
  production assets silently.
* The catalogue is treated at the classification of the most sensitive thing it describes
  (`restricted`), because it is the map of where the regulated columns are.
* Metadata is untrusted execution input: an asset name never becomes a shell or SQL string,
  and a plan may only name pre-registered executable job ids.

## Rollback

* Set `metadata_plane.mode: disabled` — one line, and every publish becomes a recorded no-op.
  Nothing else in the platform changes, which is the property the whole design is built for.
* `bash scripts/datahub-local.sh down --execute` stops the local plane and keeps the store;
  `nuke --execute` removes it. Neither touches AWS.
* The new modules (`cdc/metadata_plane.py`, `cdc/urns.py`, `cdc/datahub_client.py`) have no
  consumer in the data plane today; deleting them affects nothing else.
* Nothing was provisioned, so there is no infrastructure to destroy.

## Validation

| Check | Result |
|---|---|
| DRP2 test suite | see `spark/tests/test_drp2_datahub.py` — modes, pins, secrets, outage policy, bounded retry, URN identity, aspects, smoke, environment separation |
| Smoke test (offline) | synthetic dataset + owner/domain/tags/glossary + dataFlow + dataJob + one lineage edge + `dataJobInputOutput`, emitted and read back by content |
| Offline sink | ndjson round-trips to a loadable MCP |
| URN uniqueness | all compiled assets mint distinct URNs |
| Pins | resolved live from PyPI and Docker Hub on 2026-09-30, digests recorded |
| Airflow provider compatibility | `apache-airflow-providers-openlineage==2.20.2` requires `apache-airflow>=2.11.0` → loads on the deployed Airflow 3.2.2 (a DRP0 open constraint, now closed) |
| Local preflight | run on this workstation: docker 27.1.1 up, 19.4 GB RAM, 113 GB free — sufficient; CLI not installed, which `up` installs |

**Not claimed.** No DataHub was started. `RestTransport` has never spoken to a GMS. No
metadata was ingested and no runtime lineage was emitted. The live smoke test is an
operator-gated command, printed in `docs/DATAHUB_OPERATIONS_RUNBOOK.md` §4, and is recorded as
NOT_RUN rather than inferred from the offline one.
