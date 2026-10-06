# DATAHUB — ARCHITECTURE

- Phase: **DRP2**. Mode machinery and the pinned local deployment are **built**;
  production is **designed and not provisioned**.
- Code: `cdc/metadata_plane.py`, `cdc/urns.py`, `cdc/datahub_client.py`
- Config: `governance/registry/metadata_plane.yaml` · Script: `scripts/datahub-local.sh`
- Related: ADR-087 (DataHub as the primary metadata plane), ADR-089

---

## 1. Three modes, and `disabled` is a real one

| Mode | What runs | Cost | Default? |
|---|---|---|---|
| `disabled` | nothing | $0 | **yes**, under `lab_low_cost` |
| `local` | the official quickstart on the workstation, on demand | $0 in AWS | no |
| `production` | a resident deployment | metered | no, and flag-gated |

`disabled` is not a broken `local`. Every publish path must work with the plane switched
off, and the default exercises that case on every test run. A code path that only works
when DataHub is running is a code path that has taken a dependency on a catalogue — and
this platform's data plane must never depend on one.

```
                  ┌─────────────────────────────────────────────┐
   data plane ───▶│ DataHubClient.publish(flow, mcps)           │
   (unchanged)    └───────────────┬─────────────────────────────┘
                                  │  mode
            ┌─────────────────────┼─────────────────────┬──────────────────┐
            ▼                     ▼                     ▼                  ▼
      NullTransport        RecordingTransport      FileTransport      RestTransport
      (disabled)           (dry run / tests)       (offline sink)     (live GMS)
        counts,              keeps every             ndjson on         urllib only,
        sends nothing        proposal                disk              no new dependency
```

## 2. The invariant

> **Business data must never be wrong, lost or blocked because the catalogue is down.**

Three consequences, enforced in code rather than left to callers:

1. **Outage behaviour is declared per flow**, not decided in an exception handler.
2. **Retry is bounded** — 3 attempts, 0.5s + 1.0s backoff, 10s timeout.
   `worst_case_seconds` is **31.5**. A number nobody can state is a number nobody has
   bounded, and unbounded retry inside a Spark task is a hang wearing a resilience costume.
3. **A BEST_EFFORT publish cannot raise.** By the time a job publishes metadata its rows are
   already committed and correct; raising would convert a catalogue outage into a data
   outage.

| Policy | Behaviour | Allowed for |
|---|---|---|
| `BEST_EFFORT` | record, count, continue | every data flow — and it is the default |
| `REQUIRED` | fail the task | only `metadata_ingestion` and `lineage_impact` |

The config **refuses** `REQUIRED` on a data flow, and refuses it as the default. Those two
exceptions are the flows whose entire purpose is metadata: if the ingestion workflow cannot
reach DataHub it has done nothing, and reporting success would make a stale catalogue look
fresh; if impact analysis cannot read the lineage graph it cannot know the blast radius, and
a recovery planned on a guess is worse than no plan.

With the plane `disabled`, a `REQUIRED` flow **refuses to start** rather than failing. That
is a different and more honest outcome: it did not do nothing badly, it may not begin.

## 3. Identity — one URN per table

The decision DRP4 depends on. One FULL_CDC table is visible to at least three ingestors: the
Glue source sees `database.table`, an S3/Iceberg source would see a bucket path, and the
Spark OpenLineage listener sees the job's write target. Three sources, three names, three
"datasets", and the lineage graph splits into fragments that each look complete.

So the mapping lives in **one function**, `UrnMinter.dataset_urn()`, and every producer
derives from it.

| Asset kind | DataHub platform | Dataset name |
|---|---|---|
| `src` (Oracle) | `oracle` | `coredb.corebank.account` |
| `src` (SQL Server) | `mssql` | `digital.dbo.app_user` |
| `topic` | `kafka` | `cdc.oracle.COREBANK.ACCOUNT` |
| `full_cdc` / `realtime` / `eod` / `curated` / `mart` / `serving` | **`glue`** | `<database>.<table>` |
| `bi_dataset` / `bi_report` | `powerbi` | the BI name |

Every lakehouse table is `glue`, because the Glue Data Catalog is this platform's catalogue
of record (CLAUDE.md §6). **An Iceberg table registered in Glue is one dataset that happens
to be stored in Iceberg** — not a Glue dataset and an Iceberg dataset that happen to agree.

A test asserts that all 84 compiled assets mint 84 distinct URNs.

## 4. Environment separation

Every dataset URN carries a fabric:

```
urn:li:dataset:(urn:li:dataPlatform:glue,kafka_dev_lab_dev_snapshot.eod_…_account,DEV)
                                                                              ^^^^
```

| Environment | Fabric | Namespace |
|---|---|---|
| `dev` | `DEV` | `cdc-lakehouse-dev` |
| `test` | `QA` | `cdc-lakehouse-test` |
| `prod` | `PROD` | `cdc-lakehouse-prod` |

Enforced three ways:

- **Two environments may not share a fabric** — refused at config load.
- **`assert_fabric()` runs on every emit.** A dev process emitting a `PROD` URN raises. This
  is the check that keeps dev lineage out of prod URNs, and the failure it prevents is
  silent: a dev edge on a prod dataset makes the production graph confidently wrong, and
  nothing downstream can tell.
- **A client and its minter may not disagree** about which environment they speak for.

Jobs carry the environment as the DataFlow **cluster**
(`urn:li:dataFlow:(airflow,eod_build,dev)`), and domains are name-scoped
(`urn:li:domain:core_banking-dev`) because DataHub domains have no fabric component — without
that, a dev asset would appear inside the production domain's asset list.

## 5. Versions — resolved, not chosen

Every pin was read from the registry on **2026-09-30**:

| Component | Pin | Evidence |
|---|---|---|
| DataHub server | `v1.7.0.1` (2026-09-03) | newest stable 1.7 image; `v1.8.0rc*` are pre-release, `v1.6.0.3` is a backport |
| `datahub-gms` digest | `sha256:74e15e98…0173` | Docker Hub |
| `datahub-frontend-react` digest | `sha256:99513cc1…310d` | Docker Hub |
| `datahub-actions` digest | `sha256:c5fd7013…4b90` | Docker Hub |
| CLI | `acryl-datahub==1.7.0.13` | PyPI, `requires_python >=3.10` |

**The server and the CLI advance on independent patch cadences within one line.** On
2026-09-30 the newest server image was `v1.7.0.1` and no `1.7.0.1` existed on PyPI at all,
while the newest CLI was `1.7.0.13`. Comparing full version strings would reject a perfectly
valid pair, so the check is the shared `major.minor.patch` prefix — `1.7.0`. A CLI a whole
**minor** ahead of the server would write aspects the server cannot read, and that is what
the check catches.

A floating tag (`latest`, `nightly`, `head`, `quickstart`) is refused (CLAUDE.md §3.9).

## 6. Local mode

```bash
bash scripts/datahub-local.sh preflight          # always safe
bash scripts/datahub-local.sh up --execute       # operator-gated
```

The script drives the **official quickstart at the pinned version** rather than a compose
file in this repository. DataHub's compose topology changes between releases; a hand-kept
copy would be correct the day it was written and silently wrong at the next upgrade — and a
stale compose file fails as a half-initialised metadata store, which looks like a DataHub
bug and is not one.

Preflight checks docker, the daemon, memory (≥10 GB), disk (≥20 GB) and the installed CLI
version before anything starts. A quickstart that OOMs halfway leaves exactly that
half-built store.

Binds to `127.0.0.1` only. No published port reaches a network interface and no inbound
security-group rule exists anywhere in this mode.

## 7. Production — design only

**Nothing below is provisioned.** `production.enable_datahub` is `false` and mode
`production` refuses to load without it.

| Concern | Design |
|---|---|
| Networking | private only; no `0.0.0.0/0` to GMS, frontend, Elasticsearch or the metadata store |
| Ingress | SSM Session Manager port-forward. No SSH, no key pair, no public load balancer |
| TLS | required, terminated in front of GMS and the frontend |
| Authentication | required; no anonymous read of the catalogue |
| Secrets | SSM SecureString / Secrets Manager, injected as `DATAHUB_GMS_TOKEN` at runtime; never a Terraform output in plaintext |
| Metadata store | persistent volume, not ephemeral container storage |
| Backup / restore | scheduled dump of the metadata store plus the search index rebuild path; restore rehearsed before it is relied on |
| Health checks | `GET /health` on GMS, wired to the same alerting as the rest of the platform |
| Sizing | **UNKNOWN — DRP0 open question 3.** GMS + frontend + Elasticsearch + metadata store + Kafka is the largest always-on component this project would have |
| Upgrade | pinned tag bump, CLI bumped in the same change, `datahub docker quickstart --version` in local first, then production; never a floating tag |
| Monitoring | emission success/failure counters from the client, ingestion freshness, GMS availability |
| Teardown | `enable_datahub=false` plus a destroy+verify script, like every other module (CLAUDE.md §4.12) |
| Tags | `Project`, `Environment`, `ManagedBy`, `Owner`, `CostCenter`, `AutoDestroyAfter` |

Mode `production` additionally refuses to load unless `networking: private_only`,
`tls: required`, `authentication: required` and `secret_source: ssm_securestring`. A metadata
plane holds the map of where every regulated column lives, so it is treated at the
classification of the most sensitive thing it describes.

## 8. Cost posture

CLAUDE.md §4.10 already keeps **Marquez** off as "a 24/7 service; `ops.lineage_event` needs
nothing running". DataHub is heavier, so it inherits a stricter version of the same rule,
not a looser one:

- default `disabled`; `local` is on demand and stops when you stop it
- `production` is flag-gated, default false, blocked on a sizing measurement
- **offline-first**: `FileTransport` writes valid metadata with nothing running, so DRP4–DRP7
  progress at $0 rather than waiting on an always-on service

## 9. What DRP2 did not do

> **Updated 2026-09-30.** The first two rows are now done; see
> `docs/DATAHUB_OPERATIONS_RUNBOOK.md` §4 and
> `artifacts/validation/data-reliability/drp2-drp6-live-evidence.json`.

| Not done | Owner |
|---|---|
| ~~a live DataHub was never started~~ | **DONE 2026-09-30** — smoke test 9/9 against v1.7.0.1 |
| ~~`RestTransport` is not live-tested~~ | **DONE** — publish and read-back both exercised |
| no Terraform module for production exists | DRP2-prod, gated on open question 3 |
| no metadata is ingested yet (no Glue/dbt/Kafka recipes) | DRP4 |
| no runtime lineage is emitted | DRP3 |
