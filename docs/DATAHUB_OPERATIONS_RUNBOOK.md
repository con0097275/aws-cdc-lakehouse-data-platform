# DATAHUB — OPERATIONS RUNBOOK

- Phase: **DRP2**. Everything marked **live** below has been run; everything marked
  **operator-gated** has not, and the exact command is given instead of a claim.
- Config: `governance/registry/metadata_plane.yaml` · Script: `scripts/datahub-local.sh`
- Related: `DATAHUB_ARCHITECTURE.md`, `DATAHUB_SECURITY.md`

---

## 1. Where am I?

```bash
python3 -c "import sys; sys.path.insert(0,'.'); \
  from cdc.metadata_plane import load; import json; \
  print(json.dumps(load().payload(), indent=2))"
```

```json
{
  "mode": "disabled",
  "platform": "datahub",
  "environment": {"name": "dev", "fabric": "DEV", "namespace": "cdc-lakehouse-dev"},
  "outage_default": "BEST_EFFORT",
  "client": {"timeout_seconds": 10.0, "max_attempts": 3, "dry_run": true,
             "worst_case_seconds": 31.5},
  "server_image_tag": "v1.7.0.1",
  "cli": "acryl-datahub==1.7.0.13"
}
```

`mode: disabled` is the expected steady state in this lab. It is not a fault.

---

## 2. Start the local plane

**Preflight first — it never starts anything.**

```bash
bash scripts/datahub-local.sh preflight
```

It checks docker, the daemon, memory (≥10 GB), free disk (≥20 GB) and whether the pinned CLI
is installed. A quickstart that runs out of memory halfway leaves a half-initialised
metadata store that looks like a DataHub bug and is not one.

Measured on this workstation (2026-09-30): 19.4 GB RAM, 113 GB free — **sufficient**. The
CLI was reported not installed, which is what `up` installs.

**Then, operator-gated:**

```bash
bash scripts/datahub-local.sh up --execute
```

Without `--execute` it prints what it would do and exits. That is deliberate: `up` pulls
~2.7 GB of images and holds ~10 GB of RAM for as long as it runs. Not billable, but not
something that should happen because a script was run with no arguments —
`docs/APPROVAL_GATES.md` reasoning, applied to the workstation.

**Point the client at it:**

```bash
export DATAHUB_GMS_URL=http://localhost:8080
export DATAHUB_GMS_TOKEN="$(…)"     # never committed; see DATAHUB_SECURITY.md
```

| URL | What |
|---|---|
| `http://localhost:9002` | frontend |
| `http://localhost:8080` | GMS API |
| `http://localhost:8080/health` | health probe |

Both bind to `127.0.0.1`.

---

## 3. Status, stop, remove

```bash
bash scripts/datahub-local.sh status              # containers + GMS health
bash scripts/datahub-local.sh down    --execute   # stop; KEEPS the metadata store
bash scripts/datahub-local.sh nuke    --execute   # DELETES containers and volumes
```

`nuke` requires a typed confirmation. No business data lives in DataHub — but the catalogue
does, and re-ingesting it is not free in time.

---

## 4. The smoke test

**What has run (live, offline):** the full smoke shape against a recording transport, in
`spark/tests/test_drp2_datahub.py::TestSmokeTest` —

- a synthetic dataset with `datasetProperties`, `ownership`, `domains`, `globalTags`,
  `glossaryTerms`
- a `dataFlow` and a `dataJob`
- one lineage edge (`upstreamLineage`) plus `dataJobInputOutput`
- retrieval: the emitted aspects are read back and asserted on by content

**What HAS now run (2026-09-30, live GMS `v1.7.0.1`):** the same shape against a real
server — **9 of 9 aspects published on the first attempt**, all six dataset aspects read
back, and the upstream URN matching what was sent. Evidence:
`artifacts/validation/data-reliability/drp2-drp6-live-evidence.json`.

The live run found **three defects the recording transport could not see**, because a
recording transport accepts any dict and DataHub validates against its PDL schema
server-side:

| | Defect | Symptom |
|---|---|---|
| L1 | `RestTransport` discarded the HTTP error body | `HTTP 422` naming none of nine proposals |
| L2 | `glossaryTerms` missing its required `auditStamp` | `/auditStamp :: field is required but not found and has no default value` |
| L3 | `dataJobInfo.type` sent as a string, not a union | `/type :: union type is not backed by a DataMap or null` |

All three are fixed and regression-tested (`TestLiveSchemaDefects`). The offline smoke test
is a rehearsal; **this** is the evidence.

To repeat it:

```bash
export DATAHUB_GMS_URL=http://localhost:8080
python3 - <<'PY'
import sys; sys.path.insert(0,'.')
from cdc.metadata_plane import load
from cdc.datahub_client import DataHubClient
from cdc.assets import AssetId

cfg = load(mode="local")
cfg = cfg.__class__(**{**cfg.__dict__, "client": cfg.client.__class__(dry_run=False)})
c = DataHubClient.build(cfg)
up   = c.minter.dataset_urn(AssetId.parse("full_cdc:oracle_coredb_corebank_account"))
down = c.minter.dataset_urn(AssetId.parse("eod:oracle_coredb_corebank_account"))
print(c.health())
print(c.publish("metadata_ingestion", [
    c.dataset_properties(down, name="eod account", description="smoke"),
    c.ownership(down, ["banking-data"]),
    c.domain(down, "core_banking"),
    c.tags(down, ["regulated"]),
    c.upstream_lineage(down, [up]),
    c.dataflow_info("eod_build"),
    c.datajob_info("eod_build", "close_account"),
    c.datajob_io("eod_build", "close_account", inputs=[up], outputs=[down]),
]).payload())
PY
```

Then verify in the UI: search for `eod_oracle_coredb_corebank_account`, open **Lineage**,
confirm the upstream edge and the owning group.

---

## 5. The offline sink — working with nothing running

ADR-087 constraint 4. This is how DRP4–DRP7 make progress at $0.

```python
from pathlib import Path
from cdc.metadata_plane import load
from cdc.datahub_client import DataHubClient

c = DataHubClient.build(load(mode="local"),
                        file_sink=Path("artifacts/metadata/mcps.ndjson"))
c.publish("metadata_ingestion", [...])       # newline-delimited MCPs on disk
```

Each line is a complete metadata change proposal, JSON, schema-checkable in CI and loadable
into a GMS later. A test asserts the file round-trips.

---

## 6. When the plane is unreachable

| Flow | Policy | What happens |
|---|---|---|
| any data flow (`eod_build`, `realtime_materialize`, `dbt_spark_build`, …) | `BEST_EFFORT` | 3 bounded attempts, then record and **continue**. `result.degraded` is true and the `degraded` counter increments. The job succeeds. |
| `metadata_ingestion`, `lineage_impact` | `REQUIRED` | 3 bounded attempts, then `MetadataPublishError`. The task fails. |

Worst case a caller pays when the plane is completely down: **31.5 s**
(3 × 10 s timeout + 0.5 s + 1.0 s backoff).

**Do not "fix" a degraded publish by failing the pipeline.** The business rows are already
committed and correct. The catalogue is stale and the counter says so; that is the designed
outcome, not a defect.

Counters:

```python
c.metrics_payload()
# {'attempts': 1, 'degraded': 0, 'failed': 0, 'published': 8, 'skipped': 0,
#  'transport_error': 0}
```

---

## 7. Health

```python
DataHubClient.build(load()).health()
# {'mode': 'disabled', 'reachable': None, 'detail': 'metadata plane is disabled; …'}
```

`reachable: None` means **NOT CHECKED**, never `False`. A health check that reports "down"
when it never looked is the same defect as a DQ check reporting PASS on an empty table.

---

## 8. Upgrading

1. Resolve the new versions — do not choose them:
   ```bash
   curl -sS https://pypi.org/pypi/acryl-datahub/json | python3 -c \
     "import sys,json;print(json.load(sys.stdin)['info']['version'])"
   curl -sS 'https://hub.docker.com/v2/repositories/acryldata/datahub-gms/tags?page_size=40&name=v1.' \
     | python3 -c "import sys,json;[print(t['name'],t['last_updated'][:10]) for t in json.load(sys.stdin)['results']]"
   ```
2. Check they share a `major.minor.patch` line. The loader refuses the pair if not.
3. Record the image **digests**, not just the tag.
4. Update `governance/registry/metadata_plane.yaml`, run the tests, then `local` first.
5. Never a floating tag. `latest`, `nightly`, `head` and `quickstart` are refused.

---

## 9. Troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| `REFUSING: versions.cli.version is empty` | the config lost a pin | restore it; an unpinned plane changes under a lineage graph with nothing recording it |
| `… are not on the same release line` | CLI and server drifted a minor apart | pin both to one line; a newer CLI writes aspects an older server cannot read |
| `refusing to emit … fabric PROD is not this emitter's DEV` | a prod URN in a dev process | working as intended — find who minted it |
| `flow 'metadata_ingestion' is REQUIRED but the metadata plane is disabled` | ingestion run with the plane off | start the plane, or do not run the flow |
| `a credential may not appear in this file` | a token was pasted into the config | move it to `DATAHUB_GMS_TOKEN` / SSM |
| quickstart hangs or dies part-way | memory | `preflight` first; ≥10 GB, and `nuke` before retrying |
| publish silently does nothing | mode is `disabled` | expected; `skipped_reason` says so and `published` stays 0 |

---

## 10. What this runbook does not cover yet

| | Owner |
|---|---|
| ingestion recipes (Glue, dbt, Kafka, Athena, Power BI) | DRP4 |
| runtime lineage from Airflow and Spark | DRP3 |
| production deployment, backup/restore rehearsal, credential rotation | DRP2-prod, gated on DRP0 open question 3 |
