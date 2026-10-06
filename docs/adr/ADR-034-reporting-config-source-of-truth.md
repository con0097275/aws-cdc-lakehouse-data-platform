# ADR-034 — Reporting config source of truth

- Status: **ACCEPTED** (Session 22)
- Related: ADR-035, ADR-036, `docs/REPORTING_REFERENCE_EVIDENCE.md` §12 F2

## Context

The framework needs a home for `job_master`, `job_flow_config`, `job_dependency` and
`resource_profile`. Three references answered this differently, and one of them answered
it twice.

- **Repo 1 (ACB), first generation** — config in Impala/Kudu tables
  (`src/c2pp/init/data_lake_init.sql:20-128`). Loading it into Airflow needed a
  database→JSON cache with a TTL and an Airflow Variable to switch between them
  (`plugins/job_scheduler/functions.py:28-203`). Populating it ended in
  `fulfill_job_master(file_path, date_of_data, sheet_name)` — **reading job master rows
  from an Excel worksheet** (`functions.py:790`, `configs/Danh_sach_bang_TEST_DWH1.xlsx`).
- **Repo 1, second generation** (`src/c2pp/`) — one YAML per report in Git, dynamic DAG
  generation. The same team moved away from their own database-as-truth design.
- **Repo 3** — `configs/bcn_pipeline.yaml`, one file, header stating "everything
  adjustable is here; you never edit the DAG file".
- **Repo 5 (TTC)** — DynamoDB `-dbt-job` rows with **no Git representation**
  (`IaC/modules/data_platform/ochestration.tf:213`); `run_dbt_job.py` reads
  `job["modelFqn"]` from a row a human maintains by hand.

## Options

| Option | Verdict |
|---|---|
| **Hybrid: Git YAML is truth, compiled to a plan, mirrored to `ops` for query** | **CHOSEN** |
| Config tables as the direct source (repo 1 gen 1, repo 5) | Rejected |
| Git YAML read directly by Airflow at parse time | Rejected |

## Decision

**Git is the source of truth. Nothing else is editable.**

```
reporting/jobs/*.yaml          authored, reviewed, versioned
reporting/layers.yaml
reporting/profiles/resource_profiles.yaml
        │
        │  make reporting-compile
        │  validate → resolve graph → detect cycles → assign turns
        │  → merge dbt manifest deps → emit plan.json + sha256
        ▼
s3://<lake>/ops/config/<config_version>/plan.json     immutable, versioned
        │
        ├──► local cache read by Airflow at DAG-parse time
        └──► ops.job_master / job_flow_config / job_dependency
             Iceberg, partitioned by config_version, APPEND-ONLY
```

Three consumers, three shapes, one origin:

1. **`plan.json`** — what the coordinator and the DAGs read. A single S3 object per
   config version, immutable once written.
2. **Local cache** — Airflow's DAG files read a file on local disk, never the warehouse.
   Repo 1 built exactly this (`functions.py:42-95`) because Airflow re-parses DAG files
   continuously. Here the argument is stronger: a parse-time Athena query is billed, and
   `envs/dev/terraform.tfvars` sets a 10 GiB bytes-scanned cutoff precisely to catch
   unbounded query patterns.
3. **`ops.*` Iceberg tables** — the Athena-queryable projection, written once per config
   version. Partitioned by `config_version` and append-only, so config history is
   queryable and joinable with execution history without ever being mutated.

The compiled `plan.json` carries a `sha256` of the authored YAML set. A coordinator run
records the `config_version` it executed under, so "what config produced this number" is
answerable from the execution row alone.

## Consequences

- Shipping a mart is a pull request. There is no production database edit in the normal
  path, and no worksheet.
- Rollback is `git revert` + recompile, not a manual UPDATE.
- A config error fails at compile time, in CI, before any AWS resource is touched.
- The `ops.job_master` Iceberg table is **read-only by contract**. A test asserts no code
  path writes it outside the compiler.
- Emergency operational overrides (disable a job at 02:00) still need a path — see
  ADR-039: they are Airflow Variables consulted by the coordinator, never edits to config.

## Cost

`plan.json` is a few hundred KB. The `ops.*` config tables gain one small snapshot per
config version — a handful per month. Effectively zero.

## Security

Config in Git means config is reviewed. No secret may appear in `reporting/**`; a test
greps for the same patterns `scripts/validate-docs.py` already enforces.

## Rollback

Recompile from an earlier commit; publish the earlier `config_version`. The previous
`plan.json` objects are retained (S3 versioning is already on for the lake bucket).

## Validation

- `make reporting-compile` is deterministic: same input → identical `plan.json` bytes.
- A test asserts no module under `airflow/dags/` imports Athena, boto3 Glue, or a Spark
  session at module scope.
- A test asserts `plan.json`'s recorded sha256 matches the authored YAML.
- A test asserts every `ops.job_master` row carries a `config_version` present in S3.
