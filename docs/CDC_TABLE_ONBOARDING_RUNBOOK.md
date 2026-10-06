# CDC Table Onboarding Runbook

**Contract**: ADR-066. **Code**: `cdc/lifecycle.py`, `cdc/precheck.py`,
`cdc/connector_plan.py`, `cdc/onboarding_checks.py`.
**For the happy path in 7 commands, read `CDC_TABLE_QUICKSTART.md` first.** This runbook is
what to do when a gate stops you.

---

## The workflow, and why it is not one button

```
SCAFFOLD -> VALIDATE -> PLAN -> APPROVE -> APPLY -> SNAPSHOT/CATCH-UP -> VALIDATE -> ACTIVE
```

`cdc-table-onboard.py` runs every step it can run **read-only**, then STOPS at the first step
that would mutate something, and prints the command that does it. Onboarding a table creates
a Kafka topic, three Iceberg tables, a nightly close and a standing storage cost. A hidden
fully-automatic mutation is how a lab table becomes a production dependency nobody chose.

```bash
python3 scripts/cdc-table-onboard.py --table oracle.coredb.corebank.loan
python3 scripts/cdc-table-onboard.py --table ... --approve-capture --record
```

`--record` is what advances the ledger. Without it the walk is a dry run.

## The four derived artifacts, and why their order is forced

One registry entry is the only thing a human writes. But four artifacts are **derived** from
it, and three of them live outside the repo. They are refreshed by command, never by hand —
and the order below is not a preference, it is a dependency chain:

| # | artifact | command | why it cannot move earlier |
|---|---|---|---|
| 1 | **Kafka topic** | `cdc-topics.py --observed … --script` | `auto.create.topics.enable=false`: Debezium cannot create it, and without it capture is silent |
| 2 | **connector capture list** | `cdc-connector-render.py --engine <e> --execute` then `register-connectors.sh update --execute` | must follow the topic, or the connector captures into nothing |
| 3 | **writer schema** (`schemas.json`) | `cdc-runtime.sh export-schemas --execute <bucket>` | **Apicurio has no schema until the connector produces its first record.** Genuinely impossible before capture |
| 4 | **code + compiled plan** | `cdc-deploy-code.sh` | anytime, but the plan must match the registry the ingest is asked to honour |

Skipping any of them fails **quietly** or misleadingly:

* no topic → connector `RUNNING`, task `RUNNING`, zero records, every health check green
* stale `schemas.json` → the ingest misses the topic and decodes with a fallback. Phase 8 got
  `FIELD_NOT_FOUND: No such struct field 'op' in status, id, event_count, data_collections`,
  which names a field and not the stale-artifact cause
* stale plan → the ingest silently honours the previous config

That is why `cdc-table-onboard.py` prints them in order rather than leaving it to memory.

## The lifecycle states

| state | means | how you leave it |
|---|---|---|
| `DRAFT` | registered in Git, nothing checked | `cdc-table-validate` |
| `VALIDATED` | config compiles **and** the source precheck passed | `cdc-table-plan` |
| `PLANNED` | the capture diff and target plan have been read | `cdc-table-provision` |
| `PROVISIONED` | FULL_CDC / REALTIME / EOD targets exist | connector update |
| `CAPTURING` | the connector carries the table | backfill or wait for catch-up |
| `BACKFILLING` | a snapshot is in progress | it completes |
| `VALIDATING` | catch-up checks and smoke tests are running | `cdc-table-smoke` judges |
| `ACTIVE` | accepted; **downstream may rely on it** | — |
| `PAUSED` | deliberately stopped, still registered | resume, or decommission |
| `DECOMMISSIONING` | being removed; data retained | `CDC_TABLE_DECOMMISSION_RUNBOOK.md` |
| `FAILED` | a step failed; **needs a person** | fix, then re-run the failed step |

Transitions are a closed set (`cdc/lifecycle.py::TRANSITIONS`). You cannot jump to `ACTIVE`,
which is the point: `ACTIVE` is the only state downstream is allowed to trust, so it has to
mean the checks ran.

Status is a property of the **table**, not of an Airflow run — an Airflow task is `SUCCESS`
the moment it returns and gone when the run is cleaned up:

```bash
python3 scripts/cdc-table-status.py                      # everything
python3 scripts/cdc-table-status.py --table <id> --history
```

---

## When a gate stops you

### VALIDATE says BLOCKED: "not found in the declared DDL"

The precheck is **declared-evidence, not live** (`evidence_kind: "declared"`): it reads the
DDL in `docker/source-lab/`, so it runs before the platform exists and cannot be fooled by a
source that happens to be reachable right now.

**Fix**: declare the table in `docker/source-lab/<engine>/01-init.sql` and its CDC
prerequisite in `02-enable-cdc.sql`. Do **not** work around this by hand-writing a primary
key into the registry: a wrong PK is accepted silently and scatters one logical row across
Kafka partitions (CLAUDE.md §5.1). This BLOCK is the check working.

### VALIDATE refuses a temporal column with no `encoding`

Debezium's temporal types are **custom Connect logical types**, so they arrive on the wire as
plain `int64`/`int32`. Casting one straight to `timestamp` yields **year 58609** and no error.
The compiler therefore refuses a temporal column without an explicit `encoding`. See
`cdc/rowspec.py::SOURCE_ENCODINGS` for the accepted values.

### PLAN shows a change you did not intend

Field-level diffs are marked `*** BUSINESS SEMANTICS ***` when they change what a number
means. Treat an unexpected mark as a stop: a `cutoff_policy` or `delete_policy` change alters
certified balances without altering a line of business SQL.

```bash
python3 scripts/cdc-table-plan.py --against artifacts/cdc/table-plan.json
```

### PROVISION says the table already exists

Provisioning is idempotent for **matching** definitions and refuses a mismatch rather than
altering a live table. If the difference is deliberate, that is a schema change — see
`SCHEMA_EVOLUTION_RUNBOOK.md`.

### The connector is RUNNING but the table gets no rows

**Check the topic exists first.** `auto.create.topics.enable=false`, so Debezium cannot create
one: the connector reports `RUNNING`, its task reports `RUNNING`, and it produces nothing —
with no error anywhere and every health check green.

```bash
python3 scripts/cdc-topics.py --observed topics.txt      # exits 1 if any are missing
python3 scripts/cdc-topics.py --observed topics.txt --script    # review, run on cdc-runtime
```

Then restart the connector's task so it retries the table:
`POST /connectors/<name>/tasks/0/restart`.

This is ADR-070, and it has now bitten twice for different reasons — once from a
lowercase/uppercase mismatch that left eight topics at offset 0 while everything was green,
and once in Phase 8 from a table onboarded by registry entry alone.

### The connector update refuses to run

```
refusing to run non-interactively; a human must type the confirmation phrase
```

Working as designed (`scripts/lib.sh::confirm_destructive`). Run it from a terminal. Do not
pipe a phrase into it and do not add a `--yes`: this is the only step in onboarding that
changes a running system, and one human reading one diff is the whole control.

**What it does**: `PUT /connectors/<name>/config`, in place. Tasks restart (~30s), **Kafka
offsets are retained** so there is no re-snapshot, and reverting the template and re-running
undoes it.

### SMOKE judges FAIL

`cdc-table-smoke` plans the checks and judges a results file; it does not run them. A failing
check names what to look at. The common ones:

* **row counts diverge** — capture started before provisioning; backfill from the source.
* **`dv_event_id` NULL** — rows written by a pre-Phase-2 writer. `MERGE ON dv_event_id` never
  matches NULL, so those rows are not re-runnable. They are excluded and **counted**, never
  silently skipped.
* **freshness past SLA** — the connector is behind, not the table. Check Connect first.

---

## Rollback

Onboarding is reversible at every stage before `ACTIVE`, and nothing it does touches the
legacy monolith:

1. Revert the connector template and re-run `register-connectors.sh update` — capture stops.
2. Drop the three provisioned tables (they hold only what capture produced).
3. Remove the registry entry and re-compile.

After `ACTIVE`, use `CDC_TABLE_DECOMMISSION_RUNBOOK.md` instead — by then something may be
reading it, and that is exactly what the six-step offboarding is for.
