# CDC Table Quickstart — onboarding a table without writing code

**Audience**: a data engineer who has been asked to land a new source table in the lakehouse.
**Time**: about 30 minutes of your attention, spread across gates that wait for you.
**Prerequisite**: the table exists in the source and its CDC prerequisites are met (§1).

> The claim this platform makes is narrow and testable: onboarding a table requires **one
> registry entry and no new code**. No Spark job, no Airflow DAG, no `CREATE TABLE`, no EOD
> script. `oracle.coredb.corebank.loan` was onboarded exactly this way as the Phase 8
> acceptance test (ADR-069); every command below is the one that was actually run.

---

## 0. The shape of it

```
  cdc/registry/sources.yaml          <- you add ~6 lines here.  This is the whole change.
             |
             |  cdc-table-validate           compile + read-only source precheck
             |  cdc-table-plan               show the diff, mutate nothing
             |  cdc-table-provision          create the 3 Iceberg tables
             |  cdc-topics                   the topic must exist BEFORE capture
             |  cdc-connector-render         render the connector capture list
             |  cdc-runtime export-schemas   only possible AFTER capture
             |  cdc-deploy-code              ship code + the compiled plan
             |  cdc-table-smoke              the acceptance checks
             v
  FULL_CDC  ->  REALTIME  (rolling window)
            ->  EOD       (certified daily snapshot)
```

FULL_CDC is canonical. REALTIME and EOD are **siblings** derived from it, never a chain —
see `reporting/layers.yaml`, which is the only file naming a physical Glue database.

---

## 1. Source prerequisites (the part the platform cannot do for you)

The platform never modifies your source. It **reads** the declared DDL and refuses to guess.

| engine | what must be true |
|---|---|
| Oracle | database in `ARCHIVELOG`; `ALTER TABLE <t> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` |
| SQL Server | SQL Server Agent running; `sys.sp_cdc_enable_table` for the table |

Declare the table in the lab DDL (`docker/source-lab/<engine>/`) so the precheck can read it.
This is deliberate: `cdc-table-validate` **BLOCKS** a table it cannot find a declaration for,
rather than assuming a primary key. A wrong primary key is worse than no primary key — it is
accepted silently and scatters one logical row across Kafka partitions (CLAUDE.md §5.1).

## 2. Scaffold the entry

```bash
python3 scripts/cdc-table-new.py --source oracle --schema corebank --table loan --discover
```

Prints YAML to stdout and **never edits the registry** — you paste it, read it, commit it.
`--discover` reads the DDL in Git; without it you get `<PRIMARY_KEY_COLUMN>` to fill in.

The result is the entire change:

```yaml
      - table: loan
        primary_key: [LOAN_ID]
        dq: {not_null: [LOAN_ID, CUSTOMER_ID]}
```

Everything else — target names, topic name, partition spec, window, cutoff, retention,
compaction target — is **derived or inherited**. See `CDC_TABLE_CONFIG_REFERENCE.md` for
every key and its precedence (`defaults` < `sources[].defaults` < `sources[].tables[]`).

## 3. Validate — no AWS, no connector, no mutation

```bash
python3 scripts/cdc-table-validate.py --table oracle.coredb.corebank.loan
```

Compiles the registry and runs the read-only source precheck. Every check reads a file
already in Git, which is what lets you run this before the platform exists.

## 4. Plan — read the diff

```bash
python3 scripts/cdc-table-plan.py --table oracle.coredb.corebank.loan
```

Turns the three silent onboarding failures — wrong topic casing, a missing
`message.key.columns` entry, a table captured but never ingested — into a diff you read.
Field-level changes are marked `*** BUSINESS SEMANTICS ***` when they alter what a number
means, so a retention tweak and a cutoff change do not look alike.

## 5. Provision the targets — **before** capture

```bash
python3 scripts/cdc-table-provision.py --table oracle.coredb.corebank.loan --show-ddl
# then the gated command it prints, which submits the generic provisioning job to EMR
```

Creates FULL_CDC, REALTIME and EOD. Order matters: **provision before capture**, because the
ingest **never creates a table** (ADR-063 §F). An ingest that auto-creates brings an unowned,
unclassified, unretained data product into existence, and the router refuses instead —
naming the registration step, not offering to fix itself.

## 6. Create the topic — **before** capture

```bash
python3 scripts/cdc-topics.py --observed <(kafka-topics --list ...)          # what is missing
python3 scripts/cdc-topics.py --observed topics.txt --script > create.sh     # review, then run
```

**Do not skip this.** `auto.create.topics.enable=false`, so Debezium **cannot** create the
topic it needs. Without it the connector reports `RUNNING`, its task reports `RUNNING`, and it
produces **nothing** for your table — no error anywhere. Phase 8 lost a live onboarding leg to
exactly this (ADR-070).

## 7. Capture — the one step that needs a human

```bash
python3 scripts/cdc-connector-render.py --engine oracle          # render, then review the diff
bash scripts/register-connectors.sh update --execute             # a human types the phrase
```

An in-place `PUT /connectors/<name>/config`: tasks restart (~30s) and **Kafka offsets are
retained**, so no re-snapshot. `confirm_destructive()` refuses to run non-interactively —
this gate is the point, not an obstacle to route around.

## 8. Export the writer schema — only possible **now**

```bash
# make at least one change in the source first, then:
bash scripts/cdc-runtime.sh export-schemas --execute <lake-bucket>
bash scripts/cdc-deploy-code.sh                  # ship code + the compiled plan together
```

Apicurio holds no schema for the table until the connector produces its first record, so this
genuinely cannot happen earlier. Skip it and the ingest looks your topic up in a
`schemas.json` that predates the table, misses, and decodes with a fallback — Phase 8 got
`FIELD_NOT_FOUND: No such struct field 'op'`, which names a field and not the real cause.

## 9. Smoke, then ACTIVE

```bash
python3 scripts/cdc-table-smoke.py   --table oracle.coredb.corebank.loan
python3 scripts/cdc-table-status.py  --table oracle.coredb.corebank.loan --history
```

`cdc-table-smoke` **plans** the checks and **judges** a results file; it does not run them,
because every one needs the live platform and a local command that pretended to have run
them would be the stub-that-lies defect this repo has already fixed once.

---

## What you did NOT do

| you did not write | because |
|---|---|
| a Spark job | `spark/jobs/full_cdc/per_table.py` is generic; the table is a parameter |
| an Airflow DAG | `airflow/dags/cdc_table_platform.py` dynamic-maps its tasks over the compiled plan, so your entry becomes a task (ADR-071) |
| `CREATE TABLE` | `cdc/provision.py` generates DDL from the entry |
| EOD code | `spark/jobs/eod/eod_engine.py` takes `table_id` + `COB_DATE` |
| a DQ rule engine | `dq.not_null` is declarative |
| a reconciliation query | the 9-check gate is generic |

## Where to go next

* `CDC_TABLE_ONBOARDING_RUNBOOK.md` — the gated workflow in full, and what to do at each stop
* `CDC_TABLE_CONFIG_REFERENCE.md` — every config key
* `REALTIME_WINDOW_RUNBOOK.md` / `EOD_SNAPSHOT_RUNBOOK.md` — operating the two derived layers
* `SCHEMA_EVOLUTION_RUNBOOK.md` — when the source changes shape
* `ICEBERG_MAINTENANCE_RUNBOOK.md` — compaction, expiry, orphans
* `docs/adr/ADR-071-…` — how the DAGs turn your entry into scheduled tasks
* `FULL_CDC_PER_TABLE.md` — the canonical layer's row contract and partitioning
* `CDC_TABLE_MIGRATION.md` — moving an existing table off the legacy monolith
* `CDC_TABLE_DECOMMISSION_RUNBOOK.md` — taking a table out again
