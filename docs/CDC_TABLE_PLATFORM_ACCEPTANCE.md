# Phase 8 — Final Acceptance Checklist

**Token**: `CONFIG_DRIVEN_CDC_TABLE_PLATFORM_PRODUCTION_READY`
**Date**: 2026-09-10 · account 111122223333 · `ap-southeast-1` · profile `my-aws-profile` · env `dev`
**ADR**: [ADR-069](adr/ADR-069-zero-custom-code-table-acceptance.md)

Evidence classes follow CLAUDE.md §9.7: `static-validated` · `planned` · `deployed` ·
`live-tested`. Nothing below is claimed at a higher class than it was verified at.

| # | Requirement | Status | Evidence |
|---|---|---|---|
| 1 | A new table is onboarded by **config only** | **PASS** `live-tested` | `oracle.coredb.corebank.loan` = 3 lines of YAML; `sqlserver.digital.dbo.payment_method` = one entry with a typed payload |
| 2 | **No new custom Spark Python** | **PASS** `static-validated` | `grep -rIl -iE "\bloan\b\|payment_method" --include=*.py` outside `docs/` and `*/tests/` returns only CLI docstring examples and two unrelated pre-existing AI-agent hits |
| 3 | **No new custom Airflow DAG** | **PASS** `static-validated` | `airflow/dags/cdc_table_platform.py` — three generic DAGs whose task list is **dynamic-mapped over the compiled plan**. Adding one registry entry took REALTIME tasks 7 → 8 and EOD 10 → 11 with the DAG file untouched. `test_no_per_table_dag_exists` fails if an `<table>_cdc_dag.py` ever appears (ADR-071) |
| 4 | **No hand-created Iceberg SQL** | **PASS** `live-tested` | all six targets created by `spark/ops/provision_cdc_tables.py` from generated DDL (runs `00g8lp0dlt18h827`, `00g8lp3ahvming27`) |
| 5 | **No custom EOD code** | **PASS** `live-tested` | `eod_engine.py` closed ACCOUNT with `--table` + `--cob-date` only |
| 6 | Source prerequisites are the operator's, and are **read** not assumed | **PASS** `live-tested` | precheck **BLOCKED** `loan` until declared in Git DDL, then 0 blocked |
| 6b | The full path runs **end to end** for a table onboarded by config alone | **PASS** `live-tested` | source → Debezium → Kafka → FULL_CDC → REALTIME, on **both** engines. See "The live capture leg" below |
| 7 | Target names are **derived**, never typed | **PASS** `static-validated` | `cdc_/rt_/eod_ + <source>_<db>_<schema>_<table>`; topic `cdc.oracle.COREBANK.LOAN` |
| 8 | Governance propagates from the entry | **PASS** `live-tested` | `cdc-maintenance.py governance`: 10 tables, **0 gaps**; `payment_method` derives `PII=True` from `confidential` |
| 9 | A **config-only change** shows as a plan diff | **PASS** `static-validated` | lookback 3→5, grace 2→3, EOD schedule, hot→warm, with `*** BUSINESS SEMANTICS ***` marking |
| 10 | EOD close is **certified against an independent count** | **PASS** `live-tested` | **320 rows / 320 distinct keys**, matching an Athena expectation of **320** computed from FULL_CDC below the same `cutoff_utc` — not read back from the writer |
| 11 | Re-closing a date **converges** (replace, not append) | **PASS** `live-tested` | second close of COB 2026-08-21 → still 320 rows in **one** partition |
| 11b | **Late events** are handled | **PASS** `live-tested` | `payment_method` 9003 updated *after* its COB certified. FULL_CDC 5 → 6 events while the certified snapshot correctly kept the old value; `--fulfill` on the same date moved it `CARD` → `CARD_REISSUED`, row count unchanged at 2, and `max_source_commit_ts` advanced 13:41:35 → 15:04:44 — the evidence the close reached further into the source |
| 12 | Certification is a **gate**, not a label | **PASS** `live-tested` | the first close passed both gates but could not write its ledger row; now `STATUS_UNVERIFIED`, exit non-zero, data retained |
| 13 | The run ledger records cutoff, counts and outcomes | **PASS** `live-tested` | `ops.eod_run`: `cutoff_utc 2026-08-22 00:00:00`, 320/320/0, `PASS`/`PASS`, `CERTIFIED` |
| 14 | A **second source type** works on the same path | **PASS** `live-tested` | SQL Server `payment_method`: captured live through Debezium → Kafka → FULL_CDC → REALTIME, **5 rows**, hex-LSN ordering |
| 14b | The **typed payload** path is real, not just accepted | **PASS** `live-tested` | the provisioned Iceberg schema carries a real struct — `created_at` is **`timestamptz`**, so the declared `micro_timestamp` encoding reached the generated DDL rather than landing as a raw `int64`; `payload_after_json` is kept alongside it so an undeclared field is never lost |
| 14c | Classification propagates into the physical table | **PASS** `live-tested` | `cdc.classification=confidential` in the Iceberg properties, and every `payload_*` column has `write.metadata.metrics=none` so PII never enters Iceberg column stats while identity columns keep `full` |
| 15 | Maintenance is metric-driven and **not a no-op** | **PASS** `live-tested` | `transaction` (40 files / 13 manifests / 53 snapshots) draws all three actions; `account` (2 files) draws none |
| 16 | Eight named runbooks exist | **PASS** `static-validated` | see below; `validate-docs.py` 14/14 |
| 17 | A final architecture diagram exists | **PASS** `static-validated` | `CDC_TABLE_PLATFORM_ARCHITECTURE.md`; mermaid parses under `validate-docs.py` |
| 18 | The full test suite is green | **PASS** `static-validated` | `make check-all` — the full **verification surface**, not pytest alone: `lint-shell`, `validate-docs` (14/14), `cdc-check`, `cdc-verify`, plus the suite (**2,283 passed, 0 failed**). Also clean: `terraform fmt -check -recursive`, `terraform validate`, `compileall` over `scripts/ cdc/ spark/ airflow/ ai/`, and all ten read-only CDC CLIs |

## The eight runbooks

| doc | covers |
|---|---|
| `CDC_TABLE_QUICKSTART.md` | onboarding in 7 commands — **start here** |
| `CDC_TABLE_CONFIG_REFERENCE.md` | every config key and its precedence |
| `CDC_TABLE_ONBOARDING_RUNBOOK.md` | the gated workflow; what to do at each stop |
| `REALTIME_WINDOW_RUNBOOK.md` | lookback / grace / retention, the two boundary kinds |
| `EOD_SNAPSHOT_RUNBOOK.md` | cutoff, per-engine ordering, delete policy, certification |
| `SCHEMA_EVOLUTION_RUNBOOK.md` | ALLOW / PLAN / BLOCK, and the alias-folding subtlety |
| `CDC_TABLE_DECOMMISSION_RUNBOOK.md` | the six ordered offboarding steps |
| `ICEBERG_MAINTENANCE_RUNBOOK.md` | metric-driven actions, ordering, retention guard |

## What the acceptance found

Three defects, each of which had been passing tests:

1. **A certification with no evidence.** A close printed `CERTIFIED`, `certified=1`, exit 0,
   with its ledger write failed. The data was correct; the *claim* was unverifiable.
2. **The root cause** — `--table` provisioning silently skips the shared OPS targets, so they
   had never been created in six phases.
3. **Maintenance was a platform-wide no-op** — the plan serialised "unset" as `[]`, which the
   runtime reads as "permit nothing".

Plus a test-hygiene defect: onboarding `loan` broke eleven fixtures that used `loan` as "a
table that does not exist", and **four still passed while asserting the opposite of their
intent**.

## The orchestration gap — closed

Point 3 originally passed for the wrong reason: **no DAG orchestrated the per-table platform
at all**, so "no custom DAG is needed" was satisfied by absence rather than by genericity.
That was recorded as open issue #7 rather than quietly claimed, and is now closed by ADR-071.

`airflow/dags/cdc_table_platform.py` adds three generic DAGs — `cdc_realtime`, `cdc_eod`,
`cdc_maintenance` — whose tasks are produced by **dynamic task mapping over the compiled
plan**. Measured: one added registry entry moved REALTIME from 7 to 8 tasks and EOD from 10 to
11, with no edit to the DAG file. The mapping reads each table's own `realtime_policy.enabled`,
so the three near-static reference tables get **no task** rather than a scheduled no-op.

Three properties are enforced by test rather than by intent:

* **no per-table DAG** — every registry table name is checked against every DAG filename;
* **no literal table list** — no registry table name may appear in the DAG's executable body,
  because such a list would drift from the registry exactly as the connector capture list and
  the Kafka topic list both did (ADR-070);
* **EOD passes no `--cob-date`** — the date is derived inside the engine from each table's own
  business timezone, so the *scheduler's* timezone can never decide a business date.

The DAGs deliberately do **not** ingest (that is a long-running streaming app), provision
(auto-creating targets on a timer is how unowned data products appear), or touch connectors
(that gate needs a human). All three are off by default and paused on creation.

**Not live-tested**: they have never run on the deployed Airflow. What is proven is that they
parse, pass all 50 DAG policy tests, and derive their task list from the registry.

## The live capture leg — closed

The connector update was applied by a human (the gate was not bypassed), and the full path
now runs end to end for **both** new tables:

| | Oracle `loan` | SQL Server `payment_method` |
|---|---|---|
| source events made | 2 inserts, 1 update, 1 delete | 3 inserts, 1 update, 1 delete |
| Kafka end-offsets | 5 (incl. tombstone) | 6 (incl. tombstone) |
| FULL_CDC rows | **4** — c=2, u=1, d=1 | **5** — c=3, u=1, d=1 |
| `position_primary` | `3287944` (numeric SCN) | `0000002c:00006d50:003a` (hex LSN) |
| REALTIME rows | 4 | 5 |
| window boundary | `calendar_day` → lower bound **midnight** 2026-09-06 | `rolling_hours` → **14:01:09**, exactly 72h back |
| `dv_pk_hash` | non-NULL for every row | non-NULL for every row |

`dual_write` put the same events in the legacy monolith too (20,390 → 20,400, the last of
them the late event in 11b), and the
per-table counts match it exactly — a genuine equivalence between the two paths rather than a
restatement of one of them.

The two position formats are the reason a second engine was required: an Oracle-shaped
ranking that casts position to decimal returns NULL for the SQL Server value and collapses
silently onto `kafka_offset`, which CLAUDE.md §5.4 forbids.

## Getting there found four more defects

Every one of them fails **silently or misleadingly**, and all four are consequences of a
derived artifact being kept in sync by hand. See ADR-070.

1. **No Kafka topic.** `auto.create.topics.enable=false`, so Debezium cannot create one:
   both connectors reported `RUNNING`, both tasks reported `RUNNING`, and they produced
   **nothing**, with every health check green. The host's topic list was a hardcoded bash
   array under a comment reading *"Keep these in sync with `table.include.list`"*.
   → `scripts/cdc-topics.py` derives it from the registry and exits non-zero.
2. **Stale `schemas.json`.** Exported before the tables existed, so the ingest missed the
   topic and decoded with a fallback:
   `FIELD_NOT_FOUND: No such struct field 'op' in status, id, event_count, data_collections`
   — the transaction-metadata envelope. It names a field, not the cause. And it **cannot** be
   fixed earlier: Apicurio holds no schema until the first record exists.
3. **A shared module missing from the deploy zip.** EMR downloads only the entrypoint, so
   `import per_table` died with `ModuleNotFoundError` *after* acquiring capacity.
4. **Kafka jars absent for streaming jobs.** `Failed to find data source: kafka`, which reads
   like a typo in the format name. They now hang off the `stream` role, so an EOD close does
   not ship 18 MB it never opens.

## Both tables are ACTIVE

Both EOD closes are **CERTIFIED**, and both match an independently computed Athena
expectation:

| | input events | distinct keys | deletes | rows | vs. independent count |
|---|---|---|---|---|---|
| `loan` | 4 | 2 | 1 | **1** | 2 − 1 = 1 ✓ |
| `payment_method` | 5 | 3 | 1 | **2** | 3 − 1 = 2 ✓ |

The reconciliation identity `distinct_keys − deletes == rows` holds on both. Full recorded
lifecycle for `loan`:

```
DRAFT -> VALIDATED       cdc-table-validate
     -> PLANNED          cdc-table-provision: targets planned
     -> PROVISIONED      cdc-table-provision --confirm-applied
     -> CAPTURING        cdc-table-smoke: capture proven by observed evidence
     -> VALIDATING       cdc-table-smoke started
     -> ACTIVE           cdc-table-smoke
```

### Two more defects, in the acceptance gate itself

1. **The judge accepted anything truthy.** Every check but `eod_status` tested only
   truthiness, so `"FAILED"` passed `connector_running`, the string `"0"` passed a row count,
   `"probably"` passed a boolean, and a malformed `{"note": …}` object passed everything.
   This is the **last** gate before a table is trusted downstream. Evidence is now
   type-checked per check — `COUNT` (a positive whole number), `FLAG` (a real `True`),
   `LITERAL` (an exact string) — and a measured zero fails with a different message than
   "not measured", because they have different fixes.
2. **A genuinely capturing table could never reach ACTIVE.** `cdc-table-onboard` refuses to
   record `CAPTURING` for a table the template already lists — correctly, since a template is
   configuration and not proof. But the confirmation gate *forces* the connector update to be
   applied out of band, so the table captured live while the ledger said `PROVISIONED` and
   acceptance could not run. Smoke now advances it on the only thing that settles the
   question: observed records on the topic **and** rows in FULL_CDC, both positive. Zero of
   either leaves it at `PROVISIONED`.

## Running the full surface found two more

Both invisible to `pytest`, which had been green throughout:

1. **The committed plan artifact was two table onboardings stale.** `make cdc-verify` catches
   it — but that target was **not in `make check`**, the aggregate a developer actually runs,
   and is not in `make test` either. Every Spark job takes `--plan`, so a stale artifact
   silently runs the previous config and the run *succeeds*.
2. **The hash was never recomputed.** `cdc-verify` and the first replacement test both
   compared the artifact's *stored* hash fields, which a truncated file carries unchanged:
   dropping a table from `plan.tables` left the recorded `plan_hash` correct and passed
   everything.

Fixed both ways deliberately — `cdc-verify` is now in `make check`, **and**
`test_the_committed_plan_artifact_is_not_stale` is in the suite, recomputing the hash from
the file's own content. Either fix alone leaves the other hole: a target nobody invokes, or a
suite that trusts a number the file supplies about itself.

## What is NOT claimed

* **LOAN's EOD snapshot does not equal its source table**, and should not: 3 of its 4 live
  rows pre-date capture. `onboarding: changes_only` means history starts now, which the
  precheck states explicitly. A backfill is a separate, deliberate act.
* Both tables read from the **legacy monolith** (`dual_write`), not per-table, until someone
  runs the cutover gate. That is the safe default, not an oversight.
