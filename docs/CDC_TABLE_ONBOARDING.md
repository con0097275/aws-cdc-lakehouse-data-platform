# Onboarding a CDC source table — the safe workflow

```
SCAFFOLD → VALIDATE → PLAN → APPROVE → APPLY → SNAPSHOT/CATCH-UP → VALIDATE → ACTIVE
```

ADR-066. **Onboarding is not one button.** Every command below runs read-only and stops at
the step that would mutate something, printing the gated command for it. An onboarding that
proceeds end to end unattended is one where a precheck failure, a missing target and a
connector restart all happen inside the same unattended minute — and the restart is the one
that touches production capture.

## The commands

| step | command | mutates |
|---|---|---|
| SCAFFOLD | `cdc-table-new.py --source … --schema … --table … [--discover]` | nothing (prints YAML) |
| VALIDATE | `cdc-table-validate.py --table <id> [--record]` | the local ledger only |
| PLAN | `cdc-table-plan.py --table <id>` | nothing |
| PRE-PROVISION | `cdc-table-provision.py --table <id> [--show-ddl]` | nothing |
| APPROVE/APPLY | `cdc-table-onboard.py --table <id> --approve-capture` | nothing — prints the gated command |
| CATCH-UP | `cdc-table-smoke.py --table <id>` | nothing (prints the checks) |
| ACCEPT | `cdc-table-smoke.py --table <id> --results obs.json --record` | the local ledger only |
| STATUS | `cdc-table-status.py [--history]` | nothing |

The only commands that change the world are the two they *print*:
`spark/ops/provision_cdc_tables.py --execute` and `cdc-runtime.sh update-connector --execute`.

## 1. Source precheck (read-only)

The things that make a table capturable are declared in Git, in the same scripts that
configure the source — so the precheck runs with the lab **torn down**, with no AWS
credential, at zero cost, and every finding names the file to change.

| engine | checks |
|---|---|
| both | table exists in the source DDL; PK present **and matching the registry**; schema history topic on the connector |
| Oracle | `ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` **per table**; ARCHIVELOG (LogMiner has no redo without it); the six LogMiner grants |
| SQL Server | `sp_cdc_enable_db`; the table in the per-table `sp_cdc_enable_table` list; the capture role |

**It reads *declared* state, not *live* state.** Every finding carries
`evidence_kind: "declared"`. A table can have supplemental logging in Git and not in a
database somebody rebuilt by hand — confirm live with the queries in §6. Claiming the live
check when only the declared one ran is the stub-that-lies defect this repo has fixed once
already.

Two findings worth knowing on sight:

* **PK mismatch is BLOCKING.** The registry's value is copied verbatim into
  `message.key.columns`; a mismatch keys the topic by the wrong column, and keying is silent
  when it is wrong.
* **`sp_cdc_enable_table` succeeds even when SQL Server Agent is not running** — the source
  script's own header calls that risk R15 in its most dangerous form. Being in the list is
  necessary, not sufficient.

## 2. Capture plan — derived, never hand-edited

`table.include.list` and `message.key.columns` are two spellings of one fact that already
lives in the registry, and this repo has been bitten by both: a lowercase include-list
against UPPERCASE Oracle topics (connector RUNNING, eight topics at offset 0, all health
checks green), and a key-columns entry updated in one place and not the other.

```
current capture  4: COREBANK.ACCOUNT, COREBANK.BRANCH, COREBANK.CUSTOMER, COREBANK.TRANSACTION
desired capture  5: … + COREBANK.LOAN
add              COREBANK.LOAN
remove           (none)
old config hash  7d1e9cc2e77d36be…
new config hash  a44f01c3…
```

**A reordering is not a change.** When the capture *set* is identical the template's own
string is kept, so the hash does not move — a spurious hash change would look like a pending
change and buy a connector restart for nothing, and a restart rebalances tasks and re-reads
offsets on a live capture.

### Removal needs its own approval

`--approve-capture` **never** implies removal. `cdc-table-onboard.py` refuses to print an
apply command at all when the diff contains one. Adding a table is additive and reversible;
removing one stops capture, and every change that happens while it is absent ages out of the
source's retention window before anyone notices. Use `--approve-removal`, deliberately.

## 3. Existing data — the onboarding modes

| mode | what happens | available today |
|---|---|---|
| `changes_only` **(default)** | captured from the restart point forward; **existing rows never appear** | yes |
| `incremental_snapshot` | Debezium snapshots the new table while streaming continues | **no** — see below |
| `initial_snapshot` | re-snapshot on first start against empty offsets | yes, and it **re-delivers every captured table** |

The default is a **description of the platform, not a preference**: both connectors run
`snapshot.mode: initial`, which snapshots only on first start against an empty offset, so
adding a table to a running connector captures it from the restart point forward. That is
what happens today whether or not anyone chooses it.

`incremental_snapshot` is the better answer and the precheck **refuses** it: neither deployed
connector configures `signal.data.collection`, so there is no channel to send an
`execute-snapshot` signal on. Defaulting to it would make every onboarding fail instead. The
preference is expressed as a warning on the default and a refusal on the unavailable mode —
not by picking something the platform cannot do.

**Do not reset Kafka Connect offsets as a normal onboarding procedure.** `initial_snapshot`
requires it and replays everything to onboard one table. The `dv_event_id` MERGE makes that
idempotent, which makes it survivable — not routine.

If schema history for the new table is unavailable, **stop**: the precheck blocks, and
recovery is the connector-specific schema-snapshot procedure for the deployed Debezium
version, not a re-registration.

## 4. Targets before capture

`cdc-table-provision.py` checks all five before capture is approved:

```
OK   FULL_CDC target  glue_catalog.…full_cdc.cdc_oracle_coredb_corebank_loan
OK   REALTIME target  glue_catalog.…stream.rt_oracle_coredb_corebank_loan
OK   EOD target       glue_catalog.…snapshot.eod_oracle_coredb_corebank_loan
OK   DQ contract      not_null=['LOAN_ID'] freshness=60m
OK   OPS metadata     owner=… class=internal domain=core_banking
```

A table whose events arrive before its target exists gets its first window quarantined or
refused — the *safe* failure (ADR-063 §F: the ingest never creates a table), but still a
failure, on real data. A **registered-but-empty** DQ contract counts as missing: an EOD close
that cannot fail a DQ rule certifies whatever it happens to produce.

## 5. No secrets in logs

The apply step records **config hashes, not configs**. The hash is over the *real* values —
hashing a redaction would report "unchanged" across a password rotation — and a hash
discloses nothing, which is what makes it the right thing to log.

The deployed templates resolve the password through `${file:…}` (FileConfigProvider), so the
literal never exists in the POSTed config, the `connect-configs` topic, or
`GET /connectors/<name>/config`. Redaction still runs: that property belongs to the *current*
template, and a redactor only correct until someone edits it is not a control.

## 6. Catch-up and acceptance

`cdc-table-smoke.py` prints the checks and, given a results file, judges them. It does not
run them — each needs the platform up, and a local command that pretended to would be a stub.

| check | required |
|---|---|
| connector and all tasks RUNNING | yes |
| snapshot complete (or none requested) | yes |
| topic carries records | yes |
| FULL_CDC received rows | yes |
| lag within the table's freshness SLA | yes |
| REALTIME produced rows | only if realtime enabled |
| EOD status is **CERTIFIED** | only if eod enabled |
| independent Athena reconciliation | yes |

**A missing required measurement is a FAIL, not a skip.** "We did not measure it" and "it was
fine" must not reach the same conclusion — that is the whole difference between an acceptance
gate and a formality. An EOD that *built but did not certify* is not acceptance evidence.

## 7. Lifecycle

```
DRAFT → VALIDATED → PLANNED → PROVISIONED → CAPTURING → BACKFILLING → VALIDATING → ACTIVE
                                                      ↘ (skipped for changes_only) ↗
        PAUSED · DECOMMISSIONING · FAILED
```

**Only `ACTIVE` is trusted.** Downstream must not read a table in any other state as
certified.

This is a **dedicated vocabulary, not Airflow's** (§G). An Airflow task status describes one
execution of one task: it is SUCCESS the moment the task returns and gone when the run is
cleaned up. A table is CAPTURING for weeks and PAUSED across deploys, and "the provisioning
task succeeded" is not the same claim as "this table is provisioned". Overloading one onto
the other is how a table that was never activated comes to look active because a retry
passed.

Transitions are a **closed set** — an onboarding that can jump DRAFT → ACTIVE is not a
workflow — and every transition records its evidence: config version, precheck result,
config hashes, smoke results. `FAILED` rejoins at the step that failed, never at `ACTIVE`.

The ledger is `artifacts/cdc/lifecycle.json`: readable with the platform torn down, diffable
in review, and needing no AWS to answer the question an operator asks first. It records the
onboarding of a table, not the data — losing it costs an audit trail, not a dataset.

## 8. Live confirmation (what the precheck cannot see)

```sql
-- Oracle: supplemental logging really on the table
SELECT log_group_type FROM all_log_groups WHERE owner='COREBANK' AND table_name='LOAN';
-- Oracle: ARCHIVELOG really on
SELECT log_mode FROM v$database;
-- SQL Server: the capture instance really exists, and the Agent job really runs
EXEC sys.sp_cdc_help_change_data_capture @source_schema='dbo', @source_name='payment';
SELECT * FROM msdb.dbo.sysjobs WHERE name LIKE 'cdc.%';
```
