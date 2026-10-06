# ADR-066 — Safe CDC table onboarding: gated workflow, source precheck, table lifecycle

* **Status**: Accepted (implementation: Phase 5; no live onboarding performed)
* **Date**: 2026-09-10
* **Extends**: ADR-062…065 (granularity, provisioning/routing, REALTIME, EOD)
* **Evidence**: `docs/CDC_TABLE_ONBOARDING.md`, `spark/tests/test_cdc_onboarding.py`,
  `docs/CDC_TABLE_ONBOARDING_DESIGN.md` §5 (the backfill gap this formalises)

## Context

Phases 1–4 made a table's *definition* config-driven: the registry compiles, targets are
provisioned from it, and routing, windowing and closing are all derived. Onboarding a **new**
table was still a sequence of hand-performed steps with no gate between them and no record of
which had been done.

The specific hazards, each already recorded in this repo's history:

* `table.include.list` and `message.key.columns` hand-edited in two places — a lowercase
  include-list against UPPERCASE Oracle topics left the connector RUNNING with eight topics at
  offset 0 and every health check green.
* Oracle **per-table** supplemental logging is easy to forget; without it an UPDATE arrives
  with unchanged columns NULL and every before/after comparison downstream is wrong — as
  wrong data, never as an error.
* SQL Server `sp_cdc_enable_table` **succeeds even when SQL Server Agent is not running**.
* Adding a table to a running connector gives it **no snapshot at all**
  (`docs/CDC_TABLE_ONBOARDING_DESIGN.md` §5), and nothing said so at onboarding time.
* Nothing recorded whether a table had been verified, so "is this table safe to read?" had no
  answer other than asking whoever added it.

## Decision

### 1. A gated workflow, not one button

`SCAFFOLD → VALIDATE → PLAN → APPROVE → APPLY → SNAPSHOT/CATCH-UP → VALIDATE → ACTIVE`.
Every command runs read-only and **stops at the step that would mutate something**, printing
the gated command for it. The only two commands that change the world —
`provision_cdc_tables.py --execute` and `cdc-runtime.sh update-connector --execute` — are
printed, never invoked.

### 2. The source precheck reads Git, not the source (§A)

Everything that makes a table capturable is *declared* in the scripts that configure the
source, so the precheck runs with the lab torn down, needs no credential, costs nothing, and
names the file to change for every finding. Every result is stamped
`evidence_kind: "declared"`, and the live queries that confirm it are documented separately —
claiming the live check when only the declared one ran is the stub-that-lies defect this
repository has already fixed once.

### 3. The capture list is derived; removal is a separate approval (§B)

Desired capture comes from the enabled tables in the registry. `--approve-capture` **never**
implies removal, and the orchestrator refuses to print an apply command at all when the diff
contains one: adding is additive and reversible, while removing stops capture and every change
that occurs while a table is absent ages out of the source's retention window before anyone
notices.

**A reordering is not a change.** When the capture set is identical the template's own string
is kept, so the config hash does not move. A spurious hash change reads as a pending change
and buys a connector restart for nothing — and a restart rebalances tasks and re-reads offsets
on a live capture.

### 4. Onboarding modes, defaulted by inspection (§C)

`changes_only` (default) | `incremental_snapshot` | `initial_snapshot`.

The default **describes the platform rather than preferring anything**: both connectors run
`snapshot.mode: initial`, so adding a table to a running connector captures it forward only.
`incremental_snapshot` is the better answer and is **refused** by the precheck, because
neither deployed connector configures `signal.data.collection` — there is no channel to send
an `execute-snapshot` on. Defaulting to it would make every onboarding fail. The preference is
expressed as a warning on the default and a refusal on the unavailable mode, not by selecting
something the platform cannot do.

`initial_snapshot` requires clearing offsets and re-delivers **every** captured table; §C rules
that out as a normal procedure and this records why.

### 5. Targets before capture (§E)

FULL_CDC / REALTIME / EOD targets, the DQ contract and the OPS metadata are all checked before
capture is approved. A **registered-but-empty** DQ contract counts as missing: an EOD close
that cannot fail a DQ rule certifies whatever it happens to produce.

### 6. Config hashes, never configs (§D)

The hash is over the **real** values — hashing a redaction would report "unchanged" across a
password rotation — and discloses nothing, which is what makes it loggable. Redaction keeps
keys and drops values, because *which* settings changed is the point of a diff.

### 7. A dedicated lifecycle vocabulary (§G)

Eleven states, a closed transition set, evidence on every transition, and **only `ACTIVE` is
trusted**. Explicitly not Airflow task status: a task status describes one execution and
vanishes with the run; a table is CAPTURING for weeks and PAUSED across deploys, and "the
provisioning task succeeded" is not "this table is provisioned". `BACKFILLING` is skippable,
because forcing it for a mode that does not backfill would make the ledger lie.

A missing required acceptance measurement is a **FAIL, not a skip** — "we did not measure it"
and "it was fine" must not reach the same conclusion.

## Options

* **A single `onboard --yes` that runs every step.** Rejected — §1. The connector restart is
  the step that touches production capture, and it would share a minute with unreviewed
  precheck output.
* **Live precheck against the running source as the default.** Rejected as the *default*: it
  cannot run before the platform exists, which is when onboarding is planned. Documented as
  the confirming step instead.
* **Default `incremental_snapshot`, as the brief prefers.** Rejected — §4. Every onboarding
  would fail its precheck today. The preference is encoded as a refusal with a remediation.
* **Lifecycle in the OPS Iceberg tables.** Rejected for now: the question "what state is this
  table in?" must be answerable with the platform torn down, which is most of this project's
  life. A JSON file is diffable in review and costs an audit trail, not a dataset, if lost.
* **Reuse Airflow task status.** Rejected — §7, and the brief says so directly.
* **Let `--approve-capture` cover removals.** Rejected — §3.

## Consequences

* Onboarding a table is now: edit the registry, `validate`, `plan`, `provision`, `onboard
  --approve-capture`, apply the two printed commands, `smoke --results`. Seven readable steps,
  six of which mutate nothing.
* The precheck found **three shipped tables with an empty DQ contract** (`branch`, `channel`,
  `merchant`). Fixed in the registry as part of this phase; the tool that found them is the
  same one that now blocks them.
* The compiled plan is **schema version 5** (the onboarding mode).
* `artifacts/cdc/lifecycle.json` is a new Git-tracked artefact.
* **`incremental_snapshot` is unavailable until `signal.data.collection` is configured.** That
  remains the single largest onboarding gap, now surfaced by a command instead of a document.

## Cost

| item | impact |
|---|---|
| the whole workflow | **$0** — every command is local and read-only |
| the precheck | $0 — reads files already in Git |
| the lifecycle ledger | $0 — a small JSON file in the repo |
| capture apply | one connector restart (~30 s RTO, RPO 0: offsets live in Kafka, ADR-002) |
| `initial_snapshot` | a **full replay of every captured table** — the reason it is not routine |
| acceptance smoke | one REALTIME run + one EOD close + one Athena reconciliation, per onboarded table |

Immaterial against the $100/month budget of record. The only recurring cost is the smoke run,
once per onboarded table.

## Security

* **No secrets in logs (§D):** config hashes only, over real values; redaction keeps keys and
  drops values; resolver placeholders keep the provider and lose the path.
* The precheck **opens no connection** — asserted against its own source by a test.
* No new AWS resource, no IAM change, no data movement.
* The workflow cannot create a table, a target or a connector by itself; it prints commands.
* Capture **removal** requires a distinct approval, so a mis-typed table name cannot silently
  stop capture on a different one.

## Rollback

* Every command is additive or read-only; reverting the phase is deleting the new files.
* The registry's DQ additions are pure additions to a contract — reverting them re-opens the
  gap the readiness check found.
* A connector capture change is rolled back by reverting the registry and re-running the apply
  command; the old config hash is in the ledger for exactly this.
* The lifecycle ledger can be deleted and rebuilt by re-running `validate`; it records
  history, not state that anything depends on.

## Validation

`pytest spark/tests/test_cdc_onboarding.py` — **57 tests**, no Spark, no AWS: a new Oracle
table and a new SQL Server table passing every precheck; missing PK, PK/source mismatch,
missing supplemental logging, missing ARCHIVELOG, missing LogMiner grants, SQL Server CDC
disabled at database and table level, missing schema history; the capture diff for an added
and a removed table; removal refused without its own approval; an unchanged set not moving the
config hash; all three onboarding modes including the incremental refusal; redaction and hash
behaviour; the full lifecycle including the illegal DRAFT→ACTIVE jump; and every section-H
command, including the workflow advancing the ledger and a failing smoke run refusing to
activate.

**Not validated, and not claimed:** no table has been onboarded live. No connector has been
updated, no snapshot taken, and no smoke check actually executed against the platform — the
smoke command judges results it is given and says so when given none.
