# Onboarding a CDC source table — config-driven design

> **THIS IS THE PHASE 0 DESIGN. IT IS IMPLEMENTED — AND IT IS NOT THE OPERATING GUIDE.**
>
> | you want | read |
> |---|---|
> | to onboard a table | **`CDC_TABLE_QUICKSTART.md`** — 9 commands, no code |
> | what to do when a gate stops you | `CDC_TABLE_ONBOARDING_RUNBOOK.md` |
> | every config key | `CDC_TABLE_CONFIG_REFERENCE.md` |
> | why it is built this way | ADR-062 … ADR-072 (§10) |
>
> This document is kept because a design is evidence of *what was decided before the answer
> was known* — including §5, where the backfill gap was first recorded, and §9, where this
> design was right and the implementation lost it. Its YAML sketches are **not** the shipped
> schema; §2 tabulates the divergence.

Companion to `docs/CDC_TABLE_PLATFORM_TARGET.md`. Phase 0 design, 2026-09-10;
**implemented through Phase 8, re-audited 2026-09-10.**

Goal: adding a source table becomes **one registry entry plus one approval**, with every
downstream artefact rendered from it — the property ADR-034 already gives the reporting
layer, applied to capture.

---

## 1. What onboarding costs today

Audited on the live templates. Six edits across five files, in a required order, with two
failure modes that leave every health check green:

| # | edit | file | if you get it wrong |
|---|---|---|---|
| 1 | create table + enable CDC | source DB | connector RUNNING, produces nothing (risk R15) |
| 2 | per-table supplemental logging | Oracle, as sysdba | UPDATEs arrive with unchanged columns NULL — **silent** |
| 3 | topic name | `docker/cdc-runtime/create-topics.sh` | Oracle folds to UPPERCASE; lowercase topic → connector RUNNING, topic at offset 0, **silent** |
| 4 | `table.include.list` | connector template | table simply not captured |
| 5 | `message.key.columns` | **same file, different line** | same PK scatters across partitions, per-key ordering broken — **silent** |
| 6 | `--topics` | every ingest submit command | table captured but never ingested |
| — | refresh schema export | `cdc-runtime.sh export-schemas` | new `globalId` quarantined |

Steps 2, 3 and 5 have each already caused a green-but-wrong platform in this repo's
history. That is the case for a registry: not typing effort, but **three silent failure
modes that a rendered artefact cannot have.**

---

## 2. The registry

`cdc/registry/sources.yaml` — one file, schema-validated, reviewed like code.

> **The YAML below is the DESIGN SKETCH and will not compile.** Field names changed during
> implementation and the shipped schema is the authority:
> **`CDC_TABLE_CONFIG_REFERENCE.md`**, with `cdc/registry/sources.yaml` as the worked example.
>
> | sketch | shipped | why it changed |
> |---|---|---|
> | `source_system` | `source_id` + `engine` | the *instance* and its *engine type* are different facts; two Oracle sources would collide on one field |
> | `source_schema` / `source_table` | `schema` / `table` | the `source_` prefix is redundant inside a `sources:` block |
> | `topic_pattern`, `key_pattern` | `topic_prefix` | patterns invite a second spelling of a derived name — the topic is now computed, not templated (ADR-062) |
> | `pii: false` | **derived from `classification`** | open question 2, resolved deliberately: two fields meaning the same thing is how a table ends up `pii: false` while holding dates of birth |
> | `grain`, `retention_days` per table | inherited defaults | the sketch repeated in every table what `defaults:` now says once |
>
> The *shape* the sketch argues for — one reviewed file, inheritance, derived names — is what
> shipped. Only the spellings moved.

```yaml
version: 1

sources:
  - source_system: oracle
    connector: corebank-source
    source_schema: COREBANK             # UPPERCASE: Oracle folds unquoted identifiers
    topic_pattern: "cdc.oracle.{schema}.{table}"
    key_pattern:   "{schema}.{table}:{pk}"
    tables:
      - source_table: ACCOUNT
        primary_key: [ACCOUNT_ID]
        owner: my-aws-profile
        domain: core_banking            # ADR-028
        grain: "one row per account change event"
        pii: false
        retention_days: 365
        freshness_sla_minutes: 15
        supplemental_logging: ALL        # Oracle only; asserted, not assumed
        dq:
          not_null: [ACCOUNT_ID, CUSTOMER_ID]
          event_date_null_tolerance: 0
        target:
          full_cdc: cdc_events                       # Phase A: the monolith
          # full_cdc: cdc_oracle_corebank_account    # Phase B: per-table

  - source_system: sqlserver
    connector: digital-source
    database: digital                   # SQL Server includes the DB in the topic
    source_schema: dbo
    topic_pattern: "cdc.sqlserver.{database}.{schema}.{table}"
    key_pattern:   "{schema}.{table}:{pk}"
    tables:
      - source_table: app_user
        primary_key: [user_id]
        owner: my-aws-profile
        domain: digital_channel
        pii: true                       # drives IAM and masking policy
        retention_days: 180
        dq: {not_null: [user_id], event_date_null_tolerance: 0}
        target: {full_cdc: cdc_events}
```

Every field either **renders an artefact** or **satisfies a stated requirement**. Nothing is
decorative:

| field | renders / satisfies |
|---|---|
| `source_table`, `source_schema`, `topic_pattern` | topic name, `table.include.list` |
| `primary_key`, `key_pattern` | `message.key.columns` — CLAUDE.md §5.1 |
| `owner`, `grain`, `retention_days`, `freshness_sla_minutes` | CLAUDE.md §6 per-table metadata requirement |
| `pii` | IAM grant scope, masking policy, AI-plane deny list |
| `dq` | the DQ suite, including the `event_date` control |
| `supplemental_logging` | the precondition check that today is only in a healthcheck |
| `target.full_cdc` | ingest routing; the single switch for Phase A → Phase B |

---

## 3. The compiler

`cdc/compile.py`, mirroring `reporting/compile.py` (ADR-034): pure, deterministic, emits a
plan with a content hash so drift is detectable.

```
sources.yaml
    |
    +-- validate ------------> schema, PK non-empty, no duplicate (system, schema, table),
    |                          topic name matches the engine's casing rule,
    |                          retention/SLA present, owner present
    |
    +-- render --------------> topics.txt              -> create-topics.sh
                               include_list.json       -> connector table.include.list
                               key_columns.json        -> connector message.key.columns
                               ingest_topics.txt       -> --topics for batch + streaming
                               dq_rules.yaml           -> DQ suite
                               catalog_metadata.json   -> Glue table properties
                               plan_hash               -> drift detection
```

**Render, never duplicate.** The compiler is the only writer of these artefacts; the
templates carry a placeholder, not a hand-maintained list. That is what makes the step-3 and
step-5 failure modes structurally impossible rather than merely documented.

### Casing, encoded once

```python
def topic_for(src, table):
    if src.source_system == "oracle":
        # Oracle folds unquoted identifiers to UPPERCASE. Emitting the lowercase name
        # creates a topic the connector never produces to: it reports RUNNING, the topic
        # sits at offset 0, and every health check stays green.
        return f"cdc.oracle.{src.source_schema.upper()}.{table.source_table.upper()}"
    # SQL Server includes the DATABASE component, and does not fold.
    return (f"cdc.sqlserver.{src.database}.{src.source_schema}."
            f"{table.source_table}")
```

---

## 4. Onboarding workflow

```
1. add the table to sources.yaml                        (one entry)
2. python3 cdc/compile.py --validate                    read-only; fails on any defect
3. review the rendered diff                             topics, include-list, key columns
4. create the table + enable CDC in the source          gated
5. bash scripts/cdc-runtime.sh create-topics --execute  rendered topic names
6. bash scripts/register-connectors.sh register --execute
7. bash scripts/cdc-runtime.sh export-schemas --execute <bucket>
8. BACKFILL  (see section 5 -- this is the hard part)
9. ingest; assert the DQ rules
```

Steps 2 and 3 are new and are where the value is: **every silent failure mode becomes a
diff you read before anything mutates.**

### Preconditions the compiler asserts, that are checks today

* Oracle table has `ALL COLUMN LOGGING` — currently only visible in `healthcheck.sh` output
* SQL Server table has a capture instance
* `primary_key` is non-empty — today an omitted `message.key.columns` entry is silent
* topic casing matches the engine
* no duplicate `(source_system, source_schema, source_table)`

---

## 5. Backfill — the real onboarding gap

**Adding a table to a running connector gives that table no snapshot.**

Both connectors run `snapshot.mode: initial`, which snapshots only on first start against an
empty offset. On restart with an extended `table.include.list`, Debezium resumes from the
stored offset and captures the new table **from that point forward only**. Rows that existed
before the restart never appear.

Neither connector configures `signal.data.collection`, so **incremental snapshot is not
available** either.

Three options, in preference order:

| option | mechanism | cost | verdict |
|---|---|---|---|
| **A. incremental snapshot** | add `signal.data.collection`, send an `execute-snapshot` signal for the new table | one connector restart, then online | **preferred.** Debezium snapshots the new table while streaming continues. No re-snapshot of existing tables |
| **B. one-off JDBC backfill** | read the table directly, write `op='r'` rows into FULL_CDC with a synthetic position | one EMR job | acceptable, but invents `event_order` values that no connector produced — they must be clearly marked |
| **C. full re-snapshot** | clear offsets, `snapshot.mode: initial` again | re-snapshots **every** table | **rejected.** Re-delivers every row of every table; the `dv_event_id` MERGE makes it idempotent, but it is a full replay for one new table |

**Recommendation: implement option A before onboarding a ninth table.** It is a connector
config change plus a signal table, and it converts the largest onboarding risk into a
routine operation. Until then, option B with the synthetic rows explicitly labelled.

> ### STATUS: this recommendation was NOT followed, and the consequence is real
>
> The ninth and tenth tables (`oracle…loan`, `sqlserver…payment_method`) were onboarded in
> Phase 8 **without** option A. `signal.data.collection` is still unconfigured on both
> connectors, so incremental snapshot remains unavailable.
>
> **The prediction in this section held exactly.** `loan` has 4 rows in its source and its
> certified EOD snapshot holds **1**, because the other 3 pre-date capture. That is not a
> defect — it is `changes_only` doing what this section says it does — but it is a real
> limitation that was accepted rather than solved.
>
> **What the platform does instead of pretending:** `assess_mode` refuses
> `incremental_snapshot` against a connector with no signal table —
> *"incremental snapshot is NOT available: the connector configures no
> `signal.data.collection`"* — with the remediation named. And `changes_only`, the default,
> carries a standing warning that existing rows will never appear. The default is a
> **description of the platform**, not a preference: defaulting to the better answer would
> make every onboarding fail its precheck instead.
>
> So the risk is disclosed at every gate rather than removed. Option A is still the right
> answer and is still not built. **Open issue #9.**

---

## 6. What onboarding looks like after this

```diff
+ - source_table: LOAN
+   primary_key: [LOAN_ID]
+   owner: my-aws-profile
+   domain: core_banking
+   pii: false
+   retention_days: 365
+   dq: {not_null: [LOAN_ID, CUSTOMER_ID], event_date_null_tolerance: 0}
+   target: {full_cdc: cdc_events}
```

plus the gated source-DB and connector steps. Topic casing, key columns, ingest topics, DQ
rules and catalog metadata are **rendered** — they cannot drift from the registry, because
nothing else writes them.

---

## 7. Relationship to the reporting registry

Deliberately the same shape, and deliberately **separate files**:

| | `reporting/jobs/*.yaml` | `cdc/registry/sources.yaml` |
|---|---|---|
| owns | marts, dbt models, flow modes, turn ordering | source tables, topics, capture, DQ |
| compiler | `reporting/compile.py` | `cdc/compile.py` |
| consumer | Airflow + dbt | connectors + ingest jobs |

They meet at exactly one place: a reporting job's `EOD_TABLE` dependency names a layer the
CDC registry ultimately feeds. Merging them would couple capture cadence to mart scheduling,
which are different decisions with different owners.

---

## 8. Open questions for implementation — ALL THREE CLOSED

Answered by the implementation this document authorised. Each answer is what was *observed*,
not what was expected.

**1. Does the include-list change need a maintenance window? — No.** Confirmed live in
Phase 8. `register-connectors.sh update` issues `PUT /connectors/<name>/config`, an **in-place**
update rather than delete-and-create: tasks restarted in ~30 s and Kafka offsets were retained,
so no re-snapshot occurred and no window was needed. Both connectors reported `RUNNING`
throughout. The prediction held.

**2. `pii: true` → automatic or explicit? — Automatic, plus the assertion test, exactly as
recommended.** `pii` is **derived from `classification`** and is never a second field:
"two fields meaning the same thing is how a table ends up `pii: false` while holding names and
dates of birth" (`cdc/decommission.py`). `test_datasets_with_pii_are_denied_to_bi` is the
assertion this section asked for. Verified on the Phase 8 table: `payment_method` is
`confidential`, and `cdc-maintenance.py governance` derives `PII=True` for it with zero gaps.

**3. Phase A or Phase B target? — Phase B, and the deferral was the right call.** The registry
carries `full_cdc_table` / `realtime_table` / `eod_table` overrides, all unset in the shipped
config: names are **derived** from the canonical id so the three layers cannot drift apart, and
the override exists solely to adopt a table that already exists under another name. The decision
was made once, in config, without touching a line of code — which is what putting it in config
was for.

## 9. Where this design was RIGHT and the implementation lost it

Worth recording, because the cost was paid twice.

**§4 step 5 puts `create-topics` BEFORE `register-connectors`, and step 7 puts
`export-schemas` after.** That ordering is correct and it is not obvious — it is forced by two
independent constraints:

* `auto.create.topics.enable=false`, so Debezium **cannot** create a topic it needs. Without
  the topic the connector reports `RUNNING`, its task reports `RUNNING`, and it produces
  **nothing**.
* Apicurio holds no schema for a table until the connector has produced its first record, so
  the schema export genuinely **cannot** happen earlier.

The implementation did not carry that ordering forward. The topic list lived in a hardcoded
bash array on the host under a comment reading *"Keep these in sync with `table.include.list`"*,
and Phase 8 rediscovered both constraints the hard way — a green connector producing nothing,
then `FIELD_NOT_FOUND: No such struct field 'op'` from a `schemas.json` that predated the
table. See ADR-070.

**The lesson is not "the design was right"** — it is that a correct ordering recorded only in
prose gets lost. It is now enforced by `scripts/cdc-topics.py` (which derives the topic set
from the registry and exits non-zero when one is missing) and printed in dependency order by
`cdc-table-onboard.py`, with `CDC_TABLE_ONBOARDING_RUNBOOK.md` tabulating why each step cannot
move earlier.

## 10. Status

This design is **implemented and superseded by its own ADRs**. It authorised Phases 1–8; the
normative record is now:

| topic | ADR |
|---|---|
| granularity, naming, partitioning | ADR-062 |
| routing, provisioning, unknown-table policy | ADR-063 |
| REALTIME window | ADR-064 |
| EOD close, cutoff, source-native ordering | ADR-065 |
| safe onboarding workflow | ADR-066 |
| per-table cutover | ADR-067 |
| lifecycle, maintenance, governance | ADR-068 |
| zero-custom-code acceptance | ADR-069 |
| topic provisioning from the registry | ADR-070 |
| config-driven orchestration | ADR-071 |
| downstream consumers follow the cutover | ADR-072 |
