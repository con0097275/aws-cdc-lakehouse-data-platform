# ADR-067 — Non-destructive per-table CDC cutover: flag, backfill, gate, benchmark

* **Status**: Accepted. **Pilot PASSED live 2026-09-10; 2 of 8 tables cut over.**
* **Date**: 2026-09-10
* **Extends**: ADR-062 (granularity — this is its Phase B), ADR-063…066
* **Evidence**: `spark/tests/test_cutover_pilot_spark.py`, `spark/tests/test_cutover_gate.py`

## Context

ADR-062 approved per-table FULL_CDC as the target and **gated the cutover on triggers**,
explicitly excluding data volume:

> 1. concurrent per-table writers, or
> 2. differentiated retention, PII or IAM policy between source tables.

**Trigger 2 is now real.** The registry carries differentiated classification
(`customer` and `app_user` are `confidential`, the rest `internal`), differentiated realtime
enablement (three tables disable it), and differentiated freshness SLAs (15 vs 60 minutes).
ADR-060 denies the AI plane `warehouse/full_cdc/` **wholesale** precisely because a per-table
grant is not expressible against a monolith — that is the concrete cost of the current shape.

The gate is therefore satisfied and Phase 6 may proceed.

## Decision

### 1. The legacy monolith is never dropped (§A)

Every mode can read it, `LEGACY` is the default, and `PER_TABLE` stops **reading** it rather
than deleting it. A test asserts `backfill.py` contains no `DROP`, `DELETE`, `TRUNCATE` or
`overwrite(` at all — "I did not intend to" is not a control.

### 2. A per-table flag, not a switch (§G)

`LEGACY` (default) | `DUAL` | `PER_TABLE`, per table, with rollout **groups** as a
convenience. **`DUAL` reads LEGACY**: a mode where writes go both ways but reads follow the
new path would put consumers on unvalidated data during the very window whose purpose is to
validate it. An unknown table defaults to `LEGACY` — adding a row to the registry must not
silently migrate anything.

### 3. The legacy read always carries its predicate (§F)

The resolver returns the monolith **with** `source_system = … AND source_table = …`, never
bare. The monolith holds every source table, so a consumer that forgets the predicate reads
eight tables' events as one — and gets a plausible number rather than an error. Returning the
predicate *with* the table makes forgetting it impossible.

`reporting/layers.yaml` remains the only file naming a physical database; `cdc/cutover.py`
reads it through `cdc/catalog.py` and a test asserts no Glue database name appears in it.

### 4. Backfill copies identity, never recomputes it (§C)

`dv_event_id` is `sha2(topic|partition|offset|kafka_timestamp)`. Recomputing it is the obvious
move and it is wrong: a recompute that rendered the timestamp one microsecond differently
would give the same events **different identities**, and the reconciliation would compare two
sets that cannot match for a reason unrelated to the data. The backfill reads the legacy
table, copies verbatim, MERGEs on `dv_event_id`, and is re-runnable.

Dry run is the default. The comparison is a count **and** an identity-set hash over sorted
`dv_event_id`s — sorted because a set has no order, and over `dv_event_id` because that is
what the layer's idempotency is defined on.

### 5. Unmeasured is not passed (§H, §I)

Nine gate checks. A check that is **absent** from the observations is a `FAIL`, not a skip.
`benchmark_measured` is itself a required check, so "we did not benchmark" cannot be reported
as "performance is fine".

`BenchmarkReport.claim()` **raises** rather than hedging when a scenario is incomplete: a
partial benchmark reads exactly like a complete one. When complete, the claim names the metric
and both numbers, and reports a regression as readily as an improvement.

### 6. Cutting over requires recorded, current evidence (§J)

`cdc-cutover.py set --mode PER_TABLE` refuses unless gate evidence exists, passed, **and was
recorded against the current `config_version`** — three separate ways it can be wrong.
Reverting to `LEGACY` or entering `DUAL` needs no gate: `DUAL` is where the evidence is
gathered, so requiring it to enter would deadlock.

### 7. Risk bands, not risk scores (§K)

The rollout plan orders ascending by risk from classification, realtime enablement and
freshness SLA. A decimal computed from a classification and two flags would look like a
measurement; a band does not.

## Options

* **Big-bang all eight tables.** Rejected — §2, and the brief forbids it. The blast radius of
  a wrong answer would be every consumer of every table at once.
* **`DUAL` reads the per-table path.** Rejected — §2.
* **Recompute `dv_event_id` during backfill.** Rejected — §4.
* **Backfill from Kafka rather than the monolith.** Rejected: it decodes the same records a
  second time, produces different `ingested_at`, and depends on a retention window that no
  longer covers the oldest events. The canonical history already exists.
* **Drop the monolith after cutover.** Rejected by ADR-062 and restated here: it is the
  reconciliation baseline for every window already captured.
* **Let the gate pass on a subset of checks.** Rejected — §5.

## Consequences

* Nothing changes for any consumer until a mode is flipped: all eight tables are `LEGACY`.
* The pilot pair is one Oracle and one SQL Server table (§B), exercising both ordering
  contracts and both payload spellings.
* **A Phase 4 defect was found and fixed here:** the EOD DQ check called an expression that
  coalesces the after-image with the *before*-image. The EOD row contract has no
  `payload_before` — a snapshot is state, not an event — so the close died with
  `UNRESOLVED_COLUMN`. All eight registered tables declare a `not_null` rule on a primary-key
  column, so this broke **every** close. Phase 4's tests missed it because no fixture table
  declared a DQ rule. The same fix stopped non-key columns passing vacuously.
* **A design limit is now documented rather than latent:** the EOD engine applies no
  source-table predicate, because it takes its source from the compiled plan and that is
  always a per-table target. Pointed at the raw monolith it reads every table's events at
  once — here it refused, because an Oracle table's numeric ordering cannot parse a SQL Server
  hex LSN. A legacy-path close must go through a filtered source, which is exactly what the
  resolver hands a consumer.

## Cost

| item | impact |
|---|---|
| the flag, resolver, gate, benchmark harness | **$0** — local, pure |
| backfill dry run | one read of the legacy slice |
| backfill execute | one read + one MERGE per pilot table. At the measured 20,390 rows this is minutes of EMR, not hours |
| `DUAL` | one extra MERGE per topic per window, plus transient duplicate storage (~$0.01/month at the lake's 300 MB) |
| the benchmark itself | 5 scenarios × 2 paths of Athena/EMR work — **the largest single cost in this phase**, and the one section I makes non-optional |
| cutover | $0. It is a config flag |
| rollback | $0. The monolith is still written and was never dropped |

Immaterial against the $100/month budget of record. The dominant cost remains platform
uptime.

## Security

* Per-table storage is what makes per-table IAM **expressible**: today any principal that can
  read `cdc_events` reads all eight source tables, which is why ADR-060 denies the AI plane
  the whole prefix. Cutting over `customer` and `app_user` (both `confidential`) is the point
  of trigger 2.
* No new resource, no IAM change in this phase, no data movement outside the lake.
* The backfill has no write path to the legacy table.
* The cutover state is a Git-tracked file reviewed like any other config; no automated run
  edits it.

## Rollback

Set the mode back to `LEGACY`. The monolith was never dropped and — under `DUAL` — is still
being written, so the rollback is a flag flip with no restore. That is the whole reason
`DUAL` reads legacy: the read path never moves ahead of the evidence.

Per-table targets left behind by a rolled-back cutover are additive; they cost storage and
can be dropped, and dropping them loses nothing the monolith does not still hold.

## Validation

1. `pytest spark/tests/test_cutover_pilot_spark.py` — **18 tests** against a real Iceberg
   catalog: a legacy monolith holding two engines' events; backfill selecting only its own
   slice; identity preserved verbatim; every canonical field surviving; the identity hash
   matching; dry run writing nothing; re-runnability; a missing target refused; both pilot
   engines backfilling independently; **an EOD close over each path certifying identical
   state**; a REALTIME window over each path holding identical events; and the resolver
   returning the same events from both paths.
2. `pytest spark/tests/test_cutover_gate.py` — **34 tests**: the gate, the unmeasured-is-a-fail
   rule, the benchmark's refusal to compare or claim incompletely, regression reporting, the
   flag and its groups, and the CLI refusing to cut over without current evidence.

**VALIDATED LIVE, 2026-09-10** (account 111122223333, profile `my-aws-profile`,
`ap-southeast-1`). Evidence: `artifacts/validation/session-43/CUTOVER_PILOT_RESULT.md`,
`artifacts/cdc/benchmark-*.json`, `artifacts/cdc/gate-*.json`,
`artifacts/cdc/cutover-gate.json`.

| step | result |
|---|---|
| register 15 surviving Iceberg tables into the empty Glue catalog | SUCCESS; monolith at **20,390 rows**, matching the Phase 0 audit exactly |
| provision the two pilot targets | SUCCESS |
| backfill dry run | 641/961 and 6,000/12,000 identity-bearing |
| backfill execute | SUCCESS, `identity_match=True` on both |
| benchmark, both paths, 9 metrics x 5 scenarios | complete; see below |
| gate | **9/9 on both tables** |
| cutover | both `PER_TABLE`; 6 tables untouched |
| legacy monolith after | **20,390 rows — unchanged** |

**The measured headline, and it matches the Phase 0 prediction:** the EOD source scan for
`oracle/ACCOUNT` fell from **89,193 to 11,231 bytes scanned (-87.4%)**. ADR-062 predicted
"≈87% with 8 evenly-weighted tables". Data files fell 14 → 2 (-85.7%) on both tables.

**And the honest other half.** `sqlserver/digital_event`'s EOD scan improved only **-7.1%**,
because it *is* 12,000 of the monolith's 20,390 rows — isolating the dominant table saves
little. `auto_correct` got **slower** on the per-table path for ACCOUNT (730 ms → 2,264 ms).
The pruning benefit is inversely proportional to a table's share of the monolith, which no
one had stated before this measurement.

**Four defects the live run found that the local pilot could not:**

1. `register_tables.py` built a Spark session with **no catalog configuration**, relying on
   submit-time `--conf`. `CREATE NAMESPACE glue_catalog.x` then resolved against the v1
   session catalog and failed with `_LEGACY_ERROR_TEMP_1055` — an error class whose own
   message template is undefined, so the surfaced text named nothing. Fixed both ways.
2. Its `--dry-run` returned **before** the failing statement, so the dry run passed and the
   real run did not.
3. **320 of 961 live `oracle/ACCOUNT` rows carry a NULL `dv_event_id`** (6,000 of 12,000 for
   `digital_event`). `MERGE ON dv_event_id` never matches NULL, so they would be re-inserted
   every run and the backfill would silently stop being re-runnable. Now excluded and
   counted — never given an invented identity.
4. **The legacy predicate uppercased only the literal.** Oracle folds identifiers, SQL Server
   does not, so it matched every Oracle table and no SQL Server one: the legacy read returned
   **zero rows with no error**, and the benchmark scored it "0 bytes, very fast". Caught
   before it reached a consumer.

**Still not claimed:** no `DUAL` window was observed — the pilot went LEGACY → backfill →
gate → PER_TABLE, because the backfill reproduces the history the monolith already holds and
`DUAL` validates *new* events. A `DUAL` window remains the right step before cutting over a
table that is actively receiving CDC.
