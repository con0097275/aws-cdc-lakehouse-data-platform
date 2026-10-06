# ADR-082 — REALTIME `shape` and `write_strategy` replace `refresh_mode`

* **Status:** Accepted
* **Date:** 2026-09-28
* **Phase:** R2-B
* **Supersedes in part:** ADR-081 (which introduced `refresh_mode: latest_state`)
* **Related:** ADR-064, ADR-075, `docs/REALTIME_LAYER_PRODUCTION_DESIGN.md`

## Context

`realtime.refresh_mode` had three values — `full_refresh`, `incremental_merge`,
`latest_state` — and answered two independent questions with one word:

* **What is in the table?** The events, or one row per business key?
* **How is it written?** Replace the window, append to it, or merge under a guard?

Three consequences, all of them real in this repo:

1. A downstream consumer asking *"may I read this table as current state?"* had to know
   which of three strings implied it. Nothing in the plan said so directly, and I got this
   wrong myself earlier in the programme — I described REALTIME as latest-state-per-PK when
   it is an event window, and wrote a verifier that asserted
   `rows == distinct dv_pk_hash`. It passed, because the sample data happened to hold one
   event per key.
2. A new write strategy could not be added without inventing a fourth product name.
3. `latest_state` carried a cross-field rule (`delete_policy: soft_flag`) that reads as a
   quirk of one enum value rather than as a property of state tables in general.

## Decision

Split the field.

`shape` ∈ {`event_window`, `latest_state`} is the **contract** — what a reader may assume.
`write_strategy` ∈ {`overwrite_window`, `append`, `guarded_merge`} is the **mechanism** — it
changes cost and failure behaviour, never meaning.

Not every pair is legal. `latest_state` admits only `guarded_merge`: an append duplicates the
key, and an overwrite drops the guard that stops a late out-of-order event regressing the
row. `event_window` refuses `guarded_merge`, which would collapse events to one row per key.

`refresh_mode` becomes a **derived property** of `RealtimePolicy`, not a stored field. Two
fields that must agree are two fields that eventually will not. The Spark engine keeps
reading it until R2-E.

Three further rules fall out of the split:

* **`latest_state` requires a usable business key.** No `primary_key`, or
  `primary_key_unsupported: true`, is refused at compile. Without the check the run reports
  SUCCESS and the table holds one row, every event having hashed to the same `dv_pk_hash`.
* **Retention means different things per shape.** `event_window` prunes by age.
  `latest_state` does not: the same numbers are a recovery horizon. A valid account may not
  change for months, and deleting its row because its last event is old would empty the table
  of exactly the entities that are most stable. Exposed as `prunes_by_age` in the plan.
* **The two spellings inherit as a group.** A level declaring either replaces whatever a less
  specific level said, in whichever spelling. Without this, putting `shape:` in the registry
  `defaults:` — the point of the phase — would refuse every table still written as
  `refresh_mode:`, for a contradiction it did not write.

## Forward-looking values are refused, not accepted and ignored

`processing.source_progress: iceberg_snapshot` (R2-C) and `rebase.on_eod_certified: true`
(R2-F) are **compile errors naming their phase**, not accepted defaults.

This is the dominant defect class of the whole programme: a declared field nothing reads.
`watermark_type: NONE` was declared and never read, so FULFILL could not backfill.
`business_date` read a var nothing passed, and three marts were silently empty with every
test green. `--catalog` and the EMR application id were the same shape of mistake. Each one
compiled, looked live, and named behaviour that never happened.

Accepting `iceberg_snapshot` today would compile a plan asserting an incremental read while
every run still scanned the whole window — and the plan would be the thing that lied. The
refusal is what gets deleted when the engine lands.

## Consequences

* `plan_hash` changed once, because the plan payload gained the new fields. `deprecations`
  sits **outside** the payload, so a warning never moves the hash — a warning that did would
  make `--verify` fail on a registry nobody edited, and the next person would learn to
  ignore the check.
* **No behaviour changed.** Every shipped table remains `event_window` / `overwrite_window`,
  which is exactly what it did before, asserted by a test. The `latest_state` flips belong to
  R2-D, with the engine and the tests that make them safe.
* Moving LOAN's day fields under `recovery:` compiles to a byte-identical `plan_hash`, which
  is how the two spellings are proven equivalent rather than asserted to be.

## Options

**Keep `refresh_mode` and add values.** A fourth and fifth product name for combinations of
two orthogonal choices; the contract stays implicit in a string.

**Accept the forward-looking values with a default.** Cheaper now, and it recreates the
exact defect the programme has spent its length removing.

## Cost

Zero. No AWS resource, job shape or schedule changed; the plan gained fields. The `shape`
split is what makes the R2-I cost model expressible per table — a `latest_state` table's
steady-state size is bounded by entity count, an `event_window`'s by retention × rate — but
this phase measures nothing.

## Security

No change. No new credential, IAM statement, network path or data exposure. The config
surface is a registry file already in Git under the same review as its neighbours;
`classification` and the payload policy are untouched.

## Rollback

`git revert` the phase. The registry's old spelling still compiles — the deprecation path
exists precisely so a revert of the *code* leaves a registry that loads. The one action
required afterwards is `make cdc-compile`, because the plan payload loses the new fields and
`plan_hash` returns to its previous value; the staleness test enforces this.

No data migration either way. REALTIME is always rebuildable from FULL_CDC, and nothing in
this phase wrote to the lake.

## Validation

`spark/tests/test_realtime_config.py` — 36 tests, all passing:

* every legacy `refresh_mode` round-trips through the pair and back
* `refresh_mode` is not a dataclass field (it cannot drift from `shape`)
* each illegal shape/strategy pair is refused, each legal one resolves
* `latest_state` without a PK, with `primary_key_unsupported`, with a repeated key column,
  or without `delete_policy: soft_flag` — refused; composite keys accepted
* `iceberg_snapshot` and `rebase.on_eod_certified: true` refused, each naming its phase
* an unknown field in any nested block is refused (a typo there is otherwise silent: the
  block parses, the field is ignored, the table runs on the default)
* `recovery:` and the top-level day fields compile to an identical `plan_hash`
* a deprecation does not move `plan_hash`
* spelling-aware inheritance in both directions
* every shipped table is still an overwritten event window, and the shipped registry
  compiles with no warnings

Plus the full suite: `spark/tests/` + `airflow/tests/`.
