# ADR-086 — A STREAM_BATCH mode declares its CDC source tables

* **Status:** Accepted
* **Date:** 2026-09-28
* **Phase:** R2-H
* **Related:** ADR-033, ADR-034, ADR-064, ADR-082

## Context

The R2-A audit found all four reporting jobs declaring `source_layer_policy: REALTIME` for
STREAM_BATCH, three CDC source tables carrying `realtime: {enabled: false}`, and **nothing
connecting the two**. It also flagged one case as UNKNOWN rather than benign:
`mart_channel_engagement_daily` reads `dim_channel`, built from
`sqlserver.digital.dbo.channel`, which is REALTIME-disabled.

That gap does not produce an error. The logical layer resolves, the physical table exists,
and it is simply never refreshed — so the mart serves a number that stopped moving, and
every health check it has says it is fine.

The compiler could not have caught it: a mart reads `source('curated', …)`, which reads a
CURATED fact, which reads EOD/REALTIME of a CDC table. Nothing in the reporting config names
the CDC table, and following the chain would mean the reporting compiler depending on the
dbt manifest, the CURATED entity map and the CDC registry at once.

## Decision

**Declare it.** A flow may carry `source_tables` (canonical CDC registry ids) and
`source_policy: {preferred, fallback}`:

```yaml
STREAM_BATCH:
  source_layer_policy: REALTIME
  source_tables: [sqlserver.digital.dbo.digital_event, sqlserver.digital.dbo.channel]
  source_policy: {preferred: REALTIME, fallback: FULL_CDC}
```

`reporting/compile.py` then refuses, at compile, when an enabled mode reads the REALTIME
layer and any declared source table has `realtime.enabled: false` **with no declared
fallback**. It also refuses a `source_tables` entry the CDC registry does not define — a
source table that does not exist cannot be checked, and the check is the point.

Two ways to satisfy it, both explicit: enable REALTIME for the table, or declare the
substitution. **There is no third, implicit way.** A silent fallback would mean the cheap
path and the correct path differ by which config happened to be set, with nothing recording
which one a given run took.

### The audit's UNKNOWN, resolved

`mart_channel_engagement_daily#STREAM_BATCH` now declares `channel` and an explicit
`fallback: FULL_CDC`. `channel` stays REALTIME-disabled: it is a four-row reference table
that changes four times a day, and a rolling window of it is a Spark run every ten minutes
forever. Reading it from the canonical layer is a bounded scan of a tiny table and is always
correct.

The resolution is a recorded decision, not a discovered behaviour.

### A missing CDC plan SKIPS the check rather than passing it

`_cdc_realtime_state` returns `{}` and the check returns early. "Not verified" and
"verified, fine" must not look the same; that difference is what stops the rule becoming
decoration in an environment where the plan was never compiled.

### `--set realtime.enabled=false` previews the blast radius

`scripts/cdc-table-plan.py --table <id> --set realtime.enabled=false` is read-only and
prints the tasks removed, the Spark runs per day removed, that the target is **retained**,
that EOD / AUTO_CORRECT / FULFILL are unaffected, and which STREAM_BATCH modes would then
fail to compile. Discovering a blocker after the apply means a broken plan committed and a
scheduler that has already picked it up.

Exactly one key is settable. A general override engine would be a second implementation of
the loader's precedence rules, and the two would disagree the first time either changed.

## Options

**Derive the lineage.** Walk the dbt manifest to CURATED to the CDC registry. It removes the
declaration but couples the reporting compiler to two more config systems, and it would
still be a guess wherever a model reads a table the manifest does not attribute.

**Check at runtime.** The mode already runs; by then the wrong number is published.

**Allow an implicit FULL_CDC fallback.** Nothing to declare, and no record of which layer a
given run read.

## Consequences

* Four job YAMLs gained `source_tables`; one gained `source_policy`.
* The reporting compile now depends on `artifacts/cdc/table-plan.json` being present to
  perform this check — and says so by skipping rather than passing when it is not.
* Turning REALTIME off for `account`, `customer` or any table three marts declare is now a
  **compile failure with a named fix**, not a silent staleness.

## Cost

None. The check is local, and the preview makes no AWS call. Indirectly it is a cost tool:
the preview prints the runs per day a disable removes, which is the lever the R2 brief §47
names.

## Security

No change. No new credential or network path; the compiler reads two local files.

## Rollback

Remove `source_tables` from the flows and the check becomes a no-op — it only applies to
modes that declare them. The schema additions are optional properties, so an older job YAML
still validates.

## Validation

`spark/tests/test_realtime_downstream.py` — 15 tests. Beyond asserting the shipped config
compiles, they **remove the declaration and assert the compile stops**: a check that cannot
fail is decoration. Covered: a missing fallback fails and the message names both the table
and the fix; an unknown source table fails; a missing CDC plan skips rather than passes;
`channel` is declared, carries a fallback, and is still REALTIME-disabled; and the preview
names blocking modes, states EOD/AUTO_CORRECT/FULFILL are unaffected, says the target is
retained, refuses an unsupported key, and mutates nothing.
