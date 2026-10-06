# ADR-070 — Topic provisioning belongs to the registry, not a hand-synced list

* **Status**: Accepted (implementation: Phase 8 live capture; ADR-069's remaining leg)
* **Date**: 2026-09-10
* **Extends**: ADR-062 (granularity), ADR-063 (routing), ADR-066 (onboarding), ADR-069
* **Evidence**: `scripts/cdc-topics.py`, `spark/tests/test_cdc_onboarding.py::TestTopicPrerequisite`

## Context

`auto.create.topics.enable=false` on this cluster (Gap 12). It is the right setting — it is
what stops a typo in a topic name from silently creating a parallel universe of data. But it
has a consequence the platform had not accounted for:

**Debezium cannot create a topic it needs.** If a captured table has no topic, the connector
reports `RUNNING`, its task reports `RUNNING`, and it produces **nothing** for that table.
There is no error anywhere.

The cluster's topic list lived in a hardcoded bash array on the `cdc-runtime` host:

```bash
for t in BRANCH CUSTOMER ACCOUNT TRANSACTION; do
  create "cdc.oracle.COREBANK.$t" 3 "$RF" ...
done
```

…under a comment reading *"Keep these in sync with `table.include.list`"*. Manual sync is
what fails, and that script's own header records it failing before, for a different reason:

> This script previously created the lowercase names. Nothing errored: the topics were
> created, the connector started, reported RUNNING, and produced into the UPPERCASE
> topics — which `auto.create.topics.enable=false` then refused to create. **Eight topics
> sat at offset 0 while every health check was green.**

Phase 8 hit the same wall from the other side. Both new tables were added to
`table.include.list` by the generated connector template, the connectors restarted cleanly,
both reported `RUNNING` — and produced nothing, because no topic existed for either. The
whole point of ADR-069 is that a table is onboarded by a registry entry; a step that requires
hand-editing a bash array on a host is a hole straight through that claim.

## Decision

### 1. The registry decides which topics must exist

`scripts/cdc-topics.py` derives the required topic set from the compiled plan and diffs it
against a real cluster listing:

```bash
python3 scripts/cdc-topics.py                                   # what the registry requires
python3 scripts/cdc-topics.py --observed topics.txt             # diff; exits 1 if any missing
python3 scripts/cdc-topics.py --observed topics.txt --script    # emit the creation script
```

The topic name is taken **from the plan**, never rebuilt. A second spelling of the topic name
is precisely how the lowercase/uppercase defect happened, and there must be exactly one.

### 2. It exits non-zero

Silence is the failure being prevented, so a check that only prints is one a pipeline does not
notice. Missing topics are a `1`.

### 3. It creates nothing

Read-only, and it emits a script for a human instead. Topic creation needs cluster credentials
and, more importantly, a **partition count that can never be changed afterwards**: the topic
key is the canonical primary key so one key always lands in one partition (CLAUDE.md §5.1),
and changing the count re-hashes every key and breaks that permanently. That is not a thing to
do as a side effect of a config command.

### 4. Onboarding names the topic step FIRST

`cdc-table-onboard.py` now prints three ordered steps at the capture gate — topic, render,
apply — because reversed, the connector begins capturing into a topic that does not exist and
reports success.

### 5. The writer schema is exported AFTER capture, and the walk says so

A second stale derived artifact, found the same way. `schemas.json` maps topic → Avro writer
schema and is exported from Apicurio — which **has no schema for a table until the connector
produces its first record for it.** So this step cannot be folded into the deploy, cannot run
at provisioning time, and is genuinely ordered after the capture gate.

Skipping it does not fail cleanly. The ingest looks the topic up, misses, and decodes with a
fallback; Phase 8 got

```
AnalysisException: [FIELD_NOT_FOUND] No such struct field `op` in
  `status`, `id`, `event_count`, `data_collections`, `ts_ms`
```

— the *transaction-metadata* envelope. That names a field and gives no hint that the cause is
an artifact older than the table.

`cdc-table-onboard.py` now prints the four derived artifacts in dependency order, and
`CDC_TABLE_ONBOARDING_RUNBOOK.md` tabulates why each cannot move earlier.

### 6. Orphan topics are reported, not deleted

A `cdc.*` table topic with no registry entry is a table somebody captured and nobody owns. It
is named; the resolution (register it, or retire it) is a decision, and deleting a topic
discards data.

### 7. Acceptance evidence is type-checked, and capture is recorded on evidence

Two defects in the acceptance gate itself, found while carrying the two tables to `ACTIVE`.

**The judge accepted anything truthy.** Only `eod_status` did an equality test; every other
check merely asked whether the value was truthy. So `"FAILED"` passed `connector_running`,
the string `"0"` passed a row count, `"probably"` passed a boolean, and a `{"observed": …}`
wrapper passed seven of eight — which is what a healthy table also produces. This is the last
gate before a table is trusted downstream.

Each check now declares what KIND of evidence it takes — `COUNT` (a positive whole number,
`bool` explicitly excluded), `FLAG` (a real `True`), `LITERAL` (an exact string). A measured
zero fails with a different message than "not measured", because they have different fixes.
The `{"observed": v, "note": …}` wrapper is accepted **explicitly** rather than passing as a
truthy object, and its note is carried into the result so the evidence explains itself.

**A genuinely capturing table could never reach `ACTIVE`.** `cdc-table-onboard` refuses to
record `CAPTURING` for a table the connector template already lists, and that refusal is
right: a template is configuration, not proof, and the walk must not claim a transition it
did not cause. But the confirmation gate *forces* the connector update to be applied by a
human out of band — so the table captured live, the ledger said `PROVISIONED`, and acceptance
could never run on it.

Smoke now advances `PROVISIONED -> CAPTURING` on the only thing that settles it: **observed
records on the topic AND rows in FULL_CDC**, both positive and both PASS. Neither can be
positive unless capture is genuinely running. Zero of either leaves the table at
`PROVISIONED` and says why. The walk's own refusal is unchanged — evidence is smoke's job.

### 8. The committed plan artifact is checked by a TEST, not only by a Makefile target

A fifth stale-derived-artifact defect, found by running the full verification surface rather
than only `pytest`.

`make cdc-verify` compares `artifacts/cdc/table-plan.json` against the registry. It reported
drift — the committed artifact was two table onboardings behind — and **the full pytest suite
had been green the entire time**, because `cdc-verify` is not part of `make test`.

Every Spark job takes `--plan`. A stale artifact silently runs the *previous* config: the old
window, the old cutoff, the old delete policy, the old table list. Nothing fails; the run
succeeds and certifies numbers built to a configuration nobody is looking at any more.

Two fixes, because either alone leaves the hole:

* **`cdc-verify` joins `make check`.** It was absent from the aggregate target, which is why
  the drift survived: the one command a developer runs did not include the one gate that
  checks this.
* **`test_the_committed_plan_artifact_is_not_stale` runs with the suite.** It skips when no
  artifact exists (a fresh clone need not have compiled one) and fails when one is present
  and wrong — absent is recoverable by one command, wrong is invisible.

The test also **recomputes the hash from the artifact's own content**, which the Makefile
gate does not. Both `cdc-verify` and the first version of the test compared the file's
*stored* hash fields, and a truncated or hand-edited plan carries those unchanged: dropping a
table from `plan.tables` left the recorded `plan_hash` correct and passed every check.
Recomputing is what makes the hash mean something rather than merely be present.

**A gate nothing runs is not a gate** — and a hash nothing recomputes is not a checksum.

## Options

* **Turn on `auto.create.topics.enable`.** Rejected, decisively. It would fix this by
  removing the protection that makes a mistyped topic loud, and the lowercase/uppercase
  defect above would have silently produced into eight wrong topics instead of none.
* **Have the onboarding command create the topic.** Rejected — §3. An irreversible partition
  count chosen by a tool that is otherwise read-only.
* **Generate `create-topics.sh` from the registry at deploy time.** A reasonable variant, and
  partly what `--script` does. Not adopted wholesale because the host script also creates
  internal topics (Connect, Apicurio, heartbeats, DLQs) that the registry knows nothing
  about; overwriting it from a partial source would drop those.
* **Leave it to the smoke test.** `cdc-table-smoke.py` does carry a `topic_has_events` check,
  which would have caught it — *after* capture was already live and an operator had waited for
  an ingest window to explain an empty table. The check belongs before the gate, not after.

## Consequences

* Onboarding a table now has **three** capture-time steps rather than two, and the first is
  the one that was implicit and undocumented.
* **Four artifacts are derived from the registry**, not one: the topic, the connector capture
  list, the writer schema and the deployed code+plan. Three live outside the repo and each
  had its own way of being silently stale. The runbook now tabulates the dependency order.
* `cdc-topics.py` needs a topic listing as input rather than talking to the cluster itself, so
  it runs offline and in CI — the same property that makes `cdc-table-validate` useful before
  the platform exists.
* The host's `create-topics.sh` keeps its hardcoded array for the **internal** topics. Its CDC
  table loop is now redundant with the registry, and the drift between them is what
  `cdc-topics.py --observed` reports.
* **This closes ADR-069's open issue #1.** Both Phase 8 tables now capture live.

## Cost

**$0.** One topic per captured table was already the design; this changes who decides the
list, not how many exist. Each `cdc.*` table topic is 3 partitions, RF 3, 24h retention —
unchanged.

## Security

* No new resource, no IAM change, no credential. `cdc-topics.py` reads a plan and a text file.
* It cannot create, alter or delete a topic, so it cannot widen what is captured or destroy
  captured data.
* The emitted script is printed for review, never executed, and carries no secrets: the
  bootstrap address and client config are supplied by the host environment it runs in.

## Rollback

Nothing to roll back — the tool is additive and read-only. The topics created for the two
Phase 8 tables can be deleted if those tables are decommissioned, which
`CDC_TABLE_DECOMMISSION_RUNBOOK.md` step 6 covers.

## Validation

0. `pytest spark/tests airflow/tests -q` — **2,283 passed, 0 failed**.
1. `pytest spark/tests/test_cdc_onboarding.py::TestTopicPrerequisite` — the required set is
   derived from the plan and not re-spelled; a missing topic prints `MISSING` **and** exits 1;
   a complete listing exits 0; `--script` emits creation commands for exactly the missing
   topics with `--partitions 3`; and the onboarding walk prints topic-before-connector.
2. **Live**: run against the cluster as it stood before the fix, the tool reported exactly the
   two missing topics and exited 1. After creating them and restarting the connector tasks,
   `cdc.oracle.COREBANK.LOAN` reached **5** end-offsets and
   `cdc.sqlserver.digital.dbo.payment_method` **6** — inserts, an update, a delete and its
   tombstone, on both engines. Re-run against the cluster now, it exits **0**.
3. `pytest spark/tests/test_cdc_onboarding.py` — `TestAcceptanceEvidenceIsTypeChecked`
   rejects `"FAILED"`, `"0"`, `"5"`, `"probably"`, `True`-for-a-count, negatives, floats and
   a wrapper with no `observed` key; `TestCaptureIsRecordedOnEvidenceNotConfiguration` shows
   positive counts advancing a `PROVISIONED` table to `ACTIVE` and zero counts leaving it
   exactly where it was.
4. **Live end to end**: both tables reached `ACTIVE` with `CERTIFIED` closes —
   `loan` 2 keys − 1 delete = **1 row**, `payment_method` 3 − 1 = **2 rows**, each matching
   an Athena count computed from FULL_CDC below the same `cutoff_utc` rather than read back
   from the job that wrote it.
