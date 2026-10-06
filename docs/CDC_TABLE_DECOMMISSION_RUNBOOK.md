# CDC Table Decommission Runbook

**Contract**: ADR-068. **Code**: `cdc/decommission.py`.

Taking a table out is harder than putting one in, because something may be reading it and the
data is the only copy of a history the source no longer has.

```bash
python3 scripts/cdc-maintenance.py decommission --table <id> [--evidence e.json]
```

Read-only. It prints the six ordered steps and judges the evidence you supply for each. It
does not stop capture, drop a table, or delete an object.

---

## The six steps, in order

### 1. `disable_downstream`

Find and stop the readers **first**. Stopping capture while something still queries the table
produces a table that silently stops changing — which reads as "no activity in the source",
not as "this table is being retired".

Evidence: the list of readers and confirmation each is stopped or repointed.

### 2. `stop_capture`

Remove the table from the connector's `table.include.list` and apply:

```bash
python3 scripts/cdc-connector-render.py --engine <engine> --allow-removal
bash scripts/register-connectors.sh update --execute        # a human types the phrase
```

`--allow-removal` is required for a removal. Rendering a shorter include list is otherwise
indistinguishable from a registry that failed to load, and applying that would silently stop
capturing every table.

**Source-side CDC stays on** until step 6. Turning it off here makes step 3 impossible.

### 3. `freeze_final_state`

Close a final EOD snapshot and record its cutoff. This is the artefact that answers "what did
this table hold when it was retired", and it must be taken **after** capture stops so that it
is stable, and **before** anything is deleted.

Evidence: the `ops.eod_run` row, `CERTIFIED`.

### 4. `retention_decision`

**A decision by a person, recorded** — how long the frozen data is kept and under what
classification. The platform will not choose a retention, because the choice is legal and
commercial, not technical.

Evidence: the retention, the owner who chose it, and the date.

### 5. `archive_document`

Write down what the table was: grain, primary key, owner, classification, the final row
count, the frozen snapshot's location, and why it was retired. A retired table with no
document becomes an unexplained S3 prefix that nobody dares delete.

### 6. `remove_capture`

Now the source-side CDC can be disabled and the registry entry removed. The Iceberg data
stays for its retention period — this step removes the **capture**, not the history.

---

## What is never dropped

**The legacy monolith stays.** ADR-062: it remains the reconciliation baseline for every
window already captured. Decommissioning a per-table target does not touch it.

**FULL_CDC is the history.** Once the source is gone it is the only copy. The retention
decision in step 4 governs it and nothing else should.

## Rollback

Reversible up to and including step 2 — re-add the table to the include list and re-run the
connector update; capture resumes from retained offsets and the gap backfills from the source
if the source still has it.

**After step 6 it is not reversible by config.** The source CDC is off, so the changes made
while the table was retired were never captured and no longer exist anywhere. Re-onboarding
gives you a table that starts from the re-onboarding date, with a hole. That is why the steps
are ordered and why the last one is last.

## Cost

Decommissioning **reduces** cost — one fewer topic, three fewer maintained tables, one fewer
nightly close. The retained data is the only continuing cost, which is exactly what step 4 is
a decision about.
