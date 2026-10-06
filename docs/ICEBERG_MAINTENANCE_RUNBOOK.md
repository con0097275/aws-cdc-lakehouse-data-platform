# Iceberg Maintenance Runbook

**Contract**: ADR-068. **Code**: `cdc/maintenance.py`, `spark/ops/cdc_maintenance_job.py`.

Maintenance is **metric-driven, not cron-driven**. A cadence gives the *opportunity* to act;
measured file metrics decide *whether* to.

---

## Why not just a nightly cron

A cron that compacts every table nightly does the same work whether or not there is work to
do. The Phase 0 audit measured the actual problem — a mean data file of **137 KiB** against a
128 MiB target, three orders of magnitude under — and the cause is **commit frequency, not
elapsed time**. A table nobody wrote to has nothing to compact; a table that took a thousand
small commits in an hour needs compacting before its cadence says so.

The cadence is still a real bound: it is what stops a hot table being compacted continuously
and spending more on maintenance than on ingest.

## Temperature sets the floor

| temperature | minimum hours between runs |
|---|---|
| `hot` | 6 |
| `warm` | 24 |
| `cold` | 168 |

Inferred from what the registry already says — a table with a tight freshness SLA commits
often, which is what makes small files; a table with no REALTIME layer is written once a day
at most. An explicit `maintenance.temperature` always wins:

```yaml
maintenance: {temperature: warm}
```

> The explicit value is carried through the **compiled plan**. It was silently dropped at
> load for one phase, so the runtime fell back to the inference — which agreed with the
> explicit value often enough to hide that it was ignoring it. Pinned by
> `test_an_explicit_temperature_survives_compilation`.

## The four actions, and the order is not arbitrary

```
rewrite_data_files -> rewrite_manifests -> expire_snapshots -> remove_orphan_files
```

* **Compaction writes new files** and leaves the old ones referenced by older snapshots, so
  expiring snapshots *afterwards* is what actually frees space. The other way round, the
  expiry has nothing to collect and storage never drops.
* **Orphan removal runs last** because it deletes files no snapshot references, and it must
  not run while a rewrite it cannot see is still in flight.

`remove_orphan_files` is **opt-in per table**: it is the one that deletes.

```yaml
maintenance: {actions: [rewrite_data_files, expire_snapshots]}
```

Omit the key to get the default set. An **empty list is refused**, not obeyed — `[]` is what
a mistake produces and is indistinguishable from a deliberate "never maintain this table",
for which there is no setting. Obeying it silently disables compaction and snapshot expiry on
a streaming table and nothing reports that it happened.

## Thresholds

| threshold | default | meaning |
|---|---|---|
| `small_file_fraction` | 0.25 | below 25% of target (32 MiB) is "small" |
| `min_small_files` | 8 | fewer than this and a rewrite costs more than it saves |
| `min_avg_size_fraction` | 0.10 | mean below 10% of target also triggers |
| `max_manifests` | 10 | more than this and manifest rewriting pays |
| `max_snapshots` | 20 | more than this and expiry has work |
| `min_orphan_files` | 1 | orphan removal needs a **measured** count, not elapsed time |

> **Both** rewrite triggers require `file_count >= min_small_files`. A freshly backfilled
> `oracle/ACCOUNT` holding 2 files averaging 48 KB once fired the mean-only trigger and asked
> to compact two files into one — an EMR run to remove one file, on every cadence, forever.

## Retention guard

`remove_orphan_files_days` must be `<=` `expire_snapshots_days`, enforced at compile. And
orphan cleanup must never run with a retention short enough to delete files a job that is
still running could commit (CLAUDE.md §6).

---

## Operating

```bash
python3 scripts/cdc-maintenance.py measure --table <id>     # live, read-only Athena
python3 scripts/cdc-maintenance.py plan --metrics m.json --max-tables 2
```

`plan` reads **measured** metrics and refuses to run without them — the decision is separated
from the doing so that "why did this table get compacted?" is answerable from a config and a
metrics row, without a cluster.

`--max-tables` bounds the batch. Maintenance rewrites data and commits, so running every
table at once turns a maintenance window into the platform's peak load, on the same capacity
the ingest uses.

### Nothing is selected

Expected, and usually correct. Check the per-table `decisions` in the plan output: each says
`RUN` or `skip` **with the measured numbers**, so "too few files for a rewrite to pay for
itself" is distinguishable from "not due yet".

### A table is always due but never selected

`--max-tables` is bounding the batch and a hotter table wins every time. Raise the bound or
run that table on its own.

### Storage did not drop after compaction

Expected until snapshots expire — the old files are still referenced. Confirm
`expire_snapshots` was in the action list and ran *after* the rewrite.

## Cost and rollback

Maintenance is **not always-on**: it costs one bounded EMR run when the metrics say there is
work. Compaction and manifest rewriting are metadata-and-data rewrites that preserve table
contents exactly, so they have no logical rollback and need none.

`expire_snapshots` and `remove_orphan_files` **do** delete, and are not reversible: expired
snapshots cannot be time-travelled to, and removed orphans are gone. That is why the first is
bounded by a retention setting and the second is opt-in and needs a measured count.

---

## REALTIME targets, and measuring instead of being told (R2-I)

Two changes, both of which were the difference between this running and not.

**`--metrics` is no longer required.** It was, and *nothing in the platform produced the
file* — no script wrote one and the `cdc_maintenance` DAG never passed the flag. Every
scheduled run would have died at argparse before touching a table, and the failure would
have read as an Airflow problem rather than as maintenance never having run at all.

The job now measures `file_count`, `small_file_count`, `avg_file_size_bytes`,
`manifest_count` and `snapshot_count` from each table's **own Iceberg metadata**
(`{table}.files`, `.manifests`, `.snapshots`). `--metrics` remains as an optional override
for replaying a decision against captured numbers.

Orphan files are reported as **zero, declared rather than guessed**: finding them means
listing the table's whole S3 prefix and diffing it against the manifests — an expensive scan
with a destructive conclusion. `remove_orphan_files` is scheduled by age policy instead.

**REALTIME targets are maintained too.** `--layers FULL_CDC,REALTIME` is the default,
because the ten-minute append path (ADR-084) writes small files faster than anything else in
the platform. A table with `realtime: {enabled: false}` is **excluded** — nothing writes to
its target, so compacting it is spend against a table that is not changing.

```bash
# dry run (the default): prints the CALLs and runs none
spark-submit spark/ops/cdc_maintenance_job.py \
  --plan s3://<lake>/artifacts/cdc/table-plan.json \
  --warehouse s3://<lake>/warehouse --max-tables 2
```

In Airflow it is dry-run until `CDC_MAINTENANCE_EXECUTE=true` is set on the node. Two of
these actions delete files, and a scheduled delete nobody turned on is how a maintenance
window becomes an incident.
