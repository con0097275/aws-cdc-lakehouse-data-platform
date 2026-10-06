# REALTIME performance and cost

R2-I. Two kinds of number, kept apart on purpose.

---

## 1. Run it

```bash
python3 scripts/realtime-benchmark.py              # cost model, offline, no AWS call
source scripts/reporting-env.sh
python3 scripts/realtime-benchmark.py --measured   # + the run ledger via Athena
```

## 2. The cost model is derived from config, and claims no dollars

```
  table                                    rt   shape         read              runs/d  vcpu rebase/d
  oracle.coredb.corebank.account           on   latest_state  iceberg_snapshot     144     2        1
  oracle.coredb.corebank.branch            OFF  event_window  window_scan            0     0        0
  …
  7 of 10 tables have REALTIME on; 3 are OFF and cost ZERO — no scheduled task, no compute,
  no maintenance. That is the lever.
  1008 materialisation runs/day + 4 rebases/day. Comparative size class: 2016.0 runs x vCPU.
```

**No dollar figure, and that is deliberate.** A per-run cost needs a duration this repository
does not have until the layer has been running. A number derived from a guessed duration
looks like a measurement, and it would be quoted back later as one. Runs and shapes are
exact; durations are measured; multiplying them is the operator's step, with both inputs
visible.

A **disabled table reports zero**, not the cadence it would have had. The brief's acceptance
gate is literally that disabled means no scheduled task, no compute and no maintenance, so a
counterfactual here would read as spend that is not happening.

## 3. The benchmark is measured, out of the run ledger

Every run records `read_mode`, `input_rows` and its start and end. So the comparison — the
old full-window scan against the new incremental read — is a **query, not a rig**:

```sql
SELECT read_mode, count(*) AS runs, avg(input_rows) AS avg_rows,
       avg(to_unixtime(finished_at) - to_unixtime(started_at)) AS avg_seconds
FROM   kafka_dev_lab_dev_ops.realtime_run
WHERE  operation = 'materialise' AND status IN ('SUCCEEDED', 'SUCCEEDED_NOOP')
GROUP BY read_mode;
```

A benchmark rig that ran the old path deliberately would be measuring a configuration
nothing uses. Both paths already occur naturally: `window_scan` on the first run of a table
and after every compaction, `incremental` the rest of the time.

## 4. Measured — 2026-09-28, live on AWS

`ops.realtime_run` after three REALTIME runs on the deployed platform
(account 111122223333, ap-southeast-1):

```
  read_mode        runs    rows read   avg rows  avg sec
  incremental         2            0          0      4.6
  noop               12            0          0      3.7
  window_scan         7         5770        824      5.9

  ROWS READ PER RUN   window_scan 824 -> incremental 0

  WHY RUNS FELL BACK TO THE FULL SCAN
    forced_rebuild                   4
    no_cursor                        3
```

**Read this carefully, because the headline number is not a speed-up.**

The two incremental runs read **zero** rows. That is a real result, not a missing one: the
FULL_CDC snapshots appended since their cursors held no events inside the window, so the
incremental scan did in 4.6s what a window scan would have done over 824 rows to reach the
same answer. The cursors advanced correctly
(`4124040414667009057 -> 917912465318976243`), which is the mechanism this phase is about.

What is **not** measured: the saving on a delta that actually contains rows. Every event in
this lake was written by one snapshot load at `2026-09-28 07:41:43`, so no run has yet seen
a non-empty incremental delta on live data. The local Iceberg test measures that case
exactly — second run reads **4 rows, not 14** — and this document does not extrapolate it to
production volume.

The twelve NOOPs are the empty-delta path: `source unchanged at <snapshot>`, no read, no
write, cursor untouched.

## 5. Partition layout — measured, and it does not discriminate yet

S3 object listing under `warehouse/stream/<table>/data/`, same day:

| table | files | total | avg file | partitions |
|---|---|---|---|---|
| `rt_oracle_coredb_corebank_account` | 5 | 300 KB | 60 KB | **1** |
| `rt_oracle_coredb_corebank_transaction` | 4 | 1.3 MB | 327 KB | **1** |
| `rt_sqlserver_digital_dbo_digital_event` | 4 | 1.9 MB | 475 KB | **1** |
| `rt_sqlserver_digital_dbo_app_user` | 4 | 126 KB | 31 KB | **1** |
| `rt_sqlserver_digital_dbo_payment_method` | 5 | 100 KB | 20 KB | **1** |

Every table is **one partition** with four or five sub-MB files, against a
`target_file_size_mb` of 128. At this volume the choice between unpartitioned,
`bucket(N, dv_pk_hash)` and `days(dv_src_ldt)` **cannot be made from evidence** — there is
one partition either way, and a hash bucket would only fragment 300 KB into N smaller files.

So **no partition change was made**, and that is the measurement's conclusion rather than a
deferral. §40 says to choose from a benchmark; the benchmark says this data is too small to
choose. It must be retaken at a volume where partitions actually differ, and the same S3
listing is the way to retake it.

What the numbers *do* justify now: these tables are compaction candidates by file size, not
by partition strategy — which is what the metric-driven planner already decides.

## 6. Maintenance

`cdc_maintenance` now measures file counts, sizes, manifests and snapshots from each table's
own Iceberg metadata, for **FULL_CDC and REALTIME** (`--layers`). It used to require a
`--metrics` file that nothing in the platform produced, so every scheduled run would have
died at argparse.

It is **dry-run by default**. `rewrite_data_files` and `remove_orphan_files` delete files,
and a scheduled delete nobody turned on is how a maintenance window becomes an incident. Set
`CDC_MAINTENANCE_EXECUTE=true` on the Airflow node to arm it.

Disabled tables are excluded from REALTIME maintenance: nothing writes to their target, so
compacting it is spend against a table that is not changing.
