# Turning REALTIME off for a table

ADR-086. Disabling is a **config change with a preview**, never a deletion.

---

## 1. Preview first

```bash
python3 scripts/cdc-table-plan.py --table <id> --set realtime.enabled=false
```

Read-only. It prints what stops, what keeps running, and — the part that matters — which
downstream modes would then **fail to compile**:

```
  SPARK RUNS/DAY REMOVED        ~144   (schedule '*/10 * * * *', profile small)
  TARGET TABLE                  RETAINED. Disabling stops PROCESSING; it does not drop …
  EOD IMPACT                    NONE. EOD reads FULL_CDC and never REALTIME
  STREAM_BATCH DEPENDANTS
    BLOCKER  mart_account_balance_daily#STREAM_BATCH  requires REALTIME with no declared fallback
```

Finding a blocker *after* the apply means a broken plan committed and a scheduler that has
already picked it up.

## 2. Clear the blockers, explicitly

A `STREAM_BATCH` mode that declares the table in `source_tables` and reads the REALTIME
layer must either keep REALTIME on, or declare the substitution:

```yaml
STREAM_BATCH:
  source_layer_policy: REALTIME
  source_tables: [sqlserver.digital.dbo.channel]
  source_policy:
    preferred: REALTIME
    fallback: FULL_CDC       # explicit. There is no implicit fallback.
```

`reporting/compile.py` refuses without it, and the message names the fix.

## 3. Apply

```yaml
# cdc/registry/sources.yaml
- table: channel
  realtime: {enabled: false}
```

```bash
python3 -m cdc.compile                      # recompile the plan
python3 -m pytest spark/tests/ airflow/tests/ -q
bash scripts/cdc-deploy-code.sh             # publish the plan the jobs read
```

The next scheduler parse produces **no mapped task** for that table. Not a task that returns
early — a skipped submission still starts an EMR application and bills for it.

## 4. What is NOT done, on purpose

Disabling **never**:

* drops the Iceberg table
* deletes anything under its S3 prefix
* removes its Glue metadata
* expires its snapshots

The target is retained, exactly as it was at the last successful run. That is what makes
re-enabling a config change rather than a rebuild, and it is why a mis-click is recoverable.

Removing the data is a **decommission**, a separate and deliberate operation:
`cdc/decommission.py`, `docs/CDC_TABLE_DECOMMISSION_RUNBOOK.md`.

## 5. Re-enabling

```bash
python3 scripts/cdc-table-plan.py --table <id> --set realtime.enabled=true
```

The target must exist before the first run — the engine refuses to create tables, because a
table it created would have no partition spec, no retention property and no owner:

```bash
python3 -m cdc.provision --table <id> --layer REALTIME        # shows the DDL
spark-submit spark/ops/provision_cdc_tables.py --execute      # applies it
```

The first run after re-enabling has no cursor, so it falls back to a full window scan
(`fallback_reason=no_cursor`) and rebuilds from FULL_CDC. That is correct and expected: the
layer is always rebuildable from the canonical one.
