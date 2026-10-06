# CDC → Kafka → Three-Layer Lakehouse — Live E2E

- **Date:** 2026-09-03, 09:47–10:28 UTC
- **Account:** `111122223333` · `ap-southeast-1` · `dev` · MSK ACTIVE, EMR `00g8g05ud5dj2u25`
- **Method:** controlled DML on reserved PKs in captured tables, traced through every layer
- **Evidence:** `artifacts/validation/final-e2e/cdc/`
- **Verdict:** **PARTIAL PASS.** 5 of 7 scenarios executed end to end and passed. **One P0
  defect was found, fixed and re-proven.** Two scenarios were not run — see §6.

## 1. What was executed

Controlled sequences on reserved PKs, well above each table's MAXPK, so no seeded business
row was touched.

| Table | PK | Sequence |
|---|---|---|
| `COREBANK.CUSTOMER` (Oracle) | 990001 | insert → update non-key → 2nd update → delete → recreate same PK |
| `dbo.app_user` (SQL Server) | 990001 | insert → update non-key → 2nd update → delete → recreate same PK |
| `COREBANK.ACCOUNT` (Oracle) | 990001 | insert → update → **delete** (stays deleted) |
| `COREBANK.ACCOUNT` (Oracle) | 990002 | insert → delete → **recreate same PK** |

The ACCOUNT pair exists because the pilot EOD job builds only the account fact, so
CUSTOMER deletes could never reach the EOD layer.

## 2. Scenario matrix

`E` = evidenced live. `n/a` = layer does not carry this table.

| # | Scenario | Source | Kafka | FULL_CDC | REALTIME | EOD | Recon |
|---|---|---|---|---|---|---|---|
| 1 | INSERT | **E** SCN 3087594 / LSN 0x2B…1D380003 | **E** off 64 / 41 | **E** `op=c` | **E** | **E** | **E** |
| 2 | UPDATE non-key | **E** SCN 3087601 | **E** off 65 | **E** `op=u` | **E** | **E** | **E** |
| 3 | SECOND UPDATE | **E** SCN 3087608 | **E** off 66 | **E** `op=u` | **E** | **E** | **E** |
| 4 | DELETE | **E** SCN 3087615 | **E** off 67 `d` + off 68 **tombstone** | **E** `op=d`, after IS NULL | **E** | **E** row removed | **E** |
| 5 | DELETE + RECREATE same PK | **E** SCN 3087622 | **E** off 69 | **E** `op=c` | **E** | **E** 990002 = 777.77 | **E** |
| 6 | Backward-compatible schema change | not run | — | — | — | — | — |
| 7 | Poison / incompatible record | not run | — | — | — | — | — |

## 3. Layer-by-layer results

### Source → Debezium
Both connectors `RUNNING`, task 0 `RUNNING`. SQL Server's own change table is the
independent check — 7 rows for PK 990001 with op codes `2,3,4,3,4,1,2` (insert, update
before+after ×2, delete, insert) and **strictly increasing fixed-width hex LSNs**, which is
exactly why `ordering.py` *validates* an LSN rather than zero-padding it. Oracle SCNs
increased monotonically across all five commits.

### Kafka
```
CUSTOMER  p0=67 p1=69 p2=64  ->  p0=67 p1=69 p2=70     +6 on p2 ONLY
app_user  p0=54 p1=55 p2=41  ->  p0=54 p1=55 p2=47     +6 on p2 ONLY
```
**Same-PK partition stability (§5.1)** holds on both engines: every event for the PK landed
on one partition and the other two never moved.

6 messages for 5 statements. Offset 68 carries `apicurio.key.globalId` but **no value
headers at all** — the null tombstone. That is the concrete proof of §5.7: a delete emits
both a `d` envelope (offset 67) *and* a tombstone (offset 68), and both reach the topic.

### FULL_CDC (canonical)
205 rows + 1 tombstone for CUSTOMER, 155 + 1 for app_user — matching the Kafka deltas
exactly. Across all 8 topics: **5,728 seeded + 10 controlled = 5,738**.

Row level for PK 990001: five rows, `c/u/u/d/c`, `position_primary` strictly increasing,
the delete carrying its before-image, `dv_event_id` and `dv_src_event_id` distinct on every
row, and **offset 68 absent** — the tombstone is counted, not ingested.

**Replay:** the identical job re-read all 360 rows and left `FULL_CDC_COUNT` at 360. Iceberg
recorded a snapshot with zero added-records. MERGE on `dv_event_id` is idempotent (§5.3).

### REALTIME
72-hour window, cutoff `2026-08-31 10:13:06` computed once and logged; 5,738 rows in
window; full-refresh `createOrReplace`, so a rerun reproduces the table. It holds 5,738
while FULL_CDC now holds 5,744 because it ran before the ACCOUNT scenarios — correct
point-in-time behaviour for a materialised window.

### EOD
`EOD_ROWS 321`, `distinct account_sk 321` — exactly one state per PK. The day is
**declared** closed, not merely populated: `ops.eod_watermark` row written *and* Iceberg tag
`EOD_2026-09-03` created, which is what the gate requires.

- 990001 — insert → update 2500.55 → **delete**: correctly **absent**
- 990002 — insert → delete → **recreate**: correctly **present** at 777.77

Reaching that took fixing a P0 defect. See §5.

### Reconciliation
Recomputed independently in Athena using the approved rule — latest event per PK by
`CAST(position_primary AS DECIMAL(38,0)) DESC, kafka_offset DESC`, deletes excluded, scoped
to the business date:

```
expected_from_full_cdc = 321
eod_rows               = 321      MATCH
```

## 4. Iceberg

14 snapshots on `full_cdc.cdc_events`; the replay appears as a commit that added zero rows.
On `curated.fact_account_daily_snapshot` the whole episode is preserved: `320` →
`322` (the defect) → `0` (partition cleared) → `321` (after the fix). **No snapshot was
expired; every piece of evidence is still queryable.**

## 5. Finding E1 (P0) — deleted rows survived into the certified EOD snapshot

**Evidence.** After the ACCOUNT scenarios, EOD produced **322** rows where 321 was correct,
and the curated fact contained:

```
990001 | 100001 | 2026-09-03 | 2500.55 | CERTIFIED     <- a DELETED account, certified
```

FULL_CDC held the delete correctly (`op=d`, `position_primary=3101545`, `payload_after IS
NULL`), so the loss happened in the EOD build.

**Root cause.** `spark/jobs/eod/job.py` derived the dedup key from `payload_after` alone:

```python
.withColumn("j", F.from_json(F.col("payload_after"), "ACCOUNT_ID string, ..."))
w = Window.partitionBy("j.ACCOUNT_ID").orderBy(position_primary.desc(), ...)
latest = acct.withColumn("rn", row_number().over(w)).filter((rn == 1) & (op != "d"))
```

A Debezium delete carries `after = null`, so its key is NULL and it lands in the **NULL
window partition** instead of its account's. The account's own partition then contains only
the insert and the update, `row_number() = 1` picks the **update**, and the `op != 'd'`
filter never sees a delete to remove — it is dead for its stated purpose.

**Impact.** Every deleted entity stays in the certified snapshot indefinitely, carrying its
last pre-delete values. For this mart that is a closed account still reporting a balance. It
fails as a wrong number, never as an error — precisely what §5.7 exists to prevent.

**Why it was never caught.** The job had never run against ACCOUNT data containing a delete.
Phase 14's driver used inline MERGEs and never invoked this job; the seeded workload's
deletes were on other tables.

**Fix (applied).** Key on `coalesce(payload_after, payload_before)`, so the delete returns to
its own partition, wins `row_number()` on its higher SCN, and is removed by the existing
filter.

**Proof.** `EOD_ROWS` 322 → **321**; 990001 absent; 990002 present; reconciliation 321 = 321.

## 6. What was NOT verified — and why

| Item | Status |
|---|---|
| Scenario 6, backward-compatible schema change | **NOT RUN** — see E2 below |
| Scenario 7, poison / incompatible record | **NOT RUN.** The repo gates this behind the typed phrase `RUN WORKLOAD 6` and warns the registry should reject it. Running it risks failing a connector task, and I would not spend the operator's window on that without their call. |
| Connect DLQ vs lakehouse quarantine | **NOT EXERCISED.** No poison record was produced, and `quarantine` holds 0 tables. Note the standing gaps: a source-connector DLQ is inert (G-P1-3) and the canonical ingest has no quarantine path at all (G-P1-1). |
| REALTIME late-arrival grace | **NOT TESTED.** Would need an event with `source_commit_ts` older than the window bound. |
| DQ suite | **NOT RUN** (`dq-check.sh`). |
| EOD late event / cutoff-watermark interaction | **PARTIAL.** The cutoff and the frozen COB tag are evidenced; a genuinely late event arriving after closure was not injected. |

### E2 (P1) — schema evolution is not actually supported by the canonical ingest (FIXED 2026-09-04)

`full_cdc/job.py` loaded writer schemas from a **static S3 export**, one schema per topic:

```python
def load_schemas(spark, uri): return json.loads(spark.read.text(uri)...)
schema = schemas[topic]
decoded = raw.select(from_avro(F.col("value"), schema).alias("v"))
```

Its own docstring claimed *"the writer schema comes from the registry by globalId"*, but the
code never read the registry and never used the `apicurio.value.globalId` header it
documented. A topic containing two schema versions therefore could not be decoded correctly,
and `schemas.json` went stale the moment any source DDL landed.

**This is why scenario 6 was not run:** adding a column would have produced messages whose
writer schema differs from the pinned export, risking silently mis-decoded rows in the
canonical layer.

#### The fix

Records are grouped by the globalId the producer actually stamped on them, and each group is
decoded with **its own** schema:

```python
raw = ... .option("includeHeaders", "true").load()
valued = raw.filter(col("value").isNotNull()).withColumn("global_id", global_id_col())
for gid in valued.select("global_id").distinct().collect():
    group_schema, provenance = select_schema(gid, topic, schemas)
```

Four decisions worth stating, because each has a wrong-looking alternative:

* **A globalId missing from the export is QUARANTINED, not decoded with the by-topic
  entry.** That entry is a different writer version by definition, and `from_avro` does not
  raise on a near-miss — it returns plausible wrong values. `SchemaNotExported` says the
  export is stale and a rerun will fix it; `AvroDecodeError` says the payload is broken and
  a rerun will not.
* **The PROJECTIONS are unioned, not the decoded envelopes.** The envelope struct differs
  between versions — that is what schema evolution *means* — while every projected column is
  an explicit scalar, so the union is total and needs no `allowMissingColumns`, which would
  quietly NULL a column that disagreed.
* **`by_topic` is kept** as the fallback for a record carrying no header, and because every
  recorded submission recipe predates the new export shape. `normalise_schema_export`
  accepts both.
* **`includeHeaders` is not optional.** Without it the `headers` column is never produced,
  every globalId reads NULL, and the whole topic degrades to the fallback — E2 behaviour
  again, invisibly. A test pins the flag.

The export is refreshed with `scripts/cdc-runtime.sh export-schemas --execute <bucket>`,
which runs `scripts/export-registry-schemas.py` **on the CDC runtime host** (Apicurio
listens on localhost:8080 there; the EMR workers have no route to it and opening 8080 for a
once-per-run read is the wrong trade). It exports **every version**, not the latest: a topic
holds records from every writer version that ever produced to it — old rows do not
re-encode themselves — so a latest-only export cannot decode the topic's own history.

**Still to prove live:** add a column to `COREBANK.ACCOUNT`, produce rows, refresh the
export, rerun the job, and expect two `FULL_CDC_DECODE … globalId=` lines for the topic.

### E3 (P2) — staged job code was stale and is not managed by Terraform

`artifacts/code/*.py` has no Terraform source mapping. The staged `full_cdc_job.py`,
`realtime_job.py` and `eod_job.py` all **lacked the UTC session-zone fix** that is in the
repo. They were re-staged from the repo before this run; without that, the E2E would have
tested code that no longer exists in version control.

### E4 (P2) — the EMR submission recipe existed nowhere

The first submission failed with `Failed to find data source: kafka`. EMR Serverless ships
the connector jars but not on the default classpath. The working recipe is now recorded in
`artifacts/validation/final-e2e/cdc/02-lakehouse-layers.txt`. No jar needed downloading.

### E5 (P1) — the EOD MERGE cannot retract

`WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT` has no delete arm, so a row that should
disappear from a rebuilt snapshot persists until its partition is cleared. The E1 fix
corrects which rows are *produced*; it cannot remove one already published. The partition
had to be deleted before the corrected build. A full rebuild of a business date needs
`insert_overwrite` semantics or an explicit partition delete.

## 7. Cost

~$0.35 of EMR Serverless across 9 job runs (one deliberate failure, one read-only probe),
plus Athena queries well under the 10 GiB workgroup cutoff. Platform baseline `$1.2340/hr`
ran throughout.
