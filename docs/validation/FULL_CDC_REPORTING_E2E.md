# Full platform end-to-end — 2026-09-20

**Status: PARTIAL — four of seven sections PROVEN LIVE (CDC, REALTIME, EOD, maintenance).
Reporting flows, Airflow execution and datamart validation did NOT run.**

Account 111122223333 · `ap-southeast-1` · env `dev` · evidence in
`artifacts/validation/phase-i/`.

## What was proven live

### §1 CDC — Kafka → FULL_CDC ✅

| table | rows | distinct `dv_event_id` | distinct `dv_src_event_id` |
|---|---|---|---|
| `account` | 350 | 350 | 350 |
| `branch` | 4 | 4 | 4 |
| `customer` | 210 | 210 | 210 |
| `loan` | 50 | 50 | 50 |
| `transaction` | 2,050 | 2,050 | 2,050 |
| **total** | **2,664** | **2,664** | **2,664** |

`ops.cdc_event_index` holds **2,664** — the index and the tables agree exactly. Rows equal
distinct transport identity equals distinct logical identity on every table: **no duplicates
of either kind**.

**Restart idempotency, proven by accident and then on purpose.** Run 3 committed its data and
then died on its own summary line (see defects below). Run 4 started from the same
identity-keyed checkpoint, found nothing new, and added **0 rows** — `account` was 350 before
and 350 after. That is the ADR-073 checkpoint contract working live.

**Phase B control plane, live for the first time** — `ops.streaming_app_state`:

| app_id | status | restart_count |
|---|---|---|
| `full-cdc-oracle` | `STOPPED` | **2** |

`restart_count = 2` is read back from the table, not from the process: run 3 was attempt 1,
run 4 attempt 2. A clean `available_now` drain ends `STOPPED`, not `ABSENT`.

**Job runs**: `00g8ttja8u24u827` (provision, SUCCESS), `00g8ttlamg3oag27` (FAILED, KMS),
`00g8ttmtou1hh027` (FAILED, packaging), `00g8ttogndv8j827` (FAILED after commit, JSON),
`00g8ttqj4bgrbg27` (**SUCCESS**), `00g8ttsc72ut7g27` (FAILED, capacity).

### Control plane provisioned ✅

`ops.eod_info` and `ops.eod_run_hist` now exist in Glue (schema-10 plan deployed,
`cdccfg-f8f32421247cc349`). Both hold 0 rows: no close has run.

## Three live defects found and fixed

**1. KMS: the Spark role could not read its own jars.**
`kafka-dev-lab-dev-spark-stream is not authorized to perform: kms:Decrypt on key/2746e5f4`.
The five Kafka jars were uploaded on 2026-09-10 under the **previous** lake CMK; the role's
policy grants Decrypt on the current one only. The failing object's size in the error
(432,340 bytes) matched `spark-sql-kafka-0-10_2.12-3.5.1.jar` exactly. Fixed by re-encrypting
the five jars under `alias/kafka-dev-lab-dev-lake`. **The underlying divergence remains**: the
lake holds objects under at least four CMKs and the role policy names one.

**2. Packaging: `ModuleNotFoundError: No module named 'full_cdc_job'`.**
`stream_job.py` imports `full_cdc_job`, which is what the *entrypoint* upload renames
`job.py` to on S3 — but the framework zip stored only `per_table.py` at its root. The
comment above `SHARED_MODULES` warns about exactly this failure and had been fixed for
`per_table` alone. `cdc-deploy-code.sh` now takes `path:zip_name`, so an import name and a
file name can differ deliberately.

**3. My own Phase B defect: `TypeError: Object of type datetime is not JSON serializable`.**
Phase B added a source watermark to `stats`, and the end-of-run summary does
`json.dumps(stats)`. The job committed its data, wrote its state row, and then failed on its
own log line — the worst shape of failure, because the run reports FAILED while its work
succeeded. Fixed with `default=str`.

## What is NOT proven, and why

| § | area | status |
|---|---|---|
| 1 | schema add, logical replay, delete-recreate as a *scripted* live sequence | the data contains I/U/D from the seeded workload, but no controlled mutation was run in this window |
| 2 | REALTIME materialization | **not run** |
| 3 | EOD close, `eod_info`, `eod_run_hist`, DQ, certification | **not run** — blocked by EMR capacity |
| 4 | reporting flows (EOD model, AUTO_CORRECT, FULFILL, STREAM_BATCH, STREAMING_RT) | **not run** |
| 5 | Airflow DAG execution, turns, branch isolation | **not run**; DAGs are deployed and paused |
| 6 | maintenance compaction on live tables | **not run** |
| 7 | datamart facts/dims/grain/serving | **not run**; `mart` and `curated` hold 0 tables |

**The blocker for §3 onward**: `ApplicationMaxCapacityExceededException: [disk: 100 GB]`. The
EMR application's `maximumCapacity` is 16 vCPU / 64 GB / **100 GB disk**, and the ingest's
workers still held capacity when the EOD close was submitted. This is a settings and
sequencing constraint, not a defect in the EOD code. Either wait for the 15-minute auto-stop
between jobs, raise `maximumCapacity`, or submit with a smaller profile (`small` in ADR-075
requests 1 driver + 1 executor).

## To continue

```bash
export AWS_PROFILE=my-aws-profile AWS_REGION=ap-southeast-1
LAKE=kafka-dev-lab-dev-lake-111122223333

# wait for the application to idle out, then:
bash scripts/emr-submit.sh eod-account s3://$LAKE/artifacts/code/eod_engine.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse \
  --table oracle.coredb.corebank.account --cob-date 2026-09-19

bash scripts/emr-submit.sh realtime-account s3://$LAKE/artifacts/code/realtime_engine.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse \
  --table oracle.coredb.corebank.account

python3 scripts/cdc-stream.py status        # the streaming control plane
```

After the close, `ops.eod_info` should hold one row for `(oracle.coredb.corebank.account,
2026-09-19)` with `certification_status = CERTIFIED` and a watermark pair, and
`ops.eod_run_hist` one row per attempt.

## Cost

Six EMR Serverless job runs, minutes each, on an ARM64 application. Budget at the end of this
window: **$51.77 actual / $95.58 forecast** of $100. MSK remains the dominant cost and is
unaffected by this work.


---

# Continuation run — 2026-09-20 16:2xZ

## §2 REALTIME ✅ · §3 EOD ✅ · §6 maintenance ✅

| layer | evidence |
|---|---|
| FULL_CDC (after maintenance) | `account` **350 rows / 350 distinct dv_event_id** — unchanged by compaction |
| REALTIME | **350 rows**; `ops.realtime_run` SUCCEEDED, frozen upper bound `2026-09-20 16:26:25` |
| EOD | snapshot **320 rows / 320 distinct keys** — one state per PK |
| `ops.eod_info` | COB `2026-09-20`, **CERTIFIED**, 320 rows, DQ PASS, recon PASS, watermark `10:50:03` |
| `ops.eod_run_hist` | 4 attempts retained, including three `BUILT_NOT_CERTIFIED` failures |
| maintenance | `e2e-maintenance-2` SUCCESS; FULL_CDC row count and identity unchanged |

**The EOD control plane behaved exactly as ADR-076 specifies, including when it refused.**
The first three closes targeted COB 2026-09-19, whose window is genuinely empty (every
captured event carries today's commit time). Each produced:

```
EOD_CLOSED ... rows=0 dq=FAIL status=BUILT_NOT_CERTIFIED
EOD_NOT_CERTIFIED ... EMPTY_WINDOW: zero events inside the cutoff
EOD_WATERMARK_HELD ... eod_info keeps None
```

No certification, no watermark, and all three attempts kept in `eod_run_hist`. The successful
close of COB 2026-09-20 then wrote `eod_info` with the pair.

## Three more live defects found and fixed

**4. `ApplicationMaxCapacityExceededException [disk: 100 GB]` with the application idle and
no job running.** EMR Serverless enables **dynamic allocation by default**, so
`spark.executor.instances` is a starting point, not a bound: a shuffle asks for more
executors, each claiming 20 GB of disk. The failure was identical at 3 executors and at 1,
which is what ruled out a sizing mistake. `emr-submit.sh` now sets
`spark.dynamicAllocation.enabled=false` and caps `maxExecutors`, and takes
`DRIVER_*`/`EXECUTOR_*` overrides.

**5. `--skip-readiness` was a dead flag.** `eod_engine.py` declared it and `main()` never
passed it to `close_table`. The same shape as Phase B's `--checkpoint required=True`: a flag
that parses and does nothing is worse than a missing one, because the operator believes the
run was configured.

**6. Maintenance requires `--metrics` and says so.** The planner is metric-driven by design
(ADR-068), so the job refuses rather than compacting blindly on a schedule. Running
`cdc-maintenance.py measure` first produced the input; the measure step also reports
`orphan_file_count = 0` as **"not measured"** rather than "none found", and orphan removal
stays off accordingly.

## Still NOT run

| § | area | why |
|---|---|---|
| 4 | reporting flows (EOD model, AUTO_CORRECT, FULFILL, STREAM_BATCH, STREAMING_RT) | needs `dbt build` on EMR against `mart`/`curated`, which hold 0 tables |
| 5 | Airflow DAG execution, turns, branch isolation | DAGs deployed and paused; enabling them is a scheduling decision |
| 7 | datamart facts/dims/grain/serving/Athena expected values | depends on §4 |

`FULL_DATA_PLATFORM_E2E_PASS` is **not** claimed. Four sections are proven with recorded
query results and job-run ids; three did not execute.

## Job runs, continuation

`00g8ttv9ksp0v027` capacity · `00g8tu16hiuje027` empty-window refusal ·
`00g8tu3lrktbl827` **EOD SUCCESS** · `00g8tu5j4jnd9827` **REALTIME SUCCESS** ·
`00g8tu77su243o27` missing `--metrics` · `00g8tu99mbnqm827` **maintenance SUCCESS**

---

## Section 6 — dbt on EMR Serverless (2026-09-21)

**Status: runtime PROVEN, mart leg BLOCKED upstream.**

### What now runs

`dbt build` executes inside the EMR Serverless Spark driver (`method: session`, ADR-044).

| fact | value |
|---|---|
| job run | `00g8tuu76hunn027` **SUCCESS** |
| dbt | 1.9.11 core / 1.9.3 spark |
| driver Python | 3.9.21, ARM64, `emr-7.2.0` |
| wheelhouse | `s3://<lake>/artifacts/dbt/wheelhouse.zip` — 53 wheels, `cp39`/`aarch64` + pure |
| project | `s3://<lake>/artifacts/dbt/dbt-project.zip` — 38 files |
| entrypoint | `s3://<lake>/artifacts/code/emr_dbt_bootstrap.py` |

`emr_dbt_bootstrap.py` is now an ENTRYPOINT and `run_dbt_job.py` a SHARED MODULE in
`cdc-framework.zip`. That split is not cosmetic: the bootstrap imports `run_dbt_job`, and
dbt does not exist in the image until the bootstrap has installed the wheelhouse, so
`run_dbt_job.py` cannot be an entrypoint — it is not importable until its own dependency
has been staged by the thing that imports it.

### Defects found

**7. The dbt version was never pinned.** `docs/VERSIONS.md` row 104 said "compatible pair"
and nothing else, while `dbt_project.yml` requires `>=1.9.0,<1.10.0`. The first wheelhouse
was built at 1.8.9 and the run failed on dbt's own version check (`00g8turrhf2ipg27`). Same
defect class as ADR-070: a derived value — here, "which dbt the wheelhouse contains" —
maintained by hand in a second place. Now pinned in `VERSIONS.md` with the run id.

**8. Two dbt-core 1.8 dependencies ship no `cp39`/`aarch64` wheel** (`logbook<1.6`,
`minimal-snowplow-tracker<0.1`). Both are pure Python with an optional C extension, so the
build re-tags them `py3-none-any`. dbt-core 1.9 drops both, so the 1.9 wheelhouse resolves
with no sdist step at all — recorded because it will resurface on any pin that moves back.

**9. `spark.archives` + `venv-pack` is the WRONG mechanism here**, and
`emr_dbt_bootstrap.py`'s own docstring says so: venv-packing needs a `cp39`/`aarch64`
interpreter to build against, which no machine in this project has. `pip download
--platform manylinux2014_aarch64 --python-version 3.9` needs only pip. A venv archive was
built and uploaded before reading that docstring; it has been deleted from S3.

### BLOCKER — the CURATED layer has no producer

`dbt build` cannot populate `mart`, and the reason is upstream of dbt.

* dbt's models read `source('curated', ...)`: `fact_transaction`,
  `fact_account_daily_snapshot`, `fact_digital_engagement_daily`.
* `reporting/layers.yaml` (ADR-033, the only file where a physical database name appears)
  resolves `CURATED` to `kafka_dev_lab_dev_curated`. **That database has 0 tables.**
* **Nothing in the repository writes it.** `grep` for a `writeTo`/`saveAsTable` against the
  CURATED layer across `spark/`, `scripts/` and `airflow/dags/` returns nothing.
* The Kimball builder that would produce those facts — `spark/dimensions/build_kimball.py`
  (Sessions 09/10) — reads `{catalog}.snapshot.banking_account` and
  `{catalog}.snapshot.banking_transaction` and writes `{catalog}.mart.*`. Neither name
  exists in the deployed platform: the EOD layer holds
  `kafka_dev_lab_dev_snapshot.eod_oracle_coredb_corebank_account`, whose business columns
  are nested inside `payload_after`. The builder predates the config-driven naming and was
  never migrated onto it.
* A third spelling disagrees with both: `reporting/jobs/mart_account_balance_daily.yaml`
  declares `upstream_dataset: kafka_dev_lab_dev_snapshot.fact_account_daily_snapshot`,
  i.e. the curated fact in the EOD database. Three conventions for one dataset.

`s3://<lake>/warehouse/curated/fact_account_daily_snapshot/` and two `mart/` tables DO hold
Iceberg metadata, but it is dated **2026-09-10** — before the stack was destroyed and
re-applied. Registering those into Glue would make `dbt build` succeed against an
11-day-old curated fact while the EOD snapshot it is supposed to summarise was closed
2026-09-20. That is the failure `spark/facts/periodic_snapshot.py` warns about in its own
docstring: complete, plausible, and wrong. **Not done deliberately.**

Reaching the mart therefore needs a decision, not a deployment step: which EOD tables map
to which conformed entity, how `payload_after` is flattened, and which of the three
spellings is authoritative. That is an ADR and a new curated build job.

---

## Section 7 — the full platform, end to end (2026-09-20)

**`FULL_DATA_PLATFORM_E2E_PASS` — 7 of 7 sections live.**

### The chain, with run ids

| stage | evidence |
|---|---|
| EOD closes, 5 Oracle tables | `00g8tvfs1v03og27` branch 4, `00g8tvhe30unto27` customer 200, `00g8tvj000ig3g27` transaction 2050, `00g8tvkmv2n0o827` loan 50, + account 320 |
| CURATED build | `00g8u02d5bug2027` SUCCESS — 8 entities, 9 dimensions, 3 facts |
| `dbt build` | `00g8u05ko9p5ng27` **PASS=56 WARN=0 ERROR=0 SKIP=0** |
| EOD flow | `00g8u0anhld1h027` SUCCEEDED, watermark advanced |
| AUTO_CORRECT flow | `00g8u0cach6jlg27` SUCCEEDED — 3 dates, 1 ESCALATED (refused to overwrite CERTIFIED) |
| FULFILL flow | 3 runs, 2026-09-18/19/20, all SUCCESS |
| STREAM_BATCH flow | `00g8u0i07m8ig027` SUCCEEDED, committed to 18:38:07Z |
| Airflow, unprompted | `streambatch-mart_account_balance_daily-1789930684` and `-1789931048` SUCCESS — submitted by the `*/10` schedule, not by hand |

### Athena, measured independently of dbt

| check | result |
|---|---|
| `mart_account_balance_daily` grain | 320 rows / 320 distinct `(account_sk, business_date)` |
| `fact_transaction` PK | 2050 rows / 2050 distinct `transaction_id`, 2 business dates |
| unresolved dimensions | **0** rows with `account_sk = -1` |
| unresolved FX | **0** rows with `amount_base IS NULL` |
| `mart_account_balance_monthly` / `_risk_daily` | 320 each |
| the four digital marts | 0 — honest zeros, see below |

### Defects found and fixed

**10. `--skip-readiness` could certify a day that had not happened.** Its own help text has
always said "Never for closing the current day", and nothing enforced it. `ops.eod_info`
held `(oracle.coredb.corebank.account, 2026-09-20)` as **CERTIFIED** while that COB's cutoff,
2026-09-21T00:00Z, was still six hours in the future — a number published as final that a
later close could still change. `close_table` now withholds the completion marker whenever
the cutoff has not passed, whatever the flag says. The snapshot is still built and written;
only the marker is held. Three tests, and it fired live on the first re-close:
`EOD_DAY_OPEN ... built, NOT certified`.

**11. `scd2.unknown_member_row` could not build a numeric natural key.** It wrote the literal
`"UNKNOWN"` into every natural-key column. Every dimension it was written against had a
string key; `dim_customer.customer_id` is a `bigint` once conformed, and the build died with
`LongType() can not accept object 'UNKNOWN'` after reading the whole history. Its docstring
already promised a row matching the schema exactly.

**12. `dim_account` never carried `customer_sk`.** Both `transform.build_fact_transaction`
and `periodic_snapshot.build` read it off the account dimension — the latter asks for it by
name — and no dimension build has ever produced it. `build_kimball.build_dimensions` builds
each dimension independently from its own source, so that path would fail here too: it has
evidently never run against a real fact.

**13. The Airflow node was wired to artifacts from 2026-09-10.**
`REPORTING_DBT_ENTRYPOINT` and `spark.submit.pyFiles` pointed into `artifacts/dbt/`, whose
objects predate the CMK rotation, so every scheduled run would have failed on `kms:Decrypt`
with nothing in the DAG to explain why. It also carried a SECOND spelling of the catalog
binding (`spark_catalog` via `SparkSessionCatalog`) where the operator script registers
`glue_catalog` and sets `spark.sql.defaultCatalog`. Both work, which is the problem. The
values file now matches `reporting-live-run.py::_spark_conf` byte for byte.

**14. `emr_dbt_bootstrap.py` took a `--catalog` argument and never used it.** dbt issues
unqualified `use <schema>`, which went to the session's built-in `spark_catalog` and failed
with `Failed to get database kafka_dev_lab_dev_curated` — a Hive warning for a database that
exists perfectly well in Glue.

**15. PyYAML is not in the EMR Serverless image.** The curated job died on
`ModuleNotFoundError: No module named 'yaml'` after acquiring capacity. `layers.yaml` and
`entities.yaml` are now COMPILED to JSON at deploy time, which is what the table plan has
always done (ADR-034).

### What is honest, not broken

* **`processing_status = PROVISIONAL_NRT`.** COB 2026-09-20 had not closed when this ran, so
  nothing is CERTIFIED and the marts say so. `assert_mart_status_is_weakest_input` passes
  *because* of this. Re-run the closes after 2026-09-21T00:00Z and the same chain certifies.
* **The four digital marts are 0.** The SQL Server connector is `RUNNING` and its five topics
  exist, but FULL_CDC holds 0 rows for all of them, so `dim_channel` and `dim_merchant` hold
  only the unknown member. The star schema is structurally complete and half-empty, and it
  reports that rather than hiding it.
* **`fact_digital_engagement_daily` is a degenerate empty table.** `dbo.digital_event` has no
  `channel_code`, which that fact joins `dim_channel` on. While the source is empty the
  column is supplied as a typed NULL so dbt has its source; the moment real events arrive the
  job refuses rather than inventing a join key.

### Still open

* `ops.eod_info` still holds the stale `CERTIFIED` row for account/2026-09-20, written before
  defect 10 was fixed. Correcting it needs one Athena `UPDATE` against shared ops state,
  which this session was not permitted to run.
* The four flow DAGs are UNPAUSED and `datamart_stream_batch` runs every 10 minutes,
  submitting a real EMR job each time. Pause with
  `airflow dags pause datamart_stream_batch` when the demo window closes.

---

## Section 8 — Phase J: production review (2026-09-20)

### The readiness checklist, audited

| # | Item | Verdict |
|---|---|---|
| 1 | Kafka→FULL_CDC production mode truly long-running | **PARTIAL** — `00g8u2gkdjulso27` ran resident 798 s as `mode=STREAMING`, state recorded `continuous_microbatch/production`; never committed a batch (Kafka drained). P2-0 |
| 2 | lab mode explicitly finite/cost-aware | **PASS** — `available_now` drains and exits; profile-selected |
| 3 | no production dependency on `run_seconds` | **PASS** — refused under `profile: production`, asserted by test |
| 4 | REALTIME periodic config-driven | **PASS** — `realtime_policy` in the plan; DAG groups by schedule |
| 5 | REALTIME != STREAM_BATCH | **PASS** — layer vs flow mode; documented and separately tested |
| 6 | EOD T-1 cutoff timezone-safe | **PASS** — `business_timezone`, UTC pinned (ADR-024) |
| 7 | source readiness before EOD certification | **PASS** — proven live: `WAITING_SOURCE ... 842 min behind the cutoff` |
| 8 | `eod_info` current state exists | **PASS** |
| 9 | `eod_run_hist` audit exists | **PASS** — retains failed attempts |
| 10 | model readiness checks same COB/certification | **PASS** — `EodTableReadyGate` names the missing dataset |
| 11 | dbt refs not duplicated | **PASS** — one node per model; 56 nodes resolve |
| 12 | no DAG-per-table explosion | **PASS** — 3 cadence DAGs for 10 tables |
| 13 | new YAML appears without DAG Python edit | **PASS** — dynamic task mapping over the plan |
| 14 | EOD/AUTO_CORRECT/FULFILL/STREAM_BATCH tested | **PASS** — all four SUCCEEDED live |
| 15 | STREAMING_RT independently tested | **PARTIAL** — unit-tested, gated off by cost. P2-1 |
| 16 | failed run never advances successful watermark | **PASS** — `EOD_WATERMARK_HELD`, observed |
| 17 | maintenance compacts small files safely | **PASS** — metric-driven, refuses without `--metrics` |
| 18 | snapshot/manifest retention controlled | **PASS** — `expire_snapshots_days: 7`, `remove_orphan_files_days: 3`, ordering enforced |
| 19 | data layout benchmark documented | **PASS** — `DATA_LAYOUT_BENCHMARK.md` |
| 20 | full E2E evidence exists | **PASS** — sections 6–8 |
| 21 | docs/runbooks current | **PASS** — 4 written this phase, 12 total |

### Defects found and fixed in Phase J

**16. `business_date()` read a var nothing passes.** The macro read `var("business_date")`;
`run_dbt_job.VAR_CONTRACT` sends `cob_date` and `date_of_data`. It silently took the
`dbt_project.yml` default of `1970-01-01`, so three marts filtered a real business date
against 1970 and returned **zero rows with every test green** — an empty table violates no
uniqueness rule and no not-null rule. `flow_context.sql` exists to prevent exactly this
("a typo in `var('cob_dat')` returns the default silently"); this was the one raw `var()`
call never moved behind an accessor. A new test now scans every macro and model.

**17. `mart_channel_engagement_daily` declared a grain it did not hold.** It selected a
fact at `(customer_sk, business_date, channel_sk, device_type)` straight into a mart whose
unique key is `(channel_sk, business_date)`. Invisible for as long as the digital fact was
empty — the grain test passed on zero rows.

**18. An incremental model cannot be fixed by fixing its SQL.** The merge key decides which
rows to *update*; rows written under the old grain stay. `--full-refresh` added to
`run_dbt_job` and forwarded by the bootstrap.

**19. `emr-submit.sh` could not submit a STREAMING job.** The one documented way to start the
production ingest by hand could not start it: a BATCH submission is killed at
`executionTimeoutMinutes`. EMR also rejects a timeout of `0` for STREAMING — it must be
absent.

**20. A correct open-day close reported FAILED.** The exit code was 1 unless every table
certified, so six `eod-*` runs showed red in the console having done exactly the right thing.
Now: an open-day build requested explicitly with `--skip-readiness` exits 0 and says
`EOD_OPEN_DAY_BUILD`; without the flag the same outcome is still a fault, because it means a
schedule fired before its own cutoff.

**21. An invalid certification could not be withdrawn.** Fixed as self-healing rather than by
hand — see P0-1.

### The digital half, filled

`full-cdc-sqlserver` had five live topics holding **3,344 events** and no state row: the app
had never been run. One submission ingested all of it, and
`fact_digital_engagement_daily` went from a degenerate empty table to 50 rows, `dim_channel`
from 1 (unknown only) to 5, `dim_merchant` to 51. Four marts that had been honest zeros now
hold data.

`dbt build` after: **PASS=56 WARN=0 ERROR=0 SKIP=0** (`00g8u203qbis8o27`), all seven marts
populated.

---

## Section 9 — re-proven on the rebuilt stack (2026-09-23)

The 20 Sep evidence was against infrastructure that no longer exists: the stack was destroyed
21 Sep and re-applied 23 Sep with a new MSK cluster, a new lake CMK and an empty Glue
catalog. **Everything below was re-run on the current deployment.**

| stage | evidence |
|---|---|
| provision | `00g9058b6f72n027` — 10 FULL_CDC / 10 EOD / 7 REALTIME / 6 ops |
| Kafka → FULL_CDC | `00g90657pppsag27` + `00g9066plfi47g27` — **5,828 events, 5,828 distinct `dv_event_id`** |
| schema evolution | export holds 28 artifacts = 14 topics x (Key + Envelope), i.e. **one version per topic**. Per-record `globalId` selection is wired; the multi-version case is unit-tested, not yet live (see the correction below) |
| REALTIME | `00g908q71nuih027` — 320 rows / 320 keys; window frozen at run start, 72h + 24h grace + 168h retention |
| EOD (10 tables) | `00g906930a2p3827` — `closed=10 certified=0 rows=5828`, every table `dq=PASS recon=PASS` |
| CURATED | `00g906biqj70k027` — 8 entities, 9 dimensions, 3 facts |
| dbt | `00g906di31908g27` — **PASS=56 WARN=0 ERROR=0 SKIP=0** |
| EOD flow | `00g906fp600aj827` SUCCEEDED |
| AUTO_CORRECT | `00g908helvjqeo27` SUCCEEDED — 1 date ESCALATED, refused to overwrite CERTIFIED |
| FULFILL | 3 dates 2026-09-21..23, all SUCCEEDED |
| STREAM_BATCH | `00g908o9rug1no27` SUCCEEDED, committed to 13:55:51Z |
| maintenance | `00g908tsgbcetg27` SUCCEEDED — planner reported nothing due and did nothing |
| **Airflow** | unpaused 14:08Z; the `*/10` schedule submitted `streambatch-...-1790172757` at 14:12Z → **SUCCESS**. Scheduler-driven, not by hand |

Athena, measured independently of dbt:

| check | result |
|---|---|
| `fact_transaction` PK | 2,000 rows / 2,000 distinct `transaction_id` |
| unresolved `account_sk` | **0** |
| unresolved `amount_base` | **0** |
| `mart_account_balance_daily` grain | 320 / 320 distinct `(account_sk, business_date)` |
| `mart_customer_360_daily` grain | 200 / 200 distinct `(customer_sk, business_date)` |

`processing_status = PROVISIONAL_NRT` throughout, correctly: COB 2026-09-23 had not ended.
The three channel marts are 0 because the seeded transactions and digital events are dated
2026-01-01, so no row falls on the COB — an honest zero, not a missing join.

### Defects found and fixed

**22. A stale checkpoint outlived its stack — twice over.** Covered in
`FULL_CDC_STREAMING_RUNBOOK.md` §4 and P2-8. The KMS failure was the lucky part; the
dangerous part was that the checkpoint claimed offsets from a **different MSK cluster**,
and resuming would have silently skipped the first several hundred events of every
partition.

**23. Job artifacts were stranded on retired CMKs.** 50 of 71 objects under `artifacts/`
were on four different old keys, so the ingest failed on `kms:Decrypt` for the Kafka jars —
`spark-sql-kafka-0-10_2.12-3.5.1.jar` first, then the next stale object, one at a time.
Fixed by re-encrypting the whole prefix onto the current key. The lake as a whole still holds
**7 CMKs across 5,557 objects** (P2-7); `reencrypt-lake-cmk.sh reencrypt --execute` converges
it and is operator-gated.

**24. `provision_cdc_tables.py` is dry-run by default.** Its first run printed
`CDC_PROVISION_DRIFT` for all 42 tables and created none. Correct per APPROVAL_GATES, and
worth knowing: the job reports SUCCESS either way.

### The one thing NOT observed

**No certified EOD close.** Every event in this lab was committed at 2026-09-23 10:27:59Z
by the connector snapshot, so the only COB containing data is 2026-09-23, whose cutoff
(2026-09-24T00:00Z) had not passed. The open-day guard therefore withheld certification —
which is the guard working.

What that means precisely: the certification **refusal** path is proven live, and the
**success** path is proven only by `spark/tests/test_eod_engine_spark.py` against real
Iceberg tables. It becomes provable after 00:00Z with no code change:

```bash
bash scripts/emr-submit.sh eod-all s3://$LAKE/artifacts/code/eod_engine.py eod \
  --plan s3://$LAKE/artifacts/cdc/table-plan.json --warehouse s3://$LAKE/warehouse \
  --table ALL --cob-date 2026-09-23
# then, expect certification_status = CERTIFIED for all ten:
SELECT table_id, certification_status, row_count, watermark_ts
FROM kafka_dev_lab_dev_ops.eod_info WHERE cob_date = DATE '2026-09-23';
```

Note there is no `--skip-readiness` on that command: after the cutoff the source-readiness
gate has real evidence to evaluate, and should be allowed to.
