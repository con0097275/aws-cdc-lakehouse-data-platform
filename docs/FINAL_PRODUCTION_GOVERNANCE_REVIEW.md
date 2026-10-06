# Final Production + Governance Readiness Review

- **Date:** 2026-09-03
- **Scope:** CDC/Kafka contracts · canonical + derived layers · reporting framework ·
  data governance · AI governance · business impact
- **Account:** `111122223333` · region `ap-southeast-1` · profile `my-aws-profile` · env `dev`
- **AWS mutation:** **none.** Read-only `describe`/`list`/`head`/`get` only. No
  `terraform plan`, no `apply`, no resource started.
- **Spend for this review:** **$0.00**
- **Branch:** `session-02-prerequisites` · HEAD `baa348e`

## 1. Verdict

**`READY_FOR_LIVE_TEST_PLANNING` — not ready for a certified live run.**

The correctness engineering in this repository is unusually strong: source-position
ordering, point-in-time feature joins, the certification tier ladder, the checkpoint/
warehouse separation and the Iceberg maintenance floor are all implemented as *enforced
code* rather than documented intent, and each carries the reasoning for why the obvious
alternative is wrong.

Two defects found here were **security- or correctness-critical, silent, and are now
fixed**. Both had the same shape — a control that was believed to hold, held only for the
inputs someone thought to write a test for.

What blocks a certified live run is not the code that exists. It is that the platform's
central execution mechanism (`dbt build`) has never run anywhere, the canonical ingest path
has no poison-record path at all, two of the five reporting modes cannot be dispatched, and
the whole stack is currently destroyed.

| Area | Verdict |
|---|---|
| 1 CDC / Kafka contracts | **PARTIAL** — contracts excellent; poison-record path absent on the live path |
| 2 Canonical / REALTIME / EOD semantics | **PARTIAL** — semantics correct; UTC guard was missing (fixed) |
| 3 dbt / coordinator / five modes | **PARTIAL** — 3 of 5 modes dispatchable; `dbt build` never executed |
| 4 Data governance | **PASS** — registry-driven, single-source, enforced |
| 5 AI governance | **PARTIAL** — strong design, one proven bypass (fixed), generation unvalidated |
| 6 Business impact | **PARTIAL** — deterministic layer proven; 1 of 6 marts, no dimensions |

## 2. Findings

24 findings. **7 fixed in this review** (software or docs, no AWS mutation).
Ids are stable and referenced from the code that was changed.

| Id | P | Area | Finding | State |
|---|---|---|---|---|
| G-P0-1 | P0 | 2 | All three canonical EMR jobs omit `spark.sql.session.timeZone=UTC` | **FIXED** |
| G-P0-2 | P0 | 5 | Unqualified table reference bypasses both SQL allow-lists | **FIXED** |
| G-P1-1 | P1 | 1 | Canonical FULL_CDC ingest has no quarantine/DLQ path | FIXED 2026-09-04 |
| G-P1-2 | P1 | 5 | `assert_no_secrets(redact(x))` raises on correctly redacted text | **FIXED** |
| G-P1-3 | P1 | 1 | Connector DLQ is inert on a source connector; poison records drop silently | **DOCUMENTED** |
| G-P1-4 | P1 | 2 | EOD ranking is Oracle-SCN-only; a SQL Server LSN degrades it to `kafka_offset` | **FIXED** |
| G-P1-5 | P1 | 2 | `stream` database called "DEPRECATED" while it holds the ACTIVE REALTIME table | **FIXED** |
| G-P1-6 | P1 | 3 | Coordinator cannot dispatch AUTO_CORRECT or FULFILL | OPEN |
| G-P1-7 | P1 | 3 | `dbt build` has never executed anywhere | OPEN |
| G-P1-8 | P1 | 5 | `AI_PLATFORM_STATE.md` claims "17/17 live"; re-run gives 8/10 + agent gate FAIL | **FIXED** |
| G-P2-1 | P2 | 1 | D5 null guard examined one field and could never fire | **FIXED** |
| G-P2-2 | P2 | 1 | Superseded L1 job crashes on a delete tombstone, outside its own guard | DOCUMENTED |
| G-P2-3 | P2 | 1 | Superseded L1 job collects a 100k-row micro-batch to the driver | DOCUMENTED |
| G-P2-4 | P2 | 1 | FULL_CDC re-reads every topic from `earliest` each run; no checkpoint | OPEN |
| G-P2-5 | P2 | 4 | `auto_destroy_after` is 9 days expired; correcting it replaces the network | OPEN |
| G-P2-6 | P2 | 4 | Budget is $100 live and in tfvars, $30 in `CLAUDE.md` / ADR-030 | OPEN |
| G-P2-7 | P2 | 3 | Five test files cannot share a JVM with the suite; full run reports 61 errors | OPEN |
| G-P2-8 | P2 | 6 | 1 of 6 marts materialised; no dimensions; 4 business dates with a hole | OPEN |
| G-P2-9 | P2 | 4 | Four enabled lake CMKs; next apply mints a fifth and orphans 660 objects | OPEN |
| G-P2-10 | P2 | 3 | Phase 14 bypassed all five mode flow modules; their logic is runtime-unproven | OPEN |
| G-P3-1 | P3 | 5 | `enforce_limit` emits a duplicate LIMIT for `LIMIT n OFFSET m` | OPEN |
| G-P3-2 | P3 | 1 | Dead inverted statement in the superseded L1 `write_batch` | DOCUMENTED |
| G-P3-3 | P3 | 4 | Two Terraform resources own `logs/airflow/`; the plan never converges | OPEN |
| G-P3-4 | P3 | 2 | FULL_CDC `event_id` scheme changed; old rows need `--backfill-identity` | OPEN |

---

## 3. P0 findings

### G-P0-1 — the three canonical jobs do not pin the UTC session time zone

**Evidence.** `spark/jobs/full_cdc/job.py`, `spark/jobs/eod/job.py` and
`spark/jobs/realtime/job.py` each build a `SparkSession` with catalog, warehouse, io-impl
and extensions — and no `spark.sql.session.timeZone`. Sixteen other Spark entry points in
the repository do set it, and the superseded `spark/jobs/l1_stream/job.py:92` sets it with
the comment *"a non-UTC session zone would partition rows into the wrong day"*. The three
jobs that actually run in production are the three that lacked the guard.

**Impact.** Every date in the canonical path is derived in the session time zone:

| Job | Expression | What shifts |
|---|---|---|
| full_cdc | `to_date(from_unixtime(source.ts_ms/1000))` | `event_date` — the Iceberg partition |
| eod | `to_date(source_commit_ts)`, `--business-date` filter | which events a certified day contains |
| realtime | `current_timestamp() - INTERVAL N HOURS` | the rolling window bound |

ADR-024 makes UTC normative precisely because this fails as *wrong numbers*, never as an
error: rows land one partition off, EOD certifies a different set of events than the cutoff
names, and REALTIME and EOD rank against different clocks — which destroys the symmetry
`realtime/job.py`'s own docstring relies on ("a variance means late data, not divergent
logic"). On `ap-southeast-1` a UTC+7 default moves the day boundary by seven hours.

**Why it survived review.** The setting is invisible when the JVM default happens to be UTC,
which it usually is on EMR Serverless. It would have produced correct results in testing and
wrong ones the first time the runtime default differed.

**Minimal fix (applied).** One config line per job, each with the reason it is there.

**Test.** `test_governance_review_regressions.py::test_canonical_job_pins_utc_session_timezone`,
parameterised over the three jobs.

**Cost.** $0. **Security.** None.

### G-P0-2 — an unqualified table reference bypassed both SQL allow-lists

**Evidence.** Both guards extracted table references with a regex that could only *match* a
qualified name:

```python
ai/guards.py            re.findall(r"\b(?:FROM|JOIN)\s+([a-zA-Z_][\w]*\.[a-zA-Z_][\w]*)", ...)
ai/agent_tools/athena_tool.py   _TABLE_REF = re.compile(r"\b(?:FROM|JOIN)\s+(\w+)\.(\w+)", re.I)
```

An unqualified operand matched nothing, so it was never checked — and a single qualified
reference elsewhere in the query kept the "no references found" refusal from firing.
Reproduced against the real modules before the fix:

```text
guards.assert_allowed_tables(
  "SELECT a.id, p.email FROM mart.dim_account a JOIN customer_pii p ON a.id=p.id")
  -> ALLOWED     (customer_pii is a denied PII dataset in the registry)

athena_tool.prepare(
  "SELECT a.b, s.secret FROM kafka_dev_lab_dev_mart.m a JOIN raw_pii s ON a.k=s.k")
  -> ALLOWED     -> "... JOIN raw_pii s ON a.k=s.k LIMIT 100"
```

**Impact.** The dataset allow-list, the PII denial (`S14-1`) and the raw-CDC denial
(`_stream`, `_full_cdc`, `_quarantine`, ADR-060) were all evadable by omitting a schema
qualifier. `athena_tool.py`'s own docstring states the rule the code did not keep: *"An
unqualified name resolves against whatever the session default happens to be, which is not a
decision this tool may leave to chance."* It was left to chance whenever one qualified
reference was present.

**Compensating control, stated honestly.** IAM is the first control and was not bypassed:
the AI role is denied the raw databases outright, so the realistic blast radius is a table
inside an already-permitted database rather than raw CDC. That is why this is P0 rather than
an incident — the second control failed, not the first.

**Minimal fix (applied).** Replaced both regexes with one shared token walk,
`guards.table_references()`, plus `guards.assert_qualified()`. Extraction now *returns* an
unqualified operand bare so each caller refuses it. Three shapes had to be handled or the
walk would be worse than the regex it replaced:

- **CTE names are not tables** — `WITH t AS (...) SELECT * FROM t` must not be refused, and
  the CTE *body* is still scanned, so a CTE cannot smuggle a denied table.
- **`EXTRACT(DAY FROM ts)` is a value expression** — reading `ts` as a table would refuse
  ordinary SQL.
- **A derived table is recursed into, never skipped** — skipping would let
  `FROM (SELECT * FROM raw_pii)` through, which is the original bug with extra steps.

Verified after the fix:

```text
SELECT * FROM mart.a JOIN raw_pii p ON 1=1              -> REFUSED  unqualified 'raw_pii'
SELECT * FROM kafka_dev_lab_dev_mart.m, raw_pii         -> REFUSED  unqualified 'raw_pii'
SELECT * FROM (SELECT * FROM ..._full_cdc.e) x          -> REFUSED  denied database
WITH t AS (SELECT * FROM stream.customer_pii) SELECT..  -> REFUSED  PII dataset
WITH t AS (SELECT * FROM mart.dim_account) SELECT * t   -> ALLOWED
SELECT extract(day FROM d) FROM mart.dim_account        -> ALLOWED
```

**Test.** `TestUnqualifiedReferenceIsRefused` — 7 cases covering both call sites, comma
joins, derived tables, CTEs and the value-function false positive.

**Cost.** $0. **Security.** Closes a proven read-path authorisation bypass; no new surface.

**Residual.** Extraction is a token walk, not a SQL parser. It is fail-closed for every
shape tested above; a sufficiently exotic dialect construct could still mis-parse. IAM
remains the control that does not depend on parsing.

---

## 4. P1 findings

### G-P1-1 — the canonical ingest path has no poison-record path (FIXED 2026-09-04)

`CLAUDE.md` §5.10 requires a poison record to reach a DLQ/quarantine with its error class,
stack hash, source coordinates and a payload reference. The only implementation of that —
`spark/jobs/l1_stream/quarantine.py` — belongs to the **superseded** L1 job that has never
successfully consumed a topic (`full_cdc/job.py` documents why: the Avro path was never
written). The job that actually runs, `spark/jobs/full_cdc/job.py`, had **no `try`, no
quarantine table and no error tolerance**: `from_avro` defaults to `FAILFAST`, so one
undecodable record failed the whole run.

A second gap sat inside the quarantine module itself: `to_quarantine_row` computes
`payload_s3_uri` and the module docstring says *"the payload is written to encrypted S3 and
REFERENCED"* — but **nothing anywhere wrote that object**. Grepped: no `put_object`, no
`boto3` in `spark/jobs/`. Every quarantine row pointed at a URI that did not exist.

**The fix, in `spark/jobs/full_cdc/job.py`.**

1. `from_avro(..., {"mode": "PERMISSIVE"})`. A record that cannot be parsed yields a NULL
   struct instead of killing the run, which turns "the job dies" into "this row is
   separable" — and a separable row is one that can be quarantined with its coordinates.
2. `quarantine_rows()` writes the **payload first, then the row**, for the same reason the
   streaming path writes its pointer first: a payload with no row is an orphan an operator
   can still find by prefix; a row promising a payload that was never written is a dead
   reference that looks like data loss. A failed payload write is logged and the row is
   still recorded — the fact that this offset was poison is the part reconciliation needs.
3. The payload is base64 in a JSON object, bounded at 1 MB and **flagged `truncated`** when
   clipped. A silently clipped payload looks like a corrupt record and sends the next
   engineer after the wrong bug.
4. Two error classes, because they need different responses: `AvroDecodeError` (the payload
   is broken — a rerun will not help) and `SchemaNotExported` (our export is stale — a
   rerun after refreshing it will succeed). See E2 below.

`--lake-bucket` is optional and derived from `--warehouse`. Making it required would have
been the tidier signature and the wrong call: every recorded submission recipe omits it, so
the flag would turn a documented rerun into an argparse error.

**Tests.** Seven cases in `spark/tests/test_governance_review_regressions.py`
(`TestCanonicalIngestQuarantine`), including the original defect stated as one assertion —
the URI recorded in the row must equal the key that is written — plus a byte-exact payload
round trip and the truncation flag. **Still to prove live:** inject a corrupt payload on a
topic and confirm the run completes, the row lands and the S3 object exists. One EMR run.

### G-P1-2 — `redact()` produced output its own detector called a leak (FIXED)

`ai/assistant.py:135` runs `assert_no_secrets(redact(context))`. The secret pattern
`(password|...)\s*[:=]\s*\S+` matched its own replacement, because `\S+` happily consumes
`[REDACTED]`:

```text
before:  redact("db password: hunter2")  -> "db password=[REDACTED]"
         assert_no_secrets(that)          -> GuardViolation: secret-shaped content survived
```

`GuardViolation` is fatal by design, so the generation path **failed closed on text that had
been redacted correctly** — and it fired precisely on documents that mention a password
field, i.e. the security documentation the corpus is built from. Never observed live only
because generation is unreachable (Bedrock `INVALID_PAYMENT_INSTRUMENT`).

Both functions had tests; the *composition* did not. That is why it survived.

**Fix (applied).** A negative lookahead so the pattern skips a complete placeholder, written
so it cannot become an escape hatch: `password=[REDACTED]realsecret` is still redacted.
**Test.** `TestRedactionIsIdempotent` — 4 cases including idempotency and the escape check.
**Cost.** $0. **Security.** Strictly improves: no detection is weakened.

### G-P1-3 — the connector DLQ is inert on a source connector (DOCUMENTED)

Both connector templates set `errors.tolerance: all` with `errors.deadletterqueue.topic.name`
and a comment stating the record *"goes to the DLQ with its headers"*. In Kafka Connect the
dead-letter queue is a **sink-connector** feature. These are source connectors: the settings
are accepted and ignored, no `cdc.dlq.*` topic is ever produced to, and with
`tolerance: all` a record failing conversion is **skipped silently** — the exact opposite of
§5.10, and undetectable after the fact.

**Not silently changed.** `tolerance: none` makes the loss loud at the cost of stopping the
task on one bad record. That is an availability-versus-evidence decision belonging to the
operator, not to this review. The comments in both templates are corrected to state the gap
where an operator will actually read it, and the settings are left in place so the gap stays
visible rather than tidied away. **Cost:** $0 either way.

### G-P1-4 — the EOD ranking is Oracle-only and degrades silently (FIXED)

`spark/jobs/eod/job.py` ranks with `col("position_primary").cast("decimal(38,0)")`. That is
correct for a numeric Oracle SCN. A SQL Server position is a hex LSN triplet
(`0000002a:00000c38:0001`); it casts to **NULL**, `desc_nulls_last()` ties every row, and the
ranking collapses onto `kafka_offset` — which `CLAUDE.md` §5.4 forbids as a comparator
because offsets are monotonic only *within* a partition. The result is a snapshot that picks
an arbitrary event per key and looks entirely normal.

Today the job filters to `oracle`/`ACCOUNT`, so nothing else reaches the window. That safety
is a property of one line elsewhere in the file that a later change could widen without ever
touching the ranking.

**Fix (applied).** A bounded (`limit(1)`) check that refuses loudly if a non-numeric position
reaches the ranking, plus a comment pointing at the correct normalisation that already exists
(`spark/jobs/l1_stream/ordering.py`, the only hex-aware LSN handling in the repo). Inert on
the current path by construction; fail-closed the moment the filter is widened.
**Test.** `test_eod_refuses_a_non_numeric_source_position`. **Cost.** $0 — one bounded scan.

### G-P1-5 — "DEPRECATED" applied to a database that holds a live table (FIXED)

`spark/jobs/full_cdc/job.py` said *"That database is now DEPRECATED and read-only"* of
`kafka_dev_lab_dev_stream`. But `reporting/layers.yaml` binds **REALTIME** to that same
database with `status: ACTIVE`, materialised by `spark/jobs/realtime/job.py`. S3 confirms
both tables live side by side: `warehouse/stream/cdc_events/` (the deprecated landing table)
and `warehouse/stream/cdc_events_realtime/` (the active window).

Acting on the deprecation note — dropping the database — would destroy the live REALTIME
layer. **Fix (applied):** the comment now scopes the deprecation to the one table and names
the live one. **Cost.** $0.

### G-P1-6 · G-P1-7 · G-P1-8 — carried forward

- **G-P1-6.** `airflow/dags/reporting_common.py:375` raises `NotImplementedError` for
  AUTO_CORRECT and FULFILL; only EOD and STREAM_BATCH dispatch. Confirmed still present.
  The refusal is honest (it gates, records, then stops rather than no-opping), but two of the
  five modes cannot be orchestrated. OPEN-28.
- **G-P1-7.** `dbt build` has never executed — not locally against Glue, not on EMR.
  dbt-core/dbt-spark are absent from the EMR image and the private subnets cannot reach
  PyPI. Phase 14 recorded `SUBSTITUTED_SPARK_SQL_MERGE`. The framework's central execution
  mechanism is compile-proven only.
- **G-P1-8. (FIXED — docs)** `AI_PLATFORM_STATE.md` asserted `AI_PLATFORM_PRODUCTION_READY`
  "verified live". Re-run today: E2E **8/10** and the agent gate **FAILS** on 6 scenarios,
  because the platform it was verified against was destroyed 2026-08-28T23:12Z. No AI code
  regressed — every failure is a tool with no backend. A status correction block now sits at
  the top of that file; the honest reading is `AI_PLATFORM_READY_PENDING_REBUILD`.

---

## 5. P2 / P3 findings

| Id | Finding and evidence |
|---|---|
| G-P2-1 | **(FIXED)** `envelope.assert_no_field_dropped`'s null check read `... and c == "event_version"`, so a guard advertised as covering nine D5 fields examined exactly one — and that one is defaulted to `1` on the line that builds it, so it could never fire. Replaced with `D5_NOT_NULL`, the two fields `L1_SCHEMA` actually declares `NOT NULL`; the other seven are nullable by contract and demanding a value would quarantine valid events. |
| G-P2-2 | Superseded L1 job: `decode_record` does `row["value"].decode()`, but a delete tombstone has `value=None`. The `AttributeError` is raised in the `rdd.map` stage, **outside** `transform_partition`'s try/except — and `AttributeError` is not in the caught tuple either. With `tombstones.on.delete: true` on both connectors, the first delete would fail the batch permanently. Documented, not fixed: repairing a superseded path spends effort making dead code look alive. |
| G-P2-3 | Same job: `write_batch` `.collect()`s an entire micro-batch (default `maxOffsetsPerTrigger=100000`) to the driver before building the DataFrame. Documented with G-P2-2. |
| G-P2-4 | `full_cdc/job.py` reads each topic `earliest`→`latest` **every run** with no checkpoint. Idempotence is preserved by the `dv_event_id` MERGE, so this is not a correctness defect — but scan cost grows linearly with topic retention, and `CLAUDE.md` §5.8's literal "every write path has a checkpoint" is unmet. |
| G-P2-5 | `auto_destroy_after = "2026-08-25T00:00:00Z"` — 9 days expired. Any apply tags every resource already-expired (ADR-027, OPEN-34). Correcting it is **not** free: the value feeds provider `default_tags`, so changing it defers the `aws_availability_zones` read and replaces all 6 subnets, cascading into MSK, EMR and every EC2 instance. |
| G-P2-6 | Budget is **$100** in `terraform.tfvars` and live in AWS; `CLAUDE.md` §4 and ADR-030 still say **$30**. Month-to-date actual: **$0.251**. |
| G-P2-7 | `test_four_flows_spark`, `test_l1_local_spark`, `test_l2_local_spark`, `test_l3_snapshot`, `test_schema_evolution` fail with `ClassNotFoundException: org.apache.iceberg.spark.SparkCatalog` when run after another test file has already started a JVM without the Iceberg jars. They pass in isolation (67 passed). A harness defect, not a product defect — but a plain `pytest spark/tests/` reports 61 errors, which reads as a broken platform. |
| G-P2-8 | Business ceiling: **1 of 6** marts materialised; `product_code`, `branch_id`, `segment_code`, `currency` correctly refused at compile time because `curated.dim_account` / `dim_customer` do not exist; **4 business dates with a hole** (2026-08-17, 20, 21, 22). Driver analysis by business dimension — the core of the brief — is blocked, and rolling averages, anomaly baselines and forecasting are out of reach on four points. |
| G-P2-9 | Four **Enabled** lake CMKs; durable objects split 653 on `383f6d53` / 7 on `8d20ebe1`. A further **161 objects reference `d1f568fd`, a key that no longer exists** — all under `logs/` and `query-results/`, both regenerable, so no durable data was lost. The standing trap: the next apply mints a *fifth* key and grants roles only that one, orphaning all 660 durable objects behind `kms:Decrypt` denied. `scripts/reencrypt-lake-cmk.sh reencrypt --execute` must run **after** any apply. |
| G-P2-10 | The Phase 14 driver called `coordinator.plan_run/gate/submit/finish` and then a MERGE **written inline in the driver**. `eod_flow.py`, `auto_correct.py`, `fulfill.py` and `stream_batch.py` were never invoked, and the ADR-042 ladder that refused the downgrades was the driver's copy, not `dbt/macros/reporting/reporting_mart.sql`. What Phase 14 proved is the coordinator lifecycle, the runtime state store and Iceberg MERGE semantics — not the five modes' own logic. |
| G-P3-1 | `athena_tool.enforce_limit` anchors its LIMIT regex at end-of-string, so `... LIMIT 5 OFFSET 10` gets a second LIMIT appended and becomes invalid SQL. Fail-closed (Athena rejects it), so it is a usability defect, not a control defect. |
| G-P3-2 | `rows = batch_df.collect() if batch_df.rdd.isEmpty() else None` in the superseded L1 job — inverted and unused. |
| G-P3-3 | `module.data_lake.aws_s3_object.prefix["logs/airflow/"]` and `module.airflow_k3s[0].aws_s3_object.log_prefix[0]` manage the same S3 key, so every plan shows a 1-resource diff that never converges. Pre-existing; needs a decision on which module owns the object. |
| G-P3-4 | FULL_CDC `event_id` moved from `sha256(topic:partition:offset)` to `dv_event_id` (which includes the record timestamp, fixing the rebuilt-cluster collision that discarded 5,728 events in Session 34). Rows written under the old scheme need `--backfill-identity` or they will not match on re-ingest. |

---

## 6. What is genuinely strong

Recording this matters as much as the defects — these are the controls a live test can rely on.

- **Source ordering (`spark/jobs/l1_stream/ordering.py`).** Oracle SCN is numeric and must
  be zero-padded to sort; SQL Server LSN is hex and already fixed-width, so it must be
  *validated* rather than padded. The module treats them oppositely and refuses anything it
  cannot normalise, rather than passing it through best-effort.
- **Two identities on every FULL_CDC row.** `dv_event_id` ("same delivered record", the
  MERGE key, includes the Kafka record timestamp so a rebuilt cluster cannot collide) versus
  `dv_src_event_id` ("same source change", carries no Kafka identity). Deliberately not
  collapsing on the second preserves redeliveries that reconciliation needs to see.
- **Point-in-time feature correctness.** `event_time_column` is mandatory, processing-time
  columns are *rejected* as join keys, and `assert_label_horizon_excluded` catches the leak
  that `feature_event_time <= label_event_time` silently permits when the label is stamped at
  the end of its horizon — the case where the obvious test passes and the leak survives.
- **Certification ladder.** A model never chooses its own tier; the flow mode assigns it, and
  the incremental filter refuses a downgrade of a `CERTIFIED` row by rank comparison.
- **Iceberg maintenance floor.** `remove_orphan_files` refuses to run below a 72h retention,
  because a short sweep deletes files out from under a running writer.
- **Checkpoint/warehouse separation.** Enforced at startup by a path check that fails before
  anything is written, not by convention.
- **Governance registry.** Ownership, classification, PII columns, retention and freshness
  SLA all derive from one file, and the AI SQL guard inherits the BI role's access from that
  same file rather than keeping a second list that would drift invisibly.
- **Registry compatibility is enforced and negatively tested.**
  `REGISTRY_RULES_GLOBAL_COMPATIBILITY: BACKWARD`, with `cdc-correctness-test.sh` asserting
  the registry *refuses* a narrowing schema — a test that fails if the setting is wrong.

---

## 7. Tests run for this review

All local, no AWS mutation, $0.00.

```text
pytest spark/tests/ (minus the 5 JVM-conflict files)    1576 passed
the 5 Iceberg files, isolated                             67 passed
  of which NEW in this review                             17 passed
python3 scripts/validate-docs.py                          14 passed / 0 failed
make lint-shell                                          exit 0 (shellcheck 0.11.0)
make reporting-validate                                  OK, waves resolve per mode
ai/eval/drills_p15.py                                    20/20 PASS, 0 P0 violations
make ai-eval-rag-gate                                    PASSED against baseline
make business-ai-eval                                    25/25, P0=0 P1=0, $0.0000/question
ai/eval/e2e_p14.py                                       8 PASS / 2 FAIL  (no Athena backend)
make ai-eval-agent-gate                                  FAILED, 6 scenarios (no Athena backend)
```

Total: **1,643 passing** (1,626 before this review, +17 regression pins). The two failing
gates fail for one reason — the platform is destroyed, so the Athena-backed tools have no
backend. Both were re-run after the guard changes and are unchanged, confirming the fixes
introduced no regression.

## 8. Files changed

| File | Finding | Change |
|---|---|---|
| `spark/jobs/full_cdc/job.py` | G-P0-1, G-P1-5 | UTC session zone; deprecation scoped to one table |
| `spark/jobs/eod/job.py` | G-P0-1, G-P1-4 | UTC session zone; refuse non-numeric source position |
| `spark/jobs/realtime/job.py` | G-P0-1 | UTC session zone |
| `ai/guards.py` | G-P0-2, G-P1-2 | shared `table_references` / `assert_qualified`; idempotent redaction |
| `ai/agent_tools/athena_tool.py` | G-P0-2 | database allow-list uses the shared walk |
| `spark/jobs/l1_stream/envelope.py` | G-P2-1 | `D5_NOT_NULL`; the null guard can now fire |
| `spark/jobs/l1_stream/job.py` | G-P2-2/3, G-P3-2 | SUPERSEDED banner with the three known defects |
| `connectors/oracle/corebank-source.json.tmpl` | G-P1-3 | DLQ comment corrected to state the gap |
| `connectors/sqlserver/digital-source.json.tmpl` | G-P1-3 | same |
| `AI_PLATFORM_STATE.md` | G-P1-8 | status correction block |
| `spark/tests/test_governance_review_regressions.py` | all fixed | new, 45 regression pins |
| `spark/jobs/full_cdc/job.py` | G-P1-1, E2 | PERMISSIVE decode + quarantine with payload write; per-`globalId` writer-schema selection |
| `scripts/export-registry-schemas.py` | E2 | new — exports every schema version from Apicurio, keyed by globalId |
| `scripts/cdc-runtime.sh` | E2 | new `export-schemas --execute <bucket>` subcommand |
| `scripts/ai-feature-run.py` | item 41 | new — feature materialisation + anomaly scoring against the live mart |
| `spark/tests/conftest.py` | test harness | one session-scoped SparkSession; 61 fixture errors on a full-suite run |

No Terraform, dbt model, DAG or reporting-config file was changed. No AWS resource was
created, started or modified.

## 9. Entry criteria for live-test planning

Ordered by what unblocks the most, and none of it requires a rebuild to *decide*:

1. **Commit the worktree.** 29 modified files and 186 untracked (~29,200 LOC) sit on one
   branch; HEAD is Session 38 while the work reached Session 41 plus Tracks C and D.
2. **G-P1-7 — make `dbt build` run once**, anywhere. It is the framework's central mechanism
   and needs no MSK; the blocker is packaging dbt into the EMR image, since the private
   subnets cannot reach PyPI.
3. ~~**G-P1-1 — give the canonical ingest a poison-record path**~~ — DONE 2026-09-04, with
   the payload write. Remaining: prove it live by injecting a corrupt payload.
4. **G-P1-3 — decide `errors.tolerance`** on the source connectors: silent skip, or a loud
   stop.
5. **G-P1-6 — wire AUTO_CORRECT and FULFILL** into the coordinator, or state that the live
   test covers three modes of five.
6. **G-P2-9 — sequence the CMK re-encryption after any apply**, before Spark reads the lake.
7. **G-P2-5 — resolve `auto_destroy_after`** knowing the network-replacement cascade.

Cost to reach a live run is unchanged and dominated by MSK: **~$1.12–1.53/hr**, of which MSK
is roughly 68% and cannot be stopped, only destroyed. Current burn is **$0.00/hr**.

---

*Read-only review. No AWS resource was created, started, modified or destroyed. Every result
above was produced by running the command shown.*
