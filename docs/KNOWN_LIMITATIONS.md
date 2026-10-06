# Known limitations

State as of the **2026-09-20** live window. Every item here was **observed**, not predicted.
Classified P0–P3, where P0/P1 are what a production-readiness claim turns on.

* **P0** — blocks the claim. Live state or behaviour is wrong.
* **P1** — required for readiness. Code-complete but unproven, or a real gap with a workaround.
* **P2** — accepted for this environment, with a named reason.
* **P3** — hygiene.

---

## P0 — blocks the claim

**None open.**

| Id | Was | Closed by |
|---|---|---|
| ~~P0-1~~ | `ops.eod_info` held a stale `CERTIFIED` row for `(oracle.coredb.corebank.account, 2026-09-20)` — a COB whose cutoff had not passed | **Resolved 2026-09-20**, run `00g8u2dg8qs2uo27`. Not by a hand-written `UPDATE`: `close_table` now WITHDRAWS a certification whose own COB cutoff is still in the future, because no valid path could have written it. Live: `EOD_DECERTIFIED ... Withdrawn.` and the row now reads `BUILT_NOT_CERTIFIED` with a NULL watermark. Certification remains final in every other case, and a test asserts a past-cutoff row is left alone. |

## P1 — required for readiness

**None open.**

| Id | Was | Closed by |
|---|---|---|
| ~~P1-1~~ | "Schema evolution is not supported by the canonical ingest — writer schemas load from a static `schemas.json`, one per topic; the docstring's registry-by-`globalId` resolution is not implemented." | **This entry was STALE, and carrying it forward was the error.** It restated finding E2 from the 2026-09-03 list without re-reading the code. `full_cdc/job.py::select_schema` has since chosen the writer schema PER RECORD by `apicurio.value.globalId`, and `normalise_schema_export` accepts both export shapes. When the export is globalId-keyed, a globalId missing from it is **quarantined, not decoded with the topic entry** — that fallback would be a known-different writer version, which is precisely the silent mis-decode E2 described. Covered by `TestWriterSchemaIsChosenPerRecord`. **Correction:** the "28 versions across 14 topics" figure originally cited here was a misread — 28 is 14 topics x (Key + Envelope), one version each. The mechanism is implemented and unit-tested; the MULTI-VERSION case is now tracked as P2-10 rather than as this blocker, because the defect E2 described (a topic-wide fallback that silently mis-decodes) is structurally gone: the fallback refuses. |

---

## P2 — accepted, with a reason

| Id | Limitation | Reason it is accepted |
|---|---|---|
| **P2-0** | **Production ingest mode is live-proven only as far as STARTING.** Run `00g8u2gkdjulso27` on 2026-09-20 submitted `full-cdc-oracle` as a RESIDENT `mode=STREAMING` job against a production-profile plan. EMR accepted it, it ran resident for 798 s, and `ops.streaming_app_state` recorded `mode=continuous_microbatch, profile=production`. It never left `STARTING`: Kafka had been fully drained by the earlier lab run, so there was no microbatch to commit. Cancelled cleanly. | What is proven: the production path starts, is resident, resolves its mode from the plan, and stops on command. What is NOT: a committed microbatch under production mode with live traffic. Needs `source-lab.sh workload --execute` (a human-gated command) running concurrently. Two defects were fixed to get this far — `emr-submit.sh` could not submit a STREAMING job at all, and EMR rejects an execution timeout of `0` as well as of `25` for that mode. |
| **P2-1** | **STREAMING_RT is live-unproven.** `is_enabled: false`, behind `ENABLE_STREAMING_RT`. Covered by `test_reporting_streaming_rt.py` (lifecycle, watermark, restart). | It is the one hourly cost in the reporting layer. CLAUDE.md §4.5 rules out always-on by default, so leaving it gated is the correct posture, not an omission. |
| **P2-2** | **Source-connector DLQ is inert.** `errors.deadletterqueue.*` is a sink-only feature; with `errors.tolerance: all` a poison record is skipped silently. | Detectable only by topic-count comparison. The FULL_CDC ingest has its own quarantine path (`--quarantine-table`, `--unknown-table-policy`), so the gap is confined to the connector hop. |
| **P2-3** | **The EOD MERGE cannot retract.** No delete arm, so a row that should vanish from a rebuilt snapshot persists until its partition is cleared. | `delete_policy: exclude_from_snapshot` covers the normal case. A retraction is a partition rewrite, which is a deliberate operator action. |
| **P2-4** | **`fact_digital_engagement_daily` joins `dim_channel` on a derived key.** `dbo.digital_event` has no channel column; the channel comes from a declared `device_type → channel_code` map. | The mapping is in `reporting/curated/entities.yaml`, reviewable, and an unmapped device resolves to `UNKNOWN_SK` rather than a default. But it IS a modelling assumption, not source truth. |
| **P2-5** | **`dim_product` is not built.** Neither source system has a product table. | Building it would mean inventing `product_name` and `product_group`. No fact joins `product_sk`. A test asserts it is the only unconfigured dimension. |
| **P2-6** | **The four `datamart_*` DAGs are UNPAUSED.** `datamart_stream_batch` submits a real EMR job every 10 minutes. | Correct for a production posture, a standing cost in a lab. `airflow dags pause datamart_stream_batch` when the window closes. |
| **P2-10** | **No topic has yet carried two writer-schema versions**, so per-record `globalId` selection has never had to choose between them live. The registry holds exactly one version per topic (28 artifacts = 14 topics x Key+Envelope). | The mechanism is wired and unit-tested, and the dangerous direction is closed: an unknown `globalId` is quarantined, never decoded with the topic's schema. Provable in one command — `source-lab.sh workload 5 --execute` performs a COMPATIBLE DDL change, which registers a second version for that table; re-export and a `2` appears against that topic's `Envelope`. Scenario 6 is the negative case: an INCOMPATIBLE change the registry should REJECT. |
| **P2-9** | **No CERTIFIED EOD close has been observed live.** Every event in the lab is committed by the connector snapshot on the day the stack is built, so the only COB holding data is the current one — whose cutoff has not passed. The open-day guard therefore withholds certification, correctly. | The certification REFUSAL path is proven live (`WAITING_SOURCE`, `EOD_DAY_OPEN`, `EOD_WATERMARK_HELD`, `EOD_DECERTIFIED`); the SUCCESS path is proven by `test_eod_engine_spark.py` against real Iceberg tables. It becomes provable after 00:00Z by re-running the close with no code change and no `--skip-readiness` — see `FULL_CDC_REPORTING_E2E.md` §9. |
| **P2-8** | **A rebuild leaves a stale streaming checkpoint in the lake.** `terraform destroy` does not empty the bucket, so `checkpoints/full_cdc/<app_id>/` outlives the stack it belonged to. The next ingest fails on `kms:Decrypt` against the retired CMK — and would have been WRONG even if readable, because the recreated FULL_CDC tables are empty while the checkpoint claims those offsets are consumed. | Recovery is documented (`streaming-reset.sh`, which moves rather than deletes) and the failure is loud. It is a rebuild-time step, not a defect: the alternative — auto-deleting a checkpoint on start — is what ADR-073 forbids. |
| **P2-7** | **The lake holds objects under more than one CMK.** Objects predating a rotation fail `kms:Decrypt` under the current role policy. | Re-upload on use. Widening the role policy to every historical key would be the wrong fix. |

---

## P3 — hygiene

| Id | Limitation |
|---|---|
| **P3-1** | 200+ uncommitted paths and no git remote. Phases B–J exist only on local disk. |
| **P3-2** | `spark/dimensions/build_kimball.py` and `spark/common/run_flow.py` still carry the Session 09/10 namespaces (`snapshot.banking_*`, `mart.*`) that nothing produces. They are the unwired path, kept because their tests cover the modules `curated_build.py` depends on (ADR-080). |
| **P3-3** | `eod_info_legacy_v` is Athena DDL and is not applied by the Spark provisioner (`make cdc-eod-legacy-view`). |

---

## Resolved since the previous edition

| Id | Was | Now |
|---|---|---|
| OPEN-28 | AUTO_CORRECT and FULFILL raised `NotImplementedError` in the Airflow coordinator | Closed 2026-09-04. Both ran live on 2026-09-20 |
| R4 | The reporting role could not emit logs; EMR treats a log-push failure as job failure | `REPORTING_EMR_LOG_URI` sends logs to S3. The `logs:` interface endpoint is not needed |
| G-P1-1 | The canonical ingest had no quarantine path | `stream_job.py` has `--quarantine-table` and `--unknown-table-policy` |
| — | The CURATED layer had **no producer at all** | ADR-080, `curated_build.py` |
