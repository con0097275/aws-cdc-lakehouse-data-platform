"""Kafka --(STRUCTURED STREAMING)--> FULL_CDC. The continuous form of the canonical ingest.

    spark-submit --py-files full_cdc_job.py full_cdc_stream_job.py \
        --bootstrap <brokers> --topics <csv> --schemas-uri s3://.../schemas.json \
        --warehouse s3://<lake>/warehouse \
        --table glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events \
        --table-plan s3://<lake>/config/table-plan.json --migration-mode dual_write

THE MODE, THE TRIGGER, THE CHECKPOINT AND THE STATE TABLE ALL COME FROM THE PLAN (ADR-073).
`--checkpoint` remains available for a local test and is otherwise derived, because a
checkpoint typed on a submit command is a checkpoint that will eventually be typed
differently -- and a different checkpoint is a replay from `startingOffsets` wearing the
appearance of a normal restart.

WHY THIS EXISTS, AND WHY IT IS NOT A SECOND IMPLEMENTATION
----------------------------------------------------------
`spark/jobs/full_cdc/job.py` reads Kafka as a BOUNDED batch (earliest..latest) and is what
has been proven live. It is the right shape for a scheduled backfill and the wrong shape for
"the lake tracks the source continuously", which is what a CDC platform is actually for.

The superseded `spark/jobs/l1_stream/job.py` had the streaming SHAPE and never worked: it
JSON-decodes an Avro payload, dies on the first tombstone (`value=None` → AttributeError,
outside its own try/except), and collects whole micro-batches to the driver.

So this module supplies the streaming LIFECYCLE only. Every decision that is expensive to
get right -- per-`globalId` writer-schema selection, PERMISSIVE decode, the `op`-based
poison test, the payload-before-row quarantine write, the schema-aligned idempotent MERGE --
is imported from `full_cdc_job` and NOT reimplemented. Two decode paths that "look
equivalent" is exactly the OPEN-28 defect: the DAG adapter and `reporting-live-run.py`
diverged on one dict key and only live execution revealed it.

COST. A structured-streaming query holds EMR Serverless capacity for as long as it runs,
which is why the LAB profile resolves to `available_now`: the query drains what Kafka holds,
commits, exits, and the application auto-stops after its idle timeout. `continuous_microbatch`
is the production target and stays resident -- it is selected by `ingestion.profile` in the
registry, priced in `docs/COST.md`, and off by default (CLAUDE.md 4.5, ADR-073).

LIVENESS. The app writes one row to `ops.streaming_app_state`, MERGEd in place: status,
offsets, watermark, snapshot, restart count and heartbeat. Before this existed, a stalled
ingest and a caught-up ingest looked identical from outside -- which is exactly how Kafka
reached 124 events against FULL_CDC's 65 with every check green (Phase A section 2).

FAILURE. `foreachBatch` swallowing an exception tells Spark the batch succeeded, so the
offsets commit and the rows are gone -- not in the target, not in a queue, nowhere. Every
error here propagates, leaving the offsets uncommitted so Spark retries with the data still
in Kafka.
"""
from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/tmp/framework")

from full_cdc_job import (  # noqa: E402
    LEGACY_ONLY, PER_TABLE_ONLY_COLUMNS, decode_topic_to_rows, global_id_col,
    handle_unrouted, load_schemas, merge_into_full_cdc, read_table_plan,
    routing_context)


def build_spark(warehouse: str) -> SparkSession:
    return (SparkSession.builder.appName("full-cdc-stream")
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
            .config("spark.sql.catalog.glue_catalog.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # ADR-024, and it matters MORE here than in the batch job: `event_date` is
            # derived with from_unixtime in the session zone, so a non-UTC default puts
            # streaming rows in a different day partition than the batch backfill of the
            # same events -- two layers disagreeing about which day a record belongs to.
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


class StreamingStateWriter:
    """The app's own row in `ops.streaming_app_state`, kept current.

    One object owns every write, so the throttle, the legal-transition check and the
    "what did I last say" memory cannot be duplicated per call site. A disabled writer
    (`--state-table ''`) is a no-op rather than a branch at every use: a local test must not
    have to care, and a production run that cannot write state must not silently look like
    a local test -- which is why a FAILED write RAISES on the start path and only warns on
    the heartbeat path.
    """

    def __init__(self, spark, identifier: str, *, policy, deployment_id: str,
                 config_version: str = ""):
        self.spark = spark
        self.identifier = identifier or ""
        self.policy = policy
        self.deployment_id = deployment_id
        self.config_version = config_version
        self.status = None
        self.last_written = None
        self.restart_count = 1

    @property
    def enabled(self) -> bool:
        return bool(self.identifier)

    def _previous(self) -> dict | None:
        if not self.enabled:
            return None
        rows = self.spark.sql(
            f"SELECT * FROM {self.identifier} WHERE app_id = '{self.policy.app_id}'"
        ).collect()
        return rows[0].asDict() if rows else None

    def write(self, status: str, *, reason: str, now, batch_id=None, offsets=None,
              watermark=None, snapshot_id=None, rows_last_batch=None, rows_total=None,
              error=None, commit_ts=None) -> None:
        from cdc import streaming_state as st

        self.status = st.transition(self.status, status)
        if not self.enabled:
            return
        row = st.StateRow(
            app_id=self.policy.app_id, deployment_id=self.deployment_id,
            mode=self.policy.mode, profile=self.policy.profile,
            trigger_interval=self.policy.trigger_interval,
            checkpoint_location=self.policy.checkpoint, status=self.status,
            last_batch_id=batch_id, kafka_offsets=offsets, source_watermark_ts=watermark,
            last_commit_ts=commit_ts, target_snapshot_id=snapshot_id,
            rows_last_batch=rows_last_batch, rows_total=rows_total,
            restart_count=self.restart_count, config_version=self.config_version,
            error=error, updated_at=now, reason=reason)
        self.spark.sql(st.merge_sql(self.identifier, row))
        self.last_written = now
        print(f"FULL_CDC_STREAM_STATE app={self.policy.app_id} status={self.status} "
              f"reason={reason} restart={self.restart_count} batch={batch_id}", flush=True)

    def starting(self, now) -> None:
        """First write of the process. Reads the previous row ONLY to count the restart."""
        from cdc import streaming_state as st

        previous = self._previous()
        self.restart_count = st.restart_count_after(previous)
        if previous:
            # Not a transition check on the stored status: a process that was killed leaves
            # RUNNING behind forever, and refusing to start because of a row the last crash
            # wrote would make the state table able to prevent recovery. It is evidence,
            # not a lock -- the lock is the checkpoint, which Spark enforces itself.
            print(f"FULL_CDC_STREAM_RESUMING app={self.policy.app_id} "
                  f"previous_status={previous.get('status')} "
                  f"previous_batch={previous.get('last_batch_id')}", flush=True)
        self.write(_ING().STATUS_STARTING, reason="process start", now=now)


def _ING():
    from cdc import ingestion as ing
    return ing


def _record_stop(state, stats, query, *, status: str, reason: str, error=None) -> None:
    """The last state write of the process: STOPPED or FAILED, with where it got to.

    Wrapped in its own try because a teardown that raises replaces the real failure with a
    state-write failure -- and the real failure is the one an operator needs.
    """
    from cdc import streaming_state as st

    try:
        state.write(status, reason=reason, now=datetime.now(timezone.utc),
                    offsets=st.offsets_json(query.lastProgress if query is not None else None),
                    watermark=stats.get("watermark"), rows_total=stats.get("rows"),
                    error=error)
    except Exception as exc:                          # noqa: BLE001
        print(f"FULL_CDC_STREAM_STATE_WRITE_FAILED final {type(exc).__name__}: {exc}",
              flush=True)


def resolve_policy(args, plan, topics):
    """Mode, trigger, heartbeat, app id, checkpoint and state table -- resolved ONCE.

    Everything here is read from the compiled plan, with a CLI override kept only where a
    local test genuinely needs one. The function returns the policy and the state table so
    that no later line has to ask "did the plan have this, or did the flag?"
    """
    from cdc import ingestion as ing

    payload = (plan or {}).get("plan") or {}
    pol = payload.get("ingestion") or {}

    profile = pol.get("profile") or ing.PROFILE_LAB
    app_id = args.app_id or (ing.app_id_for_topics(plan, topics) if plan
                             else ing.app_id_for("", ""))
    policy = ing.resolve(
        {"mode": pol.get("mode"), "trigger_interval": pol.get("trigger_interval"),
         "heartbeat_interval": pol.get("heartbeat_interval"), "app_id": app_id},
        profile=profile, lake_bucket=args.lake_bucket, what="ingestion")

    if args.trigger_seconds:
        # TEST_ONLY, and it says so in the log rather than only in the help text.
        print(f"FULL_CDC_STREAM_TEST_OVERRIDE trigger={args.trigger_seconds}s "
              f"(plan says {policy.trigger_seconds}s)", flush=True)
        policy = dataclasses.replace(policy, trigger_seconds=args.trigger_seconds)

    if args.checkpoint:
        policy = dataclasses.replace(policy, checkpoint=args.checkpoint)
    elif not plan:
        raise SystemExit(
            "no checkpoint: pass --checkpoint, or compile a plan carrying ingestion. A "
            "streaming query with no durable checkpoint replays from scratch on every "
            "restart and cannot be resumed.")

    # CLAUDE.md 5.9. `remove_orphan_files` walks table locations and deletes anything it
    # does not recognise, so a checkpoint under warehouse/ is state a routine maintenance
    # job will eventually delete -- silently, and visibly only much later when the stream
    # restarts from nothing and replays.
    wh = args.warehouse.rstrip("/")
    ckpt = policy.checkpoint.rstrip("/")
    if ckpt == wh or ckpt.startswith(wh + "/"):
        raise SystemExit(
            f"REFUSING: checkpoint {policy.checkpoint} is under the Iceberg warehouse "
            f"{wh}. remove_orphan_files would delete streaming state (CLAUDE.md 5.9).")

    # TEST_ONLY means test only. A wall-clock budget under `profile: production` would be
    # the Phase A defect restored -- a resident application that quietly stops after N
    # seconds, with every dashboard reporting a clean exit.
    if args.run_seconds and profile == ing.PROFILE_PRODUCTION:
        raise SystemExit(
            "REFUSING: --test-only-run-seconds under profile 'production'. A production "
            "app is resident; a wall-clock budget makes it a bounded session again "
            "(ADR-073). Use profile 'lab_low_cost', whose mode is available_now.")

    state_table = args.state_table if args.state_table is not None else pol.get("state_table")
    return policy, (state_table or "")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topics", required=True)
    ap.add_argument("--schemas-uri", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--table", required=True)
    # NOT required: the plan carries the app map and the checkpoint root, so the durable
    # path is DERIVED from identity (ADR-073 section 5). The flag stays for a local test and
    # for a deliberate one-off, and a run with neither a plan nor a flag is refused rather
    # than given a default -- a defaulted checkpoint is a silent replay.
    ap.add_argument("--checkpoint", default=None)
    ap.add_argument("--app-id", default=None,
                    help="streaming app identity; derived from the subscribed topics and "
                         "the plan when omitted")
    ap.add_argument("--deployment-id", default=None,
                    help="which deployment started this process; EMR job run id by "
                         "convention. Recorded in ops.streaming_app_state, never in a path")
    ap.add_argument("--state-table", default=None,
                    help="ops.streaming_app_state identifier; read from the plan when "
                         "omitted. '' disables the state write (local tests only)")
    ap.add_argument("--lake-bucket", default=None)
    ap.add_argument("--quarantine-table",
                    default="glue_catalog.kafka_dev_lab_dev_quarantine.full_cdc_rejects")
    # DEPRECATED as a production control. The mode and trigger come from the compiled plan
    # (ADR-073); this only overrides them for a local test. Phase A found production
    # behaviour being inferred from these flags, which is how a stream became a session.
    ap.add_argument("--trigger-seconds", type=int, default=None,
                    help="TEST_ONLY override; production reads ingestion.trigger_interval "
                         "from --table-plan")
    ap.add_argument("--test-only-run-seconds", dest="run_seconds", type=int, default=None,
                    help="wall-clock budget; the query is stopped when it expires")
    # PER-TABLE ROUTING -- the same flags, the same defaults and the SAME helper as the
    # batch job. A stream that routed by its own rules would be the second decode path all
    # over again, one layer up.
    ap.add_argument("--migration-mode", default=LEGACY_ONLY,
                    choices=["legacy_only", "dual_write", "per_table_only"])
    ap.add_argument("--table-plan", default=None,
                    help="compiled CDC plan (local path or s3://); required unless "
                         "--migration-mode legacy_only")
    ap.add_argument("--unknown-table-policy", default="quarantine",
                    choices=["quarantine", "reject"])
    ap.add_argument("--event-index", action="store_true")
    ap.add_argument("--run-id", default=None)
    # EARLIEST, and only ever consulted when the checkpoint does not exist yet.
    #
    # It was `latest`, on the reasoning that a full replay should be opt-in. For a CDC
    # ingest that is backwards: on a FRESH checkpoint `latest` skips every event already in
    # Kafka and not yet in FULL_CDC -- silently, permanently, and with a green run. Measured
    # 2026-09-17 when Phase B moved checkpoints to identity-keyed paths: `LOAN` held 1,668
    # Kafka offsets against 65 FULL_CDC rows, so the first start under `full-cdc-oracle`
    # would have discarded ~1,600 source changes. A replay costs MERGE work; `dv_event_id`
    # makes it idempotent (test_full_cdc_streaming_spark::test_a_replayed_micro_batch_adds_
    # nothing). Losing CDC events costs the layer's whole contract (CLAUDE.md 5.2).
    ap.add_argument("--starting-offsets", default="earliest",
                    choices=["earliest", "latest"],
                    help="where a NEW checkpoint starts. Ignored once a checkpoint exists. "
                         "'earliest' (default) is idempotent via the MERGE on dv_event_id; "
                         "'latest' deliberately skips what Kafka already holds.")
    args = ap.parse_args()

    if not args.lake_bucket:
        args.lake_bucket = args.warehouse.split("://", 1)[-1].split("/", 1)[0]

    # The checkpoint-under-warehouse guard now lives in `resolve_policy`, because the path
    # is no longer known at this point: it is derived from the plan a few lines below.
    topics = [t for t in args.topics.split(",") if t]
    spark = build_spark(args.warehouse)
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {args.table} (
        event_id STRING, source_system STRING, source_schema STRING, source_table STRING,
        op STRING, event_date DATE, source_commit_ts TIMESTAMP,
        position_primary STRING, position_secondary STRING,
        kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
        kafka_timestamp TIMESTAMP, payload_after STRING, payload_before STRING,
        ingested_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (source_system, event_date)
      TBLPROPERTIES ('format-version'='2')""")

    schemas = load_schemas(spark, args.schemas_uri)
    writes, router, run_id = routing_context(spark, args, "full_cdc_stream")
    # The plan again, for the INGESTION policy. `routing_context` reads it for routing and
    # does not hand it back; re-reading one small JSON is cheaper than widening that
    # function's return type for every caller.
    plan = read_table_plan(spark, args.table_plan) if args.table_plan else None
    metrics = None
    if router is not None:
        from cdc.router import RouterMetrics
        metrics = RouterMetrics()
        import per_table
    index_table = ((router.event_index or {}).get("identifier")
                   if writes.event_index else None)

    # Everything about HOW this runs, resolved once, before a single Kafka connection.
    policy, state_table = resolve_policy(args, plan, topics)
    deployment_id = (args.deployment_id or os.environ.get("EMR_JOB_RUN_ID")
                     or os.environ.get("SPARK_APPLICATION_ID") or run_id or "local")
    state = StreamingStateWriter(
        spark, state_table, policy=policy, deployment_id=deployment_id,
        config_version=(plan or {}).get("config_version", ""))
    state.starting(datetime.now(timezone.utc))

    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", ",".join(topics))
              .option("startingOffsets", args.starting_offsets)
              # Without this the `headers` column is absent, every globalId reads NULL and
              # the whole stream silently degrades to the by-topic fallback (finding E2).
              .option("includeHeaders", "true")
              .option("kafka.security.protocol", "SASL_SSL")
              .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
              .option("kafka.sasl.jaas.config",
                      "software.amazon.msk.auth.iam.IAMLoginModule required;")
              .option("kafka.sasl.client.callback.handler.class",
                      "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
              .load())

    stats = {"batches": 0, "rows": 0, "poison": 0, "tombstones": 0, "watermark": None}
    query_ref: dict = {}

    def record_batch(batch_id: int, rows_written: int) -> None:
        """State after a committed batch -- THROTTLED (`cdc/streaming_state.py`).

        Called from inside `process`, so it runs BEFORE Spark commits the offsets for this
        batch: state can therefore be a batch ahead of the offsets after a crash, never a
        batch behind. Ahead is the safe direction -- the checkpoint is authoritative for
        position, and this row is a mirror an operator reads.

        A failure to write state must NOT fail the batch. Losing a heartbeat costs
        visibility; failing the batch costs the rows, because `foreachBatch` raising is how
        Spark is told the batch did not happen.
        """
        from cdc import streaming_state as st

        now = datetime.now(timezone.utc)
        try:
            if not st.heartbeat_due(last_written=state.last_written, now=now,
                                    interval_seconds=policy.heartbeat_seconds,
                                    rows=rows_written,
                                    status_changed=state.status != _ING().STATUS_RUNNING):
                return
            query = query_ref.get("q")
            offsets = st.offsets_json(query.lastProgress if query is not None else None)
            snapshot = None
            if rows_written:
                snap = spark.sql(f"SELECT snapshot_id FROM {args.table}.snapshots "
                                 f"ORDER BY committed_at DESC LIMIT 1").collect()
                snapshot = snap[0][0] if snap else None
            state.write(_ING().STATUS_RUNNING, reason="batch", now=now, batch_id=batch_id,
                        offsets=offsets, watermark=stats["watermark"],
                        snapshot_id=snapshot, rows_last_batch=rows_written,
                        rows_total=stats["rows"], commit_ts=now)
        except Exception as exc:                       # noqa: BLE001 - see docstring
            print(f"FULL_CDC_STREAM_STATE_WRITE_FAILED batch={batch_id} {type(exc).__name__}: "
                  f"{exc}", flush=True)

    def process(batch_df, batch_id: int) -> None:
        """One micro-batch. Per TOPIC, because the writer schema is per topic."""
        batch_df.persist()
        try:
            tomb = batch_df.filter(F.col("value").isNull()).count()
            stats["tombstones"] += tomb
            present = [r["topic"] for r in
                       batch_df.select("topic").distinct().collect()]
            total = 0
            for topic in present:
                raw = batch_df.filter(F.col("topic") == topic)
                route = router.route_topic(topic) if router is not None else None
                if route is not None and not route.routed:
                    metrics.record(route, handle_unrouted(spark, raw, route, args, topic))
                    if not writes.legacy:
                        continue
                enrich = None
                if route is not None and route.routed and writes.per_table:
                    enrich = per_table.canonical_extras(route.entry, run_id=run_id,
                                                        job="full_cdc_stream")
                rows, n_poison = decode_topic_to_rows(
                    spark, raw, topic, schemas, args.lake_bucket, args.quarantine_table,
                    enrich=enrich)
                stats["poison"] += n_poison
                if n_poison:
                    print(f"FULL_CDC_STREAM_QUARANTINED batch={batch_id} {topic} "
                          f"rows={n_poison}", flush=True)
                if rows is None:
                    continue
                n = 0
                if writes.legacy:
                    legacy_rows = rows
                    if enrich is not None:
                        legacy_rows = rows.drop(*[c for c in rows.columns
                                                  if c in PER_TABLE_ONLY_COLUMNS])
                    n = merge_into_full_cdc(spark, legacy_rows, args.table)
                if route is not None and route.routed and writes.per_table:
                    written = per_table.write_routed(spark, rows, route,
                                                     event_index_table=index_table)
                    metrics.record(route, written["rows"])
                    n = n or written["rows"]
                if n:
                    # The BUSINESS watermark, one aggregation over a DataFrame the MERGE
                    # has already materialised (`merge_into_full_cdc` counts it first), and
                    # skipped entirely when the topic wrote nothing. Source time, not Kafka
                    # time: "how far behind the source are we" is the question this answers,
                    # and transport time cannot answer it.
                    mark = rows.selectExpr("max(source_commit_ts) AS w").first()
                    if mark and mark["w"] and (stats["watermark"] is None
                                               or mark["w"] > stats["watermark"]):
                        stats["watermark"] = mark["w"]
                total += n
                print(f"FULL_CDC_STREAM_TOPIC batch={batch_id} {topic} rows={n}",
                      flush=True)
            stats["batches"] += 1
            stats["rows"] += total
            print(f"FULL_CDC_STREAM_BATCH id={batch_id} rows={total} "
                  f"tombstones={tomb} poison={stats['poison']}", flush=True)
            record_batch(batch_id, total)
        finally:
            batch_df.unpersist()

    # --- mode and trigger come from the PLAN, not from the submit command -------------
    # (resolved in `resolve_policy` above, before Kafka was touched)
    _ing = _ING()
    mode, trigger_s, checkpoint = policy.mode, policy.trigger_seconds, policy.checkpoint

    writer = (stream.writeStream
              .foreachBatch(process)
              .option("checkpointLocation", checkpoint))

    # THE ONLY DIFFERENCE BETWEEN THE TWO MODES IS THE TRIGGER.
    # Both run the identical `process` function -- the same decode, contract validation,
    # normalisation, routing and Iceberg write. A separate "batch ingestion path" would be
    # two implementations of one contract, and they would drift.
    if mode == _ing.MODE_CONTINUOUS:
        writer = writer.trigger(processingTime=f"{trigger_s} seconds")
    else:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    query_ref["q"] = query
    print(f"FULL_CDC_STREAM_STARTED id={query.id} app={policy.app_id} mode={mode} "
          f"topics={len(topics)} trigger={trigger_s}s heartbeat={policy.heartbeat_seconds}s "
          f"checkpoint={checkpoint} offsets={args.starting_offsets} "
          f"deployment={deployment_id} restart={state.restart_count}", flush=True)

    failure = None
    try:
        if mode == _ing.MODE_CONTINUOUS and args.run_seconds:
            # TEST_ONLY. A resident production app never takes this branch: the budget is
            # None unless a test asked for one.
            print(f"FULL_CDC_STREAM_TEST_BUDGET {args.run_seconds}s -- this is a TEST "
                  f"path and must not be used to operate production", flush=True)
            started = time.monotonic()
            while query.isActive and (time.monotonic() - started) < args.run_seconds:
                query.awaitTermination(timeout=5)
        else:
            # Resident, or AvailableNow draining to completion. `awaitTermination` with no
            # timeout is what makes the process RESIDENT -- it returns only when the query
            # ends or fails, and a failure raises here rather than reading as a clean exit.
            query.awaitTermination()
    except BaseException as exc:                       # noqa: BLE001
        # The state row is the only durable record that this process died, and why. It is
        # written BEFORE the exception continues, because an EMR job run that failed shows
        # a status and a log group -- not which app id, which checkpoint, or how far it got.
        failure = exc
        _record_stop(state, stats, query, status=_ing.STATUS_FAILED,
                     reason=f"{type(exc).__name__}", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        if query.isActive:
            query.stop()
            print("FULL_CDC_STREAM_STOPPED reason=test_budget_reached", flush=True)
        else:
            print("FULL_CDC_STREAM_STOPPED reason=query_ended", flush=True)

    _record_stop(state, stats, query, status=_ing.STATUS_STOPPED,
                 reason="query ended" if failure is None else "failed")

    import json
    if metrics is not None:
        for line in metrics.lines():
            print(line, flush=True)
    # default=str:  carries the source watermark, which is a datetime. Without it
    # the run dies with 
    # AFTER the data is committed -- a job that did its work and then failed its own
    # summary line. Found live on the Phase I run.
    print(f"FULL_CDC_STREAM_SUMMARY {json.dumps(stats, default=str)}", flush=True)
    snap = spark.sql(f"SELECT snapshot_id FROM {args.table}.snapshots "
                     f"ORDER BY committed_at DESC LIMIT 1").collect()
    print(f"FULL_CDC_STREAM_SNAPSHOT {snap[0][0] if snap else None}", flush=True)
    print(f"FULL_CDC_COUNT {spark.table(args.table).count()}", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
