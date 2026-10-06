"""STREAMING_RT app 1 of 4 -- the LONG-RUNNING stream. Kafka -> rt_account_stream.

    spark-submit rt_stream_app.py --bootstrap ... --run-seconds 240

This is a PROCESS, not a job. It has its own trigger loop (30s micro-batches) and lives
until stopped. `--run-seconds` exists purely so a demonstration costs minutes instead of a
working day: the reference runs 08:00->20:00, this stops itself after a bounded window and
reports what it did. Nothing else about the shape changes.

WHAT IT DOES PER MICRO-BATCH
  1. decode the ACCOUNT CDC envelope        (fact)
  2. dedup to one row per account BY EVENT TIME, not arrival   <- rt_common.dedup_latest
  3. refresh the CUSTOMER dim cache          (dim)
  4. detect dim VALUE changes -> pointer table 2
  5. enrich; split complete / incomplete
  6. complete   -> MERGE into rt_account_stream
     incomplete -> hold briefly, then flag into pointer table 1 AND publish with NULL dims
  7. write BOTH -- pointer first, row second

WHY POINTER-BEFORE-ROW (step 7) IS NOT ARBITRARY
The reference orders these deliberately. If the pointer write succeeds and the row write
fails, the correction pass repairs an account that was never published -- harmless. Reverse
the order and a failure publishes a dim-incomplete row with NOTHING flagging it, so it stays
wrong until the next full EOD rebuild and no query can tell.

THE FAILURE CONTRACT
A partial failure RAISES. With `foreachBatch`, swallowing an exception tells Spark the batch
succeeded, so it commits the offsets and the rows are gone -- not in the target, not in a
queue, nowhere. The reference records ~100K offsets lost exactly this way. Raising leaves
the offsets uncommitted and Spark retries with the data still in Kafka.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

sys.path.insert(0, "/tmp/framework")
sys.path.insert(0, "/opt/spark/realtime")

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro

from cdc_source import full_cdc_for, latest_state_window          # noqa: E402
from rt_common import (AccountEvent, CustomerDim, PendingEntry, PendingQueue,
                       dedup_latest_per_account, detect_dim_changes, enrich,
                       split_by_completeness)

STREAM_TABLE = "glue_catalog.kafka_dev_lab_dev_stream.rt_account_stream"
PENDING_TABLE = "glue_catalog.kafka_dev_lab_dev_ops.rt_pending_dim"
AUDIT_TABLE = "glue_catalog.kafka_dev_lab_dev_ops.rt_dim_change_audit"
#: The CUSTOMER dimension's canonical source, named by canonical id rather than by physical
#: table. `cdc_source.full_cdc_for` resolves where it currently lives via the per-table
#: cutover mode -- naming `cdc_events` directly meant this read went stale, silently, the
#: moment CUSTOMER cut over to PER_TABLE (ADR-072).
CUSTOMER_TABLE_ID = "oracle.coredb.corebank.customer"


def build_spark(app: str) -> SparkSession:
    return (SparkSession.builder.appName(app)
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # ADR-024. event_date and every cutoff derive from the session zone; a non-UTC
            # default silently shifts rows between business days.
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def load_dim_cache(spark) -> dict[int, CustomerDim]:
    """The CUSTOMER dimension, from the canonical layer.

    Read from FULL_CDC rather than a second Kafka subscription: the dimension is small and
    slow-moving, and one broadcast-sized snapshot per micro-batch is cheaper and simpler than
    maintaining a second stateful stream. The reference broadcasts its small dims for the
    same reason -- and learned, at a 622K-row table, that `F.broadcast()` is a FORCING hint
    that ignores the size threshold and OOMs the driver. This dim is ~200 rows.
    """
    # Resolved, not named: in LEGACY mode this is the monolith WITH its mandatory predicate
    # applied for us; in PER_TABLE mode the table itself is the filter.
    df = (full_cdc_for(spark, CUSTOMER_TABLE_ID)
          .withColumn("j", F.from_json(F.coalesce(F.col("payload_after"),
                                                  F.col("payload_before")),
                                       "CUSTOMER_ID string, SEGMENT_CODE string, "
                                       "BRANCH_ID string, STATUS string")))
    # Source-native ordering, shared with the certified EOD close. The previous form cast
    # position to DECIMAL -- Oracle-only, NULL for a SQL Server hex LSN -- and tie-broke on
    # `kafka_offset` without `kafka_partition`, which CLAUDE.md 5.4 forbids.
    w = latest_state_window("oracle", "j.CUSTOMER_ID")
    latest = (df.withColumn("rn", F.expr(w))
                .filter((F.col("rn") == 1) & (F.col("op") != "d")))
    out: dict[int, CustomerDim] = {}
    for r in latest.select("j.CUSTOMER_ID", "j.SEGMENT_CODE", "j.BRANCH_ID").collect():
        if r["CUSTOMER_ID"] is None:
            continue
        out[int(r["CUSTOMER_ID"])] = CustomerDim(
            customer_id=int(r["CUSTOMER_ID"]),
            segment_code=r["SEGMENT_CODE"],
            branch_id=int(r["BRANCH_ID"]) if r["BRANCH_ID"] else None)
    return out


def decode_account_batch(batch_df, schema: str) -> list[AccountEvent]:
    """Debezium ACCOUNT envelope -> facts.

    Tombstones carry a null value and are EXCLUDED from decode (counted by the caller):
    `from_avro` cannot parse them, and the delete is already represented by the `d` envelope
    that precedes the tombstone.
    """
    decoded = (batch_df.filter(F.col("value").isNotNull())
               .select(F.col("offset").alias("kafka_offset"),
                       from_avro(F.col("value"), schema).alias("v")))
    rows = decoded.select(
        F.col("v.op").alias("op"),
        F.to_timestamp(F.from_unixtime(F.col("v.source.ts_ms") / 1000)).alias("source_ts"),
        F.to_json(F.col("v.after")).alias("after"),
    ).collect()
    out: list[AccountEvent] = []
    for r in rows:
        # A delete has after=null. The reference DROPS deletes from the fast path entirely
        # and lets the nightly rebuild handle them; same choice here, and the same honesty
        # about it -- a closed account keeps its last balance in the stream layer until EOD.
        if r["op"] == "d" or not r["after"]:
            continue
        a = json.loads(r["after"])
        if a.get("ACCOUNT_ID") is None:
            continue
        out.append(AccountEvent(
            account_id=int(a["ACCOUNT_ID"]),
            customer_id=int(a["CUSTOMER_ID"]) if a.get("CUSTOMER_ID") is not None else None,
            balance=float(a.get("BALANCE") or 0),
            currency=a.get("CURRENCY"),
            product_code=a.get("PRODUCT_CODE"),
            account_status=a.get("STATUS"),
            source_ts=r["source_ts"]))
    return out


ROW_SCHEMA = ("account_id bigint, customer_id bigint, balance decimal(18,2), "
              "currency string, product_code string, account_status string, "
              "segment_code string, branch_id bigint, source_ts timestamp, "
              "dim_complete boolean, batch_id bigint, written_at timestamp")


def merge_rows(spark, rows: list, batch_id: int) -> int:
    """MERGE into the stream table, keyed on account_id.

    `WHEN MATCHED AND s.source_ts >= t.source_ts` is the guard that stops an out-of-order
    micro-batch overwriting a newer balance with an older one. Without it, a retried batch
    carrying stale events silently rewinds the published figure.
    """
    if not rows:
        return 0
    now = datetime.now(timezone.utc)
    # DECIMAL, not float. The pure layer carries balance as a float because that is the
    # right type for arithmetic in tests; the WRITE boundary is where money becomes exact.
    # PySpark rejects a float for DecimalType outright (CANNOT_ACCEPT_OBJECT_IN_TYPE), and
    # quantising here also stops a float artefact (12345.669999) reaching the ledger.
    data = [(r.account_id, r.customer_id,
             Decimal(str(r.balance)).quantize(Decimal("0.01")),
             r.currency, r.product_code,
             r.account_status, r.segment_code, r.branch_id, r.source_ts,
             r.dim_complete, batch_id, now) for r in rows]
    spark.createDataFrame(data, schema=ROW_SCHEMA).createOrReplaceTempView("rt_src")
    spark.sql(f"""MERGE INTO {STREAM_TABLE} t USING rt_src s
                  ON t.account_id = s.account_id
                  WHEN MATCHED AND s.source_ts >= t.source_ts THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    return len(rows)


def flag_pending(spark, entries: list[PendingEntry]) -> int:
    if not entries:
        return 0
    now = datetime.now(timezone.utc)
    data = [(e.event.account_id, e.event.customer_id, e.missing_dims, e.enqueued_at,
             e.retry_count, None, None) for e in entries]
    (spark.createDataFrame(
        data, schema=("account_id bigint, customer_id bigint, missing_dims string, "
                      "first_seen_ts timestamp, retry_count int, resolved_ts timestamp, "
                      "resolved_by string"))
     .writeTo(PENDING_TABLE).append())
    return len(entries)


def record_dim_changes(spark, changes: list[dict]) -> int:
    if not changes:
        return 0
    now = datetime.now(timezone.utc)
    data = [(c["customer_id"], c["changed_field"], c["old_value"], c["new_value"], now, None)
            for c in changes]
    (spark.createDataFrame(
        data, schema=("customer_id bigint, changed_field string, old_value string, "
                      "new_value string, detected_ts timestamp, processed_ts timestamp"))
     .writeTo(AUDIT_TABLE).append())
    return len(changes)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", default="cdc.oracle.COREBANK.ACCOUNT")
    ap.add_argument("--schemas-uri", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--checkpoint", required=True,
                    help="MUST NOT live under warehouse/ -- CLAUDE.md 5.9")
    ap.add_argument("--trigger-seconds", type=int, default=30)
    ap.add_argument("--resolver-seconds", type=int, default=5,
                    help="how often the pending resolver re-checks the dim cache, "
                         "INDEPENDENT of micro-batches (reference: 5s)")
    ap.add_argument("--run-seconds", type=int, default=240,
                    help="stop after this long. A demo, not a working day.")
    args = ap.parse_args()

    if "/warehouse/" in args.checkpoint:
        raise SystemExit(
            f"REFUSING: checkpoint {args.checkpoint} is under warehouse/. Iceberg's "
            "remove_orphan_files walks table locations and would delete streaming state "
            "(CLAUDE.md 5.9).")

    spark = build_spark("rt-stream")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    schema = json.loads("".join(
        spark.read.text(args.schemas_uri).rdd.map(lambda r: r[0]).collect()))[args.topic]

    queue = PendingQueue()
    dim_state: dict[int, CustomerDim] = {}
    stats = {"batches": 0, "facts": 0, "full": 0, "pending": 0, "flagged": 0, "changes": 0,
             "resolver_cycles": 0, "resolved_late": 0}
    started = time.monotonic()
    stop_flag = {"stop": False}

    # THE RESOLVER RUNS ON ITS OWN THREAD, every `resolver_seconds`.
    #
    # Draining the pending queue only inside `handle()` ties the timeout to the arrival of
    # NEW DATA, which is exactly backwards: a quiet source is when a pending row most needs
    # to age out and be flagged, and it is precisely when no batch fires. Observed live on
    # the first run of this app -- one account held in the queue for the entire window with
    # `timed_out=0` and an empty pointer table. The reference implementation runs its
    # resolver on a separate 5s thread for this reason.
    def resolver_loop() -> None:
        while not stop_flag["stop"]:
            time.sleep(args.resolver_seconds)
            try:
                now = datetime.now(timezone.utc)
                dims = load_dim_cache(spark)
                recovered, _ = queue.retry_resolvable(dims, now)
                expired = queue.drain_expired(now)
                if expired:
                    # Pointer BEFORE row -- see the module docstring.
                    stats["flagged"] += flag_pending(spark, expired)
                    merge_rows(spark, [enrich(e.event, dims) for e in expired], -1)
                if recovered:
                    stats["resolved_late"] += merge_rows(spark, recovered, -1)
                stats["resolver_cycles"] += 1
                if recovered or expired:
                    print(f"RT_RESOLVER recovered={len(recovered)} "
                          f"timed_out={len(expired)} queue={len(queue)}", flush=True)
            except Exception as exc:                              # noqa: BLE001
                # The resolver must not kill the stream. A failed cycle retries next tick;
                # the entries are still queued and still ageing.
                print(f"RT_RESOLVER_ERROR {type(exc).__name__}: {exc}", flush=True)

    def handle(batch_df, batch_id: int) -> None:
        nonlocal dim_state
        now = datetime.now(timezone.utc)

        events = decode_account_batch(batch_df, schema)
        deduped = dedup_latest_per_account(events)

        dims = load_dim_cache(spark)
        changes = detect_dim_changes(dim_state, dims)
        dim_state = dims

        rows = [enrich(e, dims) for e in deduped]
        full, partial = split_by_completeness(rows)

        by_id = {e.account_id: e for e in deduped}
        for r in partial:
            entry = PendingEntry(event=by_id[r.account_id], missing_dims="customer",
                                 enqueued_at=now)
            if not queue.add(entry):
                # Backpressure, not an error: flag immediately rather than grow the queue.
                flag_pending(spark, [entry])
                merge_rows(spark, [r], batch_id)
                stats["flagged"] += 1

        recovered, _ = queue.retry_resolvable(dims, now)
        expired = queue.drain_expired(now)

        # ORDER IS LOAD-BEARING: pointer first, then the row. See the module docstring.
        stats["flagged"] += flag_pending(spark, expired)
        timed_out_rows = [enrich(e.event, dims) for e in expired]
        stats["changes"] += record_dim_changes(spark, changes)
        stats["full"] += merge_rows(spark, full + recovered + timed_out_rows, batch_id)

        stats["batches"] += 1
        stats["facts"] += len(deduped)
        stats["pending"] = len(queue)
        print(f"RT_BATCH id={batch_id} facts={len(deduped)} full={len(full)} "
              f"recovered={len(recovered)} timed_out={len(expired)} "
              f"dim_changes={len(changes)} queue={len(queue)}", flush=True)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest")
           .option("kafka.security.protocol", "SASL_SSL")
           .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
           .option("kafka.sasl.jaas.config",
                   "software.amazon.msk.auth.iam.IAMLoginModule required;")
           .option("kafka.sasl.client.callback.handler.class",
                   "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
           .option("failOnDataLoss", "false")
           .load())

    q = (raw.writeStream
         .foreachBatch(handle)
         .option("checkpointLocation", args.checkpoint)
         .trigger(processingTime=f"{args.trigger_seconds} seconds")
         .start())

    print(f"RT_STREAM_STARTED id={q.id} runId={q.runId} trigger={args.trigger_seconds}s "
          f"resolver={args.resolver_seconds}s budget={args.run_seconds}s", flush=True)

    import threading
    resolver = threading.Thread(target=resolver_loop, daemon=True, name="rt-resolver")
    resolver.start()

    # The bounded run. `awaitTermination(timeout)` returns False on timeout, which is the
    # normal exit here -- a demonstration that stops itself rather than an operator killing
    # a process and leaving the checkpoint mid-batch.
    while time.monotonic() - started < args.run_seconds:
        if q.awaitTermination(timeout=5):
            break
    if q.isActive:
        q.stop()
        print("RT_STREAM_STOPPED reason=run_seconds_budget_reached", flush=True)
    # Stop the resolver and give it one grace period to finish an in-flight cycle, so a
    # pointer write already under way is not abandoned half-done.
    stop_flag["stop"] = True
    resolver.join(timeout=args.resolver_seconds + 10)

    print(f"RT_STREAM_SUMMARY {json.dumps(stats)}", flush=True)
    print(f"RT_STREAM_LAST_PROGRESS {q.lastProgress}", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
