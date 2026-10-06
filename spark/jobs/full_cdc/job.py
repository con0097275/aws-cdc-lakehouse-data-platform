"""Kafka -> FULL_CDC.  The canonical CDC layer, written directly from the topics.

ARCHITECTURE (docs/TARGET_ARCHITECTURE.md section 3, corrected 2026-08-21)
--------------------------------------------------------------------------
    Kafka --(this job, append-only)--> FULL_CDC   <- CANONICAL
                                          |
                              +-----------+-----------+
                              v                       v
                        REALTIME (T-N -> T)      EOD (as-of T-1)

There is NO staging layer between Kafka and FULL_CDC. The previous pipeline landed into
`kafka_dev_lab_dev_stream.cdc_events` and then MERGEd into full_cdc, which put an
intermediate in the reporting path that ADR-033 forbids reading from -- so the extra hop
bought nothing and cost a full copy of every event.

SCOPE OF THAT DEPRECATION -- IT IS ONE TABLE, NOT THE DATABASE.
Deprecated: `kafka_dev_lab_dev_stream.cdc_events`, the old landing table. Read-only.
STILL LIVE:  `kafka_dev_lab_dev_stream.cdc_events_realtime` -- the SAME database holds the
ACTIVE REALTIME layer (reporting/layers.yaml binds REALTIME to it with `status: ACTIVE`,
materialised by spark/jobs/realtime/job.py). An earlier version of this paragraph said
"that database is now DEPRECATED", which reads as an invitation to drop it and would
destroy the live REALTIME table with it (governance review 2026-09-03, finding G-P1-5).

WHY THIS FILE RATHER THAN spark/jobs/l1_stream/job.py
-----------------------------------------------------
That job's `decode_record` does `json.loads(row["value"].decode())`. The connectors emit
AVRO through Apicurio (ADR-003), so every record fails to parse. Its own docstring concedes
the gap: "In the deployed job the Avro payload is decoded against Apicurio; the local test
injects already-decoded dicts". The Avro path was never written. This decodes for real and
reuses the repository's TESTED envelope mapping so the contract is not re-implemented.

Wire format, confirmed by reading a record off the topic:
    headers  apicurio.value.globalId = 8-byte big-endian long
             apicurio.value.encoding = BINARY
    payload  raw Avro binary, NO magic-byte prefix
So the writer schema comes from the registry by globalId, and `from_avro` decodes the
column in the JVM -- no per-row Python Avro library needed.

IDEMPOTENCE. Append-only is the contract (CLAUDE.md 5.2/5.3: keep every I/U/D, no business
dedup), but a RERUN must not double the history, so the write is a MERGE on `event_id`
rather than a blind append.
"""
# REQUIRED, not stylistic. EMR Serverless emr-7.2.0 runs PYTHON 3.9, and the annotations
# below use PEP 604 unions (`str | None`), which 3.9 evaluates at def-time and rejects with
# `TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'`. The job then dies
# at IMPORT, before a single line runs, 18 seconds into an EMR run. This makes every
# annotation a lazily-evaluated string, so 3.9 never evaluates them.
from __future__ import annotations

import argparse, base64, json, sys, urllib.request
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro

sys.path.insert(0, "/tmp/framework")


def load_schemas(spark, uri: str) -> dict:
    """Writer schemas, read from S3 rather than from the registry over HTTP.

    The registry is the authority and these were exported from it verbatim. EMR Serverless
    workers sit in the private subnets and the cdc-runtime security group has no rule for
    them on 8080, so a direct call times out -- staging the export avoids opening a port
    for a read that only needs to happen once per run.

    Returns the NORMALISED export (see `normalise_schema_export`), not the raw document.
    """
    raw = json.loads("".join(spark.read.text(uri).rdd.map(lambda r: r[0]).collect()))
    return normalise_schema_export(raw)


def normalise_schema_export(raw: dict) -> dict:
    """Accept BOTH export shapes and return {"by_global_id": {...}, "by_topic": {...}}.

    Legacy shape is `{topic: schema}` -- one schema per topic, which is what made schema
    evolution impossible (finding E2): a topic that holds two writer versions has no single
    correct entry, so whichever one was exported last silently mis-decodes the other.

    Current shape adds `by_global_id`, keyed by the `apicurio.value.globalId` the producer
    stamps on every record. `by_topic` is KEPT rather than replaced: it is the fallback for
    a record whose header is absent or whose id is not in the export, and dropping it would
    break every recorded submission recipe that still points at a legacy export.
    """
    if "by_global_id" in raw or "by_topic" in raw:
        return {"by_global_id": {str(k): v for k, v in
                                 (raw.get("by_global_id") or {}).items()},
                "by_topic": dict(raw.get("by_topic") or {})}
    return {"by_global_id": {}, "by_topic": dict(raw)}


def select_schema(global_id, topic: str, schemas: dict):
    """(schema, provenance) for one record group, or (None, reason) if we must quarantine.

    Provenance is returned because "which schema decoded this?" is the first question asked
    of a mis-decoded row, and it is unanswerable after the fact if the choice is implicit.

    THE REFUSAL, AND ITS ONE EXCEPTION.

    When the export IS globalId-keyed, a globalId missing from it is NOT decoded with the
    topic fallback. That fallback is a DIFFERENT writer version by definition, and decoding
    against the wrong version is the failure mode E2 describes: `from_avro` does not raise
    on a compatible-looking mismatch, it returns plausible wrong values. Quarantine is the
    only honest answer -- the record is kept, with its coordinates, for a rerun after the
    export is refreshed.

    But a LEGACY export has no `by_global_id` map at all, and every record on the topic
    carries a header, so that rule would quarantine the entire topic on a job that used to
    run. There is no basis for the refusal there: the by-topic entry is not a wrong choice
    among several, it is the only thing the export knows, and it is exactly as correct as
    it was before this change. So the fallback applies, and says so in the provenance --
    which is the signal to run `cdc-runtime.sh export-schemas`.
    """
    legacy_export = not schemas["by_global_id"]
    if global_id is not None:
        key = str(global_id)
        if key in schemas["by_global_id"]:
            return schemas["by_global_id"][key], f"globalId={key}"
        if legacy_export and topic in schemas["by_topic"]:
            return (schemas["by_topic"][topic],
                    f"by_topic={topic} (LEGACY EXPORT, globalId={key} unverifiable -- "
                    f"run cdc-runtime.sh export-schemas)")
        return None, f"globalId={key} not in export"
    if topic in schemas["by_topic"]:
        return schemas["by_topic"][topic], f"by_topic={topic} (no globalId header)"
    return None, f"no globalId header and no by_topic entry for {topic}"



def _source_position_cols(decoded):
    """The source-side ordering fields THIS topic actually carries.

    Oracle's Debezium envelope exposes `source.scn`, SQL Server's exposes
    `source.commit_lsn` / `source.lsn`; neither has the other's. Naming a field that is not
    in the topic's schema raises AnalysisException, and hardcoding one connector's names
    would make the loop work for four topics and fail for the other four. So the columns
    are resolved per topic from the decoded schema, in a FIXED order -- the hash must not
    depend on dict ordering, or the same source change hashes differently between runs.

    `after` is included because a snapshot record carries no SCN/LSN at all: without it
    every snapshot row of a table would collapse to one identity.
    """
    from pyspark.sql.types import StructType
    src_field = next((f for f in decoded.schema["v"].dataType.fields
                      if f.name == "source"), None)
    names = ({f.name for f in src_field.dataType.fields}
             if src_field is not None and isinstance(src_field.dataType, StructType)
             else set())
    cols = []
    for candidate in ("scn", "commit_scn", "commit_lsn", "lsn", "change_lsn",
                      "event_serial_no", "ts_ms"):
        if candidate in names:
            cols.append(F.coalesce(F.col(f"v.source.{candidate}").cast("string"),
                                   F.lit("")))
    cols.append(F.coalesce(F.to_json(F.col("v.after")), F.lit("")))
    return cols


QUARANTINE_SCHEMA = ("failed_at timestamp, pipeline string, error_class string, "
                     "error_message string, stack_hash string, source_topic string, "
                     "source_partition int, source_offset bigint, schema_id string, "
                     "event_id string, payload_s3_uri string, retry_count int, run_id string")

#: A payload larger than this is not a schema mismatch, and copying it wholesale is how a
#: quarantine prefix becomes the biggest thing in the bucket.
MAX_QUARANTINE_PAYLOAD_BYTES = 1_048_576
#: One poison batch should not turn into an unbounded driver-side collect.
MAX_QUARANTINE_ROWS = 1000


def build_quarantine_records(collected, topic: str, lake_bucket: str, now,
                             error_class: str = "AvroDecodeError",
                             detail: str | None = None):
    """Pure: coordinates -> (s3 key, payload bytes, quarantine row). No Spark, no boto3.

    Split out from the write because THIS is the part that is easy to get wrong and
    impossible to check by reading: the key must match the URI recorded in the row, the
    payload must survive a round trip, and the bound must actually bound. A helper that
    takes plain dicts can be tested on a laptop; the collect-and-put around it cannot.

    Every field of the returned tuple is positional against QUARANTINE_SCHEMA.

    `error_class` separates the two ways a record reaches here, which need different
    responses: AvroDecodeError means the payload is broken, SchemaNotExported means OUR
    export is stale and a rerun after refreshing it will succeed.
    """
    day = now.date().isoformat()
    out = []
    for r in collected:
        ev = f"{r['topic']}-{r['partition']}-{r['offset']}"
        key = f"quarantine-payloads/kafka_to_full_cdc/{day}/{ev}.json"
        uri = f"s3://{lake_bucket}/{key}"
        # The raw bytes are what a re-decode needs, so they are kept verbatim (base64 so
        # the object is text-safe).
        raw = bytes(r["value"] or b"")[:MAX_QUARANTINE_PAYLOAD_BYTES]
        body = json.dumps({
            "topic": r["topic"], "partition": int(r["partition"]),
            "offset": int(r["offset"]),
            "kafka_timestamp": str(r["timestamp"]),
            "error_class": error_class,
            "value_base64": base64.b64encode(raw).decode(),
            "value_bytes": len(raw),
            "truncated": len(bytes(r["value"] or b"")) > MAX_QUARANTINE_PAYLOAD_BYTES,
        }).encode()
        row = (now, "kafka_to_full_cdc", error_class,
               detail or (f"from_avro returned NULL for {topic} under the writer schema "
                          f"selected for this record"),
               error_class.lower(), r["topic"], int(r["partition"]),
               int(r["offset"]), None, ev, uri, 0, f"fullcdc-{day}")
        out.append((key, body, row))
    return out


def quarantine_rows(spark, poison, topic: str, lake_bucket: str, table: str,
                    error_class: str = "AvroDecodeError",
                    detail: str | None = None) -> None:
    """Persist undecodable records: a ROW with coordinates, and the PAYLOAD itself.

    Both halves matter and the second was the one missing. `to_quarantine_row` has always
    computed a `payload_s3_uri`, and its docstring says the payload "is written to encrypted
    S3 and REFERENCED" -- but nothing anywhere performed that write. Every quarantine row
    therefore pointed at an object that did not exist, so the one artefact an engineer needs
    to diagnose a poison record was the one thing not kept.

    The payload is written BEFORE the row, for the same reason the streaming path writes its
    pointer first: a payload with no row is an orphan an operator can still find by prefix;
    a row promising a payload that was never written is a dead reference that looks like
    data loss.
    """
    import boto3

    now = datetime.now(timezone.utc)
    collected = (poison.select("topic", "partition", "offset", "timestamp", "value")
                       .limit(MAX_QUARANTINE_ROWS).collect())
    records = build_quarantine_records(collected, topic, lake_bucket, now,
                                       error_class=error_class, detail=detail)
    if not records:
        return

    s3 = boto3.client("s3")
    rows = []
    for key, body, row in records:
        # Failing to write the payload must not stop the row being recorded: a row whose
        # payload is missing is still a record that this offset was poison, which is the
        # part reconciliation needs.
        try:
            # NO ServerSideEncryption ARGUMENT. `ServerSideEncryption="aws:kms"` without
            # an SSEKMSKeyId does NOT mean "the bucket's KMS key" -- it means the
            # AWS-MANAGED `aws/s3` key, which the Spark and AI roles are not granted. The
            # quarantine payload then becomes unreadable by the very jobs that need it,
            # which is the dead-reference defect again wearing a different hat (observed
            # live 2026-09-06: the first payload landed on 7f03af9e, not the lake CMK).
            # Omitting the argument lets the bucket's default encryption apply, which IS
            # the lake CMK and follows it across a key rotation for free.
            s3.put_object(Bucket=lake_bucket, Key=key, Body=body,
                          ContentType="application/json")
        except Exception as exc:                                   # noqa: BLE001
            print(f"FULL_CDC_QUARANTINE_PAYLOAD_FAILED s3://{lake_bucket}/{key}: "
                  f"{type(exc).__name__}: {exc}", flush=True)
        rows.append(row)

    ddl = QUARANTINE_SCHEMA.replace(" timestamp", " TIMESTAMP")
    db = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} ({ddl})
                  USING iceberg TBLPROPERTIES ('format-version'='2')""")
    spark.createDataFrame(rows, schema=QUARANTINE_SCHEMA).writeTo(table).append()


GLOBAL_ID_HEADER = "apicurio.value.globalId"


def global_id_col():
    """The `apicurio.value.globalId` header, as a long. NULL when absent.

    Apicurio stamps the writer schema's registry id on every record as an 8-byte
    big-endian long (confirmed by reading a record off the topic; see the module
    docstring). This is the field the module always CLAIMED to use and never did --
    finding E2. `conv(hex(...), 16, 10)` is the decode: there is no Spark function
    that reads a binary column as a big-endian integer directly.

    NULL rather than an error when the header is missing, because a record produced
    by anything other than the Apicurio converter is a legitimate case for the
    by-topic fallback, not a failure.
    """
    h = F.filter(F.col("headers"),
                 lambda x: x["key"] == F.lit(GLOBAL_ID_HEADER))
    return F.conv(F.hex(F.element_at(h, 1)["value"]), 16, 10).cast("long")


def build_rows(decoded, topic: str, system: str, src, extra=()):
    """The FULL_CDC projection for ONE decoded schema group.

    Extracted from the loop so it can run per writer version and the results unioned.
    Unioning the PROJECTIONS rather than the decoded envelopes is deliberate: the
    envelope struct differs between versions (that is what schema evolution means),
    while every column below is an explicit scalar, so the union is total and needs
    no allowMissingColumns -- which would quietly NULL a column that disagreed.

    `extra` appends the canonical columns the PER-TABLE targets carry and the legacy
    monolith does not (source_database, schema_global_id, dv_pk_hash, ingest_run_id,
    ingest_job -- see spark/jobs/full_cdc/per_table.py). It is empty for the legacy
    write, so the monolith's schema is untouched, and the two rows still share ONE set
    of identity expressions: computing dv_event_id twice is how two writers of the same
    event end up disagreeing about whether it is the same event.
    """
    return decoded.select(
        # TWO IDENTITIES, answering two different questions
        # (docs/TARGET_ARCHITECTURE.md, "Two identities on every FULL_CDC row").
        #
        # dv_event_id -- "is this the same DELIVERED RECORD?"
        # Kafka coordinates PLUS the broker-assigned record timestamp. The triple
        # (topic, partition, offset) alone is not unique across the life of the
        # project: a rebuilt MSK cluster restarts every partition at offset 0, so a
        # genuinely new snapshot of different data reproduces the same triple. Session
        # 34 hit exactly this -- 5,728 fresh events were all discarded as duplicates by
        # `MERGE ... WHEN NOT MATCHED`, the table stayed at its previous count, no
        # curated partition appeared for the new business date, and nothing errored.
        # The record timestamp separates two ingests while staying CONSTANT for
        # re-reads of the same record, which is what rerun idempotence needs
        # (CLAUDE.md 5.3). This is the MERGE key.
        F.sha2(F.concat_ws("|", F.col("topic"), F.col("partition"),
                           F.col("offset"), F.col("timestamp")), 256).alias("dv_event_id"),
        # dv_src_event_id -- "is this the same SOURCE CHANGE?"
        # Derived from the source's own coordinates and carries NO Kafka identity, so
        # the same commit redelivered through a new cluster, a re-snapshot or a
        # connector reset is still recognisable as one business event. Deliberately
        # NOT the MERGE key: collapsing on it would erase a legitimate redelivery that
        # downstream reconciliation needs to see.
        F.sha2(F.concat_ws("|", F.lit(system), F.lit(src[-2]), F.lit(src[-1]),
                           *_source_position_cols(decoded)), 256).alias("dv_src_event_id"),
        F.lit(system).alias("source_system"),
        F.lit(src[-2]).alias("source_schema"),
        F.lit(src[-1]).alias("source_table"),
        F.col("v.op").alias("op"),
        F.to_date(F.from_unixtime(F.col("v.source.ts_ms") / 1000)).alias("event_date"),
        F.to_timestamp(F.from_unixtime(F.col("v.source.ts_ms") / 1000)).alias("source_commit_ts"),
        # DATA_CONTRACTS section 4: the ordering position is source-specific, and the
        # two sources do not share field names. Spark resolves struct fields at ANALYSIS
        # time, so referencing commit_lsn against an Oracle schema fails the job outright
        # rather than yielding null -- the branch has to happen here, not in a coalesce.
        #   Oracle      commit_scn (primary), scn (secondary)
        #   SQL Server  commit_lsn (primary), change_lsn (secondary)
        (F.col("v.source.commit_scn") if system == "oracle"
         else F.col("v.source.commit_lsn")).cast("string").alias("position_primary"),
        (F.col("v.source.scn") if system == "oracle"
         else F.col("v.source.change_lsn")).cast("string").alias("position_secondary"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.to_json(F.col("v.after")).alias("payload_after"),
        F.to_json(F.col("v.before")).alias("payload_before"),
        F.current_timestamp().alias("ingested_at"),
        *extra)


def decode_topic_to_rows(spark, raw, topic: str, schemas: dict, lake_bucket: str,
                         quarantine_table: str, enrich=None):
    """Kafka rows for ONE topic -> FULL_CDC projection, + the poison count.

    Extracted so the BATCH job and the STREAMING job share one decode path. Two
    implementations of this would be the OPEN-28 defect all over again: the DAG
    adapter and `reporting-live-run.py` looked equivalent and were not, and the
    difference only surfaced live. Everything expensive to get right lives here --
    per-globalId schema selection, PERMISSIVE decode, the `op`-based poison test,
    the payload write -- so neither caller can drift from it.

    Returns `(rows_df_or_None, n_poison)`. None means nothing decodable in this
    batch, which is a normal outcome for a streaming micro-batch, not an error.

    `enrich(decoded, global_id)` -> extra columns, called once per WRITER VERSION. The
    globalId is only in scope here, which is why the callback lives here rather than
    around the outside: the per-table row records which schema decoded it, and after
    this loop that fact is gone.
    """
    valued = (raw.filter(F.col("value").isNotNull())
                 .withColumn("global_id", global_id_col()))
    valued.cache()
    src = topic.split(".")
    system = "oracle" if "oracle" in topic else "sqlserver"
    group_ids = [r["global_id"] for r in
                 valued.select("global_id").distinct().collect()]
    frames, n_poison = [], 0
    for gid in group_ids:
        subset = valued.filter(F.col("global_id").eqNullSafe(F.lit(gid)))
        group_schema, provenance = select_schema(gid, topic, schemas)
        if group_schema is None:
            # NOT decoded with the by-topic fallback: that is a different writer
            # version by definition, and decoding against the wrong one is the very
            # failure this fix exists to stop. Kept whole for a rerun once the export
            # is refreshed.
            k = subset.count()
            n_poison += k
            quarantine_rows(spark, subset, topic, lake_bucket,
                            quarantine_table, error_class="SchemaNotExported",
                            detail=f"no writer schema for {provenance}; refresh the "
                                   f"registry export and rerun this topic")
            print(f"FULL_CDC_SCHEMA_MISSING {topic} {provenance} rows={k}", flush=True)
            continue
        print(f"FULL_CDC_DECODE {topic} {provenance}", flush=True)
        # PERMISSIVE, NOT THE DEFAULT FAILFAST -- this is the quarantine path
        # (CLAUDE.md 5.10, finding G-P1-1).
        #
        # `from_avro` defaults to FAILFAST, so ONE undecodable record killed the entire
        # run and the canonical layer had no quarantine path at all: the only
        # implementation of 5.10 lived in the superseded l1_stream job, whose
        # `payload_s3_uri` was never even written. PERMISSIVE yields a NULL struct for a
        # record that cannot be parsed, which turns "the job dies" into "this row is
        # separable" -- and a row we can separate is a row we can quarantine with its
        # coordinates instead of losing.
        #
        # Dropping them silently would be worse than failing. A poison record that
        # vanishes is indistinguishable from one that never existed, and the topic
        # counts stop reconciling with no way to find out why.
        decoded_all = subset.select(
            F.col("topic"), F.col("partition"), F.col("offset"),
            F.col("timestamp"), F.col("value"),
            from_avro(F.col("value"), group_schema,
                      {"mode": "PERMISSIVE"}).alias("v"))
        decoded_all.cache()
        # `v IS NULL` IS NOT ENOUGH -- proven live 2026-09-06.
        #
        # PERMISSIVE `from_avro` does not return a NULL STRUCT for an unparseable
        # payload. Avro binary carries no magic bytes and no checksum, so arbitrary
        # bytes decode "successfully" into a struct whose FIELDS are all null. A
        # deliberately corrupt record injected into cdc.oracle.COREBANK.BRANCH was
        # therefore ingested rather than quarantined, landing in the CANONICAL layer
        # with op, position_primary and payload_after all NULL -- silently corrupt,
        # which is worse than the FAILFAST crash this path replaced.
        #
        # `op` is the discriminator: every Debezium envelope carries one ('c','u','d',
        # 'r'), and tombstones -- the only legitimate null-valued records -- were
        # already filtered out above by `value IS NOT NULL`. So a decoded row with a
        # null `op` did not come from a Debezium envelope, whatever from_avro claims.
        poison = decoded_all.filter(F.col("v").isNull() | F.col("v.op").isNull())
        k = poison.count()
        if k:
            n_poison += k
            quarantine_rows(spark, poison, topic, lake_bucket,
                            quarantine_table)
        decoded = decoded_all.filter(F.col("v").isNotNull()
                                     & F.col("v.op").isNotNull())
        if decoded.head(1):
            extra = enrich(decoded, gid) if enrich else ()
            frames.append(build_rows(decoded, topic, system, src, extra))
    if not frames:
        # None, not an empty DataFrame: the caller decides what "nothing decodable" means.
        # For the batch job it is a topic with only tombstones or only poison; for the
        # streaming job it is the ordinary case of a micro-batch with no new records.
        return None, n_poison
    rows = frames[0]
    for f_ in frames[1:]:
        rows = rows.unionByName(f_)
    return rows, n_poison


def merge_into_full_cdc(spark, rows, table: str, backfill_identity: bool = False) -> int:
    """Align to the deployed schema and MERGE on `event_id`. Shared, for the same
    reason `decode_topic_to_rows` is: append-only is the LAYER's contract, and the
    idempotent-rerun MERGE that implements it must not exist twice.

    THE SESSION COMES FROM THE DATAFRAME, NOT THE ARGUMENT.

    `foreachBatch` hands the callback a DataFrame owned by a CLONED SparkSession, not the
    one that started the query. `createOrReplaceTempView` registers the view on the
    DataFrame's session, so a `spark.sql("MERGE ... USING full_cdc_src")` against the
    ORIGINAL session cannot see it and fails with
    `TABLE_OR_VIEW_NOT_FOUND: full_cdc_src` -- on the first micro-batch that has rows,
    after the query is already running. Batch mode never showed it because there the two
    sessions are the same object.

    Rebinding here rather than at the call site keeps both callers correct by construction.
    """
    spark = rows.sparkSession
    n = rows.count()
    # MERGE, not append: rerunning this job over the same topics must be a no-op
    # (CLAUDE.md 5.3). Append-only describes the LAYER -- no row is ever updated or
    # deleted -- not the write mode of a job that can be retried.
    # SCHEMA ALIGNMENT.
    #
    # FULL_CDC must carry the FULL envelope plus Kafka metadata (kafka_timestamp,
    # ingested_at). The deployed table predates that: it was created by the old
    # staging job with the narrower set and a `loaded_at` column, so a bare
    # `INSERT *` fails with UNRESOLVED_COLUMN on whichever side is missing a name.
    #
    # Iceberg evolves by name, so widen the table with any column it lacks (existing
    # rows read NULL, which is accurate -- they were loaded before the column existed)
    # and supply NULL for any legacy column the producer no longer emits.
    # `event_id` is retained as an alias of dv_event_id: the deployed table and every
    # downstream reader (EOD job, reconciliation) still name it, and renaming a column
    # that other jobs MERGE on is a separate, coordinated change.
    rows = (rows.withColumn("event_id", F.col("dv_event_id"))
                .withColumn("loaded_at", F.current_timestamp()))
    target = {f.name: f for f in spark.table(table).schema.fields}
    for f in rows.schema.fields:
        if f.name not in target:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN "
                      f"{f.name} {f.dataType.simpleString()}")
            print(f"FULL_CDC_SCHEMA_EVOLVED added={f.name}")
    target = {f.name: f for f in spark.table(table).schema.fields}
    for name, f in target.items():
        if name not in rows.columns:
            rows = rows.withColumn(name, F.lit(None).cast(f.dataType))
    rows = rows.select(*target.keys())

    rows.createOrReplaceTempView("full_cdc_src")
    # WHEN NOT MATCHED only. FULL_CDC is append-only: a matched row is the SAME
    # delivered record and must not be rewritten, or the layer stops being an event log.
    matched_clause = ""
    if backfill_identity:
        # The exception, and it is a MIGRATION rather than a data change: dv_event_id /
        # dv_src_event_id were added after these rows were written, so they read NULL.
        # Only the identity columns are touched; no business column is in the SET list.
        matched_clause = ("WHEN MATCHED THEN UPDATE SET "
                          "t.dv_event_id = s.dv_event_id, "
                          "t.dv_src_event_id = s.dv_src_event_id ")
    spark.sql(f"""MERGE INTO {table} t USING full_cdc_src s
                  ON t.event_id = s.event_id
                  {matched_clause}
                  WHEN NOT MATCHED THEN INSERT *""")
    return n


# --------------------------------------------------------------------------- #
# PER-TABLE ROUTING (ADR-062 Phase B, ADR-063). Off by default.
# --------------------------------------------------------------------------- #

LEGACY_ONLY = "legacy_only"

#: Columns the per-table row carries and the monolith does not. Dropped before the legacy
#: MERGE so dual-write cannot widen the deployed table as a side effect (section H).
PER_TABLE_ONLY_COLUMNS = ("source_database", "schema_global_id", "dv_pk_hash",
                          "ingest_run_id", "ingest_job", "payload_after_typed",
                          "payload_before_typed")


class LegacyWrites:
    """What `--migration-mode legacy_only` does: the monolith, and nothing else.

    A hand-written stand-in for `cdc.router.WritePlan` ON PURPOSE. The default path must not
    import the `cdc` package at all: it is staged separately (spark.submit.pyFiles), and a
    job that imported it unconditionally would start FAILING AT IMPORT on every existing
    submission recipe the moment the package was absent -- turning an opt-in feature into a
    production outage. `test_cdc_router.py` asserts this class and `writes_for(LEGACY_ONLY)`
    agree, so the stand-in cannot drift from the thing it stands in for.
    """
    legacy = True
    per_table = False
    event_index = False

    def describe(self):
        return "legacy"


def read_table_plan(spark, uri: str) -> dict:
    """The compiled plan, from S3 or from a local path. Read through Spark for the same
    reason `load_schemas` is: EMR workers have no direct HTTP path to anything, and the
    plan is a small text object already staged beside the schemas export."""
    if uri.startswith(("s3://", "s3a://", "file:")):
        text = "".join(spark.read.text(uri).rdd.map(lambda r: r[0]).collect())
    else:
        with open(uri) as fh:
            text = fh.read()
    return json.loads(text)


def routing_context(spark, args, job_name: str):
    """(writes, router, run_id). Imports `cdc` ONLY when a per-table write is requested."""
    run_id = args.run_id or f"{job_name}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    if args.migration_mode == LEGACY_ONLY:
        print(f"CDC_MIGRATION_MODE {LEGACY_ONLY} writes=legacy run_id={run_id}", flush=True)
        return LegacyWrites(), None, run_id

    from cdc.router import TableRouter, parse_mode, parse_policy, writes_for

    if not args.table_plan:
        raise SystemExit(
            f"--migration-mode {args.migration_mode} needs --table-plan: routing is driven "
            f"by the COMPILED plan, never by the YAML registry, so a run cannot route "
            f"against a config nobody hashed. Compile it with "
            f"`python3 -m cdc.compile --out artifacts/cdc/table-plan.json`.")
    mode = parse_mode(args.migration_mode)
    plan = read_table_plan(spark, args.table_plan)
    router = TableRouter.from_plan(plan, parse_policy(args.unknown_table_policy))
    writes = writes_for(mode, event_index=args.event_index)
    print(f"CDC_MIGRATION_MODE {mode.value} writes={writes.describe()} "
          f"config_version={router.config_version} tables={len(router.table_ids)} "
          f"policy={args.unknown_table_policy} run_id={run_id}", flush=True)
    return writes, router, run_id


def handle_unrouted(spark, raw, route, args, topic: str) -> int:
    """An event whose table is not registered, or is registered and disabled.

    QUARANTINE keeps it with its coordinates and payload so it can be replayed once the
    table is registered -- the same treatment a poison record gets, for the same reason:
    a record we cannot place is worse lost than kept. REJECT stops the run instead, which
    is right when continuing would bank more of a misconfiguration, and wrong as a default
    because it also stops the seven healthy topics.

    Under NO policy is the event written to a target of our choosing, and under no policy
    is a table created for it (phase brief sections E and F).
    """
    n = raw.filter(F.col("value").isNotNull()).count()
    print(f"CDC_ROUTER_UNROUTED topic={topic} outcome={route.outcome.value} rows={n} "
          f"reason={route.reason}", flush=True)
    if args.unknown_table_policy == "reject":
        raise RuntimeError(
            f"CDC_ROUTER_REJECT {topic}: {route.reason} "
            f"(--unknown-table-policy reject). Register and provision the table, or rerun "
            f"with --unknown-table-policy quarantine to keep the records for a replay.")
    if n:
        quarantine_rows(spark, raw.filter(F.col("value").isNotNull()), topic,
                        args.lake_bucket, args.quarantine_table,
                        error_class="UnroutedTable", detail=route.reason)
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topics", required=True)
    ap.add_argument("--schemas-uri", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--table", required=True)
    # Optional, and derived from --warehouse when omitted. Making it REQUIRED would have
    # been the tidier signature and the wrong call: every recorded submission recipe in
    # artifacts/ and docs/ omits it, so the flag would turn a documented rerun into an
    # argparse error on a job that has nothing to quarantine. The warehouse URI already
    # names the lake bucket.
    ap.add_argument("--lake-bucket", default=None,
                    help="bucket for quarantined payloads (CLAUDE.md 5.10); "
                         "defaults to the bucket in --warehouse")
    ap.add_argument("--quarantine-table",
                    default="glue_catalog.kafka_dev_lab_dev_quarantine.full_cdc_rejects")
    # PER-TABLE ROUTING. Every default here reproduces today's behaviour exactly: mode
    # legacy_only, no plan, no index. Section H -- the cutover is a deliberate act, not a
    # consequence of deploying this file.
    ap.add_argument("--migration-mode", default=LEGACY_ONLY,
                    choices=["legacy_only", "dual_write", "per_table_only"],
                    help="legacy_only (default) writes ONLY the monolith and behaves "
                         "exactly as before; dual_write writes both; per_table_only "
                         "writes only the routed per-table targets (ADR-063)")
    ap.add_argument("--table-plan", default=None,
                    help="compiled CDC plan (local path or s3://). Required unless "
                         "--migration-mode legacy_only")
    ap.add_argument("--unknown-table-policy", default="quarantine",
                    choices=["quarantine", "reject"],
                    help="what to do with an event whose table is not registered")
    ap.add_argument("--event-index", action="store_true",
                    help="also write the cross-source coordinate index in OPS "
                         "(ADR-062 section G). Coordinates only, never payloads")
    ap.add_argument("--run-id", default=None,
                    help="stamped on every per-table row as ingest_run_id")
    ap.add_argument("--backfill-identity", action="store_true",
                    help="One-time migration: populate dv_event_id / dv_src_event_id on "
                         "rows that already exist. FULL_CDC is immutable as to BUSINESS "
                         "content; this rewrites identity columns only, and only for "
                         "records still readable from Kafka.")
    args = ap.parse_args()
    if not args.lake_bucket:
        args.lake_bucket = args.warehouse.split("://", 1)[-1].split("/", 1)[0]

    spark = (SparkSession.builder.appName("l1-load")
             .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.glue_catalog.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
             .config("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
             .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             # ADR-024: cutoffs and partitions are UTC. `event_date` below is derived with
             # from_unixtime(source.ts_ms), which resolves in the SESSION time zone -- so a
             # non-UTC default silently partitions rows into the wrong day and shifts the
             # EOD cutoff by that offset. It fails as wrong numbers, never as an error.
             # Relying on the JVM default being UTC is exactly what ADR-024 forbids.
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
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
    writes, router, run_id = routing_context(spark, args, "full_cdc_batch")
    metrics = None
    if router is not None:
        from cdc.router import RouterMetrics
        metrics = RouterMetrics()
        import per_table
    index_table = None
    if writes.event_index:
        index_table = (router.event_index or {}).get("identifier")
    total = 0
    for topic in args.topics.split(","):
        # No `schemas[topic]` lookup here any more -- the schema is chosen per record
        # group, below. A KeyError at this point also used to kill the whole run for a
        # topic that simply had nothing exported yet.
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               # Without this the `headers` column is not produced at all and every
               # globalId reads NULL, which silently degrades the whole topic to the
               # by-topic fallback -- i.e. back to the E2 behaviour, invisibly.
               .option("includeHeaders", "true")
               .option("kafka.security.protocol", "SASL_SSL")
               .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
               .option("kafka.sasl.jaas.config",
                       "software.amazon.msk.auth.iam.IAMLoginModule required;")
               .option("kafka.sasl.client.callback.handler.class",
                       "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
               .load())

        # Tombstones carry a null value by design (CLAUDE.md 5.7). from_avro would fail on
        # them, and dropping them silently would make deletes ambiguous downstream, so they
        # are counted and excluded from the decode here rather than ignored.
        tombstones = raw.filter(F.col("value").isNull()).count()
        # PERMISSIVE, NOT THE DEFAULT FAILFAST -- this is the quarantine path (CLAUDE.md
        # 5.10, finding G-P1-1).
        #
        # `from_avro` defaults to FAILFAST, so ONE undecodable record killed the entire run
        # and the canonical layer had no quarantine path at all: the only implementation of
        # 5.10 lived in the superseded l1_stream job, whose `payload_s3_uri` was never even
        # written. PERMISSIVE yields a NULL struct for a record that cannot be parsed, which
        # turns "the job dies" into "this row is separable" -- and a row we can separate is
        # a row we can quarantine with its coordinates instead of losing.
        #
        # Dropping them silently would be worse than failing. A poison record that vanishes
        # is indistinguishable from one that never existed, and the topic counts stop
        # reconciling with no way to find out why.
        # ONE DECODE PER WRITER VERSION, not one per topic (finding E2).
        #
        # This used to pick `schemas[topic]` and decode the whole topic with it. A topic
        # that holds two writer versions -- which is exactly what happens the moment any
        # source DDL lands -- has no single correct entry, so the added column was decoded
        # against a schema that does not describe it. `from_avro` does not raise on that:
        # it returns PLAUSIBLE WRONG VALUES, in the canonical layer, silently. That is why
        # the schema-evolution scenario was never run.
        #
        # So records are grouped by the globalId the producer actually stamped on them and
        # each group is decoded with ITS OWN schema. The projections are unioned, not the
        # envelopes -- see build_rows.
        # ROUTE FIRST, decode second. A topic with no registered table must not be
        # decoded into a projection nobody has a target for -- and under `reject` the run
        # should stop before it spends a decode on data it will refuse to write.
        route = router.route_topic(topic) if router is not None else None
        if route is not None and not route.routed:
            metrics.record(route, handle_unrouted(spark, raw, route, args, topic))
            if not writes.legacy:
                continue

        enrich = None
        if route is not None and route.routed and writes.per_table:
            enrich = per_table.canonical_extras(route.entry, run_id=run_id,
                                                job="full_cdc_batch")
        rows, n_poison = decode_topic_to_rows(
            spark, raw, topic, schemas, args.lake_bucket, args.quarantine_table,
            enrich=enrich)
        if n_poison:
            print(f"FULL_CDC_QUARANTINED {topic} rows={n_poison}", flush=True)
        if rows is None:
            print(f"FULL_CDC_TOPIC {topic} rows=0 tombstones={tombstones} "
                  f"quarantined={n_poison}")
            continue
        n = 0
        if writes.legacy:
            # The legacy projection is the SAME rows minus the extra columns. Selecting
            # them off rather than decoding twice keeps one decode and one set of identity
            # expressions; `merge_into_full_cdc` would otherwise widen the monolith with
            # per-table columns as a side effect of dual-write, which section H forbids.
            legacy_rows = rows
            if enrich is not None:
                legacy_rows = rows.drop(*[c for c in rows.columns
                                          if c in PER_TABLE_ONLY_COLUMNS])
            n = merge_into_full_cdc(spark, legacy_rows, args.table,
                                    args.backfill_identity)
        if route is not None and route.routed and writes.per_table:
            written = per_table.write_routed(spark, rows, route,
                                             event_index_table=index_table)
            metrics.record(route, written["rows"])
            n = n or written["rows"]
        total += n
        print(f"FULL_CDC_TOPIC {topic} rows={n} tombstones={tombstones}")

    if metrics is not None:
        for line in metrics.lines():
            print(line, flush=True)
    print(f"FULL_CDC_INGESTED {total}")
    if writes.legacy:
        snap = spark.sql(f"SELECT snapshot_id, committed_at FROM {args.table}.snapshots "
                         f"ORDER BY committed_at DESC LIMIT 1").collect()
        print(f"L1_SNAPSHOT {snap[0][0] if snap else None}")
        print(f"FULL_CDC_COUNT {spark.table(args.table).count()}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
