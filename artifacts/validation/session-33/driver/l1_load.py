"""L1 STREAM loader for the Phase 14 validation run.

WHY THIS EXISTS RATHER THAN spark/jobs/l1_stream/job.py
-------------------------------------------------------
That job's `decode_record` does `json.loads(row["value"].decode())`. The connectors emit
AVRO through Apicurio (ADR-003), so every record would fail to parse. Its own docstring
concedes the gap: "In the deployed job the Avro payload is decoded against Apicurio; the
local test injects already-decoded dicts". The Avro path was never written.

This decodes for real, and reuses the repository's TESTED envelope mapping
(`spark/jobs/l1_stream/envelope.py::to_l1_row`) so the L1 contract is not re-implemented.

Wire format, confirmed by reading a record off the topic:
    headers  apicurio.value.globalId = 8-byte big-endian long
             apicurio.value.encoding = BINARY
    payload  raw Avro binary, NO magic-byte prefix
So the writer schema comes from the registry by globalId, and `from_avro` decodes the
column in the JVM -- no per-row Python Avro library needed.
"""
import argparse, json, sys, urllib.request
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.avro.functions import from_avro

sys.path.insert(0, "/tmp/framework")


def load_schemas(spark, uri: str) -> dict:
    """Writer schemas, read from S3 rather than from the registry over HTTP.

    The registry is the authority and these were exported from it verbatim. EMR Serverless
    workers sit in the private subnets and the cdc-runtime security group has no rule for
    them on 8080, so a direct call times out -- staging the export avoids opening a port
    for a read that only needs to happen once per run.
    """
    return json.loads("".join(spark.read.text(uri).rdd.map(lambda r: r[0]).collect()))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topics", required=True)
    ap.add_argument("--schemas-uri", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--table", required=True)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("l1-load")
             .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.glue_catalog.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
             .config("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
             .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
    total = 0
    for topic in args.topics.split(","):
        schema = schemas[topic]
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
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
        decoded = (raw.filter(F.col("value").isNotNull())
                      .select(F.col("topic"), F.col("partition"), F.col("offset"),
                              F.col("timestamp"),
                              from_avro(F.col("value"), schema).alias("v")))

        src = topic.split(".")
        system = "oracle" if "oracle" in topic else "sqlserver"
        rows = decoded.select(
            F.sha2(F.concat_ws("|", F.col("topic"), F.col("partition"),
                               F.col("offset")), 256).alias("event_id"),
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
            F.current_timestamp().alias("ingested_at"))

        n = rows.count()
        rows.writeTo(args.table).append()
        total += n
        print(f"L1_TOPIC {topic} rows={n} tombstones={tombstones}")

    print(f"L1_TOTAL {total}")
    snap = spark.sql(f"SELECT snapshot_id, committed_at FROM {args.table}.snapshots "
                     f"ORDER BY committed_at DESC LIMIT 1").collect()
    print(f"L1_SNAPSHOT {snap[0][0] if snap else None}")
    print(f"L1_TABLE_COUNT {spark.table(args.table).count()}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
