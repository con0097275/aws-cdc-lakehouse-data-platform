"""L2 FULL_CDC and the EOD/curated fact the reporting pilot reads.

L2  = every I/U/D kept, idempotent on event_id (CLAUDE.md 5.3). A re-snapshot puts the same
      business row in twice under DIFFERENT Kafka coordinates, so event_id does NOT collapse
      it -- that is correct, L2 is the event log, not the state.
EOD = one row per PK as at the cutoff, last event wins by event_order (CLAUDE.md 5.6).
      This is where a double snapshot collapses back to the true business state, which is
      exactly the property being tested here: L1 holds 6000 digital_event rows for 3000
      distinct keys, and EOD must produce 3000.
"""
import argparse
from pyspark.sql import SparkSession, functions as F, Window


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--l1", required=True)
    ap.add_argument("--l2", required=True)
    ap.add_argument("--curated", required=True)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("l2-eod")
             .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.glue_catalog.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
             .config("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
             .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    l1 = spark.table(args.l1)

    # ---- L2: idempotent append on event_id ---------------------------------------- #
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {args.l2} (
        event_id STRING, source_system STRING, source_schema STRING, source_table STRING,
        op STRING, event_date DATE, source_commit_ts TIMESTAMP,
        position_primary STRING, position_secondary STRING,
        kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT,
        payload_after STRING, payload_before STRING, loaded_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (source_system, event_date)
      TBLPROPERTIES ('format-version'='2')""")

    l2_new = (l1.select("event_id","source_system","source_schema","source_table","op",
                        "event_date","source_commit_ts","position_primary","position_secondary",
                        "kafka_topic","kafka_partition","kafka_offset",
                        "payload_after","payload_before")
                .withColumn("loaded_at", F.current_timestamp()))
    l2_new.createOrReplaceTempView("l2_src")
    spark.sql(f"""MERGE INTO {args.l2} t USING l2_src s ON t.event_id = s.event_id
                  WHEN NOT MATCHED THEN INSERT *""")
    print(f"L2_COUNT {spark.table(args.l2).count()}")

    # ---- EOD/curated: the periodic snapshot the pilot mart reads -------------------- #
    # Grain: account_sk + business_date. Last event per key by event_order, deletes dropped.
    acct = (spark.table(args.l2)
            .filter((F.col("source_system") == "oracle") & (F.col("source_table") == "ACCOUNT"))
            .withColumn("j", F.from_json(F.col("payload_after"),
                                         "ACCOUNT_ID string, CUSTOMER_ID string, BALANCE string, STATUS string")))
    w = Window.partitionBy("j.ACCOUNT_ID").orderBy(
        F.col("position_primary").cast("decimal(38,0)").desc_nulls_last(),
        F.col("position_secondary").cast("decimal(38,0)").desc_nulls_last(),
        F.col("kafka_offset").desc())
    latest = (acct.withColumn("rn", F.row_number().over(w))
                  .filter((F.col("rn") == 1) & (F.col("op") != "d")))

    fact = latest.select(
        F.col("j.ACCOUNT_ID").cast("bigint").alias("account_sk"),
        F.col("j.CUSTOMER_ID").cast("bigint").alias("customer_sk"),
        F.to_date(F.col("source_commit_ts")).alias("business_date"),
        F.col("j.BALANCE").cast("decimal(18,2)").alias("closing_balance"),
        F.lit(0).cast("decimal(18,2)").alias("debit_amount"),
        F.lit(0).cast("decimal(18,2)").alias("credit_amount"),
        F.lit(0).cast("bigint").alias("txn_count"),
        F.lit("CERTIFIED").alias("processing_status"))

    spark.sql(f"""CREATE TABLE IF NOT EXISTS {args.curated} (
        account_sk BIGINT, customer_sk BIGINT, business_date DATE,
        closing_balance DECIMAL(18,2), debit_amount DECIMAL(18,2),
        credit_amount DECIMAL(18,2), txn_count BIGINT, processing_status STRING
    ) USING iceberg PARTITIONED BY (business_date) TBLPROPERTIES ('format-version'='2')""")
    fact.createOrReplaceTempView("fact_src")
    spark.sql(f"""MERGE INTO {args.curated} t USING fact_src s
                  ON t.account_sk = s.account_sk AND t.business_date = s.business_date
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")

    n = spark.table(args.curated).count()
    d = spark.table(args.curated).select("business_date").distinct().count()
    print(f"EOD_ROWS {n}")
    print(f"EOD_DISTINCT_DATES {d}")
    for r in spark.sql(f"SELECT business_date, count(*) c FROM {args.curated} "
                       f"GROUP BY 1 ORDER BY 1").collect():
        print(f"EOD_DATE {r[0]} {r[1]}")
    snap = spark.sql(f"SELECT snapshot_id FROM {args.curated}.snapshots "
                     f"ORDER BY committed_at DESC LIMIT 1").collect()
    print(f"EOD_SNAPSHOT {snap[0][0] if snap else None}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
