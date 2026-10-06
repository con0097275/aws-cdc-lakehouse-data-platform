"""STREAMING_RT app 2 of 4 -- AUTOCORRECT. Repairs BASE from what the stream flagged.

    spark-submit rt_autocorrect.py --warehouse ...

Runs periodically (reference: every 3h), finishes, exits. It is the other half of "the
stream does not fix its own mistakes": the stream recorded what it could not do, and this
pass does it, from a fresher source, with no latency budget to respect.

THE GOVERNING PRINCIPLE: DO NOT TRUST THE STREAM'S DIM CACHE.
The stream enriched from whatever the cache held at that instant. This pass rebuilds the
dimension from the canonical layer and re-enriches. If it merely re-read the stream's own
output it would faithfully reproduce the stream's mistakes.

WHAT NEEDS FIXING -- the work list, 3 sources (the reference uses 5)
  1. rt_pending_dim   WHERE resolved_ts IS NULL    -- stream said "I could not enrich this"
  2. rt_dim_change_audit WHERE processed_ts IS NULL -- a dim VALUE changed; rows carrying
                                                      the old value are stale, and nothing
                                                      in the FACT stream would ever reveal it
  3. rt_account_base  WHERE dim_complete = false    -- belt and braces: rows that reached
                                                      BASE incomplete by any other path

Sources 4 and 5 in the reference (today's CDC window, and accounts in silver but not in
BASE) are dropped here on purpose: EOD rebuilds BASE from the canonical layer nightly, so a
brand-new account is picked up then. That is the cost-saving simplification -- it trades a
slightly longer worst-case window for one fewer full scan per correction cycle.

An empty work list is a NO-OP that marks nothing. Marking pointers resolved when no repair
ran would erase the evidence that the repair is still outstanding.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

import pathlib as _pl, sys as _sys                                # noqa: E402
_sys.path.insert(0, str(_pl.Path(__file__).resolve().parent))
from cdc_source import full_cdc_for, latest_state_window          # noqa: E402

BASE = "glue_catalog.kafka_dev_lab_dev_stream.rt_account_base"
STREAM = "glue_catalog.kafka_dev_lab_dev_stream.rt_account_stream"
PENDING = "glue_catalog.kafka_dev_lab_dev_ops.rt_pending_dim"
AUDIT = "glue_catalog.kafka_dev_lab_dev_ops.rt_dim_change_audit"
#: Canonical ids, not physical tables. `cdc_source.full_cdc_for` resolves where each table's
#: FULL_CDC currently lives from its per-table cutover mode; naming `cdc_events` directly
#: meant AUTO_CORRECT silently re-derived from a table that stopped receiving these events
#: the moment they cut over (ADR-072). AUTO_CORRECT exists to fix wrong numbers, so reading
#: a stale source is the one failure it must not have.
CUSTOMER_TABLE_ID = "oracle.coredb.corebank.customer"
ACCOUNT_TABLE_ID = "oracle.coredb.corebank.account"


def build_spark(app: str) -> SparkSession:
    return (SparkSession.builder.appName(app)
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.session.timeZone", "UTC")   # ADR-024
            .getOrCreate())


def fresh_dim(spark):
    """CUSTOMER dimension rebuilt from the canonical layer -- the 'fresher source'."""
    df = (full_cdc_for(spark, CUSTOMER_TABLE_ID)
          .withColumn("j", F.from_json(F.coalesce(F.col("payload_after"),
                                                  F.col("payload_before")),
                                       "CUSTOMER_ID string, SEGMENT_CODE string, "
                                       "BRANCH_ID string")))
    w = latest_state_window("oracle", "j.CUSTOMER_ID")
    return (df.withColumn("rn", F.expr(w))
              .filter((F.col("rn") == 1) & (F.col("op") != "d"))
              .select(F.col("j.CUSTOMER_ID").cast("bigint").alias("customer_id"),
                      F.col("j.SEGMENT_CODE").alias("segment_code"),
                      F.col("j.BRANCH_ID").cast("bigint").alias("branch_id")))


def fresh_facts(spark):
    """Latest ACCOUNT state per account from the canonical layer.

    Ordered by source position (SCN), not arrival: the canonical layer holds every I/U/D and
    the LAST one by source order is the current state. Deletes are excluded -- a deleted
    account should leave the active snapshot, which is CLAUDE.md 5.7.
    """
    df = (full_cdc_for(spark, ACCOUNT_TABLE_ID)
          .withColumn("j", F.from_json(F.coalesce(F.col("payload_after"),
                                                  F.col("payload_before")),
                                       "ACCOUNT_ID string, CUSTOMER_ID string, "
                                       "BALANCE string, CURRENCY string, "
                                       "PRODUCT_CODE string, STATUS string")))
    # Source-native ordering shared with the certified EOD close, so AUTO_CORRECT and the
    # snapshot can never disagree about which update was latest.
    w = latest_state_window("oracle", "j.ACCOUNT_ID")
    return (df.withColumn("rn", F.expr(w))
              .filter((F.col("rn") == 1) & (F.col("op") != "d"))
              .select(F.col("j.ACCOUNT_ID").cast("bigint").alias("account_id"),
                      F.col("j.CUSTOMER_ID").cast("bigint").alias("customer_id"),
                      F.col("j.BALANCE").cast("decimal(18,2)").alias("balance"),
                      F.col("j.CURRENCY").alias("currency"),
                      F.col("j.PRODUCT_CODE").alias("product_code"),
                      F.col("j.STATUS").alias("account_status"),
                      F.col("source_commit_ts").alias("source_ts")))


def work_list(spark):
    """The distinct accounts needing repair, from the three sources."""
    pend = (spark.table(PENDING).filter(F.col("resolved_ts").isNull())
            .select("account_id"))
    # A dim change is expressed per CUSTOMER; fan it out to every account of that customer.
    # Deliberately NOT scoped to a business date: a customer's segment changing affects all
    # of their accounts regardless of when those rows were written.
    changed = (spark.table(AUDIT).filter(F.col("processed_ts").isNull())
               .select("customer_id").distinct()
               .join(spark.table(BASE).select("account_id", "customer_id"), "customer_id")
               .select("account_id"))
    incomplete = (spark.table(BASE).filter(F.col("dim_complete") == F.lit(False))
                  .select("account_id"))
    return pend.union(changed).union(incomplete).distinct()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    args = ap.parse_args()

    spark = build_spark("rt-autocorrect")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    now = datetime.now(timezone.utc)

    todo = work_list(spark).persist()
    n = todo.count()
    print(f"RT_AUTOCORRECT_WORKLIST {n}", flush=True)
    if n == 0:
        # Mark nothing. See the module docstring.
        print("RT_AUTOCORRECT_NOOP nothing to fix", flush=True)
        spark.stop()
        return 0

    repaired = (fresh_facts(spark).join(todo, "account_id", "inner")
                .join(fresh_dim(spark), "customer_id", "left")
                .withColumn("dim_complete", F.col("segment_code").isNotNull())
                .withColumn("built_by", F.lit("AUTOCORRECT"))
                .withColumn("built_at", F.lit(now).cast("timestamp"))
                .select("account_id", "customer_id", "balance", "currency", "product_code",
                        "account_status", "segment_code", "branch_id", "source_ts",
                        "dim_complete", "built_by", "built_at"))
    repaired.createOrReplaceTempView("rt_fix")
    spark.sql(f"""MERGE INTO {BASE} t USING rt_fix s
                  ON t.account_id = s.account_id
                  WHEN MATCHED THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    fixed = spark.table("rt_fix").count()
    still_incomplete = spark.sql(
        "SELECT COUNT(*) c FROM rt_fix WHERE dim_complete = false").collect()[0]["c"]

    # Resolve pointers ONLY for accounts that actually came out complete. An account whose
    # dimension genuinely does not exist yet must stay flagged, or the next cycle forgets it.
    spark.sql(f"""MERGE INTO {PENDING} t
                  USING (SELECT account_id FROM rt_fix WHERE dim_complete = true) s
                  ON t.account_id = s.account_id AND t.resolved_ts IS NULL
                  WHEN MATCHED THEN UPDATE SET
                    t.resolved_ts = TIMESTAMP '{now:%Y-%m-%d %H:%M:%S}',
                    t.resolved_by = 'AUTOCORRECT'""")
    spark.sql(f"""UPDATE {AUDIT} SET processed_ts = TIMESTAMP '{now:%Y-%m-%d %H:%M:%S}'
                  WHERE processed_ts IS NULL""")

    print(f"RT_AUTOCORRECT_REPAIRED {fixed}", flush=True)
    print(f"RT_AUTOCORRECT_STILL_INCOMPLETE {still_incomplete}", flush=True)
    print(f"RT_BASE_COUNT {spark.table(BASE).count()}", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
