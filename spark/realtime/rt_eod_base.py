"""STREAMING_RT app 3 of 4 -- EOD. Rebuilds BASE from scratch and moves the watermark.

    spark-submit rt_eod_base.py --warehouse ... --business-date 2026-09-03

Slow, complete, exact. It does not patch: it REBUILDS BASE from the canonical layer, which
is why it repairs every defect the fast path could have introduced, including ones nobody
flagged. That is the safety net under the whole design -- the correction pass handles the
known-unknowns, and this handles the unknown-unknowns by not depending on knowing.

WRITING THE WATERMARK IS THE POINT, NOT A SIDE EFFECT.
`watermark_ts` is what makes the merge view correct. It says "BASE is authoritative up to
here", so the view knows which STREAM rows may still override BASE and which are yesterday's
fast-path guesses that BASE has since settled exactly. A rebuild that produced the rows but
did not move the watermark would leave the view preferring stale STREAM rows forever, and
every number would look plausible.

The reference has a real gap here worth naming: its correction pass never writes this table
despite the table being called `dim_autocorrect_watermark`. Only EOD writes it. Same here,
and deliberately -- AUTOCORRECT repairs individual accounts, which does not make BASE
authoritative up to a new point in time.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

from rt_autocorrect import BASE, FULL_CDC, build_spark, fresh_dim, fresh_facts

WATERMARK = "glue_catalog.kafka_dev_lab_dev_ops.rt_base_watermark"
PENDING = "glue_catalog.kafka_dev_lab_dev_ops.rt_pending_dim"
AUDIT = "glue_catalog.kafka_dev_lab_dev_ops.rt_dim_change_audit"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--business-date", required=True)
    ap.add_argument("--clear-pointers", action="store_true",
                    help="drop outstanding pointer rows after the rebuild (reference "
                         "behaviour: EOD clears them nightly because the rebuild already "
                         "repaired everything they pointed at)")
    args = ap.parse_args()

    spark = build_spark("rt-eod-base")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    now = datetime.now(timezone.utc)

    facts = fresh_facts(spark)
    scoped = facts.filter(F.to_date(F.col("source_ts")) <= F.lit(args.business_date))
    built = (scoped.join(fresh_dim(spark), "customer_id", "left")
             .withColumn("dim_complete", F.col("segment_code").isNotNull())
             .withColumn("built_by", F.lit("EOD"))
             .withColumn("built_at", F.lit(now).cast("timestamp"))
             .select("account_id", "customer_id", "balance", "currency", "product_code",
                     "account_status", "segment_code", "branch_id", "source_ts",
                     "dim_complete", "built_by", "built_at"))

    # REPLACE, not MERGE. A rebuild that merged would leave behind accounts that have since
    # been deleted at source -- the exact class of stale row this pass exists to eliminate.
    built.writeTo(BASE).using("iceberg").createOrReplace()

    n = spark.table(BASE).count()
    incomplete = spark.table(BASE).filter(F.col("dim_complete") == F.lit(False)).count()

    # The watermark is the MAX source time actually built, not the wall clock. Using `now`
    # would claim authority over events that arrived while this job was running and were
    # never included -- the view would then suppress the STREAM rows that do cover them.
    hi = spark.table(BASE).agg(F.max("source_ts").alias("m")).collect()[0]["m"]
    wm = hi or now
    spark.sql(f"DELETE FROM {WATERMARK} WHERE layer = 'RT_BASE'")
    spark.createDataFrame(
        [("RT_BASE", wm, "EOD", now)],
        schema="layer string, watermark_ts timestamp, written_by string, written_at timestamp"
    ).writeTo(WATERMARK).append()

    if args.clear_pointers:
        spark.sql(f"DELETE FROM {PENDING} WHERE resolved_ts IS NULL")
        spark.sql(f"DELETE FROM {AUDIT} WHERE processed_ts IS NULL")
        print("RT_EOD_POINTERS_CLEARED", flush=True)

    print(f"RT_EOD_BASE_ROWS {n}", flush=True)
    print(f"RT_EOD_BASE_INCOMPLETE {incomplete}", flush=True)
    print(f"RT_EOD_WATERMARK {wm}", flush=True)
    snap = spark.sql(f"SELECT snapshot_id FROM {BASE}.snapshots "
                     f"ORDER BY committed_at DESC LIMIT 1").collect()
    print(f"RT_EOD_SNAPSHOT {snap[0][0] if snap else None}", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
