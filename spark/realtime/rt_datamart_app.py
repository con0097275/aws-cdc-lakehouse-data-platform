"""STREAMING_RT app 4 of 4 -- the LONG-RUNNING datamart. Polls the merge view.

    spark-submit rt_datamart_app.py --warehouse ... --run-seconds 180

No new business logic: it reads the BASE+STREAM merge and produces aggregates. What makes it
worth its own app is that it is a PROCESS, not a job -- it recomputes every cycle so the
numbers track the stream, and it exits only when told to.

TWO THINGS THE REFERENCE LEARNED THE EXPENSIVE WAY, both implemented here.

1. SOURCE CACHE -- read each source ONCE per cycle.
   Sections are written as independent SQL over the same source. Spark cannot combine them
   because each carries a different predicate, so N sections means N full scans. The
   reference measured 244 scans per cycle over a ~15M-row source (~3.6 billion row-reads),
   and cut it to 6 by materialising each source once at the top of the cycle and registering
   a temp view. That is the difference between a demo and a bill.

   It is worse than it sounds for a MERGE-written table: `rt_account_stream` is
   merge-on-read, so EVERY scan re-applies the outstanding position-delete files.

2. SNAPSHOT CONSISTENCY -- the reason the cache is correctness, not just speed.
   Without it, section 1 and section 3 read the table minutes apart while the stream is
   still writing, so one cycle can publish two mutually inconsistent numbers. Materialising
   once per cycle makes every section see the SAME instant. The performance win is the
   incidental part.

Sections here: 3 (reference: 12 across 6 parallel workers). The simplification is the
section count, not the mechanism.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from rt_autocorrect import BASE, STREAM, build_spark

WATERMARK = "glue_catalog.kafka_dev_lab_dev_ops.rt_base_watermark"
METRICS = "glue_catalog.kafka_dev_lab_dev_ops.rt_datamart_metrics"

#: (section, grain, SQL). Each reads ONLY the cached temp view `v_rt_account`, never the
#: underlying tables -- that is what keeps the scan count at one per cycle.
SECTIONS: list[tuple[str, str, str]] = [
    ("I_total", "portfolio",
     "SELECT 'ALL' AS dim_value, 'total_balance' AS metric_name, "
     "       CAST(SUM(balance) AS DECIMAL(24,4)) AS metric_value, COUNT(*) AS row_count "
     "FROM v_rt_account"),
    ("II_by_segment", "segment_code",
     "SELECT COALESCE(segment_code, '(unresolved)') AS dim_value, "
     "       'balance_by_segment' AS metric_name, "
     "       CAST(SUM(balance) AS DECIMAL(24,4)) AS metric_value, COUNT(*) AS row_count "
     "FROM v_rt_account GROUP BY COALESCE(segment_code, '(unresolved)')"),
    # The health section. It reports the fast path's own incompleteness as a first-class
    # number: without it, "unresolved" hides inside the segment breakdown and nobody can see
    # how much of the portfolio is currently published with missing dimensions.
    ("III_dim_health", "dim_complete",
     "SELECT CAST(dim_complete AS STRING) AS dim_value, 'accounts' AS metric_name, "
     "       CAST(COUNT(*) AS DECIMAL(24,4)) AS metric_value, COUNT(*) AS row_count "
     "FROM v_rt_account GROUP BY dim_complete"),
]


def build_cycle_view(spark) -> int:
    """Materialise BASE+STREAM once and register the merge as `v_rt_account`.

    This is the view from ddl.sql, built in Spark so the cycle owns a single snapshot of it.
    STREAM wins for any account whose row is newer than the watermark; BASE supplies the
    rest (LEFT ANTI). Not a row-level "prefer newer" -- see rt_common.merge_base_and_stream.
    """
    wm_rows = (spark.table(WATERMARK).filter(F.col("layer") == "RT_BASE")
               .agg(F.max("watermark_ts").alias("w")).collect())
    wm = wm_rows[0]["w"] if wm_rows else None

    cols = ["account_id", "customer_id", "balance", "currency", "product_code",
            "account_status", "segment_code", "branch_id", "source_ts", "dim_complete"]
    stream = spark.table(STREAM)
    if wm is not None:
        stream = stream.filter(F.col("source_ts") > F.lit(wm))
    w = "ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY source_ts DESC)"
    fresh = (stream.withColumn("rn", F.expr(w)).filter(F.col("rn") == 1)
             .select(*cols, "written_at"))
    base_full = spark.table(BASE).select(*cols, "built_at")

    # RT-1. A BASE row REPAIRED after the stream row was written wins: the correction pass
    # fixes one account without moving the watermark, so otherwise the repair stays masked
    # until the next EOD. Kept identical to rt_common.merge_base_and_stream, which is where
    # this rule is unit-tested.
    j = fresh.alias("s").join(base_full.alias("b"), "account_id", "left")
    repaired = (j.filter(F.col("b.built_at").isNotNull()
                         & F.col("s.written_at").isNotNull()
                         & (F.col("b.built_at") > F.col("s.written_at")))
                 .select(*[F.col(f"b.{c}").alias(c) for c in cols if c != "account_id"],
                         F.col("account_id")))
    still_stream = (j.filter(~(F.col("b.built_at").isNotNull()
                               & F.col("s.written_at").isNotNull()
                               & (F.col("b.built_at") > F.col("s.written_at"))))
                     .select(*[F.col(f"s.{c}").alias(c) for c in cols if c != "account_id"],
                             F.col("account_id")))
    untouched = base_full.join(fresh.select("account_id"), "account_id",
                               "left_anti").select(*cols)
    merged = still_stream.select(*cols).unionByName(
        repaired.select(*cols)).unionByName(untouched)
    merged.cache()
    n = merged.count()          # forces the single materialisation for this cycle
    merged.createOrReplaceTempView("v_rt_account")
    return n


def run_cycle(spark, cycle_id: int) -> dict:
    rows_in_view = build_cycle_view(spark)
    now = datetime.now(timezone.utc)
    out = []
    for section, grain, sql in SECTIONS:
        for r in spark.sql(sql).collect():
            out.append((section, grain, r["dim_value"], r["metric_name"],
                        r["metric_value"], int(r["row_count"]), cycle_id, now))
    if out:
        (spark.createDataFrame(
            out, schema=("section string, grain string, dim_value string, "
                         "metric_name string, metric_value decimal(24,4), "
                         "row_count bigint, cycle_id bigint, build_ts timestamp"))
         .writeTo(METRICS).append())
    spark.catalog.dropTempView("v_rt_account")
    spark.catalog.clearCache()
    return {"cycle": cycle_id, "view_rows": rows_in_view, "metrics_written": len(out)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--poll-seconds", type=int, default=45)
    ap.add_argument("--run-seconds", type=int, default=180)
    args = ap.parse_args()

    spark = build_spark("rt-datamart")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
    spark.sparkContext.setLogLevel("WARN")

    started = time.monotonic()
    cycle = 0
    summary = []
    print(f"RT_DATAMART_STARTED poll={args.poll_seconds}s budget={args.run_seconds}s",
          flush=True)
    while time.monotonic() - started < args.run_seconds:
        cycle += 1
        t0 = time.monotonic()
        res = run_cycle(spark, cycle)
        res["seconds"] = round(time.monotonic() - t0, 1)
        summary.append(res)
        print(f"RT_DATAMART_CYCLE {json.dumps(res)}", flush=True)
        # Only sleep off the remainder; a cycle slower than the interval must not stack.
        remaining = args.poll_seconds - (time.monotonic() - t0)
        if remaining > 0 and time.monotonic() - started + remaining < args.run_seconds:
            time.sleep(remaining)

    print(f"RT_DATAMART_SUMMARY {json.dumps({'cycles': cycle, 'detail': summary})}",
          flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
