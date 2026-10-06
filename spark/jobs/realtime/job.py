"""FULL_CDC -> REALTIME. The rolling window, materialised. Phase 18 finding P1-3.

SUPERSEDED BY `engine.py` FOR NEW WORK, AND KEPT DELIBERATELY.
-------------------------------------------------------------
`spark/jobs/realtime/realtime_engine.py` is the config-driven engine (ADR-064): it takes a table id
and the compiled plan, resolves the window from the registry, records a ledger row and
handles calendar-day as well as rolling-hour boundaries. Prefer it.

This file remains because every recorded submission recipe in `artifacts/` and `docs/` calls
it with `--full-cdc/--realtime/--window-hours`, and deleting it would turn a documented
rerun into a "no such file". It now DELEGATES its window arithmetic to `cdc.realtime` rather
than computing its own, so the two cannot disagree about what `[T-N, T)` means -- two
implementations of a window is precisely how a rolling-hours run and a calendar run would
come to serve different rows while both reporting success.

WHY THIS EXISTS
---------------
`REALTIME` was bound to a database but nothing built it, so STREAM_BATCH read a BOUNDED
PROJECTION of FULL_CDC instead and recorded `source_layer_substituted=true` on every
execution. That was honest and it was correct -- the projection returns the same rows -- but
it is the wrong cost profile: every micro-batch scanned the entire history with a predicate
rather than reading a small table. At `*/10 * * * *` that is the dominant cost driver of the
whole platform, and it grows with history while the window it serves does not.

REALTIME and EOD are SIBLINGS of FULL_CDC, not a chain (docs/TARGET_ARCHITECTURE.md §3).
This job is the same function as the EOD build with a different bound:

    EOD       <= T-1 23:59:59      frozen COB cutoff
    REALTIME  [T-N, T)             rolling, N configurable

Same `event_order` ranking in both. That symmetry is deliberate: if REALTIME ranked
differently, a variance between a provisional number and a certified one would be ambiguous
between "late data" and "divergent logic", and only one of those is a bug worth chasing.

FULL REFRESH, NOT INCREMENTAL
-----------------------------
The table is REPLACED each run rather than merged into. A rolling window's contents shrink
from the back as well as grow at the front, and an incremental MERGE has no way to express
"these rows have aged out" -- it would accumulate forever and quietly stop being a window.
Replacement is also what makes the job idempotent: running it twice produces the same table.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/tmp/framework")


def _declared_shape(realtime_identifier: str) -> str:
    """The `shape` the compiled plan declares for a REALTIME target, or "" if unknown.

    "" when there is no plan on this machine, and the caller then proceeds. That is the
    right default HERE and not in the engine: this is a manual recovery entry point run by
    an operator who named both tables explicitly, and refusing it because a plan file is
    missing would take away the tool people reach for when things are already broken.
    """
    import json as _json
    import os as _os
    from pathlib import Path as _Path
    path = _os.environ.get(
        "CDC_TABLE_PLAN",
        str(_Path(__file__).resolve().parents[3] / "artifacts" / "cdc" / "table-plan.json"))
    try:
        payload = _json.loads(_Path(path).read_text())
    except Exception:                                              # noqa: BLE001
        return ""
    want = realtime_identifier.rsplit(".", 1)[-1]
    for entry in (payload.get("plan") or payload).get("tables") or []:
        ident = (entry.get("targets") or {}).get("realtime_identifier", "")
        if ident.rsplit(".", 1)[-1] == want:
            return (entry.get("realtime_policy") or {}).get("shape", "")
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full-cdc", required=True, help="canonical source, read-only")
    ap.add_argument("--realtime", required=True, help="target table to REPLACE")
    ap.add_argument("--window-hours", type=int, default=72,
                    help="N in [T-N, T). Must EXCEED the largest safety_overlap of any "
                         "STREAM_BATCH job, or a batch asks for rows this job aged out.")
    ap.add_argument("--warehouse", required=True)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("realtime-window")
             .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.glue_catalog.catalog-impl",
                     "org.apache.iceberg.aws.glue.GlueCatalog")
             .config("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
             .config("spark.sql.catalog.glue_catalog.io-impl",
                     "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             # ADR-024. The window bound is current_timestamp() - INTERVAL N HOURS compared
             # against source_commit_ts; both resolve in the SESSION time zone. A non-UTC
             # default shifts the window by the offset, so REALTIME and EOD would rank the
             # same events against different clocks -- and the symmetry this job's docstring
             # depends on (a variance means late data, not divergent logic) would be false.
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # REFUSE THE ONE COMBINATION THAT IS SILENTLY WRONG IN A PER-TABLE WORLD (ADR-072).
    #
    # This entry point applies NO source_table predicate -- it predates per-table FULL_CDC
    # and its source is whatever the caller names. That was safe when both sides were the
    # legacy monolith. It stopped being safe when FULL_CDC became one table per source
    # table: reading the monolith (which holds EVERY table) into a PER-TABLE REALTIME target
    # writes eight tables' events into one table's window, and the result is a plausible
    # number rather than an error.
    #
    # The mirror case is safe and stays allowed: a per-table source into a per-table target,
    # and the legacy monolith into the legacy target, are both self-consistent.
    _legacy_source = args.full_cdc.rsplit(".", 1)[-1] == "cdc_events"
    _per_table_target = args.realtime.rsplit(".", 1)[-1].startswith("rt_") and \
        args.realtime.rsplit(".", 1)[-1] not in ("rt_account_stream", "rt_account_base")
    if _legacy_source and _per_table_target:
        raise SystemExit(
            f"REFUSING: --full-cdc names the legacy monolith ({args.full_cdc}) while "
            f"--realtime names a per-table target ({args.realtime}). This job applies no "
            f"source_table predicate, so it would write EVERY table's events into one "
            f"table's window and report success. Use "
            f"spark/jobs/realtime/realtime_engine.py --table <id>, which resolves the "
            f"source from the compiled plan and the per-table cutover mode.")

    # AND IT MUST REFUSE A `latest_state` TARGET (R2-D/R2-J).
    #
    # This entry point predates `shape` entirely: it writes the raw event window with
    # `overwrite`. Pointed at a `latest_state` table it would replace one-row-per-key state
    # with many rows per key -- a table whose whole contract is one row per business key,
    # silently holding several -- and report success. Every consumer of that table then
    # double-counts, and nothing errors.
    #
    # Resolved from the COMPILED PLAN, not from the table name: the name is derivable and
    # the shape is not.
    _shape = _declared_shape(args.realtime)
    if _shape == "latest_state":
        raise SystemExit(
            f"REFUSING: {args.realtime} is declared `shape: latest_state` in the compiled "
            f"plan, and this entry point writes the raw event WINDOW. It would replace "
            f"one row per business key with every event for that key, and report success. "
            f"Use spark/jobs/realtime/realtime_engine.py --table <id>, which dispatches on "
            f"the declared shape (ADR-082).")

    src = spark.table(args.full_cdc)
    total = src.count()

    # The bound is computed ONCE, in Python, from a FROZEN run instant -- not from
    # `current_timestamp()` evaluated inside the query. Section D of ADR-064: a bound the
    # engine re-derives per step differs from the one it reported by however long the run
    # took, and every later comparison is off by that amount in a way that looks like late
    # data. `resolve_window` is the SAME function `engine.py` uses.
    from cdc.realtime import BOUNDARY_ROLLING_HOURS, resolve_window

    run_upper = datetime.now(timezone.utc)
    win = resolve_window(
        {"boundary": BOUNDARY_ROLLING_HOURS, "window_hours": args.window_hours,
         # This entry point has no grace and no retention of its own: it predates both, and
         # inventing values here would make it quietly serve a different window than the
         # recipes that call it have always got.
         "late_grace_hours": 0, "retention_hours": args.window_hours},
        table_id=args.realtime, run_id=f"legacy-{run_upper:%Y%m%dT%H%M%SZ}",
        run_upper_bound=run_upper)
    cutoff = win.grace_lower
    print(f"REALTIME_WINDOW hours={args.window_hours} cutoff={cutoff.isoformat()} "
          f"upper={win.upper.isoformat()}")

    # Upper-bounded too, which the previous version was not: an unbounded upper meant the
    # window's contents depended on when each partition happened to be read.
    window = (src.filter(F.col("source_commit_ts") >= F.lit(cutoff))
                 .filter(F.col("source_commit_ts") < F.lit(win.upper)))
    kept = window.count()
    print(f"REALTIME_SOURCE_ROWS full_cdc={total} in_window={kept}")

    if kept == 0:
        # Refuse rather than publish an empty window. An empty REALTIME table is
        # indistinguishable, to STREAM_BATCH, from "nothing changed" -- and it would report
        # a successful batch over no data, advancing its watermark past rows it never read.
        print("REALTIME_ABORT window is empty; refusing to replace the table")
        spark.stop()
        return 1

    # overwrite, NOT createOrReplace. Both are atomic, and only one of them leaves the
    # table DEFINITION alone: `createOrReplace` resets the partition spec, the write
    # properties and the governance metadata `cdc/provision.py` set, silently, to whatever
    # the DataFrame implies. That was harmless while nothing provisioned this table and is
    # not harmless now (ADR-064).
    if spark.catalog.tableExists(args.realtime):
        window.writeTo(args.realtime).overwrite(F.lit(True))
    else:
        window.writeTo(args.realtime).using("iceberg").create()

    snap = spark.sql(f"SELECT snapshot_id FROM {args.realtime}.snapshots "
                     f"ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
    print(f"REALTIME_ROWS {spark.table(args.realtime).count()}")
    print(f"REALTIME_SNAPSHOT {snap}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
