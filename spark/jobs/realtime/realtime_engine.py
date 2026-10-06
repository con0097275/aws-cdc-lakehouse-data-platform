"""FULL_CDC -> REALTIME. ONE generic engine, driven by the compiled table config.

    spark-submit engine.py --plan s3://.../table-plan.json \
        --table oracle.coredb.corebank.account --warehouse s3://.../warehouse/

    --table ALL                     every table whose realtime policy is enabled
    --as-of 2026-08-22T09:15:00Z    freeze the upper bound explicitly (rebuild, backfill)
    --rebuild                       rebuild from FULL_CDC; no Kafka, no replay

NO PER-TABLE CODE (section A). The engine takes a table_id, a run timestamp and the resolved
config, and everything that differs between tables -- the window, the boundary mode, the
refresh mode, the retention, the target -- is a value read from the compiled plan. Adding a
table is a registry entry and a provision, never a new job.

IT READS FULL_CDC, NEVER KAFKA. REALTIME is a DERIVATION of the canonical layer, not a
second consumer of the topics. Two independent readers of Kafka would decode the same
records twice, quarantine independently, and drift apart in exactly the ways FULL_CDC exists
to prevent -- and a rebuild would need a topic replay that retention may no longer allow.
Deriving instead makes `--rebuild` a re-read of data the platform already owns (section G).

A TIMESTAMP THAT HAS BEEN THROUGH PYTHON IS NOT THE TIMESTAMP YOU STORED
------------------------------------------------------------------------
`collect()` renders a Spark TIMESTAMP into a NAIVE Python datetime in the DRIVER's local
zone. Store midnight UTC, collect it on a machine in UTC+7, and you get `07:00` with no
tzinfo -- and formatting that back into a SQL literal, which Spark reads in the SESSION
zone, moves the value by the driver's offset.

On an EMR driver in UTC the offset is zero and nothing happens. Run the same code from a
laptop and every comparison shifts by hours, silently. So every timestamp this module
compares against data is either built in Python and never round-tripped (the window bounds,
which `resolve_window` refuses to accept naive for exactly this reason) or formatted INSIDE
Spark with `date_format` and carried as a string.

THE UPPER BOUND IS FROZEN ONCE (section D). `resolve_window` is called with a caller-supplied
instant and every later step uses the bounds it returned. A job that consulted the clock per
step would filter with one bound, prune with another and record a third, all differing by
however long the run took -- and every downstream comparison against the ledger would be off
by that amount in a way that looks exactly like late-arriving data.
"""
# EMR Serverless emr-7.2.0 runs PYTHON 3.9, which evaluates PEP 604 unions at def-time and
# dies at IMPORT. Every annotation here is a lazily-evaluated string because of it.
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/tmp/framework")

from cdc.eod import DELETE_EXCLUDE, DELETE_SOFT                   # noqa: E402
from cdc.realtime import (REFRESH_FULL, REFRESH_INCREMENTAL,      # noqa: E402
                          REFRESH_LATEST_STATE, SHAPE_EVENT_WINDOW,
                          SHAPE_LATEST_STATE, WRITE_APPEND, WRITE_GUARDED_MERGE,
                          WRITE_OVERWRITE_WINDOW, DEFAULT_SOURCE_PROGRESS,
                          order_key_expr, prunes_by_age,
                          RUN_LEDGER_COLUMNS, STATUS_FAILED,
                          STATUS_SKIPPED_DISABLED, STATUS_SUCCEEDED, resolve_window)
from cdc.realtime_state import (MODE_INCREMENTAL, MODE_NOOP,      # noqa: E402
                                MODE_WINDOW_SCAN, REALTIME_INFO_COLUMNS,
                                SourceSnapshot, advance_cursor, merge_info_sql, plan_read)
from cdc.realtime import (SHAPE_WRITE_STRATEGIES     # noqa: E402
                          as ALLOWED_FOR_SHAPE)
from cdc.realtime_state import (OPERATION_MATERIALISE,  # noqa: E402
                                OPERATION_REBASE, rebase_decision,
                                rebase_merge_sql)

#: A run that read nothing because the source had not moved. A DISTINCT status from
#: SUCCEEDED: "the cursor did not advance and no rows were written" and "a normal run
#: happened to produce the same count" look identical in a ledger that conflates them, and
#: only one of them means the upstream has stopped.
STATUS_SUCCEEDED_NOOP = "SUCCEEDED_NOOP"


def build_spark(warehouse: str) -> SparkSession:
    return (SparkSession.builder.appName("realtime-engine")
            .config("spark.sql.catalog.glue_catalog",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
            .config("spark.sql.catalog.glue_catalog.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # ADR-024. The calendar boundary is computed in Python against the table's
            # business timezone, but `source_commit_ts` is compared in the SESSION zone -- a
            # non-UTC default shifts every comparison by the offset, silently.
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def read_plan(spark, uri: str) -> dict:
    if uri.startswith(("s3://", "s3a://", "file:")):
        return json.loads("".join(spark.read.text(uri).rdd.map(lambda r: r[0]).collect()))
    with open(uri) as fh:
        return json.load(fh)


def table_exists(spark, identifier: str) -> bool:
    try:
        spark.table(identifier)
        return True
    except Exception:                                              # noqa: BLE001
        return False


def require_target(spark, identifier: str, table_id: str) -> None:
    """Same refusal as the FULL_CDC writer, for the same reason (ADR-063 section F).

    A CREATE here would mean a REALTIME table springs into existence with no partition spec,
    no retention property and no owner -- all of which the provisioner takes from the
    registry.
    """
    if not table_exists(spark, identifier):
        raise RuntimeError(
            f"REALTIME_TARGET_MISSING {identifier} (for {table_id}). This engine does NOT "
            f"create tables: they are provisioned from Git config first. Run "
            f"`python3 -m cdc.provision --table {table_id} --layer REALTIME` to see the "
            f"DDL and `spark-submit spark/ops/provision_cdc_tables.py --execute` to apply "
            f"it, then rerun this window.")


def current_snapshot_id(spark, identifier: str):
    """The snapshot a read saw, or None for a table with no snapshots yet."""
    try:
        rows = spark.sql(f"SELECT snapshot_id FROM {identifier}.snapshots "
                         f"ORDER BY committed_at DESC LIMIT 1").collect()
    except Exception:                                              # noqa: BLE001
        return None
    return rows[0][0] if rows else None


def snapshot_lineage(spark, identifier: str) -> "list":
    """`{table}.snapshots`, OLDEST FIRST, as the pure planner's value type.

    `operation` is what makes the incremental decision possible: Iceberg's append scan is
    defined only over `append` snapshots, and FULL_CDC is append-ORIENTED but not
    append-ONLY -- `rewrite_data_files` writes a `replace`. Reading the operation here and
    deciding in `cdc/realtime_state.plan_read` keeps the decision testable without Spark.
    """
    try:
        rows = spark.sql(f"SELECT snapshot_id, parent_id, operation FROM "
                         f"{identifier}.snapshots ORDER BY committed_at").collect()
    except Exception:                                              # noqa: BLE001
        # No metadata table, or no snapshots. The planner treats an empty lineage as
        # "cannot verify", which falls back to the window scan rather than reading less.
        return []
    return [SourceSnapshot(snapshot_id=r[0], parent_id=r[1], operation=(r[2] or ""))
            for r in rows]


def read_incremental(spark, source: str, from_snapshot, to_snapshot):
    """Rows appended to FULL_CDC in `(from_snapshot, to_snapshot]`.

    `start-snapshot-id` is EXCLUSIVE and `end-snapshot-id` is INCLUSIVE, which is exactly
    the half-open range the cursor means: everything after what we have already
    incorporated, up to and including the snapshot we froze.

    Getting that backwards is not a crash. An inclusive start re-reads the cursor snapshot,
    which for `append` duplicates rows and for `guarded_merge` is a no-op -- so the bug
    would show only on one shape, and only as a count.
    """
    return (spark.read.format("iceberg")
                 .option("start-snapshot-id", str(from_snapshot))
                 .option("end-snapshot-id", str(to_snapshot))
                 .load(source))


def source_rows(spark, source: str, window, read_plan):
    """The rows this run processes, read the way `read_plan` decided.

    THE WINDOW FILTER IS APPLIED EITHER WAY, and that is not redundant. The window is the
    CONTRACT -- what the layer promises to serve. The cursor is an optimisation of how
    candidates are FOUND. An incremental delta can contain an event whose
    `source_commit_ts` is older than `grace_lower` (a connector catching up after a long
    stall publishes genuinely old changes now); admitting it because it happened to arrive
    in this snapshot range would put a row outside the promised window into the table, and
    the next rebuild would remove it again. A row that appears and disappears across runs
    is the hardest kind of discrepancy to chase.
    """
    if read_plan.mode == MODE_INCREMENTAL:
        rows = read_incremental(spark, source, read_plan.from_snapshot,
                                read_plan.to_snapshot)
    else:
        rows = spark.table(source)
    return (rows.filter(F.col("source_commit_ts") >= F.lit(window.grace_lower))
                .filter(F.col("source_commit_ts") < F.lit(window.upper)))


def window_rows(spark, source: str, window):
    """The materialised window: [grace_lower, upper).

    Half-open, deliberately and at both ends. An inclusive upper bound would put an event
    committed exactly at the boundary in TWO consecutive runs' windows, and a consumer
    counting events across runs would double it. `grace_lower` rather than `logical_lower`
    is what is read, because the grace band is the whole point of having one: an event that
    committed before the promised window but arrived late must be PRESENT, not silently
    absent.
    """
    return (spark.table(source)
                 .filter(F.col("source_commit_ts") >= F.lit(window.grace_lower))
                 .filter(F.col("source_commit_ts") < F.lit(window.upper)))


def materialise(spark, rows, target: str, window, refresh_mode: str = "",
                engine: str | None = None, rebuild: bool = False,
                delete_policy: str = DELETE_EXCLUDE,
                shape: str = "", write_strategy: str = "") -> int:
    """Write the window. Returns the row count in the target afterwards.

    FULL REFRESH USES `overwrite`, NOT `createOrReplace`.

    A rolling window shrinks from the back as well as growing at the front, so an append or
    a partition-wise overwrite cannot express "these rows have aged out" -- it accumulates
    forever and quietly stops being a window. `createOrReplace` does express it, and it also
    REPLACES THE TABLE DEFINITION: the partition spec, the write properties and the
    governance metadata the provisioner set would all be silently reset to whatever the
    DataFrame implies. `overwrite(lit(True))` replaces the DATA in one atomic snapshot and
    leaves the table definition alone, which is what a provisioned table requires.
    """
    # DISPATCH ON `shape` AND `write_strategy` (R2-D), falling back to `refresh_mode`
    # for a plan compiled before ADR-082. The pair is the contract; `refresh_mode` is
    # its deprecated single-word spelling and the mapping is 1:1, so this is a rename at
    # the call site -- but the dispatch now reads the field a table actually declares.
    if not shape:
        shape = (SHAPE_LATEST_STATE if refresh_mode == REFRESH_LATEST_STATE
                 else SHAPE_EVENT_WINDOW)
    if not write_strategy:
        write_strategy = (WRITE_GUARDED_MERGE if shape == SHAPE_LATEST_STATE else
                          WRITE_APPEND if refresh_mode == REFRESH_INCREMENTAL
                          else WRITE_OVERWRITE_WINDOW)
    if write_strategy not in ALLOWED_FOR_SHAPE.get(shape, ()):
        # The compiler refuses this pair (ADR-082). Reaching it here means a plan
        # compiled before that rule, and continuing would write one shape's data with
        # another shape's mechanism -- an append onto a state table duplicates keys,
        # and nothing downstream would report it as anything but a count that grew.
        raise ValueError(
            f"shape {shape!r} cannot be written with write_strategy "
            f"{write_strategy!r}. Allowed: "
            f"{', '.join(ALLOWED_FOR_SHAPE.get(shape, ()))}. Recompile the plan.")

    if write_strategy == WRITE_GUARDED_MERGE:
        if not engine:
            # NOT defaulted. Oracle ranks on a zero-padded SCN and SQL Server on a
            # lowered LSN; ranking one with the other's rule silently picks the wrong
            # winner per key, which is the exact failure this mode exists to avoid.
            raise ValueError(
                "latest_state needs the source engine to rank events; pass engine=")
        return _materialise_latest_state(spark, rows, target, engine,
                                         rebuild=rebuild,
                                         delete_policy=delete_policy)

    if write_strategy == WRITE_APPEND:
        # MERGE on the delivered-record identity, then prune separately. Correct when the
        # window is large enough that rewriting it every run is the dominant cost -- but it
        # needs the prune to do the ageing-out that a full refresh gets for free.
        rows.createOrReplaceTempView("rt_src")
        spark.sql(f"""MERGE INTO {target} t USING rt_src s
                      ON t.dv_event_id = s.dv_event_id
                      WHEN NOT MATCHED THEN INSERT *""")
        spark.catalog.dropTempView("rt_src")
    else:
        rows.writeTo(target).overwrite(F.lit(True))
    return spark.table(target).count()


def last_successful_upper(spark, ledger_table: str, table_id: str):
    """The previous SUCCESSFUL run's upper bound as `YYYY-MM-DD` in the SESSION zone.

    SUCCESSFUL, not latest: a failed run must not count as "we already rebuilt today", or a
    day-boundary rebuild is skipped exactly when the state is least trustworthy.

    AND NOT `SUCCEEDED_NOOP`. A NOOP materialised nothing -- it is what a run does when the
    source has not moved -- so counting it would let the FIRST run of a new day, on a quiet
    source, record the new date without rebuilding. Every later run that day would then
    upsert onto the previous day's state, and the tombstones and aged-out keys that only a
    rebuild removes would never leave. Found by the test that was already asserting on this
    exact string.

    A STRING, formatted by `date_format` inside Spark, not a collected datetime. Collecting
    it renders the instant in the DRIVER's local zone, so on a machine in UTC+7 the
    comparison against `window.upper.date()` is wrong for the seven hours either side of
    midnight -- the rebuild is skipped on the day it is needed, or run on the day it is not,
    and nothing reports either.
    """
    try:
        rows = spark.sql(
            f"SELECT date_format(max(upper_bound), 'yyyy-MM-dd') FROM {ledger_table} "
            f"WHERE table_id = '{table_id}' AND status = '{STATUS_SUCCEEDED}'").collect()
    except Exception:                                              # noqa: BLE001
        # No ledger yet is a first run, which rebuilds anyway.
        return None
    return rows[0][0] if rows else None


def latest_state_needs_rebuild(last_upper, window) -> bool:
    """True when this run must REBUILD rather than upsert.

    THE HOT PATH CANNOT UPSERT FOREVER, and the reason is out-of-order deletes.

    A delete is kept as a TOMBSTONE during the day (see below), which stops a late older
    event resurrecting the key. But tombstones accumulate, keys whose events age out of the
    window never leave, and any gap in any single run persists indefinitely -- an upsert has
    no way to express "this row should no longer exist at all".

    A periodic full rebuild re-derives the whole window from FULL_CDC in one pass, which
    fixes all three at once: tombstones whose delete is final disappear, aged-out keys
    disappear, and drift from any earlier run is gone. The day boundary is the natural
    cadence because it is where the EOD close draws its own line -- rebuilding there keeps
    the hot path and the cold path describing the same day.

    `last_upper` is the previous SUCCESSFUL run's upper bound, from `ops.realtime_run`.
    None -- no previous run -- also rebuilds: there is no state to add to.
    """
    if last_upper is None:
        return True
    # `last_upper` is `YYYY-MM-DD` in the session zone; compare against the same rendering
    # of the frozen upper bound rather than against a collected datetime (see the module
    # docstring on why a round-tripped timestamp is not the one you stored).
    prev = (last_upper.strftime("%Y-%m-%d") if hasattr(last_upper, "strftime")
            else str(last_upper))
    return prev != window.upper.strftime("%Y-%m-%d")


#: THE TOMBSTONE PREDICATE, in one place.
#:
#: REALTIME carries the FULL_CDC row shape (`cdc/rowspec.py`): it has `op`, and it does NOT
#: have `is_deleted` -- that column belongs to the EOD shape, where the close computes it.
#: Spelling "this row is a delete" inline is how the two get confused, and the confusion is
#: an AnalysisException at best and a filter that matches nothing at worst.
TOMBSTONE_OP = "d"


def is_tombstone(alias: str = ""):
    """A column expression: true when this row is a delete."""
    col = F.col(f"{alias}.op" if alias else "op")
    return F.coalesce(col == F.lit(TOMBSTONE_OP), F.lit(False))


def _materialise_latest_state(spark, rows, target: str, engine: str,
                              rebuild: bool, delete_policy: str) -> int:
    """One row per BUSINESS KEY. Upsert for freshness, rebuild at the day boundary.

    TWO TRAPS, AND THEY ARE DIFFERENT.

    1. OUT-OF-ORDER UPDATES. Debezium delivers by COMMIT time, not event order: a connector
       catching up after a stall publishes logically older changes now. Overwriting on
       arrival lets an older event replace a newer state, undetectably -- the row is
       complete, merely stale, and the next run does not fix it because that key has no new
       event. So the MERGE is GUARDED on the EOD order key.

    2. OUT-OF-ORDER DELETES. This one defeats the guard, and physically deleting the row is
       what causes it: with the row GONE there is nothing left to compare against, so a late
       older insert falls to `WHEN NOT MATCHED` and RESURRECTS a key the source deleted.

       So a delete is written as a TOMBSTONE -- the row stays, `op = 'd'`, carrying the
       delete's order key. A late older event now loses the comparison and is correctly
       ignored. The tombstone is removed by the next REBUILD, not by this run.

    The rebuild is what keeps that from growing without bound, and it is the only operation
    that can express "this row should no longer exist at all".
    """
    from pyspark.sql import Window

    # Collapse to ONE row per key first. Spark refuses a MERGE whose source matches a target
    # row twice, and that error is the GOOD outcome -- the bad one is a silent pick.
    #
    # THE DATAFRAME API, NOT `SELECT * EXCEPT (...)`. That syntax is Databricks SQL; open
    # Spark 3.5 -- which is what EMR Serverless 7.2 runs -- rejects it with a
    # PARSE_SYNTAX_ERROR, so the whole shape failed at the first real run. It was invisible
    # because the tests for this function asserted on the SQL TEXT it generates rather than
    # on what the table contains afterwards: the string was exactly as intended, and
    # unparseable. `.drop()` needs no dialect support at all.
    ranked = (rows.withColumn(
                  "_rt_rank",
                  F.row_number().over(Window.partitionBy("dv_pk_hash")
                                            .orderBy(F.expr(order_key_expr(engine)).desc())))
                  .filter(F.col("_rt_rank") == 1)
                  .drop("_rt_rank"))
    ranked.createOrReplaceTempView("rt_src")

    if rebuild:
        # REPLACE the data, not the table. `createOrReplace` would also replace the table
        # DEFINITION -- partition spec, write properties, governance metadata the
        # provisioner set -- silently resetting them to whatever the DataFrame implies.
        final = spark.table("rt_src")
        if delete_policy != DELETE_SOFT:
            # A rebuild is the only place a deleted key can actually leave: here the whole
            # window has been re-ranked, so a DELETE on the winner means the delete IS the
            # latest fact about that key, not merely the latest one seen so far.
            #
            # `op = 'd'`, NOT `is_deleted`. REALTIME carries the FULL_CDC row shape, which
            # has `op` and no `is_deleted` -- that column belongs to the EOD shape
            # (cdc/rowspec.py). Filtering on it here raised AnalysisException against any
            # real REALTIME table, and the SQL-text tests could not see it.
            final = final.filter(~is_tombstone())
        final.writeTo(target).overwrite(F.lit(True))
    else:
        tgt_key = order_key_expr(engine, "t")
        src_key = order_key_expr(engine, "s")
        spark.sql(f"""
            MERGE INTO {target} t USING rt_src s
              ON t.dv_pk_hash = s.dv_pk_hash
            WHEN MATCHED AND {src_key} > {tgt_key} THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        # NO `DELETE FROM ... WHERE is_deleted` here. That is the resurrection bug: removing
        # the tombstone leaves nothing for the next batch's guard to compare against.

    spark.catalog.dropTempView("rt_src")
    return spark.table(target).count()



def prune(spark, target: str, window) -> int:
    """Enforce physical retention with a ROW-LEVEL DELETE (section F).

    Never a file delete. Removing objects under a table's location out from under Iceberg
    leaves metadata pointing at files that are gone: every reader fails, and the table cannot
    even be time-travelled back to a good state because the snapshots reference the same
    missing files. `DELETE FROM` produces a normal snapshot, so the removal is itself
    revertible until snapshots expire.

    Under `full_refresh` this is usually a no-op -- the overwrite already dropped anything
    outside the window -- and it is still run, because `retention_bound` is the property the
    config PROMISES and a mode change must not silently stop enforcing it.
    """
    before = spark.table(target).count()
    spark.sql(f"DELETE FROM {target} WHERE source_commit_ts < "
              f"TIMESTAMP '{window.retention_bound.strftime('%Y-%m-%d %H:%M:%S')}'")
    return before - spark.table(target).count()


def ledger_row(window, *, entry: dict, target: str, source: str, refresh_mode: str,
               source_snapshot, target_snapshot, row_count: int, pruned: int,
               status: str, failure_reason, started_at, finished_at,
               config_version: str, shape: str = SHAPE_EVENT_WINDOW,
               write_strategy: str = WRITE_OVERWRITE_WINDOW, attempt: int = 1,
               read_mode: str = MODE_WINDOW_SCAN, fallback_reason: str = "",
               source_snapshot_before=None, input_rows: int = 0,
               spark_app_id: str = "", baseline_cob_date=None,
               operation: str = OPERATION_MATERIALISE,
               prev_baseline_cob_date=None) -> tuple:
    """One ledger row, positional against RUN_LEDGER_COLUMNS.

    Positional against a schema defined elsewhere is a real hazard -- inserting a column in
    the middle of `RUN_LEDGER_COLUMNS` and forgetting this function shifts every value one
    place, and Spark only complains when two adjacent types differ. `test_realtime_window`
    asserts the names match; the assertion below asserts the COUNT does, which is what
    catches an insertion that happens to be type-compatible.
    """
    row = (window.run_id, window.table_id, target, source, window.boundary,
           window.business_timezone, refresh_mode,
           window.logical_lower, window.grace_lower, window.upper,
           window.retention_bound,
           operation, prev_baseline_cob_date,
           shape, write_strategy, int(attempt), read_mode, fallback_reason or "",
           int(source_snapshot_before) if source_snapshot_before is not None else None,
           int(source_snapshot) if source_snapshot is not None else None,
           int(input_rows), spark_app_id or "", baseline_cob_date,
           int(target_snapshot) if target_snapshot is not None else None,
           int(row_count), int(pruned), status, failure_reason,
           started_at, finished_at, config_version)
    assert len(row) == len(RUN_LEDGER_COLUMNS), (
        f"ledger row has {len(row)} values for {len(RUN_LEDGER_COLUMNS)} columns -- a "
        f"column was added to RUN_LEDGER_COLUMNS without updating ledger_row()")
    return row


_SPARK_TYPES = {}


def _schema_for(columns):
    """A Spark schema from a `(name, sql_type, comment)` contract."""
    from pyspark.sql.types import (DateType, IntegerType, LongType, StringType,
                                   StructField, StructType, TimestampType)
    types = {"STRING": StringType(), "BIGINT": LongType(), "INT": IntegerType(),
             "DATE": DateType(), "TIMESTAMP": TimestampType()}
    unknown = sorted({t for _, t, _ in columns} - set(types))
    if unknown:
        # A type added to the contract and not here would otherwise raise a KeyError deep
        # inside the write, after the table was already materialised.
        raise RuntimeError(f"no Spark type mapping for {', '.join(unknown)}")
    return StructType([StructField(n, types[t], True) for n, t, _ in columns])


def write_ledger(spark, ledger_table: str, row: tuple) -> None:
    """Append one run's outcome. Best-effort BY DESIGN, and it says so when it fails.

    A ledger write that could fail the run would mean a successfully materialised table gets
    reported as a failure and re-materialised -- trading a missing audit row for real
    duplicated work. The failure is printed with a marker so it is greppable, because a
    ledger with silent holes is worse than one with none: it invites the reader to conclude
    a run never happened.
    """
    try:
        spark.createDataFrame([row], schema=_schema_for(RUN_LEDGER_COLUMNS)) \
             .writeTo(ledger_table).append()
    except Exception as exc:                                       # noqa: BLE001
        print(f"REALTIME_LEDGER_WRITE_FAILED {ledger_table}: "
              f"{type(exc).__name__}: {exc}", flush=True)


def attempt_number(spark, ledger_table: str, table_id: str, upper) -> int:
    """How many times this window has been attempted, counted from HISTORY.

    From the ledger rather than from a process counter, because the retry that matters is
    the one that happens after the process died. A counter that resets with the JVM records
    every attempt as attempt 1, which makes "this table needed four goes" unanswerable.
    """
    try:
        rows = spark.sql(
            f"SELECT count(*) FROM {ledger_table} WHERE table_id = '{table_id}' "
            f"AND upper_bound = TIMESTAMP '{upper.strftime('%Y-%m-%d %H:%M:%S')}'"
        ).collect()
    except Exception:                                              # noqa: BLE001
        return 1
    return int(rows[0][0] or 0) + 1


# --------------------------------------------------------------------------- #
# ops.realtime_info -- the cursor
# --------------------------------------------------------------------------- #

def read_cursor(spark, info_table: str, table_id: str) -> dict:
    """This table's row of `ops.realtime_info`, or `{}` for a table with no row yet.

    A MISSING ROW AND A FAILED READ BOTH RETURN `{}`, and that is safe by construction:
    `plan_read` treats a null cursor as `no_cursor` and falls back to the full window scan.
    The expensive outcome, never the wrong one. Returning a stale or guessed cursor would
    invert that.
    """
    if not info_table:
        return {}
    try:
        rows = spark.sql(f"SELECT * FROM {info_table} "
                         f"WHERE table_id = '{table_id}'").collect()
    except Exception as exc:                                       # noqa: BLE001
        print(f"REALTIME_INFO_READ_FAILED {info_table} {table_id}: "
              f"{type(exc).__name__}: {exc} -- falling back to the full window scan",
              flush=True)
        return {}
    return rows[0].asDict() if rows else {}


def write_info(spark, info_table: str, row: tuple) -> None:
    """MERGE one table's current state. One row per `table_id`, forever.

    NOT best-effort in the way the ledger is -- but it still does not fail the run, for a
    different reason. The rows are already committed to the target; failing here would
    re-materialise data that is correct. What it must never do is SILENTLY leave a stale
    cursor: the next run would re-read a range it already incorporated. For `guarded_merge`
    and `overwrite_window` that converges; for `append` it duplicates. So the failure is
    printed with a marker, and R2-E's append path is idempotent on `dv_event_id` precisely
    because this write can fail.
    """
    if not info_table:
        return
    try:
        (spark.createDataFrame([row], schema=_schema_for(REALTIME_INFO_COLUMNS))
              .createOrReplaceTempView("rt_info_src"))
        spark.sql(merge_info_sql(info_table))
        spark.catalog.dropTempView("rt_info_src")
    except Exception as exc:                                       # noqa: BLE001
        print(f"REALTIME_INFO_WRITE_FAILED {info_table}: "
              f"{type(exc).__name__}: {exc} -- the cursor did NOT advance; the next run "
              f"re-reads this range", flush=True)


def info_row(*, table_id: str, target: str, source: str, shape: str, write_strategy: str,
             source_progress: str, baseline_cob_date, cursor, current_snapshot,
             window, target_snapshot, run_id: str, input_rows: int, output_rows: int,
             read_mode: str, fallback_reason: str, status: str, started_at, finished_at,
             config_version: str, err_msg) -> tuple:
    """One `realtime_info` row, positional against REALTIME_INFO_COLUMNS."""
    row = (table_id, target, source, shape, write_strategy, source_progress,
           baseline_cob_date,
           int(cursor) if cursor is not None else None,
           int(current_snapshot) if current_snapshot is not None else None,
           window.grace_lower, window.upper,
           int(target_snapshot) if target_snapshot is not None else None,
           run_id, int(input_rows), int(output_rows), read_mode, fallback_reason or "",
           status, started_at, finished_at, config_version, err_msg,
           datetime.now(timezone.utc))
    assert len(row) == len(REALTIME_INFO_COLUMNS), (
        f"info row has {len(row)} values for {len(REALTIME_INFO_COLUMNS)} columns")
    return row


def run_table(spark, entry: dict, *, plan: dict, run_id: str, as_of: datetime,
              ledger_table: str, info_table: str = "", prune_enabled: bool = True,
              force_window_scan: bool = False,
              force_rebuild: bool = False) -> dict:
    """Materialise ONE table's window. The whole engine, for any registered table.

    THE ORDER OF THE LAST FOUR STEPS IS THE POINT (R2 brief §20):

        read -> write target -> VERIFY the commit -> advance the cursor

    The cursor moves last, and only when the commit is verified. Advancing it earlier is the
    subtle version of the bug: the rows look present, so it LOOKS finished -- but if the
    commit did not actually land, that range becomes unreachable without a full rebuild, and
    nothing reports the gap. A cursor that is behind costs a re-read. A cursor that is ahead
    costs data.
    """
    table_id = entry["table_id"]
    policy = entry.get("realtime_policy") or {}
    targets = entry.get("targets") or {}
    target = targets.get("realtime_identifier")
    source = targets.get("full_cdc_identifier")
    config_version = plan.get("config_version", "")
    started_at = datetime.now(timezone.utc)
    app_id = ""
    try:
        app_id = spark.sparkContext.applicationId or ""
    except Exception:                                              # noqa: BLE001
        pass

    if not entry.get("enabled", True) or not policy.get("enabled", True):
        print(f"REALTIME_SKIPPED {table_id} reason=realtime_disabled", flush=True)
        return {"table_id": table_id, "status": STATUS_SKIPPED_DISABLED, "rows": 0}

    window = resolve_window(policy, table_id=table_id, run_id=run_id,
                            run_upper_bound=as_of,
                            business_timezone=policy.get("business_timezone", "UTC"))
    # `shape` is the contract; `refresh_mode` is its deprecated single-word spelling, kept
    # so a plan compiled before R2-B still runs (ADR-082).
    refresh_mode = policy.get("refresh_mode", REFRESH_FULL)
    shape = policy.get("shape") or (SHAPE_LATEST_STATE
                                    if refresh_mode == REFRESH_LATEST_STATE
                                    else SHAPE_EVENT_WINDOW)
    write_strategy = policy.get("write_strategy") or (
        WRITE_GUARDED_MERGE if shape == SHAPE_LATEST_STATE else
        WRITE_APPEND if refresh_mode == REFRESH_INCREMENTAL else WRITE_OVERWRITE_WINDOW)
    source_progress = policy.get("source_progress") or DEFAULT_SOURCE_PROGRESS

    require_target(spark, source, table_id)
    require_target(spark, target, table_id)

    # Recorded BEFORE the read, so the ledger names the snapshot the run actually saw. A
    # snapshot id captured afterwards could be a later one written by a concurrent ingest,
    # which would make the run look reproducible against data it never read.
    source_snapshot = current_snapshot_id(spark, source)
    state = read_cursor(spark, info_table, table_id)
    cursor = state.get("last_source_snapshot_id")
    baseline_cob = state.get("baseline_cob_date")
    attempt = attempt_number(spark, ledger_table, table_id, window.upper)

    # The hot path rebuilds at the day boundary and upserts within it. `last_upper` is the
    # previous SUCCESSFUL run's bound, so a failed run does not count as "already rebuilt".
    rebuild = force_rebuild or latest_state_needs_rebuild(
        last_successful_upper(spark, ledger_table, window.table_id), window)
    # A rebuild always needs the WHOLE window: an incremental read has no way to express
    # "this row should no longer exist". For an event window the overwrite itself is the
    # rebuild, so the flag only matters where the write is a merge.
    forced_rebuild = rebuild if shape == SHAPE_LATEST_STATE else force_rebuild

    read_plan = plan_read(
        source_progress=source_progress, last_snapshot=cursor,
        current_snapshot=source_snapshot, lineage=snapshot_lineage(spark, source),
        forced_rebuild=forced_rebuild or force_window_scan,
        write_strategy=write_strategy)
    print(f"REALTIME_WINDOW {window.describe()} shape={shape} "
          f"write={write_strategy} read={read_plan.describe()}", flush=True)

    def _record(status, *, row_count, pruned, input_rows, target_snapshot, failure,
                new_cursor):
        finished = datetime.now(timezone.utc)
        write_ledger(spark, ledger_table, ledger_row(
            window, entry=entry, target=target, source=source, refresh_mode=refresh_mode,
            source_snapshot=source_snapshot, target_snapshot=target_snapshot,
            row_count=row_count, pruned=pruned, status=status, failure_reason=failure,
            started_at=started_at, finished_at=finished, config_version=config_version,
            shape=shape, write_strategy=write_strategy, attempt=attempt,
            read_mode=read_plan.mode, fallback_reason=read_plan.fallback_reason,
            source_snapshot_before=cursor, input_rows=input_rows, spark_app_id=app_id,
            baseline_cob_date=baseline_cob))
        write_info(spark, info_table, info_row(
            table_id=table_id, target=target, source=source, shape=shape,
            write_strategy=write_strategy, source_progress=source_progress,
            baseline_cob_date=baseline_cob, cursor=new_cursor,
            current_snapshot=source_snapshot, window=window,
            target_snapshot=target_snapshot, run_id=run_id, input_rows=input_rows,
            output_rows=row_count, read_mode=read_plan.mode,
            fallback_reason=read_plan.fallback_reason, status=status,
            started_at=started_at, finished_at=finished, config_version=config_version,
            err_msg=failure))

    if read_plan.mode == MODE_NOOP:
        # SUCCESS_NOOP (R2 brief §21). The source has not moved, so there is nothing to
        # read and nothing to write -- and the cursor MUST NOT advance, because advancing
        # past data never seen is exactly how a gap is created silently.
        #
        # THE PRUNE STILL RUNS. Retention is a function of the CLOCK, not of new data: the
        # window has moved on even though the source has not, and under `append` the prune
        # is the only thing that removes an aged row. Skipping it meant a table whose source
        # went quiet kept serving rows outside the window it promises -- indefinitely, and
        # reporting SUCCESS every ten minutes while it did.
        pruned = (prune(spark, target, window)
                  if prune_enabled and prunes_by_age(shape) else 0)
        row_count = spark.table(target).count()
        print(f"REALTIME_NOOP {table_id} {read_plan.detail} rows={row_count} "
              f"pruned={pruned}", flush=True)
        _record(STATUS_SUCCEEDED_NOOP, row_count=row_count, pruned=pruned, input_rows=0,
                target_snapshot=current_snapshot_id(spark, target), failure=None,
                new_cursor=cursor)
        return {"table_id": table_id, "status": STATUS_SUCCEEDED_NOOP, "rows": row_count,
                "pruned": pruned, "window": window, "read_mode": read_plan.mode,
                "fallback_reason": read_plan.fallback_reason, "input_rows": 0,
                "cursor": cursor}

    target_before = current_snapshot_id(spark, target)
    try:
        rows = source_rows(spark, source, window, read_plan)
        input_rows = rows.count()
        if shape == SHAPE_LATEST_STATE:
            print(f"REALTIME_LATEST_STATE {window.table_id} "
                  f"{'REBUILD (day boundary)' if rebuild else 'upsert'}", flush=True)
        # An EVENT WINDOW rebuilds by OVERWRITING. Appending during a rebuild would merge
        # the re-read window onto whatever is already there, which is the opposite of
        # rebuilding -- and it could not remove a row that should no longer be present.
        write_now = (WRITE_OVERWRITE_WINDOW
                     if (rebuild and shape == SHAPE_EVENT_WINDOW) else write_strategy)
        row_count = materialise(
            spark, rows, target, window, refresh_mode,
            shape=shape, write_strategy=write_now,
            engine=(entry.get('source') or {}).get('engine'), rebuild=rebuild,
            delete_policy=((entry.get('eod_policy') or {}).get('delete_policy')
                           or DELETE_EXCLUDE))
        # Retention is a PRUNE only where the shape says so. For `latest_state` the same
        # numbers are a recovery horizon: a valid entity may not change for months, and
        # deleting its row because its last event is old would empty the table of exactly
        # the entities that are most stable (ADR-082).
        pruned = (prune(spark, target, window)
                  if prune_enabled and prunes_by_age(shape) else 0)
        row_count = spark.table(target).count()
        status, failure = STATUS_SUCCEEDED, None
    except Exception as exc:                                       # noqa: BLE001
        # Recorded as FAILED and re-raised, with the cursor LEFT WHERE IT WAS. Swallowing
        # would leave a half-materialised window reported as success, which is the one
        # outcome a consumer cannot detect.
        _record(STATUS_FAILED, row_count=0, pruned=0, input_rows=0, target_snapshot=None,
                failure=f"{type(exc).__name__}: {exc}"[:1000], new_cursor=cursor)
        print(f"REALTIME_FAILED {table_id}: {type(exc).__name__}: {exc}", flush=True)
        raise

    target_snapshot = current_snapshot_id(spark, target)
    # THE COMMIT CHECK. A write that produced no new snapshot did not land, however cleanly
    # it returned -- and an unchanged snapshot with rows read is the signature of exactly
    # that. `input_rows == 0` is the legitimate case: a range that contained no events
    # inside the window commits nothing, and that is not a failure.
    committed = (target_snapshot is not None
                 and (target_snapshot != target_before or input_rows == 0))
    if not committed:
        print(f"REALTIME_COMMIT_UNVERIFIED {table_id} target snapshot did not move "
              f"({target_before} -> {target_snapshot}) after reading {input_rows} rows; "
              f"the cursor is NOT advanced", flush=True)
    new_cursor = advance_cursor(read_plan, committed=committed, validated=True,
                                previous=cursor)
    _record(status, row_count=row_count, pruned=pruned, input_rows=input_rows,
            target_snapshot=target_snapshot, failure=failure, new_cursor=new_cursor)
    print(f"REALTIME_MATERIALISED {table_id} -> {target} rows={row_count} "
          f"pruned={pruned} input={input_rows} read={read_plan.mode} "
          f"fallback={read_plan.fallback_reason or '-'} "
          f"cursor={cursor} -> {new_cursor} "
          f"source_snapshot={source_snapshot} target_snapshot={target_snapshot}",
          flush=True)
    return {"table_id": table_id, "status": status, "rows": row_count,
            "pruned": pruned, "window": window, "read_mode": read_plan.mode,
            "fallback_reason": read_plan.fallback_reason, "input_rows": input_rows,
            "cursor": new_cursor}


def cob_for(entry: dict, run_instant: datetime):
    """The business date this table's last close covered, from ITS OWN policy.

    Delegates to `cdc.eod.cob_date_for`, which the EOD engine uses -- so the rebase and the
    close cannot disagree about which day is being closed. A second implementation of "what
    is yesterday" is how one layer rebases onto a day the other has not certified, and the
    only symptom is a rebase that silently skips forever.
    """
    from cdc.eod import cob_date_for
    eod_policy = entry.get("eod_policy") or {}
    return cob_date_for(run_instant,
                        business_timezone=eod_policy.get("business_timezone", "UTC"),
                        lag_days=int(eod_policy.get("business_date_lag_days", 1)))


def read_eod_info(spark, eod_info_table: str, table_id: str, cob_date):
    """The EOD control-plane row for one `(table, COB)`, or None.

    None on ANY failure, including a missing table. `rebase_decision` treats that as
    "not certified" and refuses to remove anything -- which is the only safe reading of "I
    could not find out what the baseline contains".
    """
    if not eod_info_table:
        return None
    try:
        # `cutoff_str` is formatted INSIDE Spark. The collected `cutoff_ts_utc` is a naive
        # datetime in the driver's local zone, and putting that back into a SQL literal
        # moves the cutoff by the driver's offset -- which would delete overlay rows the
        # certified baseline does not contain.
        rows = spark.sql(
            f"SELECT *, date_format(cutoff_ts_utc, 'yyyy-MM-dd HH:mm:ss') AS cutoff_str "
            f"FROM {eod_info_table} WHERE table_id = '{table_id}' "
            f"AND cob_date = DATE '{cob_date}'").collect()
    except Exception as exc:                                       # noqa: BLE001
        print(f"REALTIME_EOD_INFO_READ_FAILED {eod_info_table} {table_id} {cob_date}: "
              f"{type(exc).__name__}: {exc}", flush=True)
        return None
    return rows[0].asDict() if rows else None


def run_rebase(spark, entry: dict, *, plan: dict, run_id: str, cob_date,
               ledger_table: str, info_table: str = "",
               eod_info_table: str = "") -> dict:
    """Rebase ONE table's overlay onto a newly CERTIFIED EOD close. R2-F, ADR-085.

        current state  =  certified EOD baseline  +  REALTIME changes after its cutoff

    While the baseline is D-1, every change since D-1's cutoff lives in REALTIME. When D
    certifies, the changes between D-1 and D are now IN the baseline, and keeping them in
    the overlay too means a consumer composing the two sees them twice.

    THE RACE THIS IS WRITTEN AROUND. The overlay keeps accepting events throughout. So the
    keys to remove are computed from a FROZEN read -- the target's snapshot at the moment
    the rebase started -- and the delete matches on `dv_pk_hash` AND `dv_event_id`. A key
    that received a new event in between no longer matches and survives. The delete removes
    only what it actually looked at.
    """
    table_id = entry["table_id"]
    policy = entry.get("realtime_policy") or {}
    targets = entry.get("targets") or {}
    target = targets.get("realtime_identifier")
    source = targets.get("full_cdc_identifier")
    config_version = plan.get("config_version", "")
    started_at = datetime.now(timezone.utc)
    shape = policy.get("shape") or SHAPE_EVENT_WINDOW

    state = read_cursor(spark, info_table, table_id)
    decision = rebase_decision(
        shape=shape, enabled=bool(policy.get("rebase_on_eod_certified", False)),
        eod_row=read_eod_info(spark, eod_info_table, table_id, cob_date),
        current_baseline=state.get("baseline_cob_date"))

    if not decision.should_rebase:
        print(f"REALTIME_REBASE_SKIPPED {table_id} cob={cob_date} "
              f"reason={decision.skip_reason} {decision.detail}", flush=True)
        return {"table_id": table_id, "status": STATUS_SKIPPED_DISABLED,
                "operation": OPERATION_REBASE, "skip_reason": decision.skip_reason,
                "removed": 0, "rows": 0}

    require_target(spark, target, table_id)
    frozen = current_snapshot_id(spark, target)
    cutoff = decision.cutoff_utc
    cutoff_str = decision.cutoff_str
    before = spark.table(target).count()

    # FROZEN read: the rows whose winning event the certified close already contains.
    # `FOR VERSION AS OF` rather than a plain read, so an event landing during the rebase
    # cannot enter the set of things to delete.
    keys = spark.sql(
        f"SELECT dv_pk_hash, dv_event_id FROM {target} "
        + (f"VERSION AS OF {frozen} " if frozen is not None else "")
        + f"WHERE source_commit_ts < TIMESTAMP '{cutoff_str}'")
    keys.createOrReplaceTempView("rt_rebase_keys")
    candidates = keys.count()
    spark.sql(rebase_merge_sql(target))
    spark.catalog.dropTempView("rt_rebase_keys")

    after = spark.table(target).count()
    removed = before - after
    target_snapshot = current_snapshot_id(spark, target)
    finished = datetime.now(timezone.utc)

    # Evidence (R2 brief section 16): both baselines, the cutoff, the frozen upper snapshot,
    # rows removed and retained, the resulting snapshot, and the run.
    window = resolve_window(policy, table_id=table_id, run_id=run_id,
                            run_upper_bound=started_at,
                            business_timezone=policy.get("business_timezone", "UTC"))
    write_ledger(spark, ledger_table, ledger_row(
        window, entry=entry, target=target, source=source,
        refresh_mode=policy.get("refresh_mode", REFRESH_FULL),
        source_snapshot=None, target_snapshot=target_snapshot, row_count=after,
        pruned=removed, status=STATUS_SUCCEEDED, failure_reason=None,
        started_at=started_at, finished_at=finished, config_version=config_version,
        shape=shape, write_strategy=policy.get("write_strategy", WRITE_GUARDED_MERGE),
        attempt=1, read_mode=MODE_NOOP, fallback_reason="",
        source_snapshot_before=frozen, input_rows=candidates, spark_app_id="",
        baseline_cob_date=decision.new_baseline, operation=OPERATION_REBASE,
        prev_baseline_cob_date=decision.prev_baseline))
    write_info(spark, info_table, info_row(
        table_id=table_id, target=target, source=source, shape=shape,
        write_strategy=policy.get("write_strategy", WRITE_GUARDED_MERGE),
        source_progress=policy.get("source_progress", DEFAULT_SOURCE_PROGRESS),
        baseline_cob_date=decision.new_baseline,
        cursor=state.get("last_source_snapshot_id"),
        current_snapshot=state.get("current_source_snapshot_id"), window=window,
        target_snapshot=target_snapshot, run_id=run_id, input_rows=candidates,
        output_rows=after, read_mode=MODE_NOOP, fallback_reason="",
        status=STATUS_SUCCEEDED, started_at=started_at, finished_at=finished,
        config_version=config_version, err_msg=None))

    print(f"REALTIME_REBASED {table_id} baseline {decision.prev_baseline} -> "
          f"{decision.new_baseline} cutoff={cutoff} frozen_snapshot={frozen} "
          f"candidates={candidates} removed={removed} retained={after}", flush=True)
    return {"table_id": table_id, "status": STATUS_SUCCEEDED,
            "operation": OPERATION_REBASE, "removed": removed, "rows": after,
            "candidates": candidates, "baseline": decision.new_baseline,
            "prev_baseline": decision.prev_baseline, "frozen_snapshot": frozen}


def select_entries(plan: dict, table_arg: str) -> list:
    payload = plan.get("plan") or plan
    tables = sorted(payload.get("tables") or [], key=lambda e: e["table_id"])
    if table_arg.upper() == "ALL":
        return tables
    wanted = {t.strip() for t in table_arg.split(",") if t.strip()}
    found = [e for e in tables if e["table_id"] in wanted]
    missing = wanted - {e["table_id"] for e in found}
    if missing:
        raise SystemExit(
            f"not in the compiled plan: {', '.join(sorted(missing))}. Register the table "
            f"in cdc/registry/sources.yaml and recompile -- this engine never materialises "
            f"a table the registry does not describe.")
    return found


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", required=True, help="compiled plan: local path or s3://")
    ap.add_argument("--table", required=True,
                    help="canonical table id, a comma-separated list, or ALL")
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--as-of", default=None,
                    help="freeze the upper bound at this ISO-8601 instant instead of now. "
                         "The whole point of section D, exposed: a rebuild of a past window "
                         "must use that window's bound, not today's")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--rebuild", action="store_true",
                    help="rebuild from FULL_CDC. Identical mechanics -- the flag exists to "
                         "make the intent explicit in logs and in the ledger, and to force "
                         "a full refresh even for an incremental_merge table")
    ap.add_argument("--rebase-cob", default=None,
                    help="REBASE the overlay onto the CERTIFIED EOD close for this business "
                         "date (YYYY-MM-DD), or AUTO to derive each table's own COB from "
                         "its business timezone and lag. Removes the overlay rows the new "
                         "baseline already contains and retains every event after its "
                         "cutoff. Refuses on a close that is not CERTIFIED")
    ap.add_argument("--force-window-scan", action="store_true",
                    help="ignore the incremental cursor and re-read the whole window. The "
                         "always-correct path: use it when a cursor is suspect, and expect "
                         "it to cost what the layer cost before R2-C")
    ap.add_argument("--no-prune", action="store_true",
                    help="skip the retention delete (diagnosis only; the config still "
                         "promises the retention)")
    args = ap.parse_args(argv)

    as_of = (datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
             if args.as_of else datetime.now(timezone.utc))
    if as_of.tzinfo is None:
        raise SystemExit("--as-of must carry a timezone offset; a naive instant is "
                         "ambiguous and the calendar boundary depends on it")
    run_id = args.run_id or f"rt-{as_of:%Y%m%dT%H%M%SZ}"

    spark = build_spark(args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    plan = read_plan(spark, args.plan)
    payload = plan.get("plan") or plan
    ledger_table = (payload.get("realtime_run_ledger") or {}).get("identifier")
    if not ledger_table:
        raise SystemExit("the compiled plan carries no realtime_run_ledger identifier -- "
                         "recompile it with `python3 -m cdc.compile --out ...`")
    info_table = (payload.get("realtime_info") or {}).get("identifier") or ""
    if not info_table:
        # NOT fatal, and deliberately so. A plan compiled before ADR-083 has no control
        # plane, and every table in it is `window_scan` -- which needs no cursor. Refusing
        # would block a correct run on a missing optimisation.
        print("REALTIME_NO_CONTROL_PLANE the compiled plan carries no realtime_info "
              "identifier; every table runs as a full window scan. Recompile to enable "
              "the incremental cursor.", flush=True)

    entries = select_entries(plan, args.table)
    print(f"REALTIME_RUN id={run_id} as_of={as_of.isoformat()} tables={len(entries)} "
          f"config_version={plan.get('config_version')} "
          f"mode={'REBUILD' if args.rebuild else 'INCREMENTAL_RUN'}", flush=True)

    if args.rebase_cob:
        from datetime import date as _date
        # AUTO derives each table's COB from ITS OWN business timezone and lag, inside this
        # process. A date computed by the scheduler would apply the SCHEDULER's timezone to
        # every table, and rebase a non-UTC table onto the wrong day's close -- the same
        # reasoning that keeps the EOD DAG from passing a date (ADR-065 section 2).
        cob = None if args.rebase_cob.upper() == "AUTO" else _date.fromisoformat(
            args.rebase_cob)
        eod_info_table = (payload.get("eod_info") or {}).get("identifier") or ""
        if not eod_info_table:
            raise SystemExit(
                "the compiled plan carries no eod_info identifier, so what the certified "
                "baseline contains cannot be read -- and a rebase that cannot read it "
                "would be deleting rows on a guess. Recompile the plan.")
        rebased = [run_rebase(spark, e, plan=plan, run_id=run_id,
                              cob_date=cob or cob_for(e, as_of),
                              ledger_table=ledger_table, info_table=info_table,
                              eod_info_table=eod_info_table)
                   for e in entries]
        done = sum(1 for r in rebased if r["status"] == STATUS_SUCCEEDED)
        print(f"REALTIME_REBASE_SUMMARY cob={cob or 'AUTO'} rebased={done} "
              f"skipped={len(rebased) - done} "
              f"removed={sum(r['removed'] for r in rebased)}", flush=True)
        spark.stop()
        return 0

    results = []
    for entry in entries:
        # `--rebuild` is a FLAG, not a change of shape.
        #
        # It used to rewrite the entry to `event_window` / `overwrite_window`, which was
        # right while every table was an event window and is destructive now: it would
        # replace a `latest_state` table's contents with the raw event window -- many rows
        # per business key, in a table whose whole contract is one -- and report SUCCESS.
        # Each shape rebuilds in its own way: the event window by overwriting, the state
        # table by re-ranking the window and overwriting (`_materialise_latest_state`).
        results.append(run_table(spark, entry, plan=plan, run_id=run_id, as_of=as_of,
                                 ledger_table=ledger_table, info_table=info_table,
                                 prune_enabled=not args.no_prune,
                                 force_window_scan=args.force_window_scan,
                                 force_rebuild=args.rebuild))

    ok = sum(1 for r in results if r["status"] == STATUS_SUCCEEDED)
    noop = sum(1 for r in results if r["status"] == STATUS_SUCCEEDED_NOOP)
    skipped = sum(1 for r in results if r["status"] == STATUS_SKIPPED_DISABLED)
    incremental = sum(1 for r in results if r.get("read_mode") == MODE_INCREMENTAL)
    fell_back = sorted({r["fallback_reason"] for r in results if r.get("fallback_reason")})
    print(f"REALTIME_SUMMARY succeeded={ok} noop={noop} skipped={skipped} "
          f"incremental={incremental} rows={sum(r['rows'] for r in results)}"
          + (f" fallback={','.join(fell_back)}" if fell_back else ""), flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
