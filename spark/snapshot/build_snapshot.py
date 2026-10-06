"""L3 SNAPSHOT as-of T-1, built from L2 FULL CDC (DATA_CONTRACTS §7).

Three properties, and each one is a separate way to get this wrong:

  1. AS-OF, not "latest". CLAUDE.md §5.6 -- rebuilding an old snapshot_date must
     reproduce the original result, so the cutoff filter is on source_commit_ts and the
     window function ranks only rows inside it.
  2. Ordered by SOURCE POSITION, never by Kafka offset alone. CLAUDE.md §5.4 -- offset
     only increases within one partition, so using it globally reorders across partitions.
  3. Deletes are EXPLICIT. §7.2 -- the active table excludes a PK whose latest event
     as-of cutoff is a delete; the opt-in _history table keeps it with is_deleted.

The ranking is TOTAL, which is what makes rebuilds deterministic (§7.3): two distinct
events cannot share all five event_order components, because kafka_partition and
kafka_offset alone are unique per record.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/opt/spark/snapshot")
from entity_config import EntityConfig, load as load_entities   # noqa: E402

# DATA_CONTRACTS §4.1 / §7.1. The order of these five is the contract, not a preference.
RANK_ORDER = """
    event_order.position_primary   DESC,
    event_order.position_secondary DESC,
    event_order.source_ts_ms       DESC,
    event_order.kafka_partition    DESC,
    event_order.kafka_offset       DESC
"""


# The same precedence as RANK_ORDER, as an EXPLICIT named_struct.
#
# Never pass `event_order` itself to max_by. Its comparison semantics depend entirely on
# how the column happens to be typed: a struct compares field-by-field in DECLARED order,
# so the ranking would silently follow the table's physical layout rather than
# DATA_CONTRACTS §4.1, and a map is not orderable at all. Listing the five fields in
# precedence order makes the comparison independent of both.
ORDER_STRUCT = """named_struct(
        'p1', event_order.position_primary,
        'p2', event_order.position_secondary,
        'ts', event_order.source_ts_ms,
        'kp', event_order.kafka_partition,
        'ko', event_order.kafka_offset)"""


class SnapshotError(RuntimeError):
    pass


def cutoff_for(snapshot_date: date) -> datetime:
    """As-of T-1: everything committed strictly BEFORE T 00:00:00 UTC.

    Exclusive. `<`, not `<=` -- an event exactly at midnight belongs to day T, not to
    the T-1 snapshot (S01-18, ADR-024).
    """
    return datetime.combine(snapshot_date + timedelta(days=1),
                            datetime.min.time(), tzinfo=timezone.utc)


def ranked_sql(cfg: EntityConfig, cutoff: datetime) -> str:
    """Rank every L2 event per PK, as-of cutoff."""
    return f"""
        SELECT *, row_number() OVER (
                    PARTITION BY primary_key
                    ORDER BY {RANK_ORDER}
                  ) AS rn
        FROM {cfg.l2_table}
        WHERE source_commit_ts < TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'
    """


def projection_sql(cfg: EntityConfig) -> str:
    """Typed projection from the `after` JSON string.

    L1/L2 keep before/after as JSON (S01-16) so source DDL changes do not force an
    Iceberg migration on the streaming table. Typing happens HERE, where a migration is
    a reviewed change rather than a stream outage.
    """
    return ",\n           ".join(
        f"CAST(get_json_object(after, '$.{src}') AS {typ}) AS {out}"
        for src, (out, typ) in cfg.projection.items()
    )


def build_snapshot(spark, cfg: EntityConfig, snapshot_date: date, run_id: str,
                   history: bool = False) -> dict:
    cutoff = cutoff_for(snapshot_date)
    target = cfg.history_table if history else cfg.snapshot_table

    if history and not cfg.enable_history:
        raise SnapshotError(
            f"{cfg.entity}: history table requested but enable_history is false. "
            "DATA_CONTRACTS §7.2 makes retaining deleted records an opt-in decision, "
            "because it conflicts with erasure obligations."
        )

    # Active table filters deletes out; history keeps them flagged.
    delete_filter = "" if history else "AND operation <> 'd'"
    is_deleted = "(operation = 'd')" if history else "CAST(false AS BOOLEAN)"

    sql = f"""
        WITH ranked AS ({ranked_sql(cfg, cutoff)})
        SELECT
           {projection_sql(cfg)},
           primary_key,
           DATE '{snapshot_date.isoformat()}' AS snapshot_date,
           '{cfg.domain}'                     AS domain,
           source_system,
           event_id                           AS source_event_id,
           source_position_type,
           source_position_primary,
           source_commit_ts,
           {is_deleted}                       AS is_deleted,
           '{run_id}'                         AS l3_run_id,
           CURRENT_TIMESTAMP()                AS l3_write_ts
        FROM ranked
        WHERE rn = 1 {delete_filter}
    """
    df = spark.sql(sql)

    # Full overwrite of THIS snapshot_date partition only (DATA_CONTRACTS §7).
    # Rebuilding one date must not disturb any other, and must be idempotent.
    #
    # overwrite(<filter>), NOT overwritePartitions(). Dynamic partition overwrite only
    # replaces partitions PRESENT IN THE WRITTEN DATAFRAME, so a rebuild whose result is
    # EMPTY -- every PK for the date deleted, which is exactly the late-delete case --
    # touches no partition and silently leaves the previous, now-wrong rows in place.
    # Overwrite-by-filter deletes the partition first, so an empty result correctly
    # empties it. Regression: test_late_delete_emptying_partition_clears_it.
    (df.sortWithinPartitions("primary_key")
       .writeTo(target)
       .overwrite(F.col("snapshot_date") == F.lit(snapshot_date)))

    return validate(spark, target, snapshot_date, cfg, run_id, history)


def reconcile_l2_l3(spark, cfg: EntityConfig, snapshot_date: date, history: bool) -> int:
    """Expected active-row count, derived from L2 INDEPENDENTLY of the build.

    Deliberately a GROUP BY / max_by aggregation rather than the build's row_number
    window. A reconciliation that reuses the build's own query proves only that the query
    is deterministic -- it agrees with the build even when the build is wrong. Two
    different derivations of the same number is the point (scope item 8).
    """
    cutoff = cutoff_for(snapshot_date)
    delete_filter = "" if history else "WHERE last_op <> 'd'"
    return spark.sql(f"""
        SELECT count(*) AS n FROM (
            SELECT primary_key, max_by(operation, {ORDER_STRUCT}) AS last_op
            FROM {cfg.l2_table}
            WHERE source_commit_ts < TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'
            GROUP BY primary_key
        ) {delete_filter}
    """).first()["n"]


def validate(spark, target: str, snapshot_date: date, cfg: EntityConfig, run_id: str,
             history: bool = False) -> dict:
    """Uniqueness + reconciliation. Certification is published only if these pass
    (scope item 8)."""
    snap = spark.table(target).where(f"snapshot_date = DATE '{snapshot_date.isoformat()}'")
    total = snap.count()
    distinct_pk = snap.select("primary_key").distinct().count()

    failures = []
    if total != distinct_pk:
        failures.append(f"PK uniqueness: {total} rows but {distinct_pk} distinct PKs "
                        f"({total - distinct_pk} duplicates)")
    if not spark.table(target).schema.fieldNames().count("source_event_id"):
        failures.append("lineage column source_event_id missing")

    # Every surviving row must belong to THIS run. A row carrying an older l3_run_id
    # means the partition was not actually replaced, so what is about to be certified is
    # a previous build's output rather than this one's. This is the check that catches a
    # write path which silently no-ops -- certifying stale data is worse than failing.
    stale = snap.where(f"l3_run_id <> '{run_id}'").count()
    if stale:
        failures.append(
            f"partition not replaced: {stale} of {total} rows carry an l3_run_id other "
            f"than {run_id!r}; the snapshot_date partition was not overwritten"
        )

    expected = reconcile_l2_l3(spark, cfg, snapshot_date, history)
    if expected != total:
        failures.append(
            f"L2->L3 reconciliation: L2 yields {expected} active PKs as-of cutoff but L3 "
            f"has {total} rows (difference {total - expected})"
        )

    return {
        "run_id": run_id, "table": target, "snapshot_date": snapshot_date.isoformat(),
        "row_count": total, "distinct_pk": distinct_pk, "expected_from_l2": expected,
        "checksum": snapshot_checksum(spark, target, snapshot_date),
        "status": "CERTIFIED" if not failures else "FAILED",
        "failures": failures,
    }


def snapshot_checksum(spark, target: str, snapshot_date: date) -> str:
    """Order-independent checksum for the reproducibility gate (§7.3).

    XORing per-row hashes makes the result independent of row ORDER, so two rebuilds
    that produce the same rows in a different physical layout still match. Summing would
    be order-independent too but collides far more readily.

    Covers the BUSINESS COLUMNS, not just (primary_key, source_event_id). Hashing only
    those two answers "did the same events win?" but not "did they produce the same
    values?", so a changed projection or type cast would rebuild to an identical
    checksum while the table content differed -- the reproducibility gate would pass on
    a real regression. l3_run_id and l3_write_ts are excluded because they are expected
    to differ on every run; including them would make every rebuild look changed.
    """
    volatile = {"l3_run_id", "l3_write_ts"}
    cols = [c for c in spark.table(target).schema.fieldNames() if c not in volatile]
    rows = (spark.table(target)
            .where(f"snapshot_date = DATE '{snapshot_date.isoformat()}'")
            .select(*sorted(cols))
            .collect())
    acc = 0
    for r in rows:
        payload = "|".join(f"{c}={r[c]!r}" for c in sorted(cols))
        acc ^= int.from_bytes(hashlib.sha256(payload.encode()).digest()[:16], "big")
    return f"{acc:032x}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot-date", required=True, help="T-1 date, YYYY-MM-DD UTC")
    ap.add_argument("--entity", required=True, help="domain.entity")
    ap.add_argument("--config", default="/opt/spark/snapshot/config/entities.json")
    ap.add_argument("--history", action="store_true")
    ap.add_argument("--run-id", default=None)
    args = ap.parse_args()

    run_id = args.run_id or f"l3-{uuid.uuid4()}"
    cfgs = load_entities(args.config)
    if args.entity not in cfgs:
        raise SystemExit(f"unknown entity {args.entity}; known: {sorted(cfgs)}")

    spark = (SparkSession.builder.appName(f"l3_snapshot_{run_id}")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             # ADR-024, and S07-1: without this the cutoff is read in the JVM's local
             # zone and the snapshot silently includes or excludes a day's boundary.
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    result = build_snapshot(spark, cfgs[args.entity],
                            date.fromisoformat(args.snapshot_date), run_id, args.history)
    print(result)
    if result["status"] != "CERTIFIED":
        raise SystemExit(f"snapshot NOT certified: {result['failures']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
