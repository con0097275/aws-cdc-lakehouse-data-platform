"""Backfill a per-table FULL_CDC target from the LEGACY monolith (section C).

    spark-submit backfill.py --plan s3://.../table-plan.json \
        --table oracle.coredb.corebank.account --warehouse s3://.../warehouse/ \
        --legacy glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events

    --dry-run    (DEFAULT) count and compare; write nothing
    --execute    perform the MERGE

READS THE LEGACY TABLE, NEVER KAFKA. The canonical history already exists in the monolith,
with its identities and coordinates intact; re-reading the topics would decode the same
records a second time, produce different `ingested_at` values, and depend on a Kafka
retention window that no longer covers the oldest events. Copying preserves what was already
decided rather than deciding it again.

THE LEGACY TABLE IS NEVER MODIFIED (section A). This job reads it. There is no write path to
it here and no DROP anywhere in the file -- asserted by a test, because "I did not intend to"
is not a control.

IDENTITY IS PRESERVED, NOT RECOMPUTED. `dv_event_id`, `dv_src_event_id`, the source position,
the source and Kafka timestamps and the operation are copied verbatim. Recomputing
`dv_event_id` would be the obvious thing and it is wrong: it is `sha2(topic|partition|offset|
kafka_timestamp)`, so a recompute that disagreed by one microsecond of timestamp rendering
would produce a target whose rows are the same events under different identities -- and the
reconciliation would then compare two sets that cannot match, for a reason unrelated to the
data.
"""
# EMR Serverless emr-7.2.0 runs PYTHON 3.9; PEP 604 unions die at IMPORT there.
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, "/tmp/framework")


def build_spark(warehouse: str) -> SparkSession:
    return (SparkSession.builder.appName("cdc-backfill")
            .config("spark.sql.catalog.glue_catalog",
                    "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl",
                    "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)
            .config("spark.sql.catalog.glue_catalog.io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            # ADR-024: `event_date` in the legacy rows was derived in the session zone.
            # Reading them under a different one would re-derive nothing, but any date
            # comparison in the compare step would shift.
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def read_plan(spark, uri: str) -> dict:
    if uri.startswith(("s3://", "s3a://", "file:")):
        return json.loads("".join(spark.read.text(uri).rdd.map(lambda r: r[0]).collect()))
    with open(uri) as fh:
        return json.load(fh)


def legacy_slice(spark, legacy_table: str, entry: dict):
    """The monolith rows belonging to ONE source table.

    The predicate matches `cdc/cutover.py`'s: `source_system` is the registry's source_id and
    `source_table` is the source table as the connector spells it. Getting this wrong does
    not error -- it silently backfills the wrong subset, or none.
    """
    source = entry.get("source") or {}
    return (spark.table(legacy_table)
                 .filter(F.col("source_system") == F.lit(source.get("source_id")))
                 .filter(F.upper(F.col("source_table")) ==
                         F.lit(str(source.get("table", "")).upper())))


def split_identifiable(rows):
    """(rows that can be backfilled, count that cannot).

    A row with a NULL `dv_event_id` HAS NO IDENTITY, and identity is what every part of this
    migration is built on:

      * the MERGE is `ON t.dv_event_id = s.dv_event_id`, and NULL never equals NULL -- so
        such a row is NEVER matched, is re-inserted by every run, and the backfill silently
        stops being re-runnable;
      * the reconciliation compares identity SETS, and a row with no identity cannot be on
        either side of that comparison;
      * two such rows cannot be distinguished from one such row written twice.

    MEASURED, not hypothetical: 320 of the 961 legacy `oracle/ACCOUNT` rows carry a NULL
    `dv_event_id`. They predate the column -- `spark/jobs/full_cdc/job.py --backfill-identity`
    exists precisely to populate it for rows still readable from Kafka, and it has not been
    run for these.

    They are EXCLUDED and COUNTED rather than dropped quietly or given an invented identity.
    Inventing one would make the reconciliation pass while comparing two sets that mean
    different things.
    """
    identifiable = rows.filter(F.col("dv_event_id").isNotNull())
    unidentifiable = rows.filter(F.col("dv_event_id").isNull()).count()
    return identifiable, unidentifiable


def align_to_target(spark, rows, target: str):
    """Name-based alignment. Columns the target has and the source lacks become NULL;
    columns the source has and the target lacks are DROPPED rather than added.

    Dropping rather than widening is deliberate: the per-table target's schema is the
    provisioned contract (ADR-063), and letting a legacy column that predates that contract
    add itself would make the backfilled table a different shape from a freshly provisioned
    one -- so two tables of the same kind would disagree about their own schema.
    """
    target_fields = {f.name: f for f in spark.table(target).schema.fields}
    for name, field in target_fields.items():
        if name not in rows.columns:
            rows = rows.withColumn(name, F.lit(None).cast(field.dataType))
    return rows.select(*target_fields.keys())


def compare(spark, source_rows, target: str, entry: dict) -> dict:
    """Count and identity comparison (section C).

    The identity hash is `sha2` over the SORTED, concatenated `dv_event_id` set. Sorted
    because a set has no order and two identical sets hashed in arrival order would differ;
    over `dv_event_id` because that is what the layer's idempotency is defined on, so two
    sides agreeing on it is the same statement as "no event was lost or duplicated".
    """
    tgt = spark.table(target)
    source_ids = source_rows.select("dv_event_id").distinct()
    target_ids = tgt.select("dv_event_id").distinct()
    src_n, tgt_n = source_ids.count(), target_ids.count()

    def digest(df):
        rows = df.orderBy("dv_event_id").agg(
            F.sha2(F.concat_ws("|", F.collect_list("dv_event_id")), 256).alias("h")
        ).collect()
        return rows[0]["h"] if rows else None

    missing = source_ids.subtract(target_ids).count()
    extra = target_ids.subtract(source_ids).count()
    return {
        "legacy_events": src_n,
        "per_table_events": tgt_n,
        "missing_in_per_table": missing,
        "extra_in_per_table": extra,
        "legacy_identity_hash": digest(source_ids),
        "per_table_identity_hash": digest(target_ids),
        "identity_match": missing == 0 and extra == 0,
    }


def backfill(spark, entry: dict, *, legacy_table: str, execute: bool) -> dict:
    """Copy one source table's legacy history into its per-table target."""
    table_id = entry["table_id"]
    target = (entry.get("targets") or {}).get("full_cdc_identifier")
    if not target:
        raise RuntimeError(f"{table_id}: the plan carries no FULL_CDC identifier")
    try:
        spark.table(target)
    except Exception:                                                  # noqa: BLE001
        raise RuntimeError(
            f"CDC_BACKFILL_TARGET_MISSING {target} (for {table_id}). Provision it first: "
            f"`python3 -m cdc.provision --table {table_id}` then "
            f"`spark-submit spark/ops/provision_cdc_tables.py --execute`.") from None

    all_rows = legacy_slice(spark, legacy_table, entry)
    n_all = all_rows.count()
    rows, n_unidentifiable = split_identifiable(all_rows)
    n_source = rows.count()
    print(f"CDC_BACKFILL_SOURCE {table_id} legacy={legacy_table} rows={n_all} "
          f"identifiable={n_source} no_identity={n_unidentifiable}", flush=True)
    if n_unidentifiable:
        print(f"CDC_BACKFILL_NO_IDENTITY {table_id} rows={n_unidentifiable} -- these carry "
              f"a NULL dv_event_id, predate the column, and are EXCLUDED: they cannot be "
              f"merged idempotently or reconciled. Run "
              f"`spark/jobs/full_cdc/job.py --backfill-identity` against the source topics "
              f"to populate their identity, then re-run this backfill.", flush=True)

    if not execute:
        print(f"CDC_BACKFILL_DRY_RUN {table_id} rows={n_source} -- nothing written. "
              f"Re-run with --execute.", flush=True)
        result = compare(spark, rows, target, entry)
        result.update({"table_id": table_id, "executed": False,
                       "legacy_rows_total": n_all,
                       "legacy_rows_without_identity": n_unidentifiable})
        return result

    aligned = align_to_target(spark, rows, target)
    view = f"backfill_src_{target.replace('.', '_')}"
    aligned.createOrReplaceTempView(view)
    # MERGE on dv_event_id, WHEN NOT MATCHED only. The layer is append-only and the backfill
    # must be re-runnable: a second run over the same window inserts nothing rather than
    # doubling the history.
    spark.sql(f"""MERGE INTO {target} t USING {view} s
                  ON t.dv_event_id = s.dv_event_id
                  WHEN NOT MATCHED THEN INSERT *""")
    spark.catalog.dropTempView(view)

    result = compare(spark, rows, target, entry)
    result.update({"table_id": table_id, "executed": True,
                   "legacy_rows_total": n_all,
                   "legacy_rows_without_identity": n_unidentifiable})
    print(f"CDC_BACKFILL_DONE {table_id} legacy={result['legacy_events']} "
          f"per_table={result['per_table_events']} "
          f"identity_match={result['identity_match']}", flush=True)
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", required=True)
    ap.add_argument("--table", required=True, help="canonical table id, or a list")
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--legacy", required=True,
                    help="the legacy monolith, read-only. NEVER modified by this job")
    ap.add_argument("--execute", action="store_true",
                    help="perform the MERGE. Without it this is a dry run that compares "
                         "and writes nothing")
    ap.add_argument("--out", default=None, help="write the comparison JSON here")
    args = ap.parse_args(argv)

    spark = build_spark(args.warehouse)
    spark.sparkContext.setLogLevel("WARN")
    plan = read_plan(spark, args.plan)
    payload = plan.get("plan") or plan
    wanted = {t.strip() for t in args.table.split(",") if t.strip()}
    entries = [e for e in payload.get("tables") or [] if e["table_id"] in wanted]
    missing = wanted - {e["table_id"] for e in entries}
    if missing:
        raise SystemExit(f"not in the compiled plan: {', '.join(sorted(missing))}")

    print(f"CDC_BACKFILL_RUN mode={'EXECUTE' if args.execute else 'DRY RUN'} "
          f"tables={len(entries)} legacy={args.legacy} "
          f"config_version={plan.get('config_version')}", flush=True)

    results = [backfill(spark, e, legacy_table=args.legacy, execute=args.execute)
               for e in entries]
    matched = sum(1 for r in results if r["identity_match"])
    print(f"CDC_BACKFILL_SUMMARY tables={len(results)} identity_match={matched}",
          flush=True)
    if not args.execute:
        # A DRY RUN CANNOT MATCH, and must not exit as though it failed. The target is empty
        # (or partial) by definition until --execute, so `identity_match` is informational
        # here -- it reports what the comparison WOULD say. Exiting non-zero made a correct
        # dry run look like a failed job, which is the kind of noise that teaches an operator
        # to ignore exit codes.
        print("CDC_BACKFILL_DRY_RUN_COMPLETE nothing was written; the identity comparison "
              "above is what --execute would be judged against", flush=True)
    if args.out:
        with open(args.out, "w") as fh:
            json.dump({"generated_at": datetime.now(timezone.utc).isoformat(),
                       "results": results}, fh, indent=1, sort_keys=True)
        print(f"CDC_BACKFILL_REPORT {args.out}", flush=True)
    spark.stop()
    return 0 if (not args.execute or matched == len(results)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
