"""Pilot feature group: customer_behavior. Batch, offline only.

Reads CURATED/MART -- the stable business layers -- never FULL_CDC or REALTIME.

WHY THIS IS SUBMITTED, NOT RUN LOCALLY
--------------------------------------
Materialising against Glue needs a Spark session with Iceberg + the Glue catalog, which
means EMR Serverless. The transformation itself is plain Spark SQL and is unit-tested
against local Spark with fixtures, so the logic is proven without spend; only the write is
deferred.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from features.materialize import MaterializationSpec, materialize, resolve_source_layer

FEATURE_GROUP = "customer_behavior"
TARGET = "glue_catalog.feature_offline.customer_behavior"

#: The transformation. `feature_event_time` is the SNAPSHOT DATE -- event time, the day the
#: balance was true -- not the day this job happened to run.
SQL = """
WITH bal AS (
  SELECT
    CAST(account_sk AS STRING)            AS entity_id,
    CAST(business_date AS TIMESTAMP)      AS feature_event_time,
    closing_balance                       AS closing_balance
  FROM {curated}.fact_account_daily_snapshot
  WHERE business_date <= DATE '{cob_date}'
),
txn AS (
  SELECT
    CAST(account_sk AS STRING)            AS entity_id,
    CAST(business_date AS TIMESTAMP)      AS feature_event_time,
    COUNT(*)                              AS txn_count_7d,
    AVG(amount_base)                      AS avg_txn_amount_30d
  FROM {mart}.fact_transaction
  WHERE business_date <= DATE '{cob_date}'
  GROUP BY account_sk, business_date
)
SELECT
  b.entity_id,
  b.feature_event_time,
  COALESCE(t.txn_count_7d, 0)             AS txn_count_7d,
  t.avg_txn_amount_30d                    AS avg_txn_amount_30d,
  b.closing_balance                       AS closing_balance
FROM bal b
LEFT JOIN txn t
  ON b.entity_id = t.entity_id
 AND b.feature_event_time = t.feature_event_time
"""


def build_spec(cob_date: str, run_id: str, group_version: str,
               curated: str, mart: str) -> MaterializationSpec:
    return MaterializationSpec(
        feature_group=FEATURE_GROUP,
        entity_keys=("entity_id",),
        feature_columns=("txn_count_7d", "avg_txn_amount_30d", "closing_balance"),
        feature_group_version=group_version,
        source_layer=resolve_source_layer("CURATED"),
        source_tables=(f"{curated}.fact_account_daily_snapshot",
                       f"{mart}.fact_transaction"),
        target_table=TARGET, cob_date=cob_date, run_id=run_id,
        watermark=f"{cob_date}T00:00:00Z")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Materialise the customer_behavior features.")
    ap.add_argument("--cob-date", required=True)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--group-version", required=True)
    ap.add_argument("--curated", default="glue_catalog.kafka_dev_lab_dev_curated")
    ap.add_argument("--mart", default="glue_catalog.kafka_dev_lab_dev_mart")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the SQL and the spec; touch nothing")
    a = ap.parse_args(argv)

    run_id = a.run_id or f"feat-{a.cob_date}-{datetime.now(timezone.utc):%H%M%S}"
    spec = build_spec(a.cob_date, run_id, a.group_version, a.curated, a.mart)
    sql = SQL.format(curated=a.curated, mart=a.mart, cob_date=a.cob_date)

    if a.dry_run:
        print(json.dumps({"spec": {**spec.__dict__,
                                   "entity_keys": list(spec.entity_keys),
                                   "feature_columns": list(spec.feature_columns),
                                   "source_tables": list(spec.source_tables)},
                          "sql": sql.strip()}, indent=2))
        print("\nDRY RUN — nothing written. Submit to EMR Serverless to materialise.")
        return 0

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"features-{FEATURE_GROUP}").getOrCreate()
    result = materialize(spark, spark.sql(sql), spec)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
