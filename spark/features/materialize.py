"""Batch feature materialization. Idempotent by construction.

SOURCE POLICY (ADR-052)
-----------------------
Batch features read the STABLE business layers -- EOD / CURATED / MART. They do not read
FULL_CDC or REALTIME, and `resolve_source_layer` refuses to. The lakehouse stays the system
of record; a feature table is a derived consumer that can be dropped and rebuilt.

IDEMPOTENCE
-----------
MERGE on `(entity_id, feature_event_time, feature_version)`. Re-running the same window with
the same definition updates the same rows rather than appending duplicates, so a retry after
a partial failure is safe and a backfill overlapping an earlier run does not double-count.

`materialization_run_id` is stamped on every row so a bad run is identifiable and
reversible; it is NOT part of the merge key, because including it would make every rerun
produce new rows and destroy the idempotence it is there to audit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

#: Layers a batch feature pipeline may read. FULL_CDC and REALTIME are absent on purpose.
ALLOWED_BATCH_SOURCE_LAYERS = ("EOD", "CURATED", "MART", "SNAPSHOT")

MERGE_KEYS = ("entity_id", "feature_event_time", "feature_version")

AUDIT_COLUMNS = ("source_cob_date", "source_watermark",
                 "materialization_run_id", "created_at")


class SourcePolicyViolation(ValueError):
    """A feature pipeline tried to read a layer it must not."""


def resolve_source_layer(layer: str, *, streaming: bool = False) -> str:
    up = layer.upper()
    if streaming:
        if up not in ("REALTIME", "FULL_CDC"):
            raise SourcePolicyViolation(
                f"streaming features read REALTIME or FULL_CDC, not {layer!r}")
        return up
    if up not in ALLOWED_BATCH_SOURCE_LAYERS:
        raise SourcePolicyViolation(
            f"batch features read {', '.join(ALLOWED_BATCH_SOURCE_LAYERS)}, not {layer!r}. "
            "Reading raw CDC from a batch feature job would make the feature store a second "
            "interpretation of the change stream (ADR-052).")
    return up


@dataclass(frozen=True)
class MaterializationSpec:
    feature_group: str
    entity_keys: tuple[str, ...]
    feature_columns: tuple[str, ...]
    feature_group_version: str
    source_layer: str
    source_tables: tuple[str, ...]
    target_table: str
    cob_date: str
    run_id: str
    watermark: str | None = None


def stamp_audit_columns(df: DataFrame, spec: MaterializationSpec) -> DataFrame:
    """Attach the audit columns every offline feature row carries (ADR-052/054)."""
    return (df
            .withColumn("feature_version", F.lit(spec.feature_group_version))
            .withColumn("source_cob_date", F.to_date(F.lit(spec.cob_date)))
            .withColumn("source_watermark", F.lit(spec.watermark))
            .withColumn("materialization_run_id", F.lit(spec.run_id))
            # PROCESSING time. Recorded for audit; never a join key.
            .withColumn("created_at",
                        F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp")))


def validate_frame(df: DataFrame, spec: MaterializationSpec) -> None:
    """Structural checks that must hold before anything is written."""
    cols = set(df.columns)
    if "entity_id" not in cols:
        raise ValueError(f"{spec.feature_group}: no entity_id column")
    if "feature_event_time" not in cols:
        raise ValueError(
            f"{spec.feature_group}: no feature_event_time. Without event time there is no "
            "point-in-time boundary and every training set built from this leaks.")
    missing = set(spec.feature_columns) - cols
    if missing:
        raise ValueError(f"{spec.feature_group}: missing feature columns {sorted(missing)}")

    # Grain assertion: one row per (entity, event_time). A duplicate here silently becomes
    # an arbitrary pick at PIT-join time, and the arbitrariness is not reproducible.
    dupes = (df.groupBy("entity_id", "feature_event_time").count()
               .filter(F.col("count") > 1).limit(1).count())
    if dupes:
        raise ValueError(
            f"{spec.feature_group}: duplicate (entity_id, feature_event_time) rows. The "
            "feature grain must be unique or the point-in-time join picks arbitrarily.")


def merge_sql(spec: MaterializationSpec, source_view: str) -> str:
    on = " AND ".join(f"t.{k} = s.{k}" for k in MERGE_KEYS)
    updatable = list(spec.feature_columns) + list(AUDIT_COLUMNS)
    sets = ", ".join(f"t.{c} = s.{c}" for c in updatable)
    return (f"MERGE INTO {spec.target_table} t\n"
            f"USING {source_view} s\nON {on}\n"
            f"WHEN MATCHED THEN UPDATE SET {sets}\n"
            f"WHEN NOT MATCHED THEN INSERT *")


def materialize(spark: SparkSession, df: DataFrame, spec: MaterializationSpec) -> dict:
    """Validate, stamp, MERGE. Returns a record for ops.job_master_execution_hist."""
    resolve_source_layer(spec.source_layer)
    stamped = stamp_audit_columns(df, spec)
    validate_frame(stamped, spec)

    view = f"_feat_src_{spec.feature_group}"
    stamped.createOrReplaceTempView(view)
    spark.sql(merge_sql(spec, view))

    return {
        "feature_group": spec.feature_group,
        "feature_group_version": spec.feature_group_version,
        "materialization_run_id": spec.run_id,
        "source_layer": spec.source_layer,
        "source_tables": list(spec.source_tables),
        "cob_date": spec.cob_date,
        "row_count": stamped.count(),
        "target_table": spec.target_table,
        "status": "SUCCEEDED",
    }
