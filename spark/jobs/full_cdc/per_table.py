"""Per-table FULL_CDC writes: the routed half of the ingest, shared by BOTH jobs.

    decoded envelope -> canonical row -> router -> the ONE table the registry names

WHY THIS IS A MODULE AND NOT TWO COPIES
---------------------------------------
`job.py` (batch) and `stream_job.py` (continuous) already share `decode_topic_to_rows` and
`merge_into_full_cdc` for exactly one reason, recorded in DECISION_LOG on 2026-09-06: two
paths that "look equivalent" is the OPEN-28 defect, where the DAG adapter and
`reporting-live-run.py` differed on one dict key and only live execution showed it. The
per-table path is held to the same rule -- everything expensive to get right (the extra
identity columns, the typed payload projection, the refusal to create a table, the
idempotent MERGE) lives here and is imported by both.

WHY THE LEGACY PROJECTION IS NOT TOUCHED
----------------------------------------
`build_rows` still emits exactly the columns the monolith has today. The canonical additions
(`source_database`, `schema_global_id`, `dv_pk_hash`, `ingest_run_id`, `ingest_job`) are
passed in as EXTRA columns and only when a per-table write is actually happening. That is
what makes `--migration-mode legacy_only` a true no-op: this module is not even imported,
so it cannot evolve the monolith's schema as a side effect of existing (phase brief
section H).

NOTHING HERE CREATES A TABLE (section F). A target that does not exist is a refusal naming
the provisioner, never a CREATE. An event that appeared because someone mistyped a topic
must not be able to mint a production data product with no owner, no retention and no DQ
rule -- and a CREATE TABLE IF NOT EXISTS in the write path is precisely that button.
"""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

#: Columns the canonical per-table row adds over the legacy projection. Named here rather
#: than inferred, so a reader of this file can see the whole delta in one place.
EXTRA_COLUMNS = ("source_database", "schema_global_id", "dv_pk_hash",
                 "ingest_run_id", "ingest_job")


def _struct_fields(decoded, path: str) -> set:
    """Field names of `v.<path>` in THIS writer version's decoded schema, or empty.

    Spark resolves struct fields at ANALYSIS time, so naming a field the schema does not
    have fails the whole job rather than yielding NULL. Every access below is guarded by
    this -- the same guard `_source_position_cols` in job.py already applies for the
    engine-specific ordering fields.
    """
    field = next((f for f in decoded.schema["v"].dataType.fields if f.name == path), None)
    if field is None or not isinstance(field.dataType, StructType):
        return set()
    return {f.name for f in field.dataType.fields}


def _pk_hash(decoded, primary_key: tuple) -> "F.Column":
    """sha2 of the primary key, after-image first, before-image for a delete.

    A delete's `after` is NULL by construction, so hashing only the after image would give
    every delete the same key hash -- and `bucket(N, dv_pk_hash)` would pile them into one
    bucket while the row they delete sits in another.

    NULL when the source has no PK, or when this writer version does not carry the PK
    column. The second case is loud (`CDC_PER_TABLE_PK_ABSENT`) but not fatal: the MERGE key
    is `dv_event_id`, so a missing hash costs pruning, not correctness, and failing the
    batch over it would stop seven healthy tables to punish one schema mismatch.
    """
    if not primary_key:
        return F.lit(None).cast("string")
    after, before = _struct_fields(decoded, "after"), _struct_fields(decoded, "before")
    parts = []
    for col in primary_key:
        if col in after and col in before:
            parts.append(F.coalesce(F.col(f"v.after.{col}").cast("string"),
                                    F.col(f"v.before.{col}").cast("string"), F.lit("")))
        elif col in after:
            parts.append(F.coalesce(F.col(f"v.after.{col}").cast("string"), F.lit("")))
        elif col in before:
            parts.append(F.coalesce(F.col(f"v.before.{col}").cast("string"), F.lit("")))
        else:
            print(f"CDC_PER_TABLE_PK_ABSENT column={col} -- not in this writer schema; "
                  f"dv_pk_hash will be NULL for this group", flush=True)
            return F.lit(None).cast("string")
    return F.sha2(F.concat_ws("|", *parts), 256)


def _typed_payload(decoded, image: str, columns) -> "F.Column":
    """`v.<image>` as a struct of the DECLARED columns, in declared order.

    Built field by field rather than with `cast`, because a struct cast in Spark matches
    POSITIONALLY: two columns of the same type swapping places in a later writer version
    would silently swap their values. Declared columns absent from this writer version are
    NULL, which is what `schema_evolution: additive_only` means -- the column exists in the
    contract and this version predates it.

    TEMPORALS ARE CONVERTED, NOT CAST. Debezium's temporal semantic types are CUSTOM Connect
    logical types, so an Avro converter carries them as plain int64/int32 and `from_avro`
    hands Spark a number. Casting that to `timestamp` produces a year-57609 value SILENTLY
    (measured); casting an int32 day count to `date` fails the batch outright. So each
    column applies the expression its declared `encoding` names, and the compiler refuses a
    temporal column that declares none -- there is no safe default (cdc/rowspec.py).
    """
    from cdc.rowspec import DEFAULT_ENCODING, encoding_expression

    present = _struct_fields(decoded, image)
    fields = []
    for spec in columns:
        name, sql_type = spec["name"], spec["type"]
        encoding = spec.get("encoding", DEFAULT_ENCODING)
        if name not in present:
            # Absent from THIS writer version. Typed as the declared type directly: there is
            # no value to convert, and running the conversion on a NULL literal would only
            # add a way for the expression to fail on a column that is not there.
            fields.append(F.lit(None).cast(sql_type).alias(name))
            continue
        ref = f"v.{image}.`{name}`"
        if encoding == DEFAULT_ENCODING:
            col = F.col(f"v.{image}.{name}").cast(sql_type)
        else:
            col = F.expr(encoding_expression(encoding, ref,
                                             what=f"{image}.{name}")).cast(sql_type)
        fields.append(col.alias(name))
    return F.struct(*fields)


def canonical_extras(entry: dict, *, run_id: str, job: str):
    """Build the `enrich` callback `decode_topic_to_rows` appends to its projection.

    A callback rather than a second projection: the identity columns (`dv_event_id`,
    `dv_src_event_id`) must be computed ONCE, from the same expressions, for the legacy and
    per-table rows alike. Two projections would be two chances for the identities to drift,
    and the identities are the MERGE key.
    """
    source = entry.get("source") or {}
    payload = entry.get("payload") or {}
    pk = tuple((entry.get("identity") or {}).get("primary_key") or ())
    typed = payload.get("mode") == "typed"
    columns = payload.get("columns") or []

    def enrich(decoded, global_id):
        cols = [
            F.lit(source.get("database")).alias("source_database"),
            # The writer schema that decoded this row -- "schema version". The decode has
            # selected per globalId since finding E2 and then thrown the id away, so a
            # mis-decoded row could not be traced back to the schema that produced it.
            F.lit(global_id).cast("bigint").alias("schema_global_id"),
            _pk_hash(decoded, pk).alias("dv_pk_hash"),
            F.lit(run_id).alias("ingest_run_id"),
            F.lit(job).alias("ingest_job"),
        ]
        if typed:
            cols += [_typed_payload(decoded, "after", columns).alias("payload_after_typed"),
                     _typed_payload(decoded, "before", columns).alias("payload_before_typed")]
        return cols

    return enrich


def shape_for_target(rows, entry: dict):
    """Give the payload columns the names the provisioned table uses.

    In `typed` mode the struct takes the `payload_after` name and the JSON moves to
    `payload_after_json`. Both are kept: a writer version can carry a field the registry has
    not declared yet, and a typed-only projection would drop it silently -- data loss
    dressed as a schema improvement (rowspec.py).
    """
    if (entry.get("payload") or {}).get("mode") != "typed":
        return rows
    for base in ("payload_after", "payload_before"):
        if f"{base}_typed" not in rows.columns:
            continue
        rows = (rows.withColumnRenamed(base, f"{base}_json")
                    .withColumnRenamed(f"{base}_typed", base))
    return rows


def target_exists(spark, identifier: str) -> bool:
    try:
        spark.table(identifier)
        return True
    except Exception:                                              # noqa: BLE001
        return False


def require_target(spark, identifier: str, table_id: str) -> None:
    """Section F, enforced at the only place it can be: the write.

    A CREATE here would mean an unregistered or un-provisioned table springs into existence
    the moment an event arrives -- with no owner, no classification, no retention, no DQ
    rule and no partition spec, all of which the provisioner takes from the registry.
    """
    if not target_exists(spark, identifier):
        raise RuntimeError(
            f"CDC_PER_TABLE_TARGET_MISSING {identifier} (for {table_id}). This job does "
            f"NOT create CDC tables: they are provisioned from Git config first. Run "
            f"`python3 -m cdc.provision --table {table_id}` to see the DDL and "
            f"`spark-submit spark/ops/provision_cdc_tables.py --execute` to apply it, "
            f"then rerun this window.")


def _align_to_target(spark, rows, identifier: str, entry: dict):
    """Name-based alignment, additive only.

    Iceberg evolves by NAME, so a column the target lacks is added and a column the target
    has but the producer does not emit is NULL-filled. A type change or a drop is never
    attempted: `schema_evolution: additive_only` is the registry default, a drop is
    unrecoverable, and a type change reinterprets every existing row.
    """
    policy = entry.get("schema_evolution", "additive_only")
    target = {f.name: f for f in spark.table(identifier).schema.fields}
    for f in rows.schema.fields:
        if f.name in target:
            continue
        if policy == "frozen":
            raise RuntimeError(
                f"CDC_PER_TABLE_SCHEMA_FROZEN {identifier}: the producer emits {f.name!r} "
                f"which the table does not have, and schema_evolution is `frozen`. "
                f"Re-provision deliberately or fix the producer.")
        spark.sql(f"ALTER TABLE {identifier} ADD COLUMN "
                  f"{f.name} {f.dataType.simpleString()}")
        print(f"CDC_PER_TABLE_SCHEMA_EVOLVED {identifier} added={f.name}", flush=True)
    target = {f.name: f for f in spark.table(identifier).schema.fields}
    for name, f in target.items():
        if name not in rows.columns:
            rows = rows.withColumn(name, F.lit(None).cast(f.dataType))
    return rows.select(*target.keys())


def _view_name(identifier: str, suffix: str) -> str:
    """A temp view per TARGET. One shared name would make two targets in the same batch
    race each other -- the second registration wins and the first MERGE reads the wrong
    source rows, which no error would report."""
    safe = identifier.replace(".", "_").replace("-", "_")
    return f"cdc_src_{suffix}_{safe}"


def merge_per_table(spark, rows, identifier: str, entry: dict) -> int:
    """Idempotent append into ONE per-table FULL_CDC target.

    WHEN NOT MATCHED only, on `dv_event_id`: the layer is append-only (CLAUDE.md 5.2/5.3),
    so a matched row is the same delivered record and must not be rewritten -- but a RERUN
    of the same window must not double the history either, which is why this is a MERGE and
    not an append.
    """
    spark = rows.sparkSession                  # foreachBatch hands over a CLONED session
    rows = shape_for_target(rows, entry)
    rows = rows.withColumn("event_id", F.col("dv_event_id"))
    n = rows.count()
    aligned = _align_to_target(spark, rows, identifier, entry)
    view = _view_name(identifier, "pt")
    aligned.createOrReplaceTempView(view)
    spark.sql(f"""MERGE INTO {identifier} t USING {view} s
                  ON t.dv_event_id = s.dv_event_id
                  WHEN NOT MATCHED THEN INSERT *""")
    spark.catalog.dropTempView(view)
    return n


def index_rows(rows, entry: dict, identifier: str):
    """Coordinates and pointers only -- never a payload (ADR-062 section G)."""
    from cdc.rowspec import EVENT_INDEX_COLUMNS, column_names

    projected = (rows.withColumn("table_id", F.lit(entry["table_id"]))
                     .withColumn("target_table", F.lit(identifier)))
    wanted = [c for c in column_names(EVENT_INDEX_COLUMNS) if c in projected.columns]
    return projected.select(*wanted)


def merge_event_index(spark, rows, index_table: str, entry: dict,
                      identifier: str) -> int:
    """The index is idempotent on the same key as the tables it points at."""
    spark = rows.sparkSession
    require_target(spark, index_table, "cdc_event_index")
    idx = index_rows(rows, entry, identifier)
    aligned = _align_to_target(spark, idx, index_table, entry)
    view = _view_name(index_table, "idx")
    aligned.createOrReplaceTempView(view)
    spark.sql(f"""MERGE INTO {index_table} t USING {view} s
                  ON t.dv_event_id = s.dv_event_id
                  WHEN NOT MATCHED THEN INSERT *""")
    spark.catalog.dropTempView(view)
    return aligned.count()


def write_routed(spark, rows, route, *, event_index_table: str | None = None) -> dict:
    """Write ONE topic's decoded rows to the target its route names.

    Returns a small dict rather than printing only, so a caller can assert on it -- a run
    whose evidence is stdout is a run nobody can gate on.
    """
    entry = route.entry
    identifier = route.target("FULL_CDC")
    require_target(spark, identifier, route.table_id)
    n = merge_per_table(spark, rows, identifier, entry)
    out = {"table_id": route.table_id, "target": identifier, "rows": n, "indexed": 0}
    if event_index_table:
        out["indexed"] = merge_event_index(spark, rows, event_index_table, entry,
                                           identifier)
    print(f"CDC_PER_TABLE_WROTE {route.table_id} -> {identifier} rows={n} "
          f"indexed={out['indexed']}", flush=True)
    return out
