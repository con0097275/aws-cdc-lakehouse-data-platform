"""Read FULL_CDC for ONE source table, following the per-table cutover. ADR-072.

    full_cdc_for(spark, "oracle.coredb.corebank.customer")  ->  DataFrame

WHY THIS EXISTS
---------------
`rt_stream_app.py` and `rt_autocorrect.py` both did this, in four places:

    spark.table("glue_catalog.kafka_dev_lab_dev_full_cdc.cdc_events")
         .filter((F.col("source_system") == "oracle")
                 & (F.col("source_table") == "CUSTOMER"))

Three defects, and every one of them fails QUIETLY.

1. THE MONOLITH IS NAMED DIRECTLY. Once a table's cutover mode is PER_TABLE the ingest stops
   writing it to `cdc_events`, so this read returns the rows that were there before cutover
   and nothing since -- a dimension that silently stops updating while every job reports
   SUCCESS. The whole point of `CutoverResolver` is that a consumer asks for a table_id and
   is told where that table's data currently lives.

2. `source_table` IS COMPARED WITHOUT `upper()`. `source_table` is written as the last
   segment of the TOPIC, so it carries the engine's own spelling: Oracle folds unquoted
   identifiers to UPPERCASE, SQL Server does not. The exact-case literal happens to match
   for Oracle CUSTOMER and would match nothing for any SQL Server table. This exact defect
   was measured live once already -- the legacy benchmark read returned zero rows, with no
   error, and scored it as "0 bytes scanned, very fast" (ADR-067).

3. THE ORDERING IS ORACLE-ONLY AND VIOLATES CLAUDE.md 5.4:

       ORDER BY CAST(position_primary AS DECIMAL(38,0)) DESC, kafka_offset DESC

   A SQL Server hex LSN casts to NULL, so every row ties and the ranking collapses onto
   `kafka_offset` -- which is monotonic only WITHIN one partition, so comparing offsets
   across partitions is meaningless. `cdc/eod.py::order_by_clause` is the corrected form and
   is reused here rather than re-derived, because two spellings of one ordering contract is
   how they drift.

This module is the single place that knows how to read FULL_CDC for a table. It returns a
DataFrame that is already correctly filtered, so a caller cannot forget the predicate.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys

_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:                                    # pragma: no cover
    sys.path.insert(0, str(_ROOT))

from cdc.cutover import CutoverResolver, load_state               # noqa: E402
from cdc.eod import order_by_clause                               # noqa: E402

#: Where the compiled plan and the cutover state live for a running job. The SAME artifacts
#: the engines read -- a consumer resolving from its own copy would disagree the moment one
#: was redeployed, and the disagreement looks like a table that sometimes updates.
PLAN_PATH = os.environ.get("CDC_TABLE_PLAN", "/tmp/framework/table-plan.json")
CUTOVER_PATH = os.environ.get("CDC_CUTOVER_STATE", "")


def _resolver(plan_path: str | None = None, state_path: str | None = None):
    path = pathlib.Path(plan_path or PLAN_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"no compiled table plan at {path}. A consumer cannot resolve where a table's "
            f"FULL_CDC currently lives without one -- publish it with "
            f"`bash scripts/cdc-deploy-code.sh` or set CDC_TABLE_PLAN.")
    plan = json.loads(path.read_text())
    sp = state_path or CUTOVER_PATH
    # `load_state` returns an all-LEGACY state when the file is absent, which is the SAFE
    # default: an unknown mode must read the monolith (which holds everything) rather than a
    # per-table target that may not have been cut over yet.
    state = load_state(pathlib.Path(sp) if sp else None)
    return CutoverResolver(plan, state)


def resolve_full_cdc(table_id: str, *, plan_path: str | None = None,
                     state_path: str | None = None):
    """Where this table's FULL_CDC lives right now, and the predicate it requires."""
    return _resolver(plan_path, state_path).resolve(table_id, "FULL_CDC")


def full_cdc_for(spark, table_id: str, *, plan_path: str | None = None,
                 state_path: str | None = None):
    """The FULL_CDC rows for ONE source table, correctly filtered for the current mode.

    In LEGACY mode the monolith holds every table, so the resolver returns a MANDATORY
    predicate and it is applied here -- a caller cannot forget it and read eight tables'
    events as one. In PER_TABLE mode the table IS the filter and no predicate is needed.
    """
    resolved = resolve_full_cdc(table_id, plan_path=plan_path, state_path=state_path)
    df = spark.table(resolved.identifier)
    if resolved.required_predicate:
        df = df.where(resolved.required_predicate)
    return df


def latest_state_window(engine: str, partition_by: str) -> str:
    """`ROW_NUMBER()` over the SOURCE-NATIVE ordering for this engine.

    Reuses `cdc.eod.order_by_clause`, so the streaming dimension and the certified EOD close
    rank identical events identically. They did not before: EOD was corrected in ADR-065 and
    these consumers kept the Oracle-only cast, which meant a dimension could disagree with
    the snapshot about which update was latest.
    """
    return f"ROW_NUMBER() OVER (PARTITION BY {partition_by} " \
           f"ORDER BY {order_by_clause(engine)})"
