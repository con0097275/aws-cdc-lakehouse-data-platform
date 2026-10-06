"""Generate the DQ rule set `dq_engine.run_suite` consumes — from config, per layer.

DRP5 / blocker B4. `governance/dq/rules.yml` is hand-written and the DRP0 audit measured it
at **1 of 7 datasets live**: it names `stream.oracle_corebank_customer` where the platform
has `stream.rt_oracle_coredb_corebank_customer`. Every run against it returns
`NOT_EVALUATED`, which correctly blocks — the engine is right and its input is three naming
generations stale.

This module replaces the FILE, not the engine. `run_suite` is untouched; only what it is
handed changes.

THE PART A HAND-WRITTEN FILE GOT WRONG, AND A GENERATED ONE CANNOT
-------------------------------------------------------------------
`cdc/registry/sources.yaml` declares `dq.not_null: [ACCOUNT_ID, CUSTOMER_ID]`. Those are
**source** column names, and at FULL_CDC and EOD they do not exist as columns at all -- they
live inside the `payload_after` JSON. A completeness check on `ACCOUNT_ID` against
`cdc_oracle_coredb_corebank_account` can only ever return NOT_EVALUATED.

They become real columns one layer later, at CURATED, under the names
`reporting/curated/entities.yaml` maps them to (`ACCOUNT_ID` -> `account_id`). So the
not-null rules are emitted **there**, and the CDC layers get the checks their own schema can
actually answer: `dv_event_id` uniqueness, `dv_pk_hash` completeness, a valid `op`.

Getting that wrong is invisible: the suite runs, every check says NOT_EVALUATED, and the
publish is blocked for a reason that has nothing to do with the data.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from .assets import AssetId, AssetKind, assets_for_table, physical_table
from .catalog import Catalog
from .config_loader import load_config
from .urns import URN_LAYERS

ROOT = Path(__file__).resolve().parents[1]

#: Columns every CDC-shaped table carries, whatever the source. Checked because the platform
#: writes them -- unlike the source columns, which are payload until CURATED.
FULL_CDC_REQUIRED = ("dv_pk_hash", "event_date", "source_commit_ts", "dv_src_event_id")
EOD_REQUIRED = ("dv_pk_hash", "source_commit_ts")

#: `op` is a closed set. An unknown value is applied by no layer and dropped by all.
VALID_OPS = "op IN ('c','u','d','r')"


def _dataset(catalog: Catalog, asset: AssetId) -> str:
    """`<database>.<table>` -- what `run_suite` appends to the catalog name."""
    return f"{catalog.database(asset.layer)}.{physical_table(asset)}"


def _entities(path: Path | None = None) -> dict:
    path = path or (ROOT / "reporting" / "curated" / "entities.yaml")
    if not path.exists():
        return {}
    return (yaml.safe_load(path.read_text()) or {}).get("entities") or {}


def build_rules(*, registry: Path | None = None, catalog: Catalog | None = None,
                entities: Path | None = None) -> dict:
    """The `{"datasets": [...]}` shape `dq_engine.run_suite` expects.

    Every dataset name resolves to a table that exists, because it is built from the same
    registry the platform provisions from.
    """
    catalog = catalog or Catalog.from_layers_file(layers=URN_LAYERS)
    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    ents = _entities(entities)
    by_table_id = {spec.get("table_id"): (name, spec)
                   for name, spec in ents.items() if isinstance(spec, dict)}

    datasets: list = []

    for table in cfg.tables:
        _, _, full_cdc, realtime, eod = assets_for_table(table)
        sla = table.dq.freshness_sla_minutes

        # ---- FULL_CDC ---------------------------------------------------
        checks = [
            # dv_event_id is the idempotency key. A duplicate means replay is no longer
            # safe, which is a correctness failure, not a quality warning.
            {"type": "uniqueness", "columns": ["dv_event_id"], "severity": "ERROR"},
            {"type": "validity", "name": "operation_is_known",
             "expression": VALID_OPS, "severity": "ERROR"},
        ]
        for col in FULL_CDC_REQUIRED:
            checks.append({"type": "completeness", "column": col,
                           "max_null_pct": 0.0, "severity": "ERROR"})
        checks.append({"type": "freshness", "column": "ingested_at",
                       "sla_minutes": sla, "severity": "ERROR"})
        datasets.append({
            "name": _dataset(catalog, full_cdc), "layer": "FULL_CDC",
            "predicate": "event_date = DATE '${business_date}'",
            "checks": checks})

        # ---- EOD ----------------------------------------------------------
        datasets.append({
            "name": _dataset(catalog, eod), "layer": "EOD",
            "predicate": "business_date = DATE '${business_date}'",
            "checks": [
                # THE EOD invariant. Two active rows per key multiplies every downstream
                # join, in a direction nobody notices.
                {"type": "uniqueness", "columns": ["dv_pk_hash"], "severity": "ERROR"},
                *[{"type": "completeness", "column": c, "max_null_pct": 0.0,
                   "severity": "ERROR"} for c in EOD_REQUIRED],
                {"type": "freshness", "column": "snapshot_ts",
                 "sla_minutes": max(sla, 1440), "severity": "WARN"},
                # FULL_CDC and EOD must agree on the key set for the COB. EXACT: both are
                # derived from the same events, so a difference is a lost or duplicated key.
                {"type": "reconciliation", "name": "full_cdc_to_eod_keys",
                 "left": _dataset(catalog, full_cdc), "right": _dataset(catalog, eod),
                 "left_predicate": "event_date = DATE '${business_date}'",
                 "right_predicate": "business_date = DATE '${business_date}'",
                 "tolerance_pct": 0.0, "severity": "WARN"},
            ]})

        # ---- REALTIME, by SHAPE -------------------------------------------
        if table.realtime.enabled:
            shape = getattr(table.realtime, "shape", "") or ""
            if shape == "latest_state":
                rt_checks = [
                    # The contract of latest_state. Two rows means a consumer reads either.
                    {"type": "uniqueness", "columns": ["dv_pk_hash"], "severity": "ERROR"}]
            else:
                rt_checks = [
                    # Under `append`, a replayed range must be a no-op rather than a duplicate.
                    {"type": "uniqueness", "columns": ["dv_event_id"], "severity": "ERROR"}]
            rt_checks.append({"type": "completeness", "column": "dv_pk_hash",
                              "max_null_pct": 0.0, "severity": "ERROR"})
            datasets.append({"name": _dataset(catalog, realtime), "layer": "REALTIME",
                             "predicate": "1=1", "checks": rt_checks})

        # ---- CURATED: where the SOURCE columns finally exist ---------------
        hit = by_table_id.get(table.table_id)
        if not hit:
            continue
        entity_name, spec = hit
        mapping = {src: (t.get("name") if isinstance(t, dict) else str(t))
                   for src, t in (spec.get("columns") or {}).items()}
        not_null = [mapping[c] for c in table.dq.not_null if c in mapping]
        if not not_null:
            continue
        datasets.append({
            "name": _dataset(catalog, AssetId(AssetKind.CURATED, entity_name)),
            "layer": "CURATED",
            "predicate": "business_date = DATE '${business_date}'",
            "checks": [
                *[{"type": "completeness", "column": c, "max_null_pct": 0.0,
                   "severity": "ERROR"} for c in not_null],
                {"type": "uniqueness", "columns": ["dv_pk_hash"], "severity": "ERROR"},
            ]})

    return {"version": 1, "source": "generated from cdc/registry/sources.yaml",
            "datasets": datasets}


def unmapped_not_null(*, registry: Path | None = None, entities: Path | None = None) -> dict:
    """Registry `not_null` columns with no CURATED mapping — reported, not dropped.

    A declared check that quietly produces no rule is worse than a missing one: the registry
    says the column is guarded and nothing guards it.
    """
    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    ents = _entities(entities)
    by_table_id = {spec.get("table_id"): spec for spec in ents.values()
                   if isinstance(spec, dict)}
    out: dict = {}
    for table in cfg.tables:
        spec = by_table_id.get(table.table_id)
        mapped = set((spec or {}).get("columns") or {})
        missing = [c for c in table.dq.not_null if c not in mapped]
        if missing:
            out[table.table_id] = missing
    return out
