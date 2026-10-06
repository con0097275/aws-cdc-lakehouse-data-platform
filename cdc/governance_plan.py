"""Compile the governance inventory — deterministic, offline, from the files the platform obeys.

DRP1. This is the module that makes ADR-087 decision 1 real: the asset list is DERIVED, and
the only thing anyone hand-writes is metadata ABOUT an asset that already exists.

DERIVATION SOURCES, AND WHY EACH ONE
-------------------------------------
    cdc/registry/sources.yaml            the 10 captured tables -> 5 identities each
    reporting/curated/entities.yaml      the conformed CURATED entities (ADR-080)
    spark/dimensions/dim_builder.py      DIMENSIONS, PARSED not imported
    spark/jobs/curated/curated_build.py  dim_date / dim_time / dim_currency
    dbt/target/manifest.json             CURATED sources and MART models
    governance/registry/derived_assets.yaml   metadata overlay -- may not invent an asset

`dim_builder.py` is parsed with `ast` rather than imported for two reasons. It uses bare
module imports (`from scd2 import ...`) that only resolve with `spark/dimensions` on
sys.path, and `cdc/` importing from `spark/` would invert the dependency direction the
whole package was restructured to protect (`cdc/__init__.py`). Parsing is the same
technique `spark/tests/test_dbt_contract.py` already uses on the dbt macro: read the
declaration, do not execute it.

WHAT THE OVERLAY MAY AND MAY NOT DO
------------------------------------
It may add metadata to a derived asset. It may NOT name an asset the platform does not
produce -- that is a compile error, and it is the single rule that keeps this file from
becoming the one DRP0 measured at 1-of-12. It also may not restate a fact that is already
derived: `dim_customer`'s PII columns come from `DimensionSpec.pii_columns`, and an overlay
that contradicts them is refused rather than merged.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from .assets import AssetId, AssetKind, assets_for_table
from .config_loader import load_config
from .governance import (GovernanceFinding, GovernanceInventory, GovernedAsset,
                         PiiCategory, PiiField, Vocabulary, resolve, validate_asset)
from .models import ConfigError

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PATHS = {
    "registry": ROOT / "cdc" / "registry" / "sources.yaml",
    "vocabulary": ROOT / "governance" / "registry" / "domains.yaml",
    "overlay": ROOT / "governance" / "registry" / "derived_assets.yaml",
    "entities": ROOT / "reporting" / "curated" / "entities.yaml",
    "dim_builder": ROOT / "spark" / "dimensions" / "dim_builder.py",
    "curated_build": ROOT / "spark" / "jobs" / "curated" / "curated_build.py",
    "manifest": ROOT / "dbt" / "target" / "manifest.json",
}

#: Dimensions `curated_build.py` generates rather than deriving from a source system. Read
#: from that file rather than listed here -- see `_generated_dimensions`.
GENERATED_DIM_PREFIX = "dim_"

#: Which certification policies mean "this asset may reach CERTIFIED", and therefore must
#: satisfy the certified-requires-DQ-and-contract rule.
CERTIFYING_POLICIES = ("certified_eod",)


# --------------------------------------------------------------------------- #
# Derivation
# --------------------------------------------------------------------------- #

def _dimension_specs(path: Path) -> dict:
    """`{name: {"pii_columns": (...)}}` parsed out of `DIMENSIONS`, without importing it."""
    if not path.exists():
        return {}
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(getattr(t, "id", "") == "DIMENSIONS" for t in node.targets):
            continue
        if not isinstance(node.value, ast.Dict):
            raise ConfigError(f"{path}: DIMENSIONS is not a dict literal; cannot derive it "
                              f"without executing the module")
        out = {}
        for key, value in zip(node.value.keys, node.value.values):
            name = key.value
            pii: tuple = ()
            if isinstance(value, ast.Call):
                for kw in value.keywords:
                    if kw.arg == "pii_columns" and isinstance(kw.value, (ast.Tuple, ast.List)):
                        pii = tuple(e.value for e in kw.value.elts)
            out[name] = {"pii_columns": pii}
        return out
    return {}


def _generated_dimensions(path: Path) -> tuple[str, ...]:
    """Dimensions written by `curated_build.py` directly, found by reading its write targets.

    Derived rather than listed so that adding `dim_something` to that job puts it in the
    inventory automatically -- an ungoverned table is the defect, and a hand-kept list here
    would reproduce it one layer up.
    """
    if not path.exists():
        return ()
    text = path.read_text()
    return tuple(sorted(set(re.findall(r"\{curated\}\.(dim_[a-z_]+)", text))))


def _dbt_assets(path: Path) -> dict:
    """CURATED sources and MART models from the dbt manifest."""
    if not path.exists():
        return {"curated": (), "mart": ()}
    doc = json.loads(path.read_text())
    curated = tuple(sorted({s["name"] for s in (doc.get("sources") or {}).values()
                            if s.get("source_name") == "curated"}))
    mart = tuple(sorted({n["name"] for n in (doc.get("nodes") or {}).values()
                         if n.get("resource_type") == "model"}))
    return {"curated": curated, "mart": mart}


def _conformed_entities(path: Path) -> tuple[str, ...]:
    if not path.exists():
        return ()
    doc = yaml.safe_load(path.read_text()) or {}
    return tuple(sorted((doc.get("entities") or {}).keys()))


@dataclass(frozen=True)
class DerivedInventory:
    """The asset universe, before any governance is attached."""

    cdc: tuple
    curated: tuple
    mart: tuple

    @property
    def all_ids(self) -> tuple:
        return tuple(sorted({*(a for a, _, _ in self.cdc), *self.curated, *self.mart}))


def derive(paths: dict | None = None) -> DerivedInventory:
    p = {**DEFAULT_PATHS, **(paths or {})}
    config = load_config(p["registry"])

    cdc: list = []
    for table in config.tables:
        facts = {
            "table_id": table.table_id,
            "enabled": table.enabled,
            "eod_retention_days": table.eod.retention_days,
            "dq_freshness_sla_minutes": table.dq.freshness_sla_minutes,
            "realtime_enabled": table.realtime.enabled,
        }
        for asset in assets_for_table(table):
            cdc.append((asset, table, facts))

    dims = _dimension_specs(p["dim_builder"])
    generated = _generated_dimensions(p["curated_build"])
    dbt = _dbt_assets(p["manifest"])
    curated = tuple(sorted({
        *_conformed_entities(p["entities"]),
        *dims.keys(),
        *generated,
        *dbt["curated"],
    }))
    mart = dbt["mart"]
    return DerivedInventory(tuple(cdc),
                            tuple(AssetId(AssetKind.CURATED, n) for n in curated),
                            tuple(AssetId(AssetKind.MART, n) for n in mart))


# --------------------------------------------------------------------------- #
# Compile
# --------------------------------------------------------------------------- #

def _raw_registry_governance(path: Path) -> tuple[dict, dict, dict]:
    """(global, {source_id: block}, {table_id: block}) read from the registry's own YAML.

    Read directly rather than through `load_config` on purpose: `TableConfig` has its own
    inheritance semantics for the executable policies, and governance needs different ones
    (`tags` accumulate, `classification` is refused here). Two different merges over one
    file is fine; one merge pretending to serve both is how a tag silently overrides.
    """
    doc = yaml.safe_load(path.read_text()) or {}
    glob = dict((doc.get("defaults") or {}).get("governance") or {})
    per_source: dict = {}
    per_table: dict = {}
    for src in doc.get("sources") or ():
        sid = src["source_id"]
        per_source[sid] = dict((src.get("defaults") or {}).get("governance") or {})
        for t in src.get("tables") or ():
            tid = ".".join([sid, src["database"], src["schema"], t["table"]])
            per_table[tid] = dict(t.get("governance") or {})
    for where, block in [("defaults.governance", glob),
                         *[(f"sources[{k}].defaults.governance", v) for k, v in per_source.items()],
                         *[(f"tables[{k}].governance", v) for k, v in per_table.items()]]:
        if "classification" in block:
            raise ConfigError(
                f"{where}: `classification` may not appear in a governance block. It has one "
                f"home -- the registry's own `classification:` key, which the CDC compiler "
                f"already inherits into every layer. Two spellings of one classification is "
                f"exactly the drift DRP1 exists to end.")
    return glob, per_source, per_table


def compile_inventory(paths: dict | None = None) -> GovernanceInventory:
    """Deterministic. Same inputs -> byte-identical payload and `config_version`."""
    p = {**DEFAULT_PATHS, **(paths or {})}
    vocab = Vocabulary.load(p["vocabulary"])
    derived = derive(p)
    glob_gov, src_gov, tbl_gov = _raw_registry_governance(p["registry"])
    overlay_doc = yaml.safe_load(p["overlay"].read_text()) if p["overlay"].exists() else {}
    overlay_doc = overlay_doc or {}
    kind_defaults = overlay_doc.get("kind_defaults") or {}
    overlay = dict(overlay_doc.get("assets") or {})
    undeclared = dict(overlay_doc.get("undeclared_assets") or {})

    out: list[GovernedAsset] = []
    findings: list[GovernanceFinding] = []

    # ---- CDC-side assets ---------------------------------------------------
    for asset, table, facts in derived.cdc:
        asset_block = dict(tbl_gov.get(table.table_id) or {})
        # The registry's own fields are the asset level for governance too -- read, never
        # restated. `owner`/`domain` come from `TableConfig`, which has already inherited
        # them, and `classification` is refused inside a governance block for that reason.
        asset_block.setdefault("owner", table.owner)
        asset_block.setdefault("domain", table.domain)
        meta = resolve(vocabulary=vocab, global_defaults=glob_gov,
                       source_defaults=src_gov.get(table.source_id),
                       asset_override=asset_block, where=str(asset))
        meta = _with_classification(meta, table.classification)
        asset_facts = dict(facts)
        asset_facts["kind"] = asset.kind.value
        certifies = (asset.kind is AssetKind.EOD
                     and meta.certification_policy in CERTIFYING_POLICIES)
        # Retention and freshness are executable properties of the LAKE layers only. A Kafka
        # topic's retention is a broker setting and a source table's is the DBA's; checking a
        # governance band against a number this registry does not own would be inventing one.
        retention = facts["eod_retention_days"] if asset.kind is AssetKind.EOD else None
        freshness = (facts["dq_freshness_sla_minutes"]
                     if asset.kind in (AssetKind.FULL_CDC, AssetKind.REALTIME, AssetKind.EOD)
                     else None)
        findings.extend(validate_asset(asset, meta, retention_days=retention,
                                       executable_freshness_minutes=freshness,
                                       certifies=certifies))
        out.append(GovernedAsset(asset, meta, asset_facts))

    # ---- derived assets ----------------------------------------------------
    dims = _dimension_specs(p["dim_builder"])
    known = {str(a) for a in (*derived.curated, *derived.mart)}
    unknown = sorted(set(overlay) - known)
    if unknown:
        raise ConfigError(
            f"{p['overlay']}: overlay names asset(s) the platform does not produce: "
            f"{', '.join(unknown)}. An overlay may add metadata to a derived asset; it may "
            f"not invent one. This is the check that keeps this file from becoming "
            f"`governance/catalog/domains.yml`.")

    for asset in (*derived.curated, *derived.mart):
        key = str(asset)
        block = dict(overlay.get(key) or {})
        derived_pii = tuple(PiiField(c, PiiCategory.DIRECT_IDENTIFIER)
                            for c in (dims.get(asset.name, {}).get("pii_columns") or ()))
        if derived_pii and "pii_fields" in block:
            raise ConfigError(
                f"{key}: pii_fields is DERIVED from DimensionSpec.pii_columns in "
                f"spark/dimensions/dim_builder.py and must not be restated in the overlay. "
                f"Two lists of PII columns is the failure `governance/catalog/domains.yml` "
                f"warned about in its own header, and then demonstrated.")
        if derived_pii:
            block["pii_fields"] = [{"column": pf.column, "category": pf.category.value}
                                   for pf in derived_pii]
            block.setdefault("classification", "confidential")
        meta = resolve(vocabulary=vocab,
                       global_defaults=kind_defaults.get(asset.kind.value),
                       source_defaults=None, asset_override=block, where=key)
        facts = {"kind": asset.kind.value,
                 "registered": key in overlay,
                 "pii_derived_from": ("spark/dimensions/dim_builder.py" if derived_pii else "")}
        certifies = meta.certification_policy in CERTIFYING_POLICIES
        findings.extend(validate_asset(asset, meta, certifies=certifies))
        out.append(GovernedAsset(asset, meta, facts))

    # ---- assets that exist only in Python ----------------------------------
    for key, block in sorted(undeclared.items()):
        asset = AssetId.parse(key)
        block = dict(block)
        declared_in = block.pop("declared_in", "")
        reason = block.pop("reason", "")
        meta = resolve(vocabulary=vocab,
                       global_defaults=kind_defaults.get(asset.kind.value),
                       source_defaults=None, asset_override=block, where=key)
        findings.append(GovernanceFinding(
            key, "undeclared_asset",
            f"exists but is declared only in {declared_in or 'code'} ({reason or 'no reason given'}); "
            f"a table nobody can find from config cannot be onboarded, decommissioned or "
            f"reasoned about"))
        findings.extend(validate_asset(asset, meta,
                                       certifies=meta.certification_policy in CERTIFYING_POLICIES))
        out.append(GovernedAsset(asset, meta,
                                 {"kind": asset.kind.value, "registered": False,
                                  "declared_in": declared_in}))

    ordered = tuple(sorted(out, key=lambda g: str(g.asset)))
    ids = [str(g.asset) for g in ordered]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise ConfigError(f"duplicate asset in the compiled inventory: {', '.join(dupes)}")
    return GovernanceInventory(ordered, tuple(sorted(
        findings, key=lambda f: (f.asset, f.rule))))


def _with_classification(meta, classification):
    """Attach the registry's classification without letting a governance block supply one."""
    from dataclasses import replace
    prov = dict(meta.provenance)
    prov["classification"] = "registry"
    return replace(meta, classification=classification, provenance=prov)
