"""Logical layer -> physical Glue database, for the CDC side.

ADR-033 fixed a rule this module obeys rather than re-litigates: `reporting/layers.yaml` is
the ONLY file in which a physical database name appears. Three of those databases hold data
applied 2026-08-15, so the names are not derivable from anything -- they must be READ.

Why this reads the YAML instead of importing `spark/reporting/source_resolver.py`, which
already does exactly this: that module does `from models import ...`, a FLAT name that
resolves against whatever is first on `sys.path`. Importing it from here is the shadowing
defect `cdc/__init__.py` documents, one level down. Reading the same file is a two-key
lookup with no import graph attached.

Nothing here calls AWS. The catalog NAME (`glue_catalog`) is a Spark session binding, not a
resource, and the databases are created by Terraform (`modules/glue_catalog`) -- this module
only says which one a layer lands in.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from .models import ConfigError

#: Iceberg catalog the Spark sessions bind. `spark/jobs/full_cdc/job.py` configures
#: `spark.sql.catalog.glue_catalog`; `reporting/layers.yaml` declares the same name at its
#: top level, and a mismatch would resolve to a table that does not exist.
DEFAULT_CATALOG = "glue_catalog"

#: The layers this registry provisions into. OPS carries the optional global event index
#: (section G) -- coordinates only, never payloads.
CDC_LAYERS = ("FULL_CDC", "REALTIME", "EOD", "OPS")

#: Repo-relative, so a caller in scripts/, spark/ or cdc/ resolves the same file.
DEFAULT_LAYERS_FILE = Path(__file__).resolve().parents[1] / "reporting" / "layers.yaml"


class Catalog:
    """Resolved physical names. Immutable in practice; built once per compile."""

    def __init__(self, catalog: str, databases: dict[str, str]):
        self.catalog = catalog
        self.databases = dict(databases)

    def database(self, layer: str) -> str:
        try:
            return self.databases[layer]
        except KeyError:
            raise ConfigError(
                f"layer {layer!r} has no database binding. Known layers: "
                f"{', '.join(sorted(self.databases))}. Layer bindings live in "
                f"reporting/layers.yaml (ADR-033) and are never spelled out here."
            ) from None

    def qualify(self, layer: str, table: str) -> str:
        """`catalog.database.table` -- the three-part name Spark and Athena both accept."""
        return f"{self.catalog}.{self.database(layer)}.{table}"

    def payload(self) -> dict:
        """The form embedded in the compiled plan, so a runtime consumer needs the plan
        and nothing else. A job that re-read layers.yaml would be reading a file that could
        have moved on since the plan was compiled and verified."""
        return {"catalog": self.catalog,
                "databases": {k: self.databases[k] for k in sorted(self.databases)}}

    @classmethod
    def from_payload(cls, payload: dict) -> "Catalog":
        if not isinstance(payload, dict) or not payload.get("databases"):
            raise ConfigError("catalog payload: expected {'catalog': ..., 'databases': {...}}")
        return cls(payload.get("catalog") or DEFAULT_CATALOG, payload["databases"])

    @classmethod
    def from_layers_file(cls, path: Path | None = None,
                         layers: tuple[str, ...] = CDC_LAYERS) -> "Catalog":
        path = path or DEFAULT_LAYERS_FILE
        if not path.exists():
            raise ConfigError(
                f"{path}: layer bindings not found. ADR-033 puts every physical database "
                f"name in this one file; without it the target of a provision is a guess.")
        doc = yaml.safe_load(path.read_text()) or {}
        raw = doc.get("layers") or {}
        databases: dict[str, str] = {}
        for layer in layers:
            binding = raw.get(layer) or {}
            db = binding.get("database")
            if not db:
                raise ConfigError(
                    f"{path}: layer {layer} has no `database`. The CDC registry provisions "
                    f"into it, so an unbound layer is a provision with no target.")
            databases[layer] = db
        return cls(doc.get("catalog") or DEFAULT_CATALOG, databases)
