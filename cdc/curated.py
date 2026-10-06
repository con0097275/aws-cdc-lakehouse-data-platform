"""The CURATED layer's contract: conformed entities out of the EOD layer.  ADR-080.

Pure Python on purpose. Everything here is config validation and SQL-expression building,
so the rules that decide what a column MEANS are testable without a Spark session -- and a
wrong rule here is not a crash, it is a plausible number. An Oracle DATE read as
microseconds does not fail; it silently reports every customer as born in 1970.

The Spark side lives in `spark/jobs/curated/curated_build.py`, which owns reading, writing
and validation ordering, and calls this module for every decision.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

#: Debezium encodings this layer knows how to decode, and the Spark expression that turns
#: the raw JSON scalar into the typed column. `{v}` is the extracted string.
#:
#: The two temporal encodings are the reason this table exists rather than a `CAST`.
#: Debezium maps an Oracle `DATE` to `io.debezium.time.Timestamp` (epoch MILLIS) and a
#: `TIMESTAMP(6)` to `io.debezium.time.MicroTimestamp` (epoch MICROS), and both arrive as
#: bare integers in the same JSON object -- `{"DOB":23673600000,"UPDATED_AT":1767225600000000}`.
#: Nothing in the value says which is which. Only the source column type does, so it is
#: declared per column and this table is the only place the factor 1000 appears.
DECODERS: dict[str, str] = {
    "bigint": "CAST({v} AS BIGINT)",
    "string": "CAST({v} AS STRING)",
    "date_millis": "CAST(TIMESTAMP_MILLIS(CAST({v} AS BIGINT)) AS DATE)",
    "timestamp_micros": "TIMESTAMP_MICROS(CAST({v} AS BIGINT))",
}

#: `decimal(p,s)` is parameterised, so it is matched rather than looked up.
_DECIMAL = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")

#: Every conformed entity carries these, taken from the EOD row rather than recomputed.
#: `business_date` is what `snapshot_date` is called in the config-driven layer; the
#: conformed table exposes BOTH so the Session-09 Kimball code (`snapshot_date`) and the
#: current platform (`business_date`) read the same table without either being rewritten
#: to the other's vocabulary.
LINEAGE_COLUMNS = ("business_date", "snapshot_date", "source_commit_ts",
                   "dv_pk_hash", "is_deleted")


class CuratedConfigError(ValueError):
    """Fatal. A conformed entity that is wrong produces a mart that is plausible."""


@dataclass(frozen=True)
class Column:
    json_key: str
    name: str
    type: str

    def decode(self, payload_column: str = "payload_after") -> str:
        """The Spark SQL expression producing this column from the payload JSON."""
        raw = f"get_json_object({payload_column}, '$.{self.json_key}')"
        decimal = _DECIMAL.match(self.type)
        if decimal:
            return f"CAST({raw} AS {self.type})"
        return DECODERS[self.type].format(v=raw)


@dataclass(frozen=True)
class Entity:
    """One conformed entity: an EOD table, projected and typed."""

    name: str
    table_id: str
    layer: str
    natural_key: tuple[str, ...]
    columns: tuple[Column, ...]
    dimension: str | None = None
    derived: dict[str, dict] = field(default_factory=dict)

    @property
    def output_columns(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns) + tuple(self.derived)

    def select_exprs(self, payload_column: str = "payload_after") -> list[str]:
        """`SELECT` expressions for the projection, EXCLUDING derived columns.

        Derived columns are applied afterwards because they read the DECODED value -- an
        age band is computed from a `date`, not from the epoch integer it arrived as.
        """
        return [f"{c.decode(payload_column)} AS {c.name}" for c in self.columns]


@dataclass(frozen=True)
class CuratedConfig:
    entities: dict[str, Entity]
    base_currency: str
    currency_rates: dict[str, str]
    age_bands: tuple[tuple[str, int | None], ...]

    def entity(self, name: str) -> Entity:
        try:
            return self.entities[name]
        except KeyError:
            raise CuratedConfigError(
                f"unknown conformed entity {name!r}; configured: "
                f"{sorted(self.entities)}") from None

    def dimension_entities(self) -> dict[str, Entity]:
        """`dimension name -> entity`, for the entities that feed a dimension."""
        return {e.dimension: e for e in self.entities.values() if e.dimension}


def value_map_expr(column: str, mapping: dict) -> str:
    """A CASE expression translating `column` through a declared lookup.

    An unmapped value becomes NULL, never a default. The fact then resolves to UNKNOWN_SK,
    which is visible in a BI filter list and countable in a reconciliation; silently folding
    an unrecognised device into the busiest channel would be a wrong number that nothing
    downstream could detect.
    """
    if not mapping:
        raise CuratedConfigError("a `map` rule needs a non-empty `values` mapping")
    whens = " ".join(
        f"WHEN {column} = '{k}' THEN '{v}'" for k, v in sorted(mapping.items()))
    return f"CASE {whens} ELSE NULL END"


def age_band_expr(column: str, as_of: str,
                  bands: tuple[tuple[str, int | None], ...]) -> str:
    """A CASE expression bucketing `column` (a date of birth) as of `as_of`.

    Age is whole years at `as_of`, via `floor(months_between/12)` rather than a year
    subtraction: `year(as_of) - year(dob)` reports a customer whose birthday has not yet
    happened this year as a year older, which moves people across a band boundary for up to
    364 days a year.
    """
    if not bands:
        raise CuratedConfigError("age_bands must not be empty")
    age = f"FLOOR(MONTHS_BETWEEN(CAST({as_of} AS DATE), {column}) / 12)"
    whens = []
    for label, max_age in bands:
        if max_age is None:
            continue
        whens.append(f"WHEN {age} <= {int(max_age)} THEN '{label}'")
    fallback = next((label for label, max_age in bands if max_age is None), None)
    if fallback is None:
        raise CuratedConfigError(
            "age_bands needs exactly one open-ended band (max_age: null); without it a "
            "customer older than the last edge silently gets NULL")
    return (f"CASE WHEN {column} IS NULL THEN NULL "
            + " ".join(whens)
            + f" ELSE '{fallback}' END")


def _column(entity: str, json_key: str, spec: Any) -> Column:
    if not isinstance(spec, dict) or "name" not in spec or "type" not in spec:
        raise CuratedConfigError(
            f"{entity}.{json_key}: expected a mapping with `name` and `type`, got {spec!r}")
    ctype = str(spec["type"])
    if ctype not in DECODERS and not _DECIMAL.match(ctype):
        raise CuratedConfigError(
            f"{entity}.{json_key}: unknown type {ctype!r}; known: "
            f"{sorted(DECODERS)} or decimal(p,s)")
    return Column(json_key=json_key, name=str(spec["name"]), type=ctype)


def parse(raw: dict) -> CuratedConfig:
    """Validate the config and build the typed view of it.

    Refuses rather than defaults, everywhere. A conformed entity is the boundary between
    the CDC platform's vocabulary and the business's; a default applied here is a business
    decision made by a shrug.
    """
    if not isinstance(raw, dict):
        raise CuratedConfigError("entities.yaml must be a mapping")

    entities: dict[str, Entity] = {}
    for name, spec in (raw.get("entities") or {}).items():
        if not spec.get("table_id"):
            raise CuratedConfigError(f"{name}: table_id is required")
        columns_raw = spec.get("columns") or {}
        if not columns_raw:
            raise CuratedConfigError(f"{name}: columns must not be empty")
        columns = tuple(_column(name, k, v) for k, v in columns_raw.items())

        seen: set[str] = set()
        for column in columns:
            if column.name in seen:
                raise CuratedConfigError(
                    f"{name}: two source keys both project to {column.name!r}")
            seen.add(column.name)

        natural_key = tuple(spec.get("natural_key") or ())
        if not natural_key:
            raise CuratedConfigError(f"{name}: natural_key is required")
        missing = [k for k in natural_key if k not in seen]
        if missing:
            raise CuratedConfigError(
                f"{name}: natural_key {missing} is not in the projection. A dimension keyed "
                f"on a column the projection does not produce fails at write time, after "
                f"the read has been paid for")

        derived = spec.get("derived") or {}
        for out, rule in derived.items():
            if out in seen:
                raise CuratedConfigError(
                    f"{name}: derived column {out!r} collides with a projected column")
            source = (rule or {}).get("from")
            if source not in seen:
                raise CuratedConfigError(
                    f"{name}: derived {out!r} reads {source!r}, which the projection does "
                    f"not produce")
            kind = (rule or {}).get("rule")
            if kind not in ("age_band", "map"):
                raise CuratedConfigError(
                    f"{name}: derived {out!r} has unknown rule {kind!r}")
            if kind == "map" and not (rule or {}).get("values"):
                raise CuratedConfigError(
                    f"{name}: derived {out!r} is a `map` rule with no `values`")

        entities[name] = Entity(
            name=name, table_id=str(spec["table_id"]),
            layer=str(spec.get("layer", "EOD")),
            natural_key=natural_key, columns=columns,
            dimension=spec.get("dimension"), derived=derived)

    if not entities:
        raise CuratedConfigError("entities must not be empty")

    dimensions = [e.dimension for e in entities.values() if e.dimension]
    duplicate = {d for d in dimensions if dimensions.count(d) > 1}
    if duplicate:
        raise CuratedConfigError(
            f"two entities feed the same dimension: {sorted(duplicate)}")

    currency = raw.get("currency") or {}
    base = currency.get("base")
    rates = {str(k): str(v) for k, v in (currency.get("rates") or {}).items()}
    if not base:
        raise CuratedConfigError("currency.base is required")
    if base not in rates:
        raise CuratedConfigError(
            f"currency.base {base!r} has no declared rate. The base currency's rate is "
            f"1.0 by definition, but it is declared rather than assumed so that the table "
            f"a reader sees is complete")

    bands_raw = raw.get("age_bands") or []
    bands = tuple((str(b["label"]), b.get("max_age")) for b in bands_raw)
    open_ended = [b for b in bands if b[1] is None]
    if len(open_ended) != 1:
        raise CuratedConfigError(
            f"age_bands needs exactly one open-ended band, found {len(open_ended)}")

    return CuratedConfig(entities=entities, base_currency=str(base),
                         currency_rates=rates, age_bands=bands)


def load(path: str) -> CuratedConfig:
    import yaml

    with open(path, encoding="utf-8") as handle:
        return parse(yaml.safe_load(handle))
