"""Canonical asset identity — one `AssetId` for every governed thing in the platform.

DRP1. The DRP0 audit measured what happens without this: three governance files naming
datasets in three different spellings, of which one in twelve existed. The cause was not
carelessness — it was that **nothing owned the identity**, so every file invented its own.

THE RULE
--------
An asset's identity is DERIVED from the registry the platform already obeys, never typed
into a governance file. For the five CDC-side kinds that means `cdc/naming.py`:
`table_id()` and `logical_name()` are called here rather than re-implemented, so the
SOURCE_TABLE, KAFKA_TOPIC, FULL_CDC, REALTIME and EOD identities of one table cannot
drift apart — they are five renderings of one call.

    oracle.coredb.corebank.account
        -> src:oracle.coredb.corebank.account
        -> topic:cdc.oracle.COREBANK.ACCOUNT
        -> full_cdc:oracle_coredb_corebank_account
        -> realtime:oracle_coredb_corebank_account
        -> eod:oracle_coredb_corebank_account

WHY A PREFIX AND NOT A BARE NAME
---------------------------------
`banking_customer` is a CURATED table and `dim_customer` is another; `customer` is a
source table, a FULL_CDC table, a REALTIME table and an EOD table. A bare name cannot say
which, and the DRP0 audit found exactly that ambiguity resolved differently in each file --
`mart.dim_customer` in the governance registry versus `curated.dim_customer` in the
catalog. The kind is part of the identity because the same word means five things.

WHAT THIS IS NOT
----------------
Not a DataHub URN. A URN is a projection of this for one tool (DRP4) and carries that
tool's platform/environment vocabulary. Binding the platform's own identity to a vendor's
shape would mean re-identifying every asset if the vendor changed -- which is the decision
ADR-087 exists to avoid.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .models import ConfigError, SourceEngine
from .naming import logical_name, table_id, topic_for


class AssetKind(str, Enum):
    """The nine governed kinds. A closed set: an unknown kind is a compile error, not a
    new category invented at runtime."""

    SOURCE_TABLE = "src"
    KAFKA_TOPIC = "topic"
    FULL_CDC = "full_cdc"
    REALTIME = "realtime"
    EOD = "eod"
    CURATED = "curated"
    MART = "mart"
    SERVING_VIEW = "serving"
    BI_DATASET = "bi_dataset"
    BI_REPORT = "bi_report"


#: Which kinds live in the lakehouse and therefore resolve to a physical Glue database
#: through `reporting/layers.yaml` (ADR-033). SOURCE_TABLE and KAFKA_TOPIC do not: they are
#: upstream of the lake. BI_* do not: they live in a BI tool.
LAYER_FOR: dict[AssetKind, str] = {
    AssetKind.FULL_CDC: "FULL_CDC",
    AssetKind.REALTIME: "REALTIME",
    AssetKind.EOD: "EOD",
    AssetKind.CURATED: "CURATED",
    AssetKind.MART: "MART",
    AssetKind.SERVING_VIEW: "SERVING",
}

#: `SERVING` has NO binding in `reporting/layers.yaml` today, although the Glue database
#: `kafka_dev_lab_dev_serving` exists and holds 0 tables. That is left as-is deliberately:
#: ADR-033 makes `layers.yaml` the only file in which a physical database name appears, so
#: binding it is a config decision, not something this module may assume. Until it is bound,
#: a SERVING_VIEW asset has an identity and `Catalog.database("SERVING")` refuses by name --
#: which is the honest outcome, and better than resolving to a database nobody chose.

#: The three layers that are renderings of ONE captured source table, in dependency order.
#: FULL_CDC is canonical; REALTIME and EOD are siblings derived from it, never a chain
#: (docs/TARGET_ARCHITECTURE.md §3).
CDC_LAYER_KINDS = (AssetKind.FULL_CDC, AssetKind.REALTIME, AssetKind.EOD)

#: Kinds a reporting flow may be asked to rebuild. BI assets are excluded on purpose: a
#: recovery plan can NAME a Power BI report as impacted, but it can never execute one --
#: there is no job. `cdc/incidents.py` relies on this tuple to split the two.
EXECUTABLE_KINDS = (AssetKind.FULL_CDC, AssetKind.REALTIME, AssetKind.EOD,
                    AssetKind.CURATED, AssetKind.MART, AssetKind.SERVING_VIEW)


@dataclass(frozen=True, order=True)
class AssetId:
    """`kind:name`. Frozen and ordered so a compiled inventory sorts deterministically.

    Determinism is not cosmetic here: DRP1's compile output is hashed into
    `config_version`, and a set-ordering difference between two runs of the same config
    would produce two "versions" of identical metadata.
    """

    kind: AssetKind
    name: str

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ConfigError("AssetId: name is required; an unnamed asset cannot be governed")
        if ":" in self.name:
            raise ConfigError(
                f"AssetId: name {self.name!r} contains ':', which separates kind from name. "
                f"A name that can be mistaken for a qualified id makes `parse` ambiguous.")

    def __str__(self) -> str:
        return f"{self.kind.value}:{self.name}"

    @property
    def qualified(self) -> str:
        return str(self)

    @property
    def layer(self) -> str | None:
        """The logical layer, or None for assets that are not lakehouse tables."""
        return LAYER_FOR.get(self.kind)

    @property
    def lineage_key(self) -> str | None:
        """The logical name shared by this table's FULL_CDC, REALTIME and EOD renderings.

        This is what lets an impact analysis move sideways: given the EOD asset for a
        table, the REALTIME asset is `lineage_key` with a different kind. `None` for kinds
        that have no sibling layers.
        """
        return self.name if self.kind in CDC_LAYER_KINDS else None

    @classmethod
    def parse(cls, raw: str) -> "AssetId":
        if not isinstance(raw, str) or ":" not in raw:
            raise ConfigError(
                f"asset id {raw!r}: expected `kind:name`. Known kinds: "
                f"{', '.join(k.value for k in AssetKind)}")
        kind_raw, _, name = raw.partition(":")
        try:
            kind = AssetKind(kind_raw)
        except ValueError:
            raise ConfigError(
                f"asset id {raw!r}: unknown kind {kind_raw!r}. Known kinds: "
                f"{', '.join(k.value for k in AssetKind)}") from None
        return cls(kind, name)


# --------------------------------------------------------------------------- #
# Builders -- the ONLY sanctioned way to mint a CDC-side identity
# --------------------------------------------------------------------------- #

def source_asset(source_id: str, database: str, schema: str, table: str) -> AssetId:
    return AssetId(AssetKind.SOURCE_TABLE, table_id(source_id, database, schema, table))


def topic_asset(engine: SourceEngine, *, topic_prefix: str, database: str,
                schema: str, table: str) -> AssetId:
    """Built from `naming.topic_for`, which is engine-aware.

    Oracle folds unquoted identifiers to uppercase, so `cdc.oracle.corebank.account` is a
    topic Debezium never writes to. The DRP0 audit found the old lineage file declaring
    exactly that lowercase form -- a declared edge pointing at a topic that does not exist.
    Calling the one function that knows the casing rule is why this cannot recur.
    """
    return AssetId(AssetKind.KAFKA_TOPIC,
                   topic_for(engine, topic_prefix=topic_prefix, database=database,
                             schema=schema, table=table))


def full_cdc_asset(source_id: str, database: str, schema: str, table: str) -> AssetId:
    return AssetId(AssetKind.FULL_CDC, logical_name(source_id, database, schema, table))


def realtime_asset(source_id: str, database: str, schema: str, table: str) -> AssetId:
    return AssetId(AssetKind.REALTIME, logical_name(source_id, database, schema, table))


def eod_asset(source_id: str, database: str, schema: str, table: str) -> AssetId:
    return AssetId(AssetKind.EOD, logical_name(source_id, database, schema, table))


def curated_asset(name: str) -> AssetId:
    return AssetId(AssetKind.CURATED, name)


def mart_asset(name: str) -> AssetId:
    return AssetId(AssetKind.MART, name)


def serving_asset(name: str) -> AssetId:
    return AssetId(AssetKind.SERVING_VIEW, name)


def bi_dataset_asset(name: str) -> AssetId:
    return AssetId(AssetKind.BI_DATASET, name)


def bi_report_asset(name: str) -> AssetId:
    return AssetId(AssetKind.BI_REPORT, name)


def physical_table(asset: AssetId) -> str:
    """The Glue table name this asset resolves to, WITHOUT the database.

    The layer prefix is part of the physical name for the three CDC layers -- `cdc_`,
    `rt_`, `eod_` -- and is applied by `cdc/naming.py`, never spelled out here.
    """
    if asset.kind is AssetKind.FULL_CDC:
        return f"cdc_{asset.name}"
    if asset.kind is AssetKind.REALTIME:
        return f"rt_{asset.name}"
    if asset.kind is AssetKind.EOD:
        return f"eod_{asset.name}"
    if asset.kind in (AssetKind.CURATED, AssetKind.MART, AssetKind.SERVING_VIEW):
        return asset.name
    raise ConfigError(
        f"{asset}: kind {asset.kind.value} has no physical Glue table. "
        f"Only lakehouse kinds resolve to one: "
        f"{', '.join(k.value for k in LAYER_FOR)}")


def assets_for_table(cfg) -> tuple[AssetId, ...]:
    """The five identities of one captured source table, from one `TableConfig`.

    Returned in dependency order: source -> topic -> FULL_CDC -> {REALTIME, EOD}. The last
    two are siblings and the order between them carries no meaning; it is fixed only so the
    inventory is deterministic.

    Asserted against the config's own precomputed names rather than trusted: `TableConfig`
    already carries `topic`, `full_cdc_table`, `realtime_table` and `eod_table`, resolved by
    the same helpers at load time. If these two ever disagree, one of them is wrong and a
    silent divergence is exactly the failure this module exists to prevent.
    """
    ident = (cfg.source_id, cfg.database, cfg.schema, cfg.table)
    built = (
        source_asset(*ident),
        AssetId(AssetKind.KAFKA_TOPIC, cfg.topic),
        full_cdc_asset(*ident),
        realtime_asset(*ident),
        eod_asset(*ident),
    )
    for asset, expected in ((built[2], cfg.full_cdc_table),
                            (built[3], cfg.realtime_table),
                            (built[4], cfg.eod_table)):
        if physical_table(asset) != expected:
            raise ConfigError(
                f"{cfg.table_id}: asset identity {asset} resolves to physical table "
                f"{physical_table(asset)!r} but the loaded config says {expected!r}. "
                f"Two spellings of one table is the DRP0 drift defect; refusing to compile.")
    return built


def assert_unique(assets) -> tuple[AssetId, ...]:
    """Sorted, deduplicated, and REFUSES a duplicate identity.

    A duplicate is never benign: two configs claiming one `AssetId` means two owners, two
    retention policies and two DQ verdicts for one physical table, with whichever loaded
    last winning silently.
    """
    seen: dict[AssetId, int] = {}
    for a in assets:
        seen[a] = seen.get(a, 0) + 1
    dupes = sorted(str(a) for a, n in seen.items() if n > 1)
    if dupes:
        raise ConfigError(
            f"duplicate asset identity: {', '.join(dupes)}. One identity, one owner, one "
            f"policy -- two claims on the same asset resolve by load order, which is not a "
            f"governance decision.")
    return tuple(sorted(seen))
