"""`AssetId` -> DataHub URN. One canonical URN per real table, carrying its fabric.

DRP2, and the load-bearing decision for DRP4. The brief for catalog ingestion says it
plainly:

    Do not create duplicate URNs for one Iceberg table.

That is easy to get wrong because one FULL_CDC table is visible to at least three
ingestors: the Glue source sees `database.table`, an S3/Iceberg source would see a bucket
path, and the Spark OpenLineage listener sees whatever the job's write target renders as.
Three sources, three names, three "datasets" in the catalogue, and the lineage graph splits
into disconnected fragments that each look complete.

So the mapping lives in ONE function, `dataset_urn()`, and every producer -- ingestion
recipes, the OpenLineage facets, the impact planner -- derives its identifier from it
rather than formatting a string locally.

THE PLATFORM CHOICE
-------------------
Every lakehouse table is `urn:li:dataPlatform:glue`, named `<database>.<table>`, because
that is what the DataHub Glue source produces and the Glue Data Catalog is this platform's
catalogue of record (CLAUDE.md §6). An Iceberg table registered in Glue is ONE dataset that
happens to be stored in Iceberg -- not a Glue dataset and an Iceberg dataset that happen to
agree.

FABRIC IS PART OF THE IDENTITY
------------------------------
`urn:li:dataset:(urn:li:dataPlatform:glue,db.table,DEV)` and the same URN with `PROD` are
different datasets, which is exactly right: they hold different rows, are owned by
different rotas and certify separately. `UrnMinter` binds one fabric and `assert_fabric()`
refuses any foreign URN, so a dev emitter cannot write an edge onto a prod dataset.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .assets import AssetId, AssetKind
from .catalog import Catalog
from .metadata_plane import Environment, Fabric
from .models import ConfigError

#: asset kind -> DataHub data platform. The five lake kinds ALL map to `glue`: one table,
#: one platform, one URN.
PLATFORM_FOR_KIND: dict[AssetKind, str] = {
    AssetKind.KAFKA_TOPIC: "kafka",
    AssetKind.FULL_CDC: "glue",
    AssetKind.REALTIME: "glue",
    AssetKind.EOD: "glue",
    AssetKind.CURATED: "glue",
    AssetKind.MART: "glue",
    AssetKind.SERVING_VIEW: "glue",
    AssetKind.BI_DATASET: "powerbi",
    AssetKind.BI_REPORT: "powerbi",
}

#: Source engines get their own platform, because a source table is genuinely a different
#: system from its CDC copy -- that edge is the first hop of the lineage graph.
PLATFORM_FOR_ENGINE = {"oracle": "oracle", "sqlserver": "mssql"}

#: Airflow is the only orchestrator. A DataFlow URN's third component is the CLUSTER, which
#: is where environment separation lands for jobs.
ORCHESTRATOR = "airflow"

#: Every logical layer a URN may name. Wider than `catalog.CDC_LAYERS`, which covers only
#: the layers the CDC registry PROVISIONS -- the catalogue has to name CURATED and MART too.
URN_LAYERS = ("FULL_CDC", "REALTIME", "EOD", "CURATED", "MART", "OPS")


def default_catalog() -> Catalog:
    """The one place URN minting resolves physical databases, so a caller cannot forget a
    layer and get a `layer has no database binding` error deep inside an emit."""
    return Catalog.from_layers_file(layers=URN_LAYERS)

_URN_DATASET = re.compile(
    r"^urn:li:dataset:\(urn:li:dataPlatform:([a-z0-9_-]+),(.+),([A-Z]+)\)$")


def _q(value: str) -> str:
    """URN components may not contain the characters that delimit them."""
    if any(c in value for c in "(),"):
        raise ConfigError(
            f"URN component {value!r} contains one of `(),`, which delimit a URN. A name "
            f"that cannot be parsed back is a name that silently merges two entities.")
    return value


@dataclass(frozen=True)
class UrnMinter:
    """Bound to ONE environment. Everything it mints carries that environment's fabric."""

    environment: Environment
    catalog: Catalog

    @property
    def fabric(self) -> Fabric:
        return self.environment.fabric

    # -- datasets ---------------------------------------------------------
    def dataset_name(self, asset: AssetId, *, source_engine: str = "",
                     source_database: str = "", source_schema: str = "") -> str:
        """The platform-native name. THE function DRP4 must not bypass."""
        if asset.kind is AssetKind.SOURCE_TABLE:
            if not source_engine:
                # `src:oracle.coredb.corebank.account` already carries all four parts.
                parts = asset.name.split(".")
                if len(parts) != 4:
                    raise ConfigError(
                        f"{asset}: a source-table asset id is "
                        f"`source_id.database.schema.table`; got {asset.name!r}")
                _, database, schema, table = parts
                return f"{database}.{schema}.{table}"
            return f"{source_database}.{source_schema}.{asset.name.split('.')[-1]}"
        if asset.kind is AssetKind.KAFKA_TOPIC:
            return asset.name
        if asset.kind in PLATFORM_FOR_KIND and asset.layer:
            from .assets import physical_table
            return f"{self.catalog.database(asset.layer)}.{physical_table(asset)}"
        if asset.kind in (AssetKind.BI_DATASET, AssetKind.BI_REPORT):
            return asset.name
        raise ConfigError(f"{asset}: no dataset name mapping for kind {asset.kind.value}")

    def platform(self, asset: AssetId, *, source_engine: str = "") -> str:
        if asset.kind is AssetKind.SOURCE_TABLE:
            engine = source_engine or asset.name.split(".")[0]
            try:
                return PLATFORM_FOR_ENGINE[engine]
            except KeyError:
                raise ConfigError(
                    f"{asset}: no DataHub platform for source engine {engine!r}. Known: "
                    f"{', '.join(sorted(PLATFORM_FOR_ENGINE))}") from None
        try:
            return PLATFORM_FOR_KIND[asset.kind]
        except KeyError:
            raise ConfigError(f"{asset}: kind {asset.kind.value} has no DataHub platform")

    def dataset_urn(self, asset: AssetId, *, source_engine: str = "",
                    source_database: str = "", source_schema: str = "") -> str:
        platform = self.platform(asset, source_engine=source_engine)
        name = self.dataset_name(asset, source_engine=source_engine,
                                 source_database=source_database,
                                 source_schema=source_schema)
        return (f"urn:li:dataset:(urn:li:dataPlatform:{platform},"
                f"{_q(name)},{self.fabric.value})")

    # -- jobs -------------------------------------------------------------
    def dataflow_urn(self, flow_id: str) -> str:
        """`urn:li:dataFlow:(airflow,<flow>,<cluster>)`. The cluster is the environment, so
        a dev DAG run and a prod DAG run are different flows rather than two runs of one."""
        return f"urn:li:dataFlow:({ORCHESTRATOR},{_q(flow_id)},{self.environment.name})"

    def datajob_urn(self, flow_id: str, job_id: str) -> str:
        return f"urn:li:dataJob:({self.dataflow_urn(flow_id)},{_q(job_id)})"

    # -- governance entities ----------------------------------------------
    @staticmethod
    def corp_group_urn(name: str) -> str:
        return f"urn:li:corpGroup:{_q(name)}"

    @staticmethod
    def corp_user_urn(name: str) -> str:
        return f"urn:li:corpuser:{_q(name)}"

    @staticmethod
    def tag_urn(name: str) -> str:
        return f"urn:li:tag:{_q(name)}"

    @staticmethod
    def glossary_term_urn(name: str) -> str:
        return f"urn:li:glossaryTerm:{_q(name)}"

    def domain_urn(self, name: str) -> str:
        """Domain URNs are environment-scoped by name.

        DataHub domains have no fabric component, so without this a dev and a prod
        `core_banking` would be the SAME domain -- and a dev asset would appear inside the
        production domain's asset list, where someone reading it has no way to tell.
        """
        return f"urn:li:domain:{_q(f'{name}-{self.environment.name}')}"

    # -- guards -----------------------------------------------------------
    def assert_fabric(self, urn: str) -> str:
        """Refuse a dataset URN belonging to another environment.

        This is the check that keeps dev lineage out of prod URNs. It is cheap, it runs on
        every emit, and the failure it prevents is silent: a dev edge written onto a prod
        dataset makes the production graph WRONG in a way that looks like more coverage.
        """
        m = _URN_DATASET.match(urn)
        if not m:
            return urn                      # not a dataset URN; jobs carry the env in the cluster
        fabric = m.group(3)
        if fabric != self.fabric.value:
            raise ConfigError(
                f"refusing to emit {urn}: fabric {fabric} is not this emitter's "
                f"{self.fabric.value}. A {fabric} edge written from a "
                f"{self.environment.name} process makes the {fabric} graph confidently "
                f"wrong, and nothing downstream can tell.")
        return urn

    def asset_for(self, urn: str) -> AssetId:
        """The reverse of `dataset_urn`. Needed because the lineage graph speaks URNs and the
        incident/recovery contracts speak `AssetId` -- two vocabularies for one thing, and a
        planner that blurred them would put a URN where a kind:name belongs and fail deep
        inside a validator rather than at the boundary.
        """
        platform, name, fabric = self.parse_dataset(urn)
        if fabric != self.fabric.value:
            raise ConfigError(
                f"{urn}: fabric {fabric} is not this minter's {self.fabric.value}")
        if platform in ("oracle", "mssql"):
            engine = {"oracle": "oracle", "mssql": "sqlserver"}[platform]
            return AssetId(AssetKind.SOURCE_TABLE, f"{engine}.{name}")
        if platform == "kafka":
            return AssetId(AssetKind.KAFKA_TOPIC, name)
        if platform == "powerbi":
            return AssetId(AssetKind.BI_DATASET, name)
        if platform != "glue":
            raise ConfigError(f"{urn}: no asset mapping for platform {platform!r}")
        database, _, table = name.partition(".")
        layer = next((lg for lg, db in self.catalog.databases.items() if db == database), "")
        if not layer:
            raise ConfigError(
                f"{urn}: database {database!r} has no layer binding in reporting/layers.yaml")
        for prefix, kind in (("cdc_", AssetKind.FULL_CDC), ("rt_", AssetKind.REALTIME),
                             ("eod_", AssetKind.EOD)):
            if layer in ("FULL_CDC", "REALTIME", "EOD") and table.startswith(prefix):
                return AssetId(kind, table[len(prefix):])
        if layer == "CURATED":
            return AssetId(AssetKind.CURATED, table)
        if layer == "MART":
            return AssetId(AssetKind.MART, table)
        raise ConfigError(
            f"{urn}: table {table!r} does not carry the layer prefix its database "
            f"({layer}) implies, so its kind cannot be recovered")

    @staticmethod
    def parse_dataset(urn: str) -> tuple[str, str, str]:
        m = _URN_DATASET.match(urn)
        if not m:
            raise ConfigError(f"{urn!r} is not a dataset URN")
        return m.group(1), m.group(2), m.group(3)
