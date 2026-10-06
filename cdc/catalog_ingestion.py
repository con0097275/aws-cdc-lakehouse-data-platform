"""DataHub catalog ingestion — recipes, and the edges no ingestor can observe.

DRP4. Two jobs.

**1. Generate ingestion recipes from config, not by hand.** Every recipe's dataset identity
has to land on the URN `cdc/urns.py` mints, or the catalogue acquires a second entity for a
table that already has one. Hand-written recipes drift; generated ones cannot.

**2. Emit the hops nothing observes.** Debezium does not speak OpenLineage, so
`source table -> Kafka topic -> FULL_CDC` can never be `observed`. It CAN be `derived`: the
CDC registry already declares every table, its primary key, its connector and — through
`naming.topic_for` — the exact topic the connector produces to. Those edges are generated
from the same registry the connectors are generated from, so they cannot describe a topology
the platform does not have.

They are still marked `declared`, and `docs/LINEAGE_SOURCE_MATRIX.md` §5 keeps the
consequence: a recovery plan may not automatically cross a declared edge.

WHAT IS DELIBERATELY NOT INGESTED
----------------------------------
*Per-event lineage.* A CDC record is not a lineage event. Dataset-level only.

*Source-table profiling.* The DataHub source-DB connectors can profile column values —
min, max, distinct, sample values. Against `corebank.customer` that reads personal data out
of the source system and writes it into a catalogue that dashboards and the AI assistant
read. Profiling is OFF for every source, and the recipes say so in the file.

*Athena query history as transformation truth.* Useful for usage and for consumer
discovery. It is not the authority for how a mart was built — the dbt manifest is.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .assets import AssetId, assets_for_table
from .catalog import Catalog
from .config_loader import load_config
from .metadata_plane import MetadataPlaneConfig
from .models import ConfigError
from .urns import URN_LAYERS, UrnMinter

ROOT = Path(__file__).resolve().parents[1]

#: Sources this platform ingests, and what each is authoritative FOR. A source that is not
#: authoritative for anything is a source that duplicates another one's assets.
SOURCE_AUTHORITY = {
    "glue": "physical tables, schemas, partitions and table properties for every lake layer",
    "dbt": "model-to-model dependencies, tests, descriptions and column lineage",
    "kafka": "topics, their schemas and their owners",
    "kafka-connect": "connector -> topic topology (DECLARED, see connector_lineage)",
    "oracle": "source-table schemas and keys (metadata only, no profiling)",
    "mssql": "source-table schemas and keys (metadata only, no profiling)",
    "athena": "usage and consumer discovery -- NEVER transformation lineage",
    "powerbi": "workspaces, semantic models, reports and their upstreams",
}

#: Sources that are enabled today. `powerbi` is absent because no Power BI workspace exists
#: in this account -- enabling a source with nothing to ingest produces an empty result that
#: is indistinguishable from a broken one.
ENABLED_SOURCES = ("glue", "dbt", "kafka", "kafka-connect")


@dataclass(frozen=True)
class Recipe:
    name: str
    source_type: str
    config: dict
    sink: dict
    notes: tuple = field(default_factory=tuple)

    def __post_init__(self) -> None:
        # A bare string here is a tuple of CHARACTERS to every consumer, and nothing
        # complains -- the notes render, the yaml is valid, and a test asserting on the
        # text sees `m e t a d a t a`. Found exactly that way.
        if isinstance(self.notes, str):
            raise ConfigError(
                f"recipe {self.name}: `notes` is a string. It must be a tuple of lines; a "
                f"string iterates as characters and every consumer silently agrees.")

    def payload(self) -> dict:
        return {"source": {"type": self.source_type, "config": self.config},
                "sink": self.sink}

    def to_yaml(self) -> str:
        header = "\n".join(f"# {n}" for n in self.notes)
        body = yaml.safe_dump(self.payload(), sort_keys=False, default_flow_style=False)
        return f"{header}\n\n{body}" if header else body


def _file_sink(out_dir: str, name: str) -> dict:
    """The DEFAULT sink. ADR-087 constraint 4: a recipe must produce and validate metadata
    with the server down, so DRP4-DRP7 progress at $0."""
    return {"type": "file", "config": {"filename": f"{out_dir}/{name}.json"}}


def _rest_sink(plane: MetadataPlaneConfig) -> dict:
    """Only the NAME of the variable that carries the token ever appears."""
    return {"type": "datahub-rest",
            "config": {"server": "${" + plane.auth.gms_url_env + "}",
                       "token": "${" + plane.auth.token_env + "}"}}


def build_recipes(plane: MetadataPlaneConfig, *, out_dir: str = "artifacts/metadata",
                  live: bool = False, catalog: Catalog | None = None,
                  registry: Path | None = None) -> dict:
    """One recipe per enabled source, with the fabric and the platform instance pinned so
    two environments cannot merge."""
    catalog = catalog or Catalog.from_layers_file(layers=URN_LAYERS)
    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    env = plane.environment
    sink = _rest_sink(plane) if live else None

    def s(name):
        return sink or _file_sink(out_dir, name)

    databases = sorted({catalog.database(layer) for layer in
                        ("FULL_CDC", "REALTIME", "EOD", "CURATED", "MART", "OPS")})

    recipes: dict = {}

    recipes["glue"] = Recipe(
        "glue", "glue",
        {"aws_region": "${AWS_REGION}",
         # Least privilege: Glue read-only through the instance profile / IRSA. No IAM user,
         # no access key (CLAUDE.md 3.2).
         "database_pattern": {"allow": [f"^{d}$" for d in databases]},
         "env": env.fabric.value,
         # The URN this produces is `glue,<database>.<table>,<FABRIC>` -- exactly what
         # `UrnMinter.dataset_urn` mints for every lake asset. That equality is the whole
         # reason the identity mapping chose `glue` for all five lake kinds.
         "extract_owners": False,        # ownership comes from the governance compile
         "extract_transforms": False,
         "profiling": {"enabled": False}},
        s("glue"),
        ("Glue is authoritative for physical tables, schemas and partitions.",
         "extract_owners is OFF: ownership is DERIVED from cdc/registry/sources.yaml and",
         "would otherwise be overwritten by whatever the Glue table happens to carry.",
         "Profiling is OFF everywhere -- it reads values, and this catalogue is read widely."))

    recipes["dbt"] = Recipe(
        "dbt", "dbt",
        {"manifest_path": "dbt/target/manifest.json",
         "catalog_path": "dbt/target/catalog.json",
         "run_results_paths": ["dbt/target/run_results.json"],
         "target_platform": "glue",
         "env": env.fabric.value,
         # dbt's own `source('curated', ...)` nodes must land on the SAME urn as the Glue
         # ingestion of those tables, or the graph breaks exactly at the CURATED -> MART hop.
         #
         # EXPLICIT, not defaulted. The CLI warns that `convert_urns_to_lowercase`
         # "defaults to true for this source but is not set in the recipe", and a DEFAULT is
         # precisely what ADR-090 refuses: if dbt lowercases a urn and the Glue ingestion
         # does not, the two emit different urns for ONE table and the graph splits at the
         # CURATED -> MART hop into two halves that each look complete.
         #
         # Only the dbt source accepts this key -- `GlueSourceConfig` rejects it outright
         # (`extra_forbidden`), which is how we learned that the Glue side has no such
         # switch and never lowercases. False therefore MATCHES Glue's behaviour; it is not
         # a preference. Verified by running both recipes, not by reading the docs.
         "convert_urns_to_lowercase": False,
         "write_semantics": "PATCH",     # never clobber ownership the governance compile set
         "include_column_lineage": True},
        s("dbt"),
        ("The dbt manifest is AUTHORITATIVE for model-to-model dependencies (ADR-087).",
         "No custom dbt lineage is emitted anywhere else -- a second dbt graph restating",
         "this one is the S11-3 objection, and it is what governance/lineage/openlineage.yml",
         "already did.",
         "write_semantics: PATCH so ingestion cannot overwrite derived ownership.",
         "convert_urns_to_lowercase is pinned FALSE to match Glue, which has no such",
         "switch; letting it default to true would split one table into two urns (ADR-090).",
         "`use_identifiers` was removed -- the CLI deprecated it and warns on every run."))

    recipes["kafka"] = Recipe(
        "kafka", "kafka",
        {"connection": {"bootstrap": "${KAFKA_BOOTSTRAP}",
                        # OAUTHBEARER, not AWS_MSK_IAM. This recipe said `AWS_MSK_IAM` for
                        # months and could never have authenticated: that is the JAVA
                        # client's mechanism name, and DataHub's Kafka source uses
                        # confluent-kafka (librdkafka), which rejects it outright --
                        #   Unsupported SASL mechanism: AWS_MSK_IAM
                        # Verified against librdkafka directly, not inferred.
                        "consumer_config": {"security.protocol": "SASL_SSL",
                                            "sasl.mechanism": "OAUTHBEARER"}},
         "topic_patterns": {"allow": [f"^{src.topic_prefix}\\..*" for src in cfg.sources]},
         "env": env.fabric.value},
        s("kafka"),
        ("Topics, schemas and owners. NO per-event lineage: a CDC record is not a lineage",
         "event, and one event per row would be millions of graph writes a day.",
         "IAM auth through the instance role -- no static credential (CLAUDE.md 3.1).",
         "",
         "MSK IAM CANNOT BE EXPRESSED FULLY IN THIS FILE. librdkafka needs an `oauth_cb`,",
         "a Python CALLABLE that mints an MSK auth token, and YAML cannot carry a callable.",
         "So `datahub ingest -c kafka.yaml` alone will not authenticate; this source must be",
         "driven from Python that sets the callback (see scripts/README or the runbook).",
         "The mechanism name is fixed here so the recipe is at least not wrong."))

    recipes["kafka-connect"] = Recipe(
        "kafka-connect", "kafka-connect",
        {"connect_uri": "${CONNECT_REST_URL}",
         "connector_patterns": {"allow": [f"^{src.connector}$" for src in cfg.sources]},
         "env": env.fabric.value,
         "platform_instance_map": {"oracle": "coredb", "mssql": "digital"}},
        s("kafka-connect"),
        ("Tried FIRST for source-table -> topic lineage. If the connector plugin does not",
         "report it reliably, `connector_lineage()` in this module emits the same edges",
         "DETERMINISTICALLY from the CDC registry -- which is the file the connectors are",
         "themselves generated from, so it cannot describe a topology that does not exist.",
         "Either way the edge stays `declared`: Debezium emits no telemetry."))

    # Present but NOT enabled -- see ENABLED_SOURCES.
    recipes["oracle"] = Recipe(
        "oracle", "oracle",
        {"host_port": "${ORACLE_HOST_PORT}", "service_name": "${ORACLE_SERVICE}",
         "username": "${ORACLE_METADATA_USER}", "password": "${ORACLE_METADATA_PASSWORD}",
         "schema_pattern": {"allow": ["^COREBANK$"]},
         "env": env.fabric.value,
         # THE important line in this recipe.
         "profiling": {"enabled": False},
         "include_view_lineage": False},
        s("oracle"),
        ("METADATA ONLY, least privilege: a read-only account with SELECT on the data",
         "dictionary and nothing else.",
         "profiling.enabled MUST stay false. Profiling reads column VALUES -- min, max,",
         "distinct, samples -- out of corebank.customer and writes them into a catalogue",
         "that dashboards and the AI assistant read."))

    recipes["mssql"] = Recipe(
        "mssql", "mssql",
        {"host_port": "${MSSQL_HOST_PORT}", "database": "digital",
         "username": "${MSSQL_METADATA_USER}", "password": "${MSSQL_METADATA_PASSWORD}",
         "schema_pattern": {"allow": ["^dbo$"]},
         "env": env.fabric.value,
         "profiling": {"enabled": False}},
        s("mssql"),
        ("Metadata only, least privilege, no profiling. Same reasoning as Oracle.",))

    recipes["athena"] = Recipe(
        "athena", "athena",
        {"aws_region": "${AWS_REGION}", "work_group": "${ATHENA_WORKGROUP}",
         "query_result_location": "${ATHENA_OUTPUT}",
         "env": env.fabric.value,
         "include_views": True,
         "profiling": {"enabled": False}},
        s("athena"),
        ("Usage and consumer discovery only. Athena query history is NOT the authority for",
         "how a mart was built -- the dbt manifest is. Treating query history as",
         "transformation truth would make an ad-hoc SELECT look like a pipeline edge."))

    recipes["powerbi"] = Recipe(
        "powerbi", "powerbi",
        {"tenant_id": "${POWERBI_TENANT_ID}", "client_id": "${POWERBI_CLIENT_ID}",
         "client_secret": "${POWERBI_CLIENT_SECRET}",
         "env": env.fabric.value,
         "extract_lineage": True, "extract_reports": True,
         "extract_ownership": False},
        s("powerbi"),
        ("NOT ENABLED: there is no Power BI workspace in this account. A source with nothing",
         "to ingest returns an empty result that looks exactly like a broken one.",
         "Service principal with read-only tenant scope when one exists."))

    return recipes


def assert_no_duplicate_identity(recipes: dict) -> None:
    """Every recipe that can produce a lake dataset must produce it on the `glue` platform.

    Two ingestors claiming one table is the failure the DRP4 brief names first, and it is
    silent: both datasets look real and the lineage splits between them.
    """
    offenders = []
    for name, r in recipes.items():
        if name in ("glue", "dbt") and r.config.get("target_platform", "glue") != "glue":
            offenders.append(name)
        if name == "athena" and r.config.get("include_tables", False):
            offenders.append("athena (would re-register the Glue tables under `athena`)")
    if offenders:
        raise ConfigError(
            f"recipes would produce a second identity for a lake table: "
            f"{', '.join(offenders)}. One table, one URN (ADR-089).")


# --------------------------------------------------------------------------- #
# The edges nothing observes
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ConnectorEdge:
    upstream: AssetId
    downstream: AssetId
    connector: str
    evidence: str = "declared"

    def payload(self) -> dict:
        return {"upstream": str(self.upstream), "downstream": str(self.downstream),
                "connector": self.connector, "evidence": self.evidence}


def connector_lineage(registry: Path | None = None) -> tuple:
    """`source table -> Kafka topic -> FULL_CDC`, derived from the CDC registry.

    Dataset level only, one edge per table per hop. Not one per CDC record -- a record is
    data, not lineage, and a per-event graph would be millions of writes a day describing
    nothing a person can read.
    """
    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    edges: list = []
    for src in cfg.sources:
        for table in src.tables:
            source, topic, full_cdc, _, _ = assets_for_table(table)
            edges.append(ConnectorEdge(source, topic, src.connector))
            # The Kafka -> FULL_CDC hop CAN be observed once the Spark listener runs
            # (DRP3), so it is emitted here only as a fallback and marked as such.
            edges.append(ConnectorEdge(topic, full_cdc, src.connector,
                                       evidence="declared_until_observed"))
    return tuple(edges)


def connector_lineage_mcps(client, registry: Path | None = None) -> list:
    """The declared edges as `upstreamLineage` proposals, grouped so each downstream gets
    ONE aspect. Emitting one aspect per edge would make the last write win and the table
    would end up with a single upstream."""
    by_downstream: dict = {}
    for edge in connector_lineage(registry):
        up = client.minter.dataset_urn(edge.upstream)
        down = client.minter.dataset_urn(edge.downstream)
        by_downstream.setdefault(down, []).append(up)
    return [client.upstream_lineage(down, ups, kind="COPY")
            for down, ups in sorted(by_downstream.items())]


def chain_for_table(minter: UrnMinter, table_id: str,
                    registry: Path | None = None) -> tuple:
    """The full identity chain for one captured table, source metadata -> BI metadata.

    The DRP4 acceptance test: every hop must resolve, and each URN must be distinct.
    """
    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    table = next((t for t in cfg.tables if t.table_id == table_id), None)
    if table is None:
        raise ConfigError(f"unknown table_id {table_id!r}")
    chain = [minter.dataset_urn(a) for a in assets_for_table(table)]
    # Above the CDC layers the chain is per-model and comes from dbt; the acceptance test
    # supplies the curated/mart assets it expects rather than this module guessing.
    return tuple(chain)


def write_recipes(recipes: dict, out_dir: Path, *, only: tuple = ENABLED_SOURCES) -> tuple:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for name in only:
        if name not in recipes:
            raise ConfigError(f"no recipe named {name!r}")
        p = out_dir / f"{name}.yaml"
        p.write_text(recipes[name].to_yaml())
        written.append(p)
    return tuple(written)
