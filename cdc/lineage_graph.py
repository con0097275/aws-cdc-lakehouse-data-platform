"""The end-to-end lineage graph — assembled from the sources that already know, per hop.

DRP6. One graph, built from the authority for each segment rather than from one system that
claims to see everything:

    source DB -> Kafka          connector topology, DERIVED from the CDC registry (declared)
    Kafka -> FULL_CDC           Spark OpenLineage (observed) -- declared until it runs
    FULL_CDC -> REALTIME / EOD  Spark OpenLineage (observed) + the OPS run ledgers (derived)
    EOD/REALTIME -> dbt -> MART dbt manifest (derived) -- authoritative for model deps
    MART -> Power BI            the Power BI connector (absent here; no workspace exists)

WHY NOT ONE SOURCE
------------------
Because they are not equally trustworthy and pretending otherwise is how a recovery rewrites
data on the strength of a config file. Every edge carries its `EvidenceClass`, and
`docs/LINEAGE_SOURCE_MATRIX.md` §5 keeps the consequence: an automatic recovery may traverse
`observed` and `derived`, never `declared`.

TWO GRAPHS, NOT ONE
-------------------
    STATIC / LOGICAL   dataset -> dataset. "What feeds this table, ever?"
    RUN                one execution. "Which run produced the rows I am looking at, from
                       which input snapshots, under which config version?"

They answer different questions and a single graph answers neither well. Impact analysis
needs the static one; a post-mortem needs the run one. Heavy operational detail stays in
OPS -- the run graph stores links and facets, not ledgers.

COLUMN LINEAGE IS PRIORITISED, NOT UNIVERSAL
---------------------------------------------
dbt gives column lineage for free where the transformation is SQL. Arbitrary Python inside a
Spark job does not, and a fuzzy inferred mapping is worse than none for a critical field:
it looks authoritative. So critical columns get an EXPLICIT declaration, everything else
gets whatever dbt can prove, and `ColumnEdge.evidence` says which.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path

from .assets import AssetId, AssetKind, EXECUTABLE_KINDS
from .incidents import EvidenceClass
from .models import ConfigError

ROOT = Path(__file__).resolve().parents[1]

#: Kinds a *critical* asset must have an upstream for. A mart with no parent is either a
#: seed or a broken edge, and the two look identical in a graph.
MUST_HAVE_UPSTREAM = (AssetKind.FULL_CDC, AssetKind.REALTIME, AssetKind.EOD,
                      AssetKind.CURATED, AssetKind.MART)


@dataclass(frozen=True)
class Edge:
    upstream: str
    downstream: str
    evidence: EvidenceClass
    source: str                  # which system asserted it
    job: str = ""

    def payload(self) -> dict:
        return {"upstream": self.upstream, "downstream": self.downstream,
                "evidence": self.evidence.value, "source": self.source, "job": self.job}


@dataclass(frozen=True)
class ColumnEdge:
    upstream_dataset: str
    upstream_column: str
    downstream_dataset: str
    downstream_column: str
    evidence: EvidenceClass
    source: str

    def payload(self) -> dict:
        return {"upstream": f"{self.upstream_dataset}.{self.upstream_column}",
                "downstream": f"{self.downstream_dataset}.{self.downstream_column}",
                "evidence": self.evidence.value, "source": self.source}


@dataclass(frozen=True)
class RunLink:
    """One execution, as links and facets. The ledgers stay in OPS."""

    run_id: str
    job: str
    inputs: tuple
    outputs: tuple
    facets: dict = field(default_factory=dict)

    def payload(self) -> dict:
        return {"run_id": self.run_id, "job": self.job, "inputs": sorted(self.inputs),
                "outputs": sorted(self.outputs),
                "facets": {k: self.facets[k] for k in sorted(self.facets)}}


@dataclass(frozen=True)
class LineageFinding:
    rule: str
    asset: str
    message: str

    def __str__(self) -> str:
        return f"[{self.rule}] {self.asset}: {self.message}"


class LineageGraph:
    """Static lineage plus a run index. Pure: nothing here talks to DataHub.

    Keeping it pure is what lets the whole of DRP6 and DRP8 be tested without a metadata
    plane -- and DataHub is `disabled` by default, so a graph that needed it would be a
    graph nobody could exercise.
    """

    def __init__(self) -> None:
        self._edges: list = []
        self._columns: list = []
        self._runs: list = []
        self._down: dict = defaultdict(set)
        self._up: dict = defaultdict(set)
        self._nodes: set = set()

    # -- construction -----------------------------------------------------
    def add_edge(self, edge: Edge) -> "LineageGraph":
        if edge.upstream == edge.downstream:
            raise ConfigError(f"self-edge on {edge.upstream}: a dataset cannot feed itself")
        self._edges.append(edge)
        self._down[edge.upstream].add(edge.downstream)
        self._up[edge.downstream].add(edge.upstream)
        self._nodes.update((edge.upstream, edge.downstream))
        return self

    def add_column_edge(self, edge: ColumnEdge) -> "LineageGraph":
        self._columns.append(edge)
        return self

    def add_run(self, run: RunLink) -> "LineageGraph":
        self._runs.append(run)
        return self

    # -- queries ----------------------------------------------------------
    @property
    def nodes(self) -> tuple:
        return tuple(sorted(self._nodes))

    def column_edges(self) -> tuple:
        """The column-level edges. Exposed as a method so callers outside this module do
        not reach into `_columns`: a private attribute read across a boundary breaks
        silently on refactor, and this graph is read by the AI copilot as well as by DRP."""
        return tuple(self._columns)

    @property
    def edges(self) -> tuple:
        return tuple(self._edges)

    def upstreams(self, node: str) -> tuple:
        return tuple(sorted(self._up.get(node, ())))

    def downstreams(self, node: str) -> tuple:
        return tuple(sorted(self._down.get(node, ())))

    def edge(self, upstream: str, downstream: str) -> Edge | None:
        return next((e for e in self._edges
                     if e.upstream == upstream and e.downstream == downstream), None)

    def descendants(self, root: str, *, max_hops: int = 10) -> tuple:
        """Breadth-first, hop-bounded, EXCLUDING the root.

        `max_hops` is not a performance guard. An unbounded traversal in a warehouse means
        one bad source table nominates every mart, and a blast radius that always says
        "everything" is one nobody reads.
        """
        if max_hops < 1:
            raise ConfigError("max_hops must be at least 1")
        seen, out = {root}, []
        queue = deque([(root, 0)])
        while queue:
            node, depth = queue.popleft()
            if depth >= max_hops:
                continue
            for child in self.downstreams(node):
                if child in seen:
                    continue
                seen.add(child)
                out.append((child, depth + 1))
                queue.append((child, depth + 1))
        return tuple(sorted(out))

    def path(self, source: str, target: str, *, max_hops: int = 12) -> tuple:
        """One shortest path, or () when there is none. Used to prove a critical field's
        route rather than to assert that a route exists somewhere."""
        if source == target:
            return (source,)
        prev, queue = {source: None}, deque([(source, 0)])
        while queue:
            node, depth = queue.popleft()
            if depth >= max_hops:
                continue
            for child in self.downstreams(node):
                if child in prev:
                    continue
                prev[child] = node
                if child == target:
                    chain, cur = [], child
                    while cur is not None:
                        chain.append(cur)
                        cur = prev[cur]
                    return tuple(reversed(chain))
                queue.append((child, depth + 1))
        return ()

    def weakest_evidence(self, chain) -> EvidenceClass:
        """The weakest link on a path. This is what the recovery gate reads.

        `ABSENT` when any hop has no edge at all -- a chain with a missing link is not a
        strong chain with one gap, it is not a chain.
        """
        order = {EvidenceClass.OBSERVED: 3, EvidenceClass.DERIVED: 2,
                 EvidenceClass.DECLARED: 1, EvidenceClass.ABSENT: 0}
        worst = EvidenceClass.OBSERVED
        for a, b in zip(chain, chain[1:]):
            e = self.edge(a, b)
            cls = e.evidence if e else EvidenceClass.ABSENT
            if order[cls] < order[worst]:
                worst = cls
        return worst

    def runs_for(self, node: str) -> tuple:
        return tuple(r for r in self._runs if node in r.outputs)

    def column_upstreams(self, dataset: str, column: str) -> tuple:
        return tuple(c for c in self._columns
                     if c.downstream_dataset == dataset and c.downstream_column == column)

    def cycles(self) -> tuple:
        """Every cycle is a defect. A lineage cycle makes a topological rerun impossible and
        an impact traversal non-terminating."""
        colour: dict = {}
        found: list = []

        def visit(node, stack):
            colour[node] = 1
            for child in self.downstreams(node):
                if colour.get(child) == 1:
                    found.append(tuple(stack[stack.index(child):] + [child]))
                elif colour.get(child, 0) == 0:
                    visit(child, stack + [child])
            colour[node] = 2

        for node in self.nodes:
            if colour.get(node, 0) == 0:
                visit(node, [node])
        return tuple(found)

    def payload(self) -> dict:
        return {"nodes": list(self.nodes),
                "edges": [e.payload() for e in self._edges],
                "columns": [c.payload() for c in self._columns],
                "runs": [r.payload() for r in self._runs]}


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #

def from_connector_lineage(graph: LineageGraph, minter, registry: Path | None = None):
    """The declared hops: source table -> topic -> FULL_CDC."""
    from .catalog_ingestion import connector_lineage

    for e in connector_lineage(registry):
        graph.add_edge(Edge(minter.dataset_urn(e.upstream), minter.dataset_urn(e.downstream),
                            EvidenceClass.DECLARED if e.evidence == "declared"
                            else EvidenceClass.DECLARED,
                            source="cdc-registry", job="debezium"))
    return graph


def from_cdc_layers(graph: LineageGraph, minter, registry: Path | None = None):
    """FULL_CDC -> REALTIME and FULL_CDC -> EOD, siblings, never a chain.

    `derived` rather than `declared`: `ops.realtime_run` and `ops.eod_run` record input
    snapshot, output snapshot and rows per table per run, which is the producer's own
    artefact. That is as trustworthy as telemetry and is what the platform has TODAY --
    the Spark listener will upgrade these to `observed` once it runs.
    """
    from .assets import assets_for_table
    from .config_loader import load_config

    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    for table in cfg.tables:
        _, _, full_cdc, realtime, eod = assets_for_table(table)
        src = minter.dataset_urn(full_cdc)
        graph.add_edge(Edge(src, minter.dataset_urn(eod), EvidenceClass.DERIVED,
                            source="ops.eod_run", job="eod_build"))
        if table.realtime.enabled:
            graph.add_edge(Edge(src, minter.dataset_urn(realtime), EvidenceClass.DERIVED,
                                source="ops.realtime_run", job="realtime_materialize"))
    return graph


def from_curated_entities(graph: LineageGraph, minter, entities: Path | None = None,
                         registry: Path | None = None, *, include_columns: bool = True):
    """EOD -> CURATED, and the column mapping through it.

    `reporting/curated/entities.yaml` is the conformance contract (ADR-080): it names the EOD
    `table_id` each conformed entity is projected from, and the SOURCE COLUMN each business
    column comes from. `curated_build.py` reads this same file to do the work.

    That makes these edges `DERIVED`, not `DECLARED` -- they are not a human's description of
    what a job does, they are the instruction the job follows. It also makes this the one
    place in the platform where column lineage exists through a NON-SQL transformation,
    which is exactly the gap `docs/COLUMN_LINEAGE_STRATEGY.md` says explicit mapping is for.
    """
    import yaml as _yaml

    from .assets import assets_for_table
    from .config_loader import load_config

    entities = entities or (ROOT / "reporting" / "curated" / "entities.yaml")
    if not entities.exists():
        return graph
    doc = _yaml.safe_load(entities.read_text()) or {}
    cfg = load_config(registry or (ROOT / "cdc" / "registry" / "sources.yaml"))
    by_table = {t.table_id: t for t in cfg.tables}

    for name, spec in (doc.get("entities") or {}).items():
        spec = spec or {}
        table = by_table.get(spec.get("table_id"))
        if table is None:
            continue
        _, _, full_cdc, realtime, eod = assets_for_table(table)
        layer = (spec.get("layer") or "EOD").upper()
        upstream_asset = {"EOD": eod, "REALTIME": realtime, "FULL_CDC": full_cdc}.get(layer, eod)
        up = minter.dataset_urn(upstream_asset)
        entity = minter.dataset_urn(AssetId(AssetKind.CURATED, name))
        graph.add_edge(Edge(up, entity, EvidenceClass.DERIVED,
                            source="curated/entities.yaml", job="curated_build"))

        dimension = spec.get("dimension")
        if dimension:
            graph.add_edge(Edge(entity, minter.dataset_urn(AssetId(AssetKind.CURATED, dimension)),
                                EvidenceClass.DERIVED, source="curated/entities.yaml",
                                job="curated_build"))

        if not include_columns:
            continue
        for source_column, target in (spec.get("columns") or {}).items():
            target_name = target.get("name") if isinstance(target, dict) else str(target)
            if not target_name:
                continue
            graph.add_column_edge(ColumnEdge(up, source_column, entity, target_name,
                                             EvidenceClass.DERIVED,
                                             "curated/entities.yaml"))
    return graph


def from_dbt_manifest(graph: LineageGraph, minter, manifest: Path | None = None,
                      *, include_columns: bool = True):
    """Model-to-model and source-to-model, AUTHORITATIVE (ADR-087).

    No second dbt graph is emitted anywhere else. Restating it is the S11-3 objection, and
    it is exactly what `governance/lineage/openlineage.yml` already did.
    """
    manifest = manifest or (ROOT / "dbt" / "target" / "manifest.json")
    if not manifest.exists():
        return graph
    doc = json.loads(manifest.read_text())
    nodes, sources = doc.get("nodes") or {}, doc.get("sources") or {}

    def urn_for(uid: str) -> str | None:
        if uid.startswith("source."):
            s = sources.get(uid)
            if not s or s.get("source_name") != "curated":
                return None
            return minter.dataset_urn(AssetId(AssetKind.CURATED, s["name"]))
        n = nodes.get(uid)
        if not n or n.get("resource_type") != "model":
            return None
        return minter.dataset_urn(AssetId(AssetKind.MART, n["name"]))

    for uid, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        down = urn_for(uid)
        if not down:
            continue
        for dep in node.get("depends_on", {}).get("nodes", []):
            up = urn_for(dep)
            if up and up != down:
                graph.add_edge(Edge(up, down, EvidenceClass.DERIVED,
                                    source="dbt-manifest", job="dbt_spark_build"))
        if not include_columns:
            continue
        for col, spec in (node.get("columns") or {}).items():
            for dep in node.get("depends_on", {}).get("nodes", []):
                up = urn_for(dep)
                if not up:
                    continue
                # dbt's manifest names a model's columns but does not, on its own, prove
                # which upstream column each came from. Claiming a same-name mapping as
                # DERIVED would be the fuzzy inference this module refuses for critical
                # fields, so it is recorded as DECLARED and must be confirmed to be trusted.
                graph.add_column_edge(ColumnEdge(up, col, down, col,
                                                 EvidenceClass.DECLARED, "dbt-manifest"))
    return graph


def declare_column_lineage(graph: LineageGraph, mappings) -> LineageGraph:
    """Explicit mappings for critical columns through non-SQL transformations.

    `evidence=DECLARED` and deliberately so: a human asserted it. Marking it `derived` would
    claim a proof that does not exist, and a critical field is precisely where a confident
    wrong answer costs the most.
    """
    for m in mappings:
        graph.add_column_edge(ColumnEdge(m["upstream_dataset"], m["upstream_column"],
                                         m["downstream_dataset"], m["downstream_column"],
                                         EvidenceClass.DECLARED, m.get("source", "declared")))
    return graph


def from_fact_contract(graph: LineageGraph, minter, path: Path | None = None):
    """CURATED entities/dimensions -> Kimball facts, as `DERIVED`.

    `reporting/curated/facts.yaml` is not a description of `build_facts` -- the job ASSERTS
    its actual inputs against it (`curated_build.assert_fact_inputs`), so a fact that starts
    reading something undeclared fails the build rather than letting the graph go quietly
    stale. A declaration checked against the code at runtime is evidence, not a reading.

    This is what lets a mart recovery be automatic: before it, every path to a mart crossed a
    `declared` edge and `decide_approval` refused.
    """
    import yaml as _yaml

    path = path or (ROOT / "reporting" / "curated" / "facts.yaml")
    if not path.exists():
        return graph
    doc = _yaml.safe_load(path.read_text()) or {}
    for fact, spec in (doc.get("facts") or {}).items():
        down = minter.dataset_urn(AssetId(AssetKind.CURATED, fact))
        graph._nodes.add(down)
        for name in [*(spec.get("entities") or []), *(spec.get("dimensions") or [])]:
            graph.add_edge(Edge(minter.dataset_urn(AssetId(AssetKind.CURATED, name)), down,
                                EvidenceClass.DERIVED,
                                source="curated/facts.yaml", job="curated_build"))
    for dim in (doc.get("generated_dimensions") or {}):
        # No parent, declared deliberately -- see `from_declared_edges`.
        graph._nodes.add(minter.dataset_urn(AssetId(AssetKind.CURATED, dim)))
    return graph


def from_declared_edges(graph: LineageGraph, minter, path: Path | None = None):
    """Edges that exist only in Python, read from `governance/registry/lineage_declared.yaml`.

    Marked `DECLARED` -- a human's reading of a function, not an instruction the function
    follows. A recovery may not cross one automatically, which is the correct outcome while
    the producer is undeclared. An empty `upstreams` list is meaningful: it says "this asset
    is GENERATED and has no parent", so a real orphan stays distinguishable from a
    deliberate one.
    """
    import yaml as _yaml

    path = path or (ROOT / "governance" / "registry" / "lineage_declared.yaml")
    if not path.exists():
        return graph
    doc = _yaml.safe_load(path.read_text()) or {}
    for entry in doc.get("edges") or ():
        down = minter.dataset_urn(AssetId.parse(entry["downstream"]))
        graph._nodes.add(down)
        for raw in entry.get("upstreams") or ():
            graph.add_edge(Edge(minter.dataset_urn(AssetId.parse(raw)), down,
                                EvidenceClass.DECLARED,
                                source=entry.get("declared_in", "lineage_declared.yaml"),
                                job=entry.get("job", "")))
    return graph


def build_static_graph(minter, *, registry: Path | None = None,
                       manifest: Path | None = None) -> LineageGraph:
    g = LineageGraph()
    from_connector_lineage(g, minter, registry)
    from_cdc_layers(g, minter, registry)
    from_curated_entities(g, minter, registry=registry)
    from_fact_contract(g, minter)
    from_declared_edges(g, minter)
    from_dbt_manifest(g, minter, manifest)
    return g


# --------------------------------------------------------------------------- #
# Lineage quality
# --------------------------------------------------------------------------- #

def audit(graph: LineageGraph, *, critical: tuple = (), expected_upstreams: dict | None = None,
          expected_bi_parents: tuple = ()) -> tuple:
    """Six rules, each naming a way a lineage graph is wrong while looking complete."""
    findings: list = []

    for node in critical:
        if node not in graph.nodes:
            findings.append(LineageFinding(
                "orphan_critical_asset", node,
                "declared critical but absent from the graph entirely; every impact query "
                "about it returns nothing, which reads as 'nothing depends on it'"))
        elif not graph.upstreams(node):
            findings.append(LineageFinding(
                "orphan_critical_asset", node,
                "critical asset with no upstream: either a seed or a broken edge, and the "
                "two look identical"))

    for node, expected in (expected_upstreams or {}).items():
        missing = sorted(set(expected) - set(graph.upstreams(node)))
        if missing:
            findings.append(LineageFinding(
                "missing_expected_upstream", node,
                f"expected upstream(s) absent: {', '.join(missing)}"))

    for cycle in graph.cycles():
        findings.append(LineageFinding(
            "unexpected_cycle", " -> ".join(cycle),
            "a cycle makes a topological rerun impossible and an impact traversal "
            "non-terminating"))

    by_name: dict = defaultdict(list)
    for node in graph.nodes:
        try:
            platform, name, fabric = _parse(node)
        except ValueError:
            continue
        by_name[(name, fabric)].append(platform)
    for (name, fabric), platforms in sorted(by_name.items()):
        if len(set(platforms)) > 1:
            findings.append(LineageFinding(
                "duplicate_canonical_asset", f"{name} ({fabric})",
                f"one table on {len(set(platforms))} platforms: {', '.join(sorted(set(platforms)))}. "
                f"The graph splits between them and each half looks complete."))

    for edge in graph.edges:
        if edge.source == "dbt-manifest" and edge.upstream not in graph.nodes:
            findings.append(LineageFinding("broken_dbt_edge", edge.downstream,
                                           f"dbt names an upstream that is not a node: "
                                           f"{edge.upstream}"))

    for bi in expected_bi_parents:
        if not graph.upstreams(bi):
            findings.append(LineageFinding(
                "missing_bi_parent", bi,
                "a BI asset with no upstream cannot be impact-analysed; a mart change would "
                "show no consumers"))

    return tuple(sorted(findings, key=lambda f: (f.rule, f.asset)))


def _parse(urn: str):
    import re
    m = re.match(r"^urn:li:dataset:\(urn:li:dataPlatform:([a-z0-9_-]+),(.+),([A-Z]+)\)$", urn)
    if not m:
        raise ValueError(urn)
    return m.group(1), m.group(2), m.group(3)


def classify(node: str) -> str:
    """What a node IS, for the impact planner. `UNKNOWN` is a real answer."""
    try:
        platform, name, _ = _parse(node)
    except ValueError:
        return "UNKNOWN"
    if platform == "powerbi":
        return "BI_ASSET"
    if platform in ("oracle", "mssql", "kafka"):
        return "DATASET"
    if platform == "glue":
        db = name.split(".")[0]
        if db.endswith("_serving"):
            return "SERVING_ASSET"
        return "EXECUTABLE_JOB" if any(
            db.endswith(suffix) for suffix in
            ("_full_cdc", "_stream", "_snapshot", "_curated", "_mart")) else "DATASET"
    return "UNKNOWN"


def executable(node: str, minter=None) -> bool:
    """Whether a rerun could even address this node. A dashboard is not a Spark job."""
    try:
        platform, name, _ = _parse(node)
    except ValueError:
        return False
    if platform != "glue":
        return False
    return classify(node) in ("EXECUTABLE_JOB",)


__all__ = ["Edge", "ColumnEdge", "RunLink", "LineageFinding", "LineageGraph",
           "from_connector_lineage", "from_cdc_layers", "from_curated_entities",
           "from_dbt_manifest", "from_declared_edges", "from_fact_contract",
           "declare_column_lineage", "build_static_graph", "audit", "classify",
           "executable", "MUST_HAVE_UPSTREAM", "EXECUTABLE_KINDS"]
