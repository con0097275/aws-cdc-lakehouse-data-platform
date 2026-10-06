"""AIGR2 — the read tools, wired to real backends.

Everything here returns platform truth. Nothing accepts a value the model produced except a
natural-language phrase, and the one function that consumes a phrase (`resolve_asset`)
REFUSES ON AMBIGUITY rather than picking the closest match -- a guessed identifier is how a
recovery lands on the wrong table.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from cdc.governance_plan import compile_inventory
from cdc.impact import LineageImpactService
from cdc.lineage_graph import build_static_graph
from cdc.urns import UrnMinter, default_catalog
from cdc.metadata_plane import load as load_plane

from .model import LineageConfidence


class ResolutionError(ValueError):
    """Raised instead of guessing. Ambiguity is a question for the user, not a coin flip."""


class LayerAmbiguity(ResolutionError):
    """One logical table, several layers.

    Distinct from a true ambiguity: `oracle_coredb_corebank_customer` exists as `src`,
    `full_cdc`, `realtime` and `eod`. For a DESCRIPTION all four are the answer; for a
    REPAIR the layer decides what gets rewritten, so the caller must still choose.
    """

    def __init__(self, message: str, candidates: tuple = ()):
        super().__init__(message)
        self.candidates = candidates


@dataclass(frozen=True)
class ResolvedAsset:
    asset_id: str
    kind: str
    name: str
    owner: str
    domain: str
    classification: str

    def payload(self) -> dict:
        return {"asset_id": self.asset_id, "kind": self.kind, "name": self.name,
                "owner": self.owner, "domain": self.domain,
                "classification": self.classification}


@lru_cache(maxsize=1)
def _inventory():
    return compile_inventory()


@lru_cache(maxsize=1)
def _graph_and_impact(environment: str = "dev"):
    plane = load_plane(mode="local")
    minter = UrnMinter(plane.environment, default_catalog())
    graph = build_static_graph(minter)
    return graph, LineageImpactService(graph, environment=environment, minter=minter)


#: Words that appear in almost any incident sentence and identify no asset. Without this,
#: "a duplicate CDC EVENT doubled BALANCE" scores against `digital_event` as strongly as
#: against the account table, producing a tie -- and the resolver then correctly refuses a
#: question it should have answered. A generic word is not evidence of an asset.
_FILLER = frozenset("""
the a an and or of for on in to is are was were be been it its this that
cdc data event events row rows record records column columns field fields table tables
value values number count total sum wrong bad broken incorrect failed failing issue problem
duplicate duplicated doubled late missing stale drift defect bug buggy
repair repairing rerun rebuild fix fixing recover recovery investigate investigating
diagnose check verify affected downstream upstream only just please can you
cob date dates day today yesterday account_id id key keys
""".split())


def _tokens(text: str) -> set[str]:
    """Distinctive tokens only. Filler is dropped BEFORE scoring, not after, because a tie
    created by filler is indistinguishable from a real ambiguity once it exists."""
    return {t for t in re.split(r"[^a-z0-9]+", text.lower())
            if t and len(t) > 1 and t not in _FILLER}


#: Layer words a user is likely to say, mapped onto the asset kinds the platform uses.
_LAYER_HINT = {"eod": "eod", "snapshot": "eod", "realtime": "realtime", "rt": "realtime",
               "stream": "stream", "mart": "mart", "curated": "curated",
               "full": "full_cdc", "cdc": "full_cdc", "raw": "full_cdc"}


#: Platform concepts the copilot is expected to be able to define. These are LAYER names,
#: not tables, and a layer name resolves to every table in that layer -- so "What is
#: FULL_CDC?" tied six ways and was refused as an ambiguous asset reference. It is not an
#: asset reference at all; it is a question about the architecture, and
#: `AI_COPILOT_SAMPLE_QUESTIONS.md` has promised an answer to it since AIGR2.
#:
#: Wording follows `docs/L3_SNAPSHOT.md` (ARCHITECTURE CORRECTION 2026-08-21): the layer
#: model is a FAN-OUT from FULL_CDC, not a chain. REALTIME and EOD are siblings. Getting
#: that backwards is the single most consequential misreading of this platform, because it
#: implies rebuilding EOD by first rebuilding REALTIME.
CONCEPTS: dict[str, str] = {
    "full_cdc": (
        "L1 FULL_CDC is the canonical layer, written directly from Kafka by decode and "
        "validate, append-only. It keeps the full history of every I/U/D with source and "
        "Kafka metadata, and it is the parent of both REALTIME and EOD. There is no raw "
        "landing layer beneath it."),
    "realtime": (
        "L2 REALTIME (STREAM) is a rolling window T-N to T, DERIVED from FULL_CDC. It is a "
        "sibling of EOD, not a step towards it: rebuilding REALTIME does not rebuild EOD "
        "and neither reads the other."),
    "eod": (
        "L3 EOD (SNAPSHOT) is one state per primary key as of T-1, derived from FULL_CDC by "
        "an explicit cutoff and a dedup that keeps the last event per PK by event_order. It "
        "is a sibling of REALTIME, built from the same parent."),
    "stream": (
        "STREAM is the older name for L2 REALTIME. It once meant the raw Kafka landing; it "
        "does not. It is a derived rolling window over FULL_CDC."),
    "curated": (
        "CURATED holds the modelled tables (dimensions and facts) built from EOD. It is "
        "downstream of the CDC layers and upstream of the marts."),
    "mart": (
        "MART holds the serving tables that reporting and Power BI read. Power BI never "
        "reads FULL_CDC or REALTIME -- only a mart or an approved serving view."),
    "event_order": (
        "event_order is the total ordering used to pick the last event per key: commit/change "
        "SCN on Oracle, commit/change LSN plus event serial number on SQL Server, then "
        "source_ts_ms, kafka_partition and kafka_offset as tie-breakers. kafka_offset only "
        "increases WITHIN one partition and is never a global clock."),
}

#: Only a question that ASKS FOR A DEFINITION. "rebuild FULL_CDC for 2026-09-28" names the
#: same word and must never be answered with a glossary entry instead of a plan.
_DEFINITIONAL = re.compile(
    r"^\s*(what\s+(is|are|does)\b|what'?s\b|define\b|explain\b|"
    r"tell me about\b|meaning of\b)", re.I)


def concept_for(question: str) -> tuple[str, str]:
    """(name, definition) when the question asks what a platform concept means, else ("","").

    Matched on the whole word so `realtime` does not fire on `realtime_account`.
    """
    if not _DEFINITIONAL.match(question or ""):
        return "", ""
    words = set(re.split(r"[^a-z0-9_]+", (question or "").lower()))
    for name, text in CONCEPTS.items():
        if name in words or name.replace("_", "") in words:
            return name, text
    return "", ""


def resolve_asset(phrase: str, *, environment: str = "dev") -> ResolvedAsset:
    """Resolve a human phrase to exactly ONE canonical asset.

    Scores every governed asset by token overlap and requires a *strict* winner. A tie is an
    ambiguity and raises -- the alternative is silently repairing whichever asset happened to
    sort first.
    """
    want = _tokens(phrase)
    if not want:
        raise ResolutionError("nothing to resolve")
    layer = next((v for k, v in _LAYER_HINT.items() if k in want), "")

    scored: list[tuple[int, object]] = []
    for ga in _inventory().assets:
        aid = ga.asset
        name = f"{aid.kind.value}:{aid.name}" if hasattr(aid.kind, "value") else str(aid)
        have = _tokens(name)
        score = len(want & have)
        if layer and layer in name.lower():
            score += 2
        if score:
            scored.append((score, ga))
    if not scored:
        raise ResolutionError(
            f"no governed asset matches {phrase!r}; refusing to guess an identifier")
    scored.sort(key=lambda s: -s[0])
    tied = [g for sc, g in scored if sc == scored[0][0]]
    if len(tied) > 1:
        names = [f"{g.asset.kind.value}:{g.asset.name}" for g in tied[:6]]
        # A tie whose candidates share one logical table name is not ambiguity about WHICH
        # table -- it is one table seen at several layers. Refusing to describe it is
        # unhelpful: the caller named the table exactly and simply did not say which layer.
        # Refusing to REPAIR it is still right, so the distinction is left to the caller.
        # Normalised, because the same logical table is spelled differently per layer:
        # `oracle_coredb_corebank_customer` at eod/full_cdc/realtime and
        # `oracle.coredb.corebank.customer` at src. Comparing raw names treats one table
        # as four different ones.
        if len({re.sub(r"[^a-z0-9]", "", g.asset.name.lower()) for g in tied}) == 1:
            raise LayerAmbiguity(
                f"{phrase!r} names one table that exists at several layers: {names}",
                candidates=tuple(_resolved(g) for g in tied))
        raise ResolutionError(
            f"{phrase!r} is ambiguous between {names}; refusing to choose. "
            "Name the layer or the exact table.")
    return _resolved(scored[0][1])


def _resolved(ga) -> ResolvedAsset:
    gov = ga.governance
    return ResolvedAsset(
        asset_id=f"{ga.asset.kind.value}:{ga.asset.name}", kind=ga.asset.kind.value,
        name=ga.asset.name, owner=getattr(gov, "owner", "") or "",
        domain=getattr(gov, "domain", "") or "",
        classification=getattr(getattr(gov, "classification", None), "value", "") or "")


def get_governance_context(asset_id: str) -> dict:
    for ga in _inventory().assets:
        if f"{ga.asset.kind.value}:{ga.asset.name}" == asset_id:
            return ga.payload()
    raise ResolutionError(f"no governed asset {asset_id!r}")


def get_dataset_lineage(asset_id: str, *, direction: str = "downstream",
                        max_hops: int = 6, environment: str = "dev") -> dict:
    """Table-level lineage from the real graph, bounded."""
    graph, svc = _graph_and_impact(environment)
    urn = _urn_for(asset_id, environment)
    result = svc.downstream(urn, max_hops=max_hops)
    return {"root": asset_id, "direction": direction, "max_hops": max_hops,
            "impacted": sorted(n.urn for n in result.nodes),
            "executable": sorted(n.urn for n in result.executable),
            "excluded": [{"asset": n.urn, "reason": "no executable producing job"}
                         for n in result.excluded]}


#: Where a recorded column-validation run lands. Confidence is read from this artifact, not
#: asserted: the file says WHEN the check ran and WHAT it confirmed, so "VALIDATED" always
#: has a date and a method behind it.
VALIDATION_ARTIFACT = (Path(__file__).resolve().parents[2]
                       / "artifacts" / "validation" / "data-reliability"
                       / "column-lineage-validation.json")


@lru_cache(maxsize=1)
def _validated_pairs() -> frozenset:
    """(table, column) pairs a real validation run confirmed exist.

    Confirmed means: a top-level Glue column, or a key genuinely present in that table's
    `payload_after` JSON in real rows. CDC layers store the source row as JSON, so a
    business column is invisible to a schema lookup while being present in every row --
    calling that "unvalidatable" would be wrong, and calling it "validated" without looking
    would be worse.
    """
    import json
    if not VALIDATION_ARTIFACT.exists():
        return frozenset()
    data = json.loads(VALIDATION_ARTIFACT.read_text())
    out = set()
    for v in data.get("verdicts", []):
        if v.get("validated"):
            out.add((v["upstream"], v["upstream_column"].lower()))
            out.add((v["downstream"], v["downstream_column"].lower()))
    return frozenset(out)


def _names_column(edge_column: str, wanted: str) -> bool:
    """Does `edge_column` actually go by the name `wanted`?

    Exact name, or one whole `_`-delimited segment of it, so a user who says "the BALANCE
    column" still reaches `closing_balance`.

    This was a naive substring test, and a substring test makes every short word a column
    name: "in" is inside clos*in*g_balance. "What does the BALANCE column IN EOD ACCOUNT
    feed?" therefore resolved `column in` with confidence DERIVED -- not ABSENT, so it
    looked like a real column whose lineage merely had not been validated yet, and the
    resolver stopped there and never tried BALANCE. A fragment is not a name.
    """
    ec, w = (edge_column or "").lower(), wanted.lower()
    return bool(ec) and (ec == w or w in ec.split("_"))


def get_column_lineage(asset_id: str, column: str, *, max_hops: int = 6,
                       environment: str = "dev") -> dict:
    """Column-level lineage WITH a confidence, because only VALIDATED may narrow a recovery.

    The graph's column edges are DECLARED in `reporting/curated/entities.yaml` and the dbt
    manifest. A declared mapping is `DERIVED` until something checks both of its endpoints
    against reality; `cdc/column_validation.py` does that check and records the result, and
    this function reads it.

    There were two ways to make column-narrowed recovery possible. One is to relabel the
    edges. The other is to confirm the columns are really there. This is the second.
    """
    graph, svc = _graph_and_impact(environment)
    urn = _urn_for(asset_id, environment)
    result = svc.downstream(urn, max_hops=max_hops, column=column)
    edges = [e for e in graph.column_edges()
             if _names_column(getattr(e, "upstream_column", ""), column)
             or _names_column(getattr(e, "downstream_column", ""), column)]
    if not edges:
        confidence, why = LineageConfidence.ABSENT, "no column edge mentions this column"
    else:
        table = asset_id.split(":", 1)[-1]
        hit = [p for p in _validated_pairs()
               if p[1] == column.lower() and table.lower() in p[0].lower()]
        if hit:
            confidence = LineageConfidence.VALIDATED
            why = (f"{hit[0][0]}.{column} confirmed against real data by "
                   f"cdc/column_validation.py")
        else:
            confidence = LineageConfidence.DERIVED
            why = ("declared in the graph but not confirmed by a validation run; "
                   "narrowing to this column is refused")
    return {"root": asset_id, "column": column,
            "confidence": confidence.value,
            "source": "cdc.lineage_graph column edges + cdc.column_validation",
            "impacted": sorted(n.urn for n in result.nodes),
            "column_edge_count": len(edges),
            "validated_pairs_known": len(_validated_pairs()),
            "note": why}


def _urn_for(asset_id: str, environment: str) -> str:
    graph, _ = _graph_and_impact(environment)
    for n in graph.nodes:
        urn = getattr(n, "urn", str(n))
        if asset_id.split(":", 1)[-1].lower() in urn.lower():
            return urn
    raise ResolutionError(f"{asset_id!r} has no node in the lineage graph")


def graph_stats(environment: str = "dev") -> dict:
    graph, svc = _graph_and_impact(environment)
    return {"nodes": len(graph.nodes), "edges": len(graph.edges),
            "column_edges": len(graph.column_edges()),
            "registered_jobs": list(svc.registered_jobs)}
