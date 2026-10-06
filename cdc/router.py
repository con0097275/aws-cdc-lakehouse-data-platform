"""Route a normalized CDC event to the ONE Iceberg target its config names.

    normalized event  ->  canonical table_id  ->  resolved config  ->  FULL_CDC target

Pure, stdlib-only and driven by the COMPILED PLAN rather than the YAML registry. Both
properties are load-bearing:

* the writer runs on EMR Serverless, where `yaml` is not guaranteed on the path and where
  re-reading the registry would mean routing against a config nobody hashed;
* routing decided by a dict lookup can be tested exhaustively on a laptop, which is where
  the interesting cases (unknown table, disabled table, a topic and an envelope that
  disagree) are cheap to construct and expensive to reproduce live.

WHAT THIS REFUSES TO DO, AND WHY (phase brief sections E and F)
---------------------------------------------------------------
An event whose table is not in the registry is NOT written anywhere. Not to a default table,
not to a table named after the topic, and above all not to a NEW table created on the spot.
A typo in a topic name, an SMT that rewrites a route, or a connector pointed at the wrong
schema would otherwise mint a production data product that no config describes, no owner
owns, no retention covers and no DQ rule checks -- and it would look like success in every
log line. The unknown event goes to quarantine (default) or fails the batch, and either way
it increments a metric that names the table it claimed to be.

Tables are provisioned from Git config first (`python3 -m cdc.provision`), then captured.
That order is the whole control.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterable

from .models import ConfigError


class MigrationMode(str, Enum):
    """Which storage layout a run writes. Section H: the DEFAULT must not change what
    production does today, so it is LEGACY_ONLY until a cutover is deliberately run."""
    LEGACY_ONLY = "legacy_only"
    DUAL_WRITE = "dual_write"
    PER_TABLE_ONLY = "per_table_only"


class UnknownTablePolicy(str, Enum):
    """What to do with an event whose table is not registered.

    QUARANTINE keeps the record with its coordinates and its payload, so it can be replayed
    once the table is registered -- the same treatment a poison record gets (CLAUDE.md 5.10),
    for the same reason: a record we cannot place is worse lost than kept.

    REJECT fails the batch instead. Correct when an unregistered table means something
    upstream is misconfigured and continuing would bank more of it -- but it stops the
    OTHER seven tables too, so it is not the default.
    """
    QUARANTINE = "quarantine"
    REJECT = "reject"


class RouteOutcome(str, Enum):
    ROUTED = "routed"
    UNKNOWN_TABLE = "unknown_table"
    DISABLED_TABLE = "disabled_table"
    #: The topic and the envelope name different source tables. Not a routing miss -- a
    #: sign that something rewrote the route, which is exactly the case where guessing is
    #: most expensive.
    AMBIGUOUS_IDENTITY = "ambiguous_identity"


@dataclass(frozen=True)
class NormalizedEvent:
    """The identity of one CDC record, before any target is chosen.

    Every field is optional except that SOMETHING must identify the table: the envelope's
    own `source.db/schema/table`, or the topic it arrived on. Debezium provides both, and
    when they disagree the router says so instead of picking one.
    """
    topic: str | None = None
    source_system: str | None = None
    source_database: str | None = None
    source_schema: str | None = None
    source_table: str | None = None

    @property
    def identity(self) -> str | None:
        parts = (self.source_system, self.source_database, self.source_schema,
                 self.source_table)
        if not all(parts):
            return None
        return ".".join(p.strip().lower().replace("-", "_").replace(".", "_")
                        for p in parts)              # type: ignore[union-attr]

    def describe(self) -> str:
        return self.identity or self.topic or "<no identity>"


@dataclass(frozen=True)
class Route:
    """The routing DECISION. `entry` and `target` are present only when ROUTED."""
    outcome: RouteOutcome
    claimed: str
    entry: dict[str, Any] | None = None
    reason: str = ""

    @property
    def routed(self) -> bool:
        return self.outcome is RouteOutcome.ROUTED

    @property
    def table_id(self) -> str | None:
        return self.entry["table_id"] if self.entry else None

    def target(self, layer: str = "FULL_CDC") -> str:
        if not self.entry:
            raise ConfigError(
                f"{self.claimed}: no target -- this event was {self.outcome.value}. "
                f"{self.reason}")
        ident = (self.entry.get("targets") or {}).get(f"{layer.lower()}_identifier")
        if not ident:
            raise ConfigError(f"{self.table_id}: the plan carries no {layer} identifier; "
                              f"recompile it with `python3 -m cdc.compile`")
        return ident


@dataclass
class RouterMetrics:
    """Counters, not logs. An unknown table that only appears in stdout is a defect nobody
    alerts on; a counter is something a run can assert against and a dashboard can show."""
    by_outcome: dict[str, int] = field(default_factory=dict)
    by_table: dict[str, int] = field(default_factory=dict)
    unknown_claims: dict[str, int] = field(default_factory=dict)

    def record(self, route: Route, n: int = 1) -> None:
        self.by_outcome[route.outcome.value] = self.by_outcome.get(
            route.outcome.value, 0) + n
        key = route.table_id or route.claimed
        self.by_table[key] = self.by_table.get(key, 0) + n
        if route.outcome is not RouteOutcome.ROUTED:
            self.unknown_claims[route.claimed] = self.unknown_claims.get(
                route.claimed, 0) + n

    @property
    def total(self) -> int:
        return sum(self.by_outcome.values())

    @property
    def unrouted(self) -> int:
        return self.total - self.by_outcome.get(RouteOutcome.ROUTED.value, 0)

    def lines(self) -> list[str]:
        """One grep-able line per counter. The prefix matches the platform's existing
        convention (`FULL_CDC_*`, `REALTIME_*`) so a run's routing shows up in the same
        log search as everything else it did."""
        out = [f"CDC_ROUTER_TOTAL {self.total}",
               f"CDC_ROUTER_UNROUTED {self.unrouted}"]
        out += [f"CDC_ROUTER_OUTCOME {k} {v}" for k, v in sorted(self.by_outcome.items())]
        out += [f"CDC_ROUTER_TABLE {k} {v}" for k, v in sorted(self.by_table.items())]
        out += [f"CDC_ROUTER_UNKNOWN_CLAIM {k} {v}"
                for k, v in sorted(self.unknown_claims.items())]
        return out


class TableRouter:
    """Built once per run from a compiled plan; consulted per topic or per event."""

    #: The OLDEST plan schema this router can route against. An older plan has no qualified
    #: identifiers, so routing against it would resolve a BARE table name against whatever
    #: database the session defaulted to -- a silently wrong target, which is the failure
    #: mode this whole module exists to make impossible.
    #:
    #: A MINIMUM, not an equality. This was `!=` and it made every additive schema bump a
    #: router change: schema 7 added an `ingestion` block the router never reads, and 15
    #: tests failed on a plan that was in every way routable. The gate should refuse a plan
    #: that is missing something needed, not one that carries something extra.
    MINIMUM_PLAN_SCHEMA = 6

    def __init__(self, entries: Iterable[dict], *, config_version: str = "",
                 event_index: dict | None = None,
                 policy: UnknownTablePolicy = UnknownTablePolicy.QUARANTINE):
        self.config_version = config_version
        self.policy = policy
        self.event_index = dict(event_index or {})
        self._by_id: dict[str, dict] = {}
        self._by_topic: dict[str, dict] = {}
        for entry in entries:
            tid = entry["table_id"]
            if tid in self._by_id:
                raise ConfigError(f"duplicate table_id {tid!r} in the compiled plan")
            self._by_id[tid] = entry
            topic = (entry.get("capture") or {}).get("topic")
            if topic:
                if topic in self._by_topic:
                    raise ConfigError(
                        f"topic {topic!r} is claimed by two tables: "
                        f"{self._by_topic[topic]['table_id']} and {tid}. One of them would "
                        f"silently receive the other's events")
                self._by_topic[topic] = entry

    @classmethod
    def from_plan(cls, plan: dict,
                  policy: UnknownTablePolicy = UnknownTablePolicy.QUARANTINE
                  ) -> "TableRouter":
        payload = plan.get("plan") or plan
        version = payload.get("plan_schema_version")
        if not isinstance(version, int) or version < cls.MINIMUM_PLAN_SCHEMA:
            raise ConfigError(
                f"compiled plan is schema version {version!r}, this router needs at least "
                f"{cls.MINIMUM_PLAN_SCHEMA}. Recompile: python3 -m cdc.compile --out "
                f"artifacts/cdc/table-plan.json")
        return cls(payload.get("tables") or [],
                   config_version=plan.get("config_version", ""),
                   event_index=payload.get("event_index"), policy=policy)

    # -- lookups ----------------------------------------------------------- #

    @property
    def table_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_id))

    @property
    def topics(self) -> tuple[str, ...]:
        """Every topic the registry expects, enabled ones only -- this is what a job's
        `--topics` should be rendered from rather than typed."""
        return tuple(sorted(t for t, e in self._by_topic.items()
                            if e.get("enabled", True)))

    def entry(self, table_id: str) -> dict | None:
        return self._by_id.get(table_id)

    # -- routing ----------------------------------------------------------- #

    def route(self, event: NormalizedEvent) -> Route:
        identity, topic = event.identity, event.topic
        by_topic = self._by_topic.get(topic) if topic else None
        by_id = self._by_id.get(identity) if identity else None

        if identity and topic and by_topic is not None and by_id is not None \
                and by_topic["table_id"] != by_id["table_id"]:
            return Route(
                RouteOutcome.AMBIGUOUS_IDENTITY, claimed=f"{topic} / {identity}",
                reason=(f"topic {topic!r} is registered to {by_topic['table_id']} but the "
                        f"envelope says {by_id['table_id']}. Something rewrote the route; "
                        f"picking either one would write an event into a table that is not "
                        f"its source"))

        entry = by_id or by_topic
        claimed = event.describe()
        if entry is None:
            return Route(
                RouteOutcome.UNKNOWN_TABLE, claimed=claimed,
                reason=(f"{claimed} is not in the registry. Register it in "
                        f"cdc/registry/sources.yaml and provision it "
                        f"(`python3 -m cdc.provision`) BEFORE it is captured -- this "
                        f"platform never creates a table because an event appeared"))
        if not entry.get("enabled", True):
            return Route(
                RouteOutcome.DISABLED_TABLE, claimed=claimed, entry=None,
                reason=(f"{entry['table_id']} is registered but `enabled: false`. Its "
                        f"events are held, not written: re-enabling and rerunning the "
                        f"window is a decision, and dropping them would make it "
                        f"irreversible"))
        return Route(RouteOutcome.ROUTED, claimed=claimed, entry=entry)

    def route_topic(self, topic: str) -> Route:
        """Convenience for the per-topic loop both ingest jobs already have."""
        return self.route(NormalizedEvent(topic=topic))


@dataclass(frozen=True)
class WritePlan:
    """Which writes a run performs. Derived from the mode ONCE, so no caller decides it
    with an `if mode ==` of its own."""
    legacy: bool
    per_table: bool
    event_index: bool

    def describe(self) -> str:
        parts = [n for n, on in (("legacy", self.legacy), ("per_table", self.per_table),
                                 ("event_index", self.event_index)) if on]
        return "+".join(parts) or "none"


def writes_for(mode: MigrationMode, *, event_index: bool = False) -> WritePlan:
    """LEGACY_ONLY is the default everywhere and writes exactly what the platform writes
    today: the monolith, and nothing else. The other two modes are cutover states, entered
    deliberately and reversibly (ADR-063)."""
    if mode is MigrationMode.LEGACY_ONLY:
        return WritePlan(legacy=True, per_table=False, event_index=False)
    if mode is MigrationMode.DUAL_WRITE:
        return WritePlan(legacy=True, per_table=True, event_index=event_index)
    return WritePlan(legacy=False, per_table=True, event_index=event_index)


def parse_mode(raw: str) -> MigrationMode:
    try:
        return MigrationMode(str(raw).strip().lower())
    except ValueError:
        allowed = ", ".join(m.value for m in MigrationMode)
        raise ConfigError(f"migration mode {raw!r} is not one of: {allowed}") from None


def parse_policy(raw: str) -> UnknownTablePolicy:
    try:
        return UnknownTablePolicy(str(raw).strip().lower())
    except ValueError:
        allowed = ", ".join(p.value for p in UnknownTablePolicy)
        raise ConfigError(
            f"unknown-table policy {raw!r} is not one of: {allowed}") from None
