"""Per-table cutover: which physical table a consumer reads, per table, per layer.

    logical layer + table_id  ->  cutover mode  ->  physical database + physical table

NOT A BIG BANG (section G). The mode is per TABLE, with rollout GROUPS as a convenience for
moving several at once. A platform-wide switch would make the blast radius of a wrong answer
every consumer of every table simultaneously, and the whole argument for a pilot is that the
blast radius should be one table you chose on purpose.

THE LEGACY TABLE IS NEVER DROPPED (section A). Every mode below can read it, `LEGACY` is the
default, and `PER_TABLE` does not delete anything -- it stops READING the monolith. That is
what makes rollback a flag flip rather than a restore: ADR-062 already fixed the rule that
the monolith stays as the reconciliation baseline for every window already captured.

WHY THIS IS NOT `spark/reporting/source_resolver.py`
-----------------------------------------------------
That module resolves `logical layer -> database` for a table name a caller already knows, and
it is imported through `sys.path` with FLAT module names (`from models import ...`). Importing
it from this package is the shadowing defect `cdc/__init__.py` documents. This module answers
a different question -- which PHYSICAL TABLE a canonical table_id maps to under the current
cutover mode -- and reads the same `reporting/layers.yaml` through `cdc/catalog.py`, so the
layer->database mapping still lives in exactly one file (ADR-033, section F).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .models import ConfigError

# --------------------------------------------------------------------------- #
# section G -- the flag
# --------------------------------------------------------------------------- #

#: Read the monolith. The default, and what every consumer does today.
LEGACY = "LEGACY"
#: Both are written; the MONOLITH is still the read path. This is the validation state:
#: divergence is detectable because both sides exist, and no consumer is at risk yet.
DUAL = "DUAL"
#: Read the per-table target. The monolith is still written (or not) but no longer read.
PER_TABLE = "PER_TABLE"

MODES = (LEGACY, DUAL, PER_TABLE)

#: Which physical table a READER gets. DUAL deliberately reads LEGACY: a mode where writes
#: go both ways but reads follow the new path would put consumers on unvalidated data during
#: the very window whose purpose is to validate it.
READ_SOURCE = {LEGACY: LEGACY, DUAL: LEGACY, PER_TABLE: PER_TABLE}


def normalise_mode(raw: str, *, what: str) -> str:
    value = str(raw).strip().upper()
    if value not in MODES:
        raise ConfigError(f"{what}: cutover mode {raw!r} is not one of "
                          f"{', '.join(MODES)}")
    return value


@dataclass(frozen=True)
class ResolvedTable:
    """Where a consumer should actually read, and why."""
    table_id: str
    layer: str
    mode: str
    database: str
    table: str
    legacy: bool
    #: Present when reading the monolith: the monolith holds every source table, so a
    #: consumer MUST filter. Returning the predicate with the table means a caller cannot
    #: forget it -- forgetting it reads eight tables' events as if they were one.
    required_predicate: str | None = None

    @property
    def identifier(self) -> str:
        return f"{self.database}.{self.table}"

    def describe(self) -> str:
        where = f" WHERE {self.required_predicate}" if self.required_predicate else ""
        return f"{self.table_id} [{self.layer}] {self.mode} -> {self.identifier}{where}"


#: The monolith, per layer. These are the tables that existed before per-table storage and
#: that ADR-062 says are never dropped.
LEGACY_TABLES = {
    "FULL_CDC": "cdc_events",
    "REALTIME": "cdc_events_realtime",
    "EOD": "fact_account_daily_snapshot",
}


@dataclass
class CutoverState:
    """Per-table cutover modes, plus the groups used to move several at once."""
    modes: dict
    groups: dict

    def mode_for(self, table_id: str) -> str:
        """A table's own mode, or its group's, or LEGACY.

        LEGACY is the default for an UNKNOWN table too, deliberately: a table nobody has
        made a decision about must not be silently migrated by adding it to the registry.
        """
        if table_id in self.modes:
            return self.modes[table_id]
        for group, spec in sorted(self.groups.items()):
            if table_id in (spec.get("tables") or ()):
                return normalise_mode(spec.get("mode", LEGACY), what=f"group {group}")
        return LEGACY

    def tables_in(self, mode: str) -> tuple:
        wanted = normalise_mode(mode, what="tables_in")
        out = {t for t, m in self.modes.items() if m == wanted}
        for spec in self.groups.values():
            if normalise_mode(spec.get("mode", LEGACY), what="group") == wanted:
                out.update(spec.get("tables") or ())
        return tuple(sorted(out))

    def payload(self) -> dict:
        return {"modes": dict(sorted(self.modes.items())), "groups": self.groups}


DEFAULT_STATE_FILE = Path("cdc/registry/cutover.yaml")


def load_state(path: Path | None = None) -> CutoverState:
    """Read the cutover state. An ABSENT file means every table is LEGACY.

    Absent-means-legacy is the safe default: the file not existing must not be a silent
    migration, and a fresh clone of this repository must behave exactly like production.
    """
    path = Path(path) if path else DEFAULT_STATE_FILE
    if not path.exists():
        return CutoverState(modes={}, groups={})
    import yaml
    raw = yaml.safe_load(path.read_text()) or {}
    modes = {k: normalise_mode(v, what=f"{path}: modes.{k}")
             for k, v in (raw.get("modes") or {}).items()}
    groups = raw.get("groups") or {}
    for name, spec in groups.items():
        normalise_mode(spec.get("mode", LEGACY), what=f"{path}: groups.{name}.mode")
        if not spec.get("tables"):
            raise ConfigError(f"{path}: group {name!r} lists no tables. An empty rollout "
                              f"group moves nothing and reads as though it did")
    # A table in two groups with different modes has no defined answer, and picking one
    # silently is how a consumer ends up on a path nobody chose.
    seen: dict[str, str] = {}
    for name, spec in sorted(groups.items()):
        for t in spec.get("tables") or ():
            if t in seen and seen[t] != name:
                raise ConfigError(
                    f"{path}: {t} is in both group {seen[t]!r} and group {name!r}. "
                    f"Two groups can disagree about its mode; put it in one")
            seen[t] = name
    return CutoverState(modes=modes, groups=groups)


class CutoverResolver:
    """Resolve (layer, table_id) -> physical table, honouring the per-table cutover mode."""

    LAYERS = ("FULL_CDC", "REALTIME", "EOD")

    def __init__(self, plan: dict, state: CutoverState, catalog=None):
        from .catalog import Catalog

        payload = plan.get("plan") or plan
        self._entries = {e["table_id"]: e for e in payload.get("tables") or []}
        self.state = state
        cat_payload = payload.get("catalog")
        self.catalog = catalog or (Catalog.from_payload(cat_payload) if cat_payload
                                   else Catalog.from_layers_file())

    def resolve(self, table_id: str, layer: str = "FULL_CDC") -> ResolvedTable:
        if layer not in self.LAYERS:
            raise ConfigError(f"{layer!r} is not a cutover-aware layer; expected one of "
                              f"{', '.join(self.LAYERS)}")
        entry = self._entries.get(table_id)
        if entry is None:
            raise ConfigError(
                f"{table_id} is not in the compiled plan. A consumer cannot resolve a table "
                f"the registry does not describe -- register it and recompile rather than "
                f"reading a name that resolves to nothing")
        mode = self.state.mode_for(table_id)
        database = self.catalog.database(layer)

        if READ_SOURCE[mode] == LEGACY:
            source = entry.get("source") or {}
            # The monolith holds every source table, so the predicate is MANDATORY and is
            # returned with the table. A consumer that forgets it reads eight tables' events
            # as one, and the result is a plausible number rather than an error.
            #
            # BOTH SIDES ARE UPPERCASED, and that is not decoration. `source_table` is
            # written by `full_cdc/job.py` as the last segment of the TOPIC, so it carries
            # the engine's own spelling: Oracle folds unquoted identifiers to UPPERCASE
            # (`ACCOUNT`) and SQL Server does not (`digital_event`). An earlier version of
            # this line uppercased only the literal, which matched every Oracle table and
            # NO SQL Server one -- the legacy read returned zero rows, with no error, and the
            # benchmark scored it as "0 bytes scanned, very fast". Measured live before it
            # reached a consumer. Comparing `upper()` on both sides is correct for either
            # engine and is what the backfill already did.
            predicate = (f"source_system = '{source.get('source_id')}' "
                         f"AND upper(source_table) = "
                         f"'{str(source.get('table', '')).upper()}'")
            return ResolvedTable(
                table_id=table_id, layer=layer, mode=mode, database=database,
                table=LEGACY_TABLES[layer], legacy=True, required_predicate=predicate)

        target = (entry.get("targets") or {}).get(f"{layer.lower()}_identifier")
        if not target:
            raise ConfigError(f"{table_id}: the plan carries no {layer} identifier; "
                              f"recompile it")
        # The identifier is `catalog.database.table`; the resolver returns the parts so a
        # caller can qualify it however its engine needs.
        parts = target.split(".")
        return ResolvedTable(
            table_id=table_id, layer=layer, mode=mode, database=parts[-2],
            table=parts[-1], legacy=False)

    def summary(self) -> list:
        return [self.resolve(t) for t in sorted(self._entries)]


def rollout_plan(plan: dict, state: CutoverState) -> list:
    """Section K: the remaining tables, ordered by the risk of moving them.

    Ordered ASCENDING by risk, so the next table to migrate is the top of the list. Risk is
    a coarse band and deliberately not a score: the inputs are a row estimate nobody has
    measured yet and a classification, and a decimal number computed from those would look
    like a measurement.
    """
    payload = plan.get("plan") or plan
    out = []
    for entry in sorted(payload.get("tables") or [], key=lambda e: e["table_id"]):
        tid = entry["table_id"]
        gov = entry.get("governance") or {}
        rt = (entry.get("realtime_policy") or {}).get("enabled", True)
        dq = entry.get("dq") or {}
        reasons = []
        risk = 0
        if gov.get("classification") in ("confidential", "restricted"):
            risk += 2
            reasons.append(f"classification={gov.get('classification')}")
        if rt:
            risk += 1
            reasons.append("realtime enabled (a live window to rebuild)")
        if int(dq.get("freshness_sla_minutes", 60)) <= 15:
            risk += 2
            reasons.append("tight freshness SLA (high change volume)")
        band = "LOW" if risk <= 1 else ("MEDIUM" if risk <= 3 else "HIGH")
        out.append({"table_id": tid, "mode": state.mode_for(tid), "risk": band,
                    "score": risk, "reasons": reasons})
    return sorted(out, key=lambda e: (e["score"], e["table_id"]))
