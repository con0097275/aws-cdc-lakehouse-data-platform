"""Source-table lifecycle: the states a table moves through while being onboarded.

    SCAFFOLD -> VALIDATE -> PLAN -> APPROVE -> APPLY -> SNAPSHOT/CATCH-UP -> VALIDATE -> ACTIVE

A DEDICATED vocabulary, not Airflow's. An Airflow task status describes one execution of one
task: it is SUCCESS the moment the task returns, and it is gone when the DAG run is cleaned
up. A source table's lifecycle is a property of the TABLE -- it survives every run, it has
states no task has (`CAPTURING`, `PAUSED`, `DECOMMISSIONING`), and "the provisioning task
succeeded" is not the same claim as "this table is provisioned". Overloading one onto the
other is how a table that was never activated comes to look active because a retry passed.

NOTHING HERE MUTATES ANYTHING. The store records what happened; the commands that actually
change the world are separate, gated, and write their evidence back here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .models import ConfigError

# --------------------------------------------------------------------------- #
# the states (section G)
# --------------------------------------------------------------------------- #

DRAFT = "DRAFT"                       # registered in Git, nothing checked
VALIDATED = "VALIDATED"               # config compiles AND the source precheck passed
PLANNED = "PLANNED"                   # the capture diff and target plan have been read
PROVISIONED = "PROVISIONED"           # FULL_CDC / REALTIME / EOD targets exist
CAPTURING = "CAPTURING"               # the connector carries the table
BACKFILLING = "BACKFILLING"           # a snapshot is in progress
VALIDATING = "VALIDATING"             # catch-up checks and smoke tests are running
ACTIVE = "ACTIVE"                     # accepted; downstream may rely on it
PAUSED = "PAUSED"                     # deliberately stopped, still registered
DECOMMISSIONING = "DECOMMISSIONING"   # being removed; data retained
FAILED = "FAILED"                     # a step failed; needs a person

STATES = (DRAFT, VALIDATED, PLANNED, PROVISIONED, CAPTURING, BACKFILLING,
          VALIDATING, ACTIVE, PAUSED, DECOMMISSIONING, FAILED)

#: The happy path, in order. Used to report "what is the next step for this table".
HAPPY_PATH = (DRAFT, VALIDATED, PLANNED, PROVISIONED, CAPTURING, BACKFILLING,
              VALIDATING, ACTIVE)

#: Legal transitions. A closed set on purpose: an onboarding that can jump from DRAFT to
#: ACTIVE is not a workflow, it is a spelling of "trust me". Every arrow below is one a
#: command has to earn with evidence.
TRANSITIONS: dict[str, tuple[str, ...]] = {
    DRAFT: (VALIDATED, FAILED),
    VALIDATED: (PLANNED, DRAFT, FAILED),
    PLANNED: (PROVISIONED, VALIDATED, FAILED),
    PROVISIONED: (CAPTURING, PLANNED, FAILED),
    # BACKFILLING is SKIPPABLE: `changes_only` onboarding has no snapshot to wait for, so
    # CAPTURING -> VALIDATING is legal. Forcing a backfill state for a mode that does not
    # backfill would make the ledger lie about what happened.
    CAPTURING: (BACKFILLING, VALIDATING, PAUSED, FAILED),
    BACKFILLING: (VALIDATING, PAUSED, FAILED),
    VALIDATING: (ACTIVE, FAILED, PAUSED),
    ACTIVE: (PAUSED, DECOMMISSIONING, VALIDATING),
    PAUSED: (CAPTURING, VALIDATING, DECOMMISSIONING, FAILED),
    DECOMMISSIONING: (PAUSED, FAILED),
    # FAILED rejoins at the step that failed, never at ACTIVE: recovering from a failure
    # means redoing the work, not declaring it done.
    FAILED: (DRAFT, VALIDATED, PLANNED, PROVISIONED, CAPTURING, BACKFILLING, VALIDATING),
}

#: States in which the table's events may be relied on by anything downstream.
TRUSTED_STATES = (ACTIVE,)


def next_step(state: str) -> str | None:
    """The state a table should reach next on the happy path, or None at the end."""
    if state not in HAPPY_PATH:
        return None
    i = HAPPY_PATH.index(state)
    return HAPPY_PATH[i + 1] if i + 1 < len(HAPPY_PATH) else None


def can_transition(current: str, target: str) -> bool:
    return target in TRANSITIONS.get(current, ())


# --------------------------------------------------------------------------- #
# the store
# --------------------------------------------------------------------------- #

@dataclass
class TableLifecycle:
    """One table's state, plus every transition that produced it."""
    table_id: str
    state: str = DRAFT
    history: list = field(default_factory=list)

    def record(self, target: str, *, reason: str, evidence: dict | None = None,
               at: datetime | None = None, force: bool = False) -> "TableLifecycle":
        """Move to `target`, refusing an illegal jump.

        `evidence` is the point of the whole store. "This table is ACTIVE" is a claim, and a
        claim with no attached run id, config version and check result is one nobody can
        audit six weeks later when a number looks wrong.
        """
        if target not in STATES:
            raise ConfigError(f"{self.table_id}: {target!r} is not a lifecycle state. "
                              f"Known: {', '.join(STATES)}")
        if not force and not can_transition(self.state, target):
            allowed = ", ".join(TRANSITIONS.get(self.state, ())) or "(none)"
            raise ConfigError(
                f"{self.table_id}: cannot go {self.state} -> {target}. Legal from "
                f"{self.state}: {allowed}. An onboarding that can jump straight to ACTIVE "
                f"is not a workflow.")
        self.history.append({
            "from": self.state, "to": target, "reason": reason,
            "at": (at or datetime.now(timezone.utc)).isoformat(),
            "evidence": evidence or {},
        })
        self.state = target
        return self

    def payload(self) -> dict:
        return {"table_id": self.table_id, "state": self.state, "history": self.history}

    @classmethod
    def from_payload(cls, raw: dict) -> "TableLifecycle":
        return cls(table_id=raw["table_id"], state=raw.get("state", DRAFT),
                   history=list(raw.get("history") or []))


DEFAULT_STORE = Path("artifacts/cdc/lifecycle.json")


class LifecycleStore:
    """A JSON file, deliberately.

    It is readable with the platform torn down, diffable in review, and needs no AWS to
    answer "what state is this table in?" -- which is the question an operator asks first and
    most often. It is NOT the system of record for the DATA; it records the onboarding of a
    table, and losing it costs an audit trail rather than a dataset.
    """

    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else DEFAULT_STORE
        self._tables: dict[str, TableLifecycle] = {}
        self.load()

    def load(self) -> "LifecycleStore":
        if self.path.exists():
            raw = json.loads(self.path.read_text() or "{}")
            self._tables = {k: TableLifecycle.from_payload(v)
                            for k, v in (raw.get("tables") or {}).items()}
        return self

    def save(self) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema": 1,
            "note": ("Source-table onboarding lifecycle (ADR-066). Written by the "
                     "cdc-table-* commands; not hand-edited."),
            "tables": {k: v.payload() for k, v in sorted(self._tables.items())},
        }
        self.path.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n")
        return self.path

    def get(self, table_id: str) -> TableLifecycle:
        return self._tables.setdefault(table_id, TableLifecycle(table_id=table_id))

    def state(self, table_id: str) -> str:
        return self.get(table_id).state

    def all(self) -> tuple:
        return tuple(self._tables[k] for k in sorted(self._tables))

    def record(self, table_id: str, target: str, *, reason: str,
               evidence: dict | None = None, force: bool = False) -> TableLifecycle:
        entry = self.get(table_id).record(target, reason=reason, evidence=evidence,
                                          force=force)
        self.save()
        return entry
