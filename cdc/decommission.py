"""Safe table offboarding (section G), and the governance surface (sections D, E).

DECOMMISSION IS A SEQUENCE, AND ITS ORDER IS THE SAFETY
--------------------------------------------------------
    disable downstream -> stop capture -> freeze final state -> retention decision
    -> archive/document -> remove capture (approval)

Stopping capture first would leave consumers reading a table that has silently stopped
advancing: every query still succeeds and every number is quietly stale. Disabling the
consumers first makes the staleness visible as an absence instead of as a wrong answer.

**NEVER DELETES HISTORY.** Not one step here removes data, and `remove capture` means
removing the table from `table.include.list` -- the Iceberg tables keep every row. That is
the same rule ADR-062 fixed for the monolith: a decommissioned table is still the only
record of what happened while it was live, and the moment you need it is the moment someone
asks about a period it covered.
"""
from __future__ import annotations

from dataclasses import dataclass

from .lifecycle import ACTIVE, DECOMMISSIONING, PAUSED

# --------------------------------------------------------------------------- #
# section G -- the steps
# --------------------------------------------------------------------------- #

DISABLE_DOWNSTREAM = "disable_downstream"
STOP_CAPTURE = "stop_capture"
FREEZE_FINAL_STATE = "freeze_final_state"
RETENTION_DECISION = "retention_decision"
ARCHIVE_DOCUMENT = "archive_document"
REMOVE_CAPTURE = "remove_capture"

STEPS = (DISABLE_DOWNSTREAM, STOP_CAPTURE, FREEZE_FINAL_STATE, RETENTION_DECISION,
         ARCHIVE_DOCUMENT, REMOVE_CAPTURE)

#: Steps that need an explicit human approval before they may be performed.
NEEDS_APPROVAL = (REMOVE_CAPTURE,)


@dataclass(frozen=True)
class Step:
    name: str
    what: str
    why: str
    destructive: bool = False
    needs_approval: bool = False


PLAN: tuple = (
    Step(DISABLE_DOWNSTREAM,
         "set every consumer of this table to stop reading it",
         "stopping capture first leaves consumers reading a table that has silently "
         "stopped advancing: every query succeeds and every number is quietly stale"),
    Step(STOP_CAPTURE,
         "pause the table in the registry (`enabled: false`) and re-plan the connector",
         "the events stop arriving; nothing already captured is touched"),
    Step(FREEZE_FINAL_STATE,
         "record the final row count, the last dv_event_id and the last source position",
         "this is the number every later question about the table is answered against, and "
         "it cannot be recovered once the source is gone"),
    Step(RETENTION_DECISION,
         "record how long the data is kept and who decided",
         "a table with no retention decision is kept forever by default, which is a "
         "decision nobody made"),
    Step(ARCHIVE_DOCUMENT,
         "write the decommission record: owner, reason, final state, retention, date",
         "in six months the only question anyone asks is 'what happened to this table', and "
         "the answer must not be institutional memory"),
    Step(REMOVE_CAPTURE,
         "remove the table from the connector's table.include.list",
         "the last step and the only one that touches the running connector. Requires "
         "explicit approval, and still deletes NO data",
         needs_approval=True),
)


@dataclass(frozen=True)
class DecommissionCheck:
    step: str
    complete: bool
    detail: str


def plan_decommission(entry: dict, *, lifecycle_state: str,
                      evidence: dict | None = None) -> tuple:
    """Which offboarding steps are done, and which is next.

    `evidence` maps step name -> whatever proves it. A step with no evidence is NOT done --
    the same rule the cutover gate applies, for the same reason.
    """
    evidence = evidence or {}
    out = []
    for step in PLAN:
        got = evidence.get(step.name)
        if got is None:
            out.append(DecommissionCheck(
                step.name, False,
                f"not recorded. {step.what}" +
                ("  [needs explicit approval]" if step.needs_approval else "")))
        elif got is False:
            out.append(DecommissionCheck(step.name, False, "explicitly refused"))
        else:
            out.append(DecommissionCheck(step.name, True, f"recorded: {got!r}"))
    return tuple(out)


def may_remove_capture(checks: tuple, *, approved: bool) -> tuple:
    """(allowed, reason). The last step, gated on every earlier one AND on approval."""
    incomplete = [c.step for c in checks
                  if not c.complete and c.step != REMOVE_CAPTURE]
    if incomplete:
        return False, (f"these steps are not recorded: {', '.join(incomplete)}. Removing "
                       f"capture first is what leaves a consumer reading a table that "
                       f"stopped advancing")
    if not approved:
        return False, ("removing a table from table.include.list stops capture on a live "
                       "connector; it needs its own explicit approval (section G)")
    return True, "every prior step recorded and removal approved"


def next_state(lifecycle_state: str) -> str:
    """The lifecycle state a decommission moves a table into.

    ACTIVE -> DECOMMISSIONING directly; a PAUSED table is already out of the trusted set and
    can enter the same way. Anything else is not a table anyone is reading, so decommission
    is a no-op on its state.
    """
    return DECOMMISSIONING if lifecycle_state in (ACTIVE, PAUSED) else lifecycle_state


# --------------------------------------------------------------------------- #
# section D -- the governance surface every table must expose
# --------------------------------------------------------------------------- #

#: (field, where it comes from in the compiled plan). Checked as a SET so a new required
#: field fails loudly for every table rather than silently for none.
GOVERNANCE_FIELDS = {
    "owner": ("governance", "owner"),
    "domain": ("governance", "domain"),
    "classification": ("governance", "classification"),
    "retention_days": ("eod_policy", "retention_days"),
    "sla_minutes": ("dq", "freshness_sla_minutes"),
    "schema_evolution": (None, "schema_evolution"),
}


def governance_of(entry: dict) -> dict:
    """The governance record for one table, flattened.

    `pii` is DERIVED from the classification rather than being a second field that can
    disagree with it. Two fields meaning the same thing is how a table ends up `pii: false`
    and `classification: confidential` at once, and nobody can say which one the deny list
    should believe.
    """
    out = {}
    for name, (section, key) in GOVERNANCE_FIELDS.items():
        source = entry.get(section) if section else entry
        out[name] = (source or {}).get(key)
    out["pii"] = out.get("classification") in ("confidential", "restricted")
    dq = entry.get("dq") or {}
    out["dq"] = {"not_null": list(dq.get("not_null") or ()),
                 "event_date_null_tolerance": dq.get("event_date_null_tolerance"),
                 "freshness_sla_minutes": dq.get("freshness_sla_minutes")}
    return out


def governance_gaps(entry: dict) -> tuple:
    """Governance fields this table does not expose. Empty means fully governed."""
    gov = governance_of(entry)
    gaps = [k for k in GOVERNANCE_FIELDS if gov.get(k) in (None, "")]
    if not gov["dq"]["not_null"]:
        gaps.append("dq.not_null")
    return tuple(sorted(gaps))


# --------------------------------------------------------------------------- #
# section E -- the observability surface
# --------------------------------------------------------------------------- #

#: Every signal section E names, with where it is obtained. Declared as DATA so the
#: monitoring surface is a list something can iterate rather than a paragraph someone has to
#: re-read -- and so a missing one is detectable.
OBSERVABILITY_SIGNALS = {
    "connector_status": "scripts/cdc-runtime.sh connector-status <connector>",
    "kafka_lag": "GetOffsetShell end offsets minus the connector's committed offsets",
    "full_cdc_freshness": "now() - max(source_commit_ts) in the FULL_CDC target",
    "event_rate": "count per hour over the FULL_CDC target",
    "duplicate_rate": "count(*) - count(distinct dv_event_id), over one window",
    "quarantine_rate": "rows in the quarantine table for this topic, per window",
    "realtime_freshness": "now() - max(source_commit_ts) in the REALTIME target",
    "eod_status": "ops.eod_run.status for the latest cob_date",
    "file_count": "the table's $files metadata table",
    "avg_file_size": "avg(file_size_in_bytes) from $files",
    "manifest_count": "the table's $manifests metadata table",
    "last_successful_compaction": "ops.maintenance_run for this table",
    "dq": "ops.eod_run.dq_status, plus the not_null contract",
    "reconciliation": "ops.eod_run.reconciliation_status",
}


def observability_plan(entry: dict) -> dict:
    """The per-table monitoring surface, resolved against this table's own targets.

    Signals for a layer the table does not have are marked not-applicable rather than
    omitted: a monitoring surface with a silent hole looks identical to one that is healthy.
    """
    targets = entry.get("targets") or {}
    rt = (entry.get("realtime_policy") or {}).get("enabled", True)
    eod = (entry.get("eod_policy") or {}).get("enabled", True)
    out = {}
    for signal, how in OBSERVABILITY_SIGNALS.items():
        applicable = True
        if signal == "realtime_freshness":
            applicable = rt
        elif signal in ("eod_status", "dq", "reconciliation"):
            applicable = eod
        out[signal] = {"how": how, "applicable": applicable,
                       "target": targets.get("full_cdc_identifier")}
    return out
