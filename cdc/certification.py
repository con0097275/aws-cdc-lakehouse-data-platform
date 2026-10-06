"""The certification ladder and the gates each flow must pass to stamp its tier.

DRP1. The rule this module exists to enforce, in one line:

    **A Spark job exiting 0 is not certification.**

The DRP0 audit found the live platform certifying on completeness of the EOD close alone --
`ops.data_certification` does not exist, and `dq_result_run_id` is passed as a literal
`None` by its only producer (`spark/reporting/ops_client.py:359`). "Certified" therefore
meant "the close finished", which is a statement about the pipeline, not about the data.

THE LADDER IS CANONICAL HERE
-----------------------------
`spark/common/flows.py` held the four-tier ladder and re-exports this one, so there is a
single definition rather than two that agree until they do not. The relative ORDER of the
original four is unchanged -- every comparison keeps its exact behaviour -- and `REALTIME`
is inserted below them.

    REALTIME              1   the bounded overlay; true now, certified never
    PROVISIONAL_NRT       2   incremental, watermark-bounded
    PROVISIONAL_CORRECTED 3   whole-of-day relook after late events
    RECONCILED            4   agreed against a counterpart
    CERTIFIED             5   closed, cutoff-bounded, DQ- and recon-clean

**0 is not a tier and never will be.** `spark/common/flows.py` ranks an UNRECOGNISED status
0 so that garbage can never win a comparison, and the dbt macro mirrors it with `ELSE 0`.
Numbering REALTIME as 0 would have made an unknown status compare EQUAL to the real bottom
tier -- so adding a tier below the bottom meant shifting the whole ladder up by one, not
extending it downwards. The relative order of the original four is unchanged, which is the
only property any stored comparison depends on.

A lower tier may never overwrite a higher one. That rule is why the ladder is ordered at
all: without it an NRT run at 09:00 would silently replace the certified figure published
at 06:00, and the number would move for a reason nobody could reconstruct.

TWO FLOW VOCABULARIES EXIST, AND DRP1 DOES NOT MERGE THEM
---------------------------------------------------------
`spark/common/flows.py` names four flows (`NRT`, `AUTO_CORRECT`, `EOD`, `FULL_FILL`);
`spark/reporting/models.py:FlowMode` names five (`EOD`, `AUTO_CORRECT`, `FULFILL`,
`STREAM_BATCH`, `STREAMING_RT`). They are the same concepts under different names, plus one
that is genuinely new. Renaming either is a change to running code and belongs to the phase
that owns it; DRP1 records the mapping in `LEGACY_FLOW_NAME` so a reader of one vocabulary
can find the other, and a test asserts the two stay in step.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .models import ConfigError
from .quality import ResultStatus, suite_blocks_publish


class Tier(str, Enum):
    REALTIME = "REALTIME"
    PROVISIONAL_NRT = "PROVISIONAL_NRT"
    PROVISIONAL_CORRECTED = "PROVISIONAL_CORRECTED"
    RECONCILED = "RECONCILED"
    CERTIFIED = "CERTIFIED"


#: THE ladder. Every rank comparison, in Python and in generated SQL, derives from this.
#: The original four keep their original numbers on purpose (see the module docstring).
STATUS_RANK: dict[str, int] = {
    Tier.REALTIME.value: 1,
    Tier.PROVISIONAL_NRT.value: 2,
    Tier.PROVISIONAL_CORRECTED.value: 3,
    Tier.RECONCILED.value: 4,
    Tier.CERTIFIED.value: 5,
}


class Flow(str, Enum):
    """The five reporting modes (ADR-040)."""

    EOD = "EOD"
    AUTO_CORRECT = "AUTO_CORRECT"
    FULFILL = "FULFILL"
    STREAM_BATCH = "STREAM_BATCH"
    STREAMING_RT = "STREAMING_RT"


#: The older four-flow vocabulary in `spark/common/flows.py`. `STREAMING_RT` has no legacy
#: name because the streaming overlay postdates that module.
LEGACY_FLOW_NAME: dict[Flow, str | None] = {
    Flow.EOD: "EOD",
    Flow.AUTO_CORRECT: "AUTO_CORRECT",
    Flow.FULFILL: "FULL_FILL",
    Flow.STREAM_BATCH: "NRT",
    Flow.STREAMING_RT: None,
}

#: The HIGHEST tier each flow may ever stamp. A flow cannot choose its tier at runtime:
#: letting STREAM_BATCH stamp CERTIFIED would defeat the ladder from inside, and the check
#: that prevents it has to live somewhere that is not the flow itself.
FLOW_MAX_TIER: dict[Flow, Tier] = {
    Flow.EOD: Tier.CERTIFIED,
    Flow.FULFILL: Tier.RECONCILED,
    Flow.AUTO_CORRECT: Tier.PROVISIONAL_CORRECTED,
    Flow.STREAM_BATCH: Tier.PROVISIONAL_NRT,
    Flow.STREAMING_RT: Tier.REALTIME,
}


class Gate(str, Enum):
    """The named preconditions. Each is something that can be FALSE and observed to be so --
    a gate nobody can fail is documentation, not a control."""

    JOB_SUCCEEDED = "job_succeeded"
    CONTRACT_DECLARED = "contract_declared"
    CUTOFF_BOUNDED = "cutoff_bounded"
    UPSTREAM_READY = "upstream_ready"
    DQ_EVALUATED = "dq_evaluated"
    DQ_NOT_BLOCKING = "dq_not_blocking"
    RECONCILED = "reconciled"
    SOURCE_CLOSED = "source_closed"


#: What each tier requires. Cumulative by construction: a tier's gates are a superset of the
#: tier below it, which is asserted by a test rather than left as a reading of the table.
GATES_FOR_TIER: dict[Tier, tuple[Gate, ...]] = {
    Tier.REALTIME: (Gate.JOB_SUCCEEDED,),
    Tier.PROVISIONAL_NRT: (Gate.JOB_SUCCEEDED, Gate.CONTRACT_DECLARED),
    Tier.PROVISIONAL_CORRECTED: (Gate.JOB_SUCCEEDED, Gate.CONTRACT_DECLARED,
                                 Gate.DQ_EVALUATED, Gate.DQ_NOT_BLOCKING),
    Tier.RECONCILED: (Gate.JOB_SUCCEEDED, Gate.CONTRACT_DECLARED,
                      Gate.DQ_EVALUATED, Gate.DQ_NOT_BLOCKING, Gate.RECONCILED),
    Tier.CERTIFIED: (Gate.JOB_SUCCEEDED, Gate.CONTRACT_DECLARED,
                     Gate.DQ_EVALUATED, Gate.DQ_NOT_BLOCKING, Gate.RECONCILED,
                     Gate.CUTOFF_BOUNDED, Gate.UPSTREAM_READY, Gate.SOURCE_CLOSED),
}

#: Why each gate exists, in the words a refusal message should use.
GATE_REASON: dict[Gate, str] = {
    Gate.JOB_SUCCEEDED: "the producing job did not succeed",
    Gate.CONTRACT_DECLARED: "no data contract is declared, so there is no stated shape to verify against",
    Gate.CUTOFF_BOUNDED: "the read was not bounded by an explicit cutoff, so the figure is not reproducible",
    Gate.UPSTREAM_READY: "an upstream dataset was not ready, so this read may be missing events that have arrived since",
    Gate.DQ_EVALUATED: "no DQ check produced a verdict; an unevaluated suite is not a clean one",
    Gate.DQ_NOT_BLOCKING: "a required DQ check failed, errored or could not be evaluated",
    Gate.RECONCILED: "reconciliation against the counterpart did not pass",
    Gate.SOURCE_CLOSED: "the source period is not closed, so late events can still change this figure",
}


@dataclass(frozen=True)
class Evidence:
    """What is actually known at the moment a tier is being claimed.

    `dq_results` and `recon_results` are the records from `cdc/quality.py`, not booleans:
    a gate that took someone's word for it is the failure mode DRP0 found in production.
    """

    job_succeeded: bool = False
    contract_declared: bool = False
    cutoff_bounded: bool = False
    upstream_ready: bool = False
    source_closed: bool = False
    dq_results: tuple = ()
    recon_results: tuple = ()

    def gate_state(self) -> dict:
        dq = list(self.dq_results)
        recon = list(self.recon_results)
        evaluated = [r for r in dq if r.status is not ResultStatus.SKIPPED]
        return {
            Gate.JOB_SUCCEEDED: self.job_succeeded,
            Gate.CONTRACT_DECLARED: self.contract_declared,
            Gate.CUTOFF_BOUNDED: self.cutoff_bounded,
            Gate.UPSTREAM_READY: self.upstream_ready,
            Gate.SOURCE_CLOSED: self.source_closed,
            Gate.DQ_EVALUATED: bool(evaluated),
            Gate.DQ_NOT_BLOCKING: bool(evaluated) and not suite_blocks_publish(evaluated),
            # An empty reconciliation list is NOT a pass. `reconciliation_policy: none` is
            # how an asset says it reconciles against nothing, and that is expressed by the
            # tier it is allowed to reach, not by an absent result silently counting as one.
            Gate.RECONCILED: bool(recon) and not suite_blocks_publish(recon),
        }


@dataclass(frozen=True)
class Decision:
    flow: Flow
    requested: Tier
    granted: Tier | None
    failed_gates: tuple[Gate, ...] = ()
    reasons: tuple[str, ...] = ()
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def certified(self) -> bool:
        return self.granted is Tier.CERTIFIED

    def payload(self) -> dict:
        return {"flow": self.flow.value, "requested": self.requested.value,
                "granted": self.granted.value if self.granted else None,
                "failed_gates": [g.value for g in self.failed_gates],
                "reasons": list(self.reasons), "notes": list(self.notes)}


def evaluate(flow: Flow, evidence: Evidence, *, requested: Tier | None = None) -> Decision:
    """Decide which tier, if any, this run may stamp.

    Grants NOTHING on failure rather than silently dropping to a lower tier. A run that
    aimed at CERTIFIED and missed is a run somebody should look at; quietly publishing it
    as PROVISIONAL_NRT would hide that, and the figure would still be on the dashboard.
    """
    ceiling = FLOW_MAX_TIER[flow]
    requested = requested or ceiling
    notes: list[str] = []
    if STATUS_RANK[requested.value] > STATUS_RANK[ceiling.value]:
        raise ConfigError(
            f"flow {flow.value} may stamp at most {ceiling.value}, but {requested.value} was "
            f"requested. A flow choosing its own tier at runtime defeats the ladder.")
    if requested is not ceiling:
        notes.append(f"requested {requested.value}, below this flow's ceiling {ceiling.value}")

    state = evidence.gate_state()
    failed = tuple(g for g in GATES_FOR_TIER[requested] if not state[g])
    if failed:
        return Decision(flow, requested, None, failed,
                        tuple(GATE_REASON[g] for g in failed), tuple(notes))
    return Decision(flow, requested, requested, (), (), tuple(notes))


def may_overwrite(existing: str | None, proposed: str) -> bool:
    """The anti-downgrade rule. A tier may replace an equal or lower one, never a higher.

    Equal is allowed on purpose: rerunning an EOD close for the same COB must be able to
    republish, or a correction to a certified figure could never be applied.
    """
    if proposed not in STATUS_RANK:
        raise ConfigError(f"unknown certification tier {proposed!r}; known: "
                          f"{', '.join(STATUS_RANK)}")
    if not existing:
        return True
    if existing not in STATUS_RANK:
        raise ConfigError(f"unknown existing certification tier {existing!r}")
    return STATUS_RANK[proposed] >= STATUS_RANK[existing]


def required_gates(flow: Flow) -> tuple[Gate, ...]:
    return GATES_FOR_TIER[FLOW_MAX_TIER[flow]]
