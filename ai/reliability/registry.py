"""AIGR4 — what each registered job can actually recompute, and the executor that does it.

DECLARED, NOT INFERRED
----------------------
Each capability below states what the corresponding platform job really supports, taken
from how that job is invoked today. A planner that *guessed* would produce plans that
silently rebuild the wrong amount of data.
"""
from __future__ import annotations

from dataclasses import dataclass

from .control import AiRecoveryPlan, Execution, ExecutionState
from .model import CapabilityKind as K
from .model import RecoveryCapability
from .planner import JobNode

#: The six deterministic lineage jobs, with the scopes each genuinely accepts.
#:
#: `eod_build` takes `--cob-date` and rebuilds that business date; it has no key predicate,
#: so BUSINESS_KEY_SET is absent and a key-scoped incident is widened to the whole COB --
#: visibly, with a reason.
#:
#: `realtime_materialize` is a bounded window driven by a watermark, hence WATERMARK_RANGE.
#: `debezium` replays from a source position, hence SOURCE_SNAPSHOT_RANGE.
CAPABILITIES: dict[str, RecoveryCapability] = {
    "eod_build": RecoveryCapability(
        "eod_build", frozenset({K.COB_DATE, K.DATE_RANGE, K.FULL_TABLE}),
        idempotent=True, max_date_span_days=92),
    "realtime_materialize": RecoveryCapability(
        "realtime_materialize", frozenset({K.WATERMARK_RANGE, K.COB_DATE, K.FULL_TABLE})),
    "curated_build": RecoveryCapability(
        "curated_build", frozenset({K.COB_DATE, K.DATE_RANGE, K.FULL_TABLE})),
    "dbt_spark_build": RecoveryCapability(
        "dbt_spark_build", frozenset({K.COB_DATE, K.DATE_RANGE, K.FULL_TABLE})),
    "reporting": RecoveryCapability(
        "reporting", frozenset({K.COB_DATE, K.BUSINESS_KEY_SET, K.FULL_TABLE}),
        max_keys=5_000),
    "debezium": RecoveryCapability(
        "debezium", frozenset({K.SOURCE_SNAPSHOT_RANGE, K.FULL_TABLE})),
}

#: Which layer each job produces, so the planner can choose an action type.
JOB_LAYER: dict[str, str] = {
    "eod_build": "eod", "realtime_materialize": "realtime", "curated_build": "curated",
    "dbt_spark_build": "mart", "reporting": "mart", "debezium": "full_cdc",
}


def job_nodes(asset_to_job: dict[str, str]) -> dict[str, JobNode]:
    """Build the planner's job map from an asset->job mapping the platform supplies."""
    out: dict[str, JobNode] = {}
    for asset_id, job_id in asset_to_job.items():
        cap = CAPABILITIES.get(job_id)
        if cap is None:
            # Refuse rather than fabricate a capability: an invented one produces a plan
            # that claims a precision the job does not have.
            raise KeyError(f"{job_id!r} has no declared RecoveryCapability")
        out[f"{job_id}@{asset_id}"] = JobNode(job_id=job_id, asset_id=asset_id,
                                              layer=JOB_LAYER[job_id], capability=cap)
    return out


@dataclass
class AthenaRecoveryExecutor:
    """Executes a plan's actions as bounded Athena statements against Iceberg.

    Deliberately narrow: it accepts a plan the control service has already revalidated and
    a closed mapping from job id to a SQL template. It cannot be handed SQL, and it will
    not run an action whose job has no template.
    """

    #: Keyed by (job_id, ActionType) -- NOT by job alone.
    #:
    #: Keyed by job only, a `DQ_RECHECK` on `eod_build` ran `eod_build`'s REBUILD SQL: the
    #: verification turn silently re-executed a DELETE + INSERT. The drill still passed,
    #: because an idempotent rebuild happens to survive being run twice -- which is luck,
    #: not correctness, and a CERTIFY that rewrites a table is exactly the kind of action
    #: nobody approves.
    templates: dict                # {(job_id, ActionType): sql}
    runner: object                 # callable(sql) -> None
    dry_run: bool = True
    statements: list = None

    def __post_init__(self):
        self.statements = []

    #: A template MUST reference the scope it was handed. Discovered the hard way on
    #: 2026-09-30: a template that repaired by a value heuristic (`balance > 350`) instead
    #: of the planned scope missed one affected row and corrupted an unaffected one -- the
    #: mart moved from wrong to differently wrong, and every gate upstream had passed.
    #: A statement that ignores the scope rewrites rows the approver never approved.
    SCOPE_TOKENS = ("{cob}", "{keys}")

    def __call__(self, plan: AiRecoveryPlan, execution: Execution) -> Execution:
        try:
            for act in sorted(plan.actions, key=lambda a: (a.turn, a.job_id)):
                tpl = self.templates.get((act.job_id, act.action))
                if tpl is None:
                    # No silent fall back to the job's rebuild SQL. An action with no
                    # template is a verification this executor cannot perform, and saying
                    # so is the only safe answer.
                    if any(k[0] == act.job_id for k in self.templates):
                        raise KeyError(
                            f"{act.job_id!r} has no template for {act.action.value}; "
                            "refusing to fall back to another action's SQL")
                    raise KeyError(f"{act.job_id!r} has no registered SQL template")
                if not any(t in tpl for t in self.SCOPE_TOKENS):
                    raise ValueError(
                        f"{act.job_id!r} template references no scope placeholder "
                        f"{self.SCOPE_TOKENS}; a statement that ignores the scope rewrites "
                        "rows outside the approved plan")
                sql = tpl.format(
                    cob=next(iter(sorted(act.scope.cob_dates))).isoformat()
                    if act.scope.cob_dates else "",
                    keys=",".join(f"'{k}'" for k in sorted(act.scope.business_keys)) or "''")
                self.statements.append(sql)
                if not self.dry_run:
                    self.runner(sql)
                execution.turns_completed = act.turn + 1
            execution.state = (ExecutionState.SUCCEEDED if not self.dry_run
                               else ExecutionState.PENDING)
            execution.detail = (f"{len(self.statements)} statement(s) "
                                f"{'executed' if not self.dry_run else 'prepared (dry run)'}")
        except Exception as e:                      # noqa: BLE001 - recorded, not swallowed
            execution.state = ExecutionState.FAILED
            execution.detail = f"{type(e).__name__}: {e}"
        return execution
