"""AIGR3/AIGR4 — root-cause taxonomy, bounded scope, and execution capability.

THE TWO QUESTIONS THIS MODULE KEEPS APART
-----------------------------------------
    column lineage      -> WHICH jobs are affected      (dependency selection)
    RecoveryCapability  -> WHAT a job can recompute     (execution granularity)

Conflating them is the main correctness trap in lineage-driven recovery. Knowing that only
`BALANCE` is wrong does NOT mean Spark can rewrite one column; it means fewer jobs need to
run, each at whatever granularity it actually supports.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

from cdc.incidents import FailureClass


class IncidentCategory(str, Enum):
    """WHY the data is wrong, at the granularity that decides what may be done about it.

    `FailureClass` (cdc/incidents.py) answers WHAT went wrong at the platform level and is
    deliberately coarse. This is finer because two of its values -- TRANSFORM_LOGIC_DEFECT
    and DQ_RULE_DEFECT -- are precisely the cases where `FailureClass` would say
    "repairable by rerun" and be wrong in a way that costs money and reproduces the defect.
    """

    BAD_SOURCE_VALUE = "BAD_SOURCE_VALUE"
    LATE_SOURCE_EVENT = "LATE_SOURCE_EVENT"
    MISSING_SOURCE_EVENT = "MISSING_SOURCE_EVENT"
    DUPLICATE_CDC_EVENT = "DUPLICATE_CDC_EVENT"
    OUT_OF_ORDER_EVENT = "OUT_OF_ORDER_EVENT"
    INGESTION_DEFECT = "INGESTION_DEFECT"
    SCHEMA_DRIFT = "SCHEMA_DRIFT"
    TRANSFORM_LOGIC_DEFECT = "TRANSFORM_LOGIC_DEFECT"
    DQ_RULE_DEFECT = "DQ_RULE_DEFECT"
    RECONCILIATION_DEFECT = "RECONCILIATION_DEFECT"
    DEPENDENCY_READINESS_DEFECT = "DEPENDENCY_READINESS_DEFECT"
    STALE_DATA = "STALE_DATA"
    TARGET_WRITE_DEFECT = "TARGET_WRITE_DEFECT"
    UNKNOWN = "UNKNOWN"


class Disposition(str, Enum):
    """What the platform is ALLOWED to do about a category. Not advice -- a gate."""

    BOUNDED_RECOVERY = "BOUNDED_RECOVERY"
    AUTO_CORRECT = "AUTO_CORRECT"
    WAITING_SOURCE_CORRECTION = "WAITING_SOURCE_CORRECTION"
    CODE_FIX_REQUIRED = "CODE_FIX_REQUIRED"
    RULE_FIX_REQUIRED = "RULE_FIX_REQUIRED"
    CONTRACT_REVIEW_REQUIRED = "CONTRACT_REVIEW_REQUIRED"
    NO_AUTOMATIC_RECOVERY = "NO_AUTOMATIC_RECOVERY"


#: The disposition table. Rerunning is the default remedy for exactly the cases where the
#: pipeline, not the input and not the code, produced the wrong answer.
DISPOSITION: dict[IncidentCategory, Disposition] = {
    # Rerunning over bad source data reproduces the same bad answer, more expensively.
    IncidentCategory.BAD_SOURCE_VALUE: Disposition.WAITING_SOURCE_CORRECTION,
    IncidentCategory.MISSING_SOURCE_EVENT: Disposition.WAITING_SOURCE_CORRECTION,
    # The event exists, it just arrived late: this is what AUTO_CORRECT is for.
    IncidentCategory.LATE_SOURCE_EVENT: Disposition.AUTO_CORRECT,
    IncidentCategory.DUPLICATE_CDC_EVENT: Disposition.BOUNDED_RECOVERY,
    IncidentCategory.OUT_OF_ORDER_EVENT: Disposition.BOUNDED_RECOVERY,
    IncidentCategory.INGESTION_DEFECT: Disposition.BOUNDED_RECOVERY,
    IncidentCategory.TARGET_WRITE_DEFECT: Disposition.BOUNDED_RECOVERY,
    IncidentCategory.DEPENDENCY_READINESS_DEFECT: Disposition.BOUNDED_RECOVERY,
    IncidentCategory.STALE_DATA: Disposition.BOUNDED_RECOVERY,
    IncidentCategory.RECONCILIATION_DEFECT: Disposition.BOUNDED_RECOVERY,
    # A schema change may be legitimate; rebuilding before the contract is reconciled
    # just bakes the drift in.
    IncidentCategory.SCHEMA_DRIFT: Disposition.CONTRACT_REVIEW_REQUIRED,
    # Rerunning broken logic over good data produces the same wrong answer and a bill.
    IncidentCategory.TRANSFORM_LOGIC_DEFECT: Disposition.CODE_FIX_REQUIRED,
    # The data may be entirely fine. Repairing it would be repairing the wrong thing.
    IncidentCategory.DQ_RULE_DEFECT: Disposition.RULE_FIX_REQUIRED,
    IncidentCategory.UNKNOWN: Disposition.NO_AUTOMATIC_RECOVERY,
}

#: Categories from which a recovery may ever be built. Everything else stops at an
#: explanation, and that is the point.
RECOVERABLE = frozenset(
    c for c, d in DISPOSITION.items()
    if d in (Disposition.BOUNDED_RECOVERY, Disposition.AUTO_CORRECT))

#: How the finer category maps onto the platform's existing coarse class, so an incident
#: written by the copilot is readable by everything built during DRP.
TO_FAILURE_CLASS: dict[IncidentCategory, FailureClass] = {
    IncidentCategory.BAD_SOURCE_VALUE: FailureClass.SOURCE_DEFECT,
    IncidentCategory.MISSING_SOURCE_EVENT: FailureClass.SOURCE_DEFECT,
    IncidentCategory.LATE_SOURCE_EVENT: FailureClass.LATE_ARRIVING_DATA,
    IncidentCategory.DUPLICATE_CDC_EVENT: FailureClass.DQ_VIOLATION,
    IncidentCategory.OUT_OF_ORDER_EVENT: FailureClass.DQ_VIOLATION,
    IncidentCategory.INGESTION_DEFECT: FailureClass.JOB_FAILURE,
    IncidentCategory.SCHEMA_DRIFT: FailureClass.SCHEMA_CHANGE,
    IncidentCategory.TRANSFORM_LOGIC_DEFECT: FailureClass.JOB_FAILURE,
    IncidentCategory.DQ_RULE_DEFECT: FailureClass.DQ_VIOLATION,
    IncidentCategory.RECONCILIATION_DEFECT: FailureClass.RECONCILIATION_BREAK,
    IncidentCategory.DEPENDENCY_READINESS_DEFECT: FailureClass.JOB_FAILURE,
    IncidentCategory.STALE_DATA: FailureClass.FRESHNESS_BREACH,
    IncidentCategory.TARGET_WRITE_DEFECT: FailureClass.JOB_FAILURE,
    IncidentCategory.UNKNOWN: FailureClass.UNKNOWN,
}


class LineageConfidence(str, Enum):
    """`VALIDATED` is the only value that may narrow a recovery to specific columns.

    Anything weaker downgrades to table level and SAYS SO. An impact analysis that is
    confidently wrong is worse than one that admits its width, because the descendants it
    silently omits are the ones that never get rebuilt.
    """

    VALIDATED = "VALIDATED"
    DERIVED = "DERIVED"
    DECLARED = "DECLARED"
    ABSENT = "ABSENT"


class ScopeSource(str, Enum):
    COLUMN_LINEAGE = "COLUMN_LINEAGE"
    TABLE_LINEAGE = "TABLE_LINEAGE"
    USER_SUPPLIED_KEYS = "USER_SUPPLIED_KEYS"
    DQ_FAILED_ROWS = "DQ_FAILED_ROWS"
    FALLBACK_FULL = "FALLBACK_FULL"


class CapabilityKind(str, Enum):
    FULL_TABLE = "FULL_TABLE"
    PARTITION = "PARTITION"
    COB_DATE = "COB_DATE"
    DATE_RANGE = "DATE_RANGE"
    BUSINESS_KEY_SET = "BUSINESS_KEY_SET"
    WATERMARK_RANGE = "WATERMARK_RANGE"
    SOURCE_SNAPSHOT_RANGE = "SOURCE_SNAPSHOT_RANGE"


#: Narrower is better. Used to pick the smallest scope a job actually supports.
CAPABILITY_WIDTH = {
    CapabilityKind.BUSINESS_KEY_SET: 1,
    CapabilityKind.SOURCE_SNAPSHOT_RANGE: 2,
    CapabilityKind.WATERMARK_RANGE: 3,
    CapabilityKind.PARTITION: 4,
    CapabilityKind.COB_DATE: 5,
    CapabilityKind.DATE_RANGE: 6,
    CapabilityKind.FULL_TABLE: 7,
}

#: Max business keys that may travel inline. Beyond this a key-set REFERENCE is
#: materialised: a million keys in a tool argument is a prompt, a cost and a truncation
#: risk all at once.
MAX_INLINE_KEYS = 500


class ScopeError(ValueError):
    """Fatal. An unbounded or unsupported scope is how a small defect becomes an outage."""


@dataclass(frozen=True)
class RecoveryCapability:
    """What ONE registered job can actually recompute.

    Declared per job, never inferred. A planner that guesses a job supports
    BUSINESS_KEY_SET produces a plan that silently rebuilds the wrong thing.
    """

    job_id: str
    supports: frozenset[CapabilityKind]
    idempotent: bool = True
    max_keys: int = 10_000
    max_date_span_days: int = 92

    def __post_init__(self) -> None:
        if not self.supports:
            raise ScopeError(f"{self.job_id}: a job must declare at least FULL_TABLE")

    def smallest_supported(self, desired: tuple[CapabilityKind, ...]) -> CapabilityKind:
        """The narrowest scope this job supports among those the incident permits.

        Falls back to FULL_TABLE rather than failing: a wider rebuild that the approver can
        see is safer than a refusal that leaves the data wrong.
        """
        usable = [k for k in desired if k in self.supports]
        if not usable:
            return CapabilityKind.FULL_TABLE
        return min(usable, key=lambda k: CAPABILITY_WIDTH[k])


@dataclass(frozen=True)
class RecoveryScope:
    """WHAT to repair, typed. Never a free-form predicate.

    `scope_confidence` and `scope_source` are load-bearing, not documentation: without them
    there is no way to tell "bounded by validated column lineage" from "bounded by a guess",
    and the policy engine refuses to auto-execute anything it cannot tell apart.
    """

    root_asset_id: str
    environment: str
    root_column: str = ""
    cob_dates: tuple[date, ...] = ()
    from_date: date | None = None
    to_date: date | None = None
    business_keys: tuple[str, ...] = ()
    key_set_ref: str = ""
    watermark_lower: str = ""
    watermark_upper: str = ""
    source_snapshot_start: str = ""
    source_snapshot_end: str = ""
    affected_partitions: tuple[str, ...] = ()
    flow_mode: str = ""
    reason: str = ""
    scope_confidence: LineageConfidence = LineageConfidence.ABSENT
    scope_source: ScopeSource = ScopeSource.FALLBACK_FULL
    requested_by: str = ""
    request_id: str = ""

    def __post_init__(self) -> None:
        if not self.root_asset_id:
            raise ScopeError("a scope needs a root asset")
        if len(self.business_keys) > MAX_INLINE_KEYS and not self.key_set_ref:
            raise ScopeError(
                f"{len(self.business_keys)} inline keys exceeds {MAX_INLINE_KEYS}; "
                "materialise a key-set reference instead of carrying them inline")
        if self.from_date and self.to_date and self.from_date > self.to_date:
            raise ScopeError("from_date is after to_date")
        if self.root_column and self.scope_confidence is not LineageConfidence.VALIDATED \
                and self.scope_source is ScopeSource.COLUMN_LINEAGE:
            raise ScopeError(
                "a column-narrowed scope requires VALIDATED lineage; downgrade to table "
                "level and say so rather than implying precision the graph cannot support")

    @property
    def key_count(self) -> int:
        return len(self.business_keys)

    @property
    def bounded(self) -> bool:
        """Unbounded is not a scope, it is the absence of one."""
        return bool(self.cob_dates or (self.from_date and self.to_date)
                    or self.business_keys or self.key_set_ref
                    or self.affected_partitions
                    or (self.watermark_lower and self.watermark_upper))

    def desired_kinds(self) -> tuple[CapabilityKind, ...]:
        """Which capability kinds this scope could be expressed as, narrowest first."""
        out: list[CapabilityKind] = []
        if self.business_keys or self.key_set_ref:
            out.append(CapabilityKind.BUSINESS_KEY_SET)
        if self.source_snapshot_start and self.source_snapshot_end:
            out.append(CapabilityKind.SOURCE_SNAPSHOT_RANGE)
        if self.watermark_lower and self.watermark_upper:
            out.append(CapabilityKind.WATERMARK_RANGE)
        if self.affected_partitions:
            out.append(CapabilityKind.PARTITION)
        if len(self.cob_dates) == 1:
            out.append(CapabilityKind.COB_DATE)
        if self.cob_dates or (self.from_date and self.to_date):
            out.append(CapabilityKind.DATE_RANGE)
        out.append(CapabilityKind.FULL_TABLE)
        return tuple(out)

    def fingerprint(self) -> str:
        parts = [self.root_asset_id, self.environment, self.root_column,
                 ",".join(sorted(d.isoformat() for d in self.cob_dates)),
                 self.from_date.isoformat() if self.from_date else "",
                 self.to_date.isoformat() if self.to_date else "",
                 ",".join(sorted(self.business_keys)), self.key_set_ref,
                 self.watermark_lower, self.watermark_upper,
                 self.source_snapshot_start, self.source_snapshot_end,
                 ",".join(sorted(self.affected_partitions)), self.flow_mode,
                 self.scope_confidence.value, self.scope_source.value]
        return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

    def payload(self) -> dict:
        return {"root_asset_id": self.root_asset_id, "environment": self.environment,
                "root_column": self.root_column,
                "cob_dates": sorted(d.isoformat() for d in self.cob_dates),
                "from_date": self.from_date.isoformat() if self.from_date else None,
                "to_date": self.to_date.isoformat() if self.to_date else None,
                "key_count": self.key_count, "key_set_ref": self.key_set_ref,
                "affected_partitions": sorted(self.affected_partitions),
                "flow_mode": self.flow_mode, "reason": self.reason,
                "scope_confidence": self.scope_confidence.value,
                "scope_source": self.scope_source.value,
                "bounded": self.bounded, "fingerprint": self.fingerprint(),
                "requested_by": self.requested_by, "request_id": self.request_id}


@dataclass(frozen=True)
class RootCause:
    """A classification is only as good as the evidence attached to it."""

    category: IncidentCategory
    confidence: float
    evidence_refs: tuple[str, ...] = ()
    detail: str = ""

    def __post_init__(self) -> None:
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("confidence must be in [0, 1]")
        if self.category is not IncidentCategory.UNKNOWN and not self.evidence_refs:
            raise ValueError(
                f"{self.category.value} asserted with no evidence reference. A cause with "
                "nothing behind it is a guess wearing a category name.")

    @property
    def disposition(self) -> Disposition:
        return DISPOSITION[self.category]

    @property
    def may_recover(self) -> bool:
        return self.category in RECOVERABLE

    def payload(self) -> dict:
        return {"category": self.category.value, "confidence": self.confidence,
                "disposition": self.disposition.value, "may_recover": self.may_recover,
                "evidence_refs": list(self.evidence_refs), "detail": self.detail,
                "failure_class": TO_FAILURE_CLASS[self.category].value}
