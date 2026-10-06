"""DQ and reconciliation RESULT contracts — the evidence a certification decision rests on.

DRP1. Two record types, one shared status vocabulary, and two rules that are the whole
reason this module is a contract rather than a convention.

RULE 1 — "WE COULD NOT TELL" IS NOT A SKIP
-------------------------------------------
The DRP1 brief names five statuses: PASS, WARN, FAIL, ERROR, SKIPPED. This module defines
SIX, and the sixth is deliberate.

    SKIPPED         we CHOSE not to run this check      -> never blocks
    NOT_EVALUATED   we ran it and there was nothing      -> ALWAYS blocks a required check

`spark/ops/dq_engine.py` is built entirely around that distinction, and its docstring
states the failure: a uniqueness query returning zero rows means "no duplicates" if the
table has data and means NOTHING AT ALL if the table is empty, the partition is missing or
the predicate matched nothing. Collapsing NOT_EVALUATED into SKIPPED would make an
unevaluated required check non-blocking, which re-opens exactly the defect the engine was
written to close. Adding a status is cheaper than losing that.

    PASS            evaluated and satisfied
    WARN            evaluated and violated, at WARN severity  -> recorded, never blocks
    FAIL            evaluated and violated, at ERROR severity -> blocks
    ERROR           the check itself could not run (exception, permission, bad SQL)
    NOT_EVALUATED   the check ran against nothing
    SKIPPED         deliberately not run (out of scope for this interval)

ERROR and NOT_EVALUATED are also different, and the difference matters when someone is
paged: ERROR is an engineering defect in the check, NOT_EVALUATED is usually an upstream
job that did not produce data. One is fixed by editing code, the other by rerunning a
pipeline.

RULE 2 — A RESULT NEVER CARRIES RAW PII
----------------------------------------
`sample_reference` is a POINTER, never a value. Failed rows go to the quarantine table,
which is already access-controlled; the result carries a reference to them.

A DQ table is the most widely-read object in a lakehouse -- dashboards, alerts, the AI
assistant, anyone debugging a pipeline. Copying failing rows into it is how a restricted
column ends up in an unrestricted table, with a perfectly good reason at the time
("just the first three, to see what broke"). `sample_reference` is validated as a
reference, so putting a value there has to be a deliberate lie rather than a convenience.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

from .contracts import (BLOCKING_SEVERITIES, CheckType, Severity,
                        WATERMARK_BLOCKING_SEVERITIES)
from .models import ConfigError


class ResultStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    ERROR = "ERROR"
    NOT_EVALUATED = "NOT_EVALUATED"
    SKIPPED = "SKIPPED"


#: Statuses that block a certified publish when the check is required (ERROR severity).
#: NOT_EVALUATED and ERROR are in the set for the same reason FAIL is: none of them is
#: evidence that the rule holds.
BLOCKING_STATUSES = (ResultStatus.FAIL, ResultStatus.ERROR, ResultStatus.NOT_EVALUATED)

#: Statuses that mean the check produced a real, trustworthy verdict.
EVALUATED_STATUSES = (ResultStatus.PASS, ResultStatus.WARN, ResultStatus.FAIL)

#: The five the DRP1 brief names, kept as a named set so the extension is visible rather
#: than silently absorbed. `docs/DATA_QUALITY_MODEL.md` §2 explains the sixth.
BRIEF_STATUSES = (ResultStatus.PASS, ResultStatus.WARN, ResultStatus.FAIL,
                  ResultStatus.ERROR, ResultStatus.SKIPPED)


def status_for(*, violated: bool, severity: Severity) -> ResultStatus:
    """The ONE place a verdict becomes a status.

    The engine decides `violated`; severity decides whether that is a FAIL or a WARN. Two
    call sites making this decision independently is how a rule gets downgraded in one
    place and stays blocking in another.
    """
    if not violated:
        return ResultStatus.PASS
    return ResultStatus.FAIL if severity in BLOCKING_SEVERITIES else ResultStatus.WARN


@dataclass(frozen=True)
class DataInterval:
    """What slice of data the check examined.

    Both an interval AND a cob_date, because they answer different questions. The interval
    is what was READ (a window, possibly spanning days); the COB is what the result is ABOUT.
    A late-arriving correction read at 09:00 on the 30th can be evidence about COB 28 -- and
    a model with only one of the two cannot say that.
    """

    start: datetime | None = None
    end: datetime | None = None
    cob_date: date | None = None

    def __post_init__(self) -> None:
        if self.start and self.end and self.start > self.end:
            raise ConfigError("DataInterval: start is after end")
        if not any((self.start, self.end, self.cob_date)):
            raise ConfigError(
                "DataInterval: at least one of start/end/cob_date is required. A result "
                "with no interval cannot be superseded by a later one, because nothing "
                "says which slice it described.")

    def payload(self) -> dict:
        return {"interval_start": self.start.isoformat() if self.start else None,
                "interval_end": self.end.isoformat() if self.end else None,
                "cob_date": self.cob_date.isoformat() if self.cob_date else None}


#: A sample reference must be a POINTER. Anything else is refused.
_SAMPLE_REF = re.compile(r"^(s3://[^\s]+|glue_catalog\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+(\?[^\s]*)?|"
                         r"quarantine://[^\s]+)$")


def validate_sample_reference(raw: str) -> str:
    """Refuse anything that is not a location. Empty is fine -- most checks have no sample."""
    if not raw:
        return ""
    if not _SAMPLE_REF.match(raw.strip()):
        raise ConfigError(
            f"sample_reference {raw!r} is not a location. It must be an s3:// URI, a "
            f"`glue_catalog.db.table` reference or a quarantine:// pointer. Failing ROWS "
            f"never go in a DQ result: the quarantine is access-controlled and this table "
            f"is read by dashboards, alerts and the AI assistant.")
    return raw.strip()


@dataclass(frozen=True)
class DqResult:
    """One check, one dataset, one interval, one run."""

    dq_run_id: str
    run_id: str
    dataset_id: str            # an AssetId in `kind:name` form
    check_id: str
    check_type: CheckType
    severity: Severity
    interval: DataInterval
    status: ResultStatus
    expected_rule: str
    engine: str                # spark | dbt | athena | python
    config_version: str        # `GovernanceInventory.config_version()`
    observed_value: float | None = None
    failed_count: int = 0
    rows_examined: int = 0
    sample_reference: str = ""
    detail: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None
    context: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("dq_run_id", "run_id", "dataset_id", "check_id", "engine", "config_version"):
            if not str(getattr(self, name) or "").strip():
                raise ConfigError(f"DqResult.{name} is required")
        object.__setattr__(self, "sample_reference",
                           validate_sample_reference(self.sample_reference))
        if self.status in EVALUATED_STATUSES and self.rows_examined == 0 \
                and self.status is not ResultStatus.PASS:
            # A violation found in zero examined rows is arithmetically impossible, and the
            # combination usually means the engine forgot to set rows_examined -- which
            # would make a NOT_EVALUATED look like a real FAIL.
            raise ConfigError(
                f"DqResult {self.check_id}: status {self.status.value} with rows_examined=0. "
                f"A verdict from nothing is NOT_EVALUATED, not a violation.")
        if self.status is ResultStatus.PASS and self.failed_count:
            raise ConfigError(
                f"DqResult {self.check_id}: PASS with failed_count={self.failed_count}")

    @property
    def blocks_publish(self) -> bool:
        """BLOCKER and ERROR checks block on FAIL, ERROR and NOT_EVALUATED.

        WARN and INFO never block, whatever the status: that is what choosing them means.
        """
        return self.severity in BLOCKING_SEVERITIES and self.status in BLOCKING_STATUSES

    @property
    def blocks_watermark(self) -> bool:
        """Only a BLOCKER stops the successful watermark.

        The distinction matters at 2 a.m.: an ERROR says "do not publish this number", a
        BLOCKER says "nothing downstream may read this partition at all". Conflating them
        either strands a recoverable day or lets a corrupt one through.
        """
        return (self.severity in WATERMARK_BLOCKING_SEVERITIES
                and self.status in BLOCKING_STATUSES)

    def payload(self) -> dict:
        out = {"dq_run_id": self.dq_run_id, "run_id": self.run_id,
               "dataset_id": self.dataset_id, "check_id": self.check_id,
               "check_type": self.check_type.value, "severity": self.severity.value,
               "status": self.status.value, "expected_rule": self.expected_rule,
               "observed_value": self.observed_value, "failed_count": self.failed_count,
               "rows_examined": self.rows_examined,
               "sample_reference": self.sample_reference, "detail": self.detail,
               "engine": self.engine, "config_version": self.config_version,
               "started_at": self.started_at.isoformat() if self.started_at else None,
               "finished_at": self.finished_at.isoformat() if self.finished_at else None,
               "context": {k: self.context[k] for k in sorted(self.context)}}
        out.update(self.interval.payload())
        return out


@dataclass(frozen=True)
class ReconResult:
    """One metric, compared across two datasets, for one interval."""

    recon_run_id: str
    run_id: str
    source_dataset: str
    target_dataset: str
    interval: DataInterval
    metric: str                 # row_count | sum:<column> | distinct:<column>
    status: ResultStatus
    config_version: str
    source_value: float | None = None
    target_value: float | None = None
    tolerance_pct: float = 0.0
    severity: Severity = Severity.ERROR
    detail: str = ""
    started_at: datetime | None = None
    finished_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("recon_run_id", "run_id", "source_dataset", "target_dataset",
                     "metric", "config_version"):
            if not str(getattr(self, name) or "").strip():
                raise ConfigError(f"ReconResult.{name} is required")
        if self.source_dataset == self.target_dataset:
            raise ConfigError(
                f"ReconResult: source and target are both {self.source_dataset!r}. A dataset "
                f"always agrees with itself, so this check can only ever pass.")

    @property
    def difference(self) -> float | None:
        """Absolute difference. `None` when either side was not measured -- which is NOT
        zero. Rendering an unmeasured comparison as a zero difference is the single most
        dangerous thing a reconciliation ledger can do."""
        if self.source_value is None or self.target_value is None:
            return None
        return self.target_value - self.source_value

    @property
    def difference_pct(self) -> float | None:
        d = self.difference
        if d is None:
            return None
        if not self.source_value:
            return None if d else 0.0
        return 100.0 * d / self.source_value

    @property
    def within_tolerance(self) -> bool | None:
        pct = self.difference_pct
        if pct is None:
            return None
        return abs(pct) <= self.tolerance_pct

    @property
    def blocks_publish(self) -> bool:
        return self.severity in BLOCKING_SEVERITIES and self.status in BLOCKING_STATUSES

    @property
    def blocks_watermark(self) -> bool:
        """A BLOCKER-severity reconciliation break holds the watermark, exactly as a DQ one
        does. A layer that disagrees with the layer below it by more than its tolerance is
        not a number to explain later -- it is a partition nothing should read."""
        return (self.severity in WATERMARK_BLOCKING_SEVERITIES
                and self.status in BLOCKING_STATUSES)

    def payload(self) -> dict:
        out = {"recon_run_id": self.recon_run_id, "run_id": self.run_id,
               "source_dataset": self.source_dataset, "target_dataset": self.target_dataset,
               "metric": self.metric, "source_value": self.source_value,
               "target_value": self.target_value, "difference": self.difference,
               "difference_pct": self.difference_pct, "tolerance_pct": self.tolerance_pct,
               "status": self.status.value, "severity": self.severity.value,
               "config_version": self.config_version, "detail": self.detail,
               "started_at": self.started_at.isoformat() if self.started_at else None,
               "finished_at": self.finished_at.isoformat() if self.finished_at else None}
        out.update(self.interval.payload())
        return out


def suite_blocks_publish(results) -> bool:
    """One rule, used by every gate. A suite that examined NOTHING also blocks.

    An empty suite passing is the same defect as an unevaluated check passing, one level up:
    "no check failed" is not "the data was checked".
    """
    results = list(results)
    if not results:
        return True
    return any(r.blocks_publish for r in results)


def summarise(results) -> dict:
    """Deterministic counts by status. Used by the certification gate and the run ledger."""
    results = list(results)
    counts = {s.value: 0 for s in ResultStatus}
    for r in results:
        counts[r.status.value] += 1
    return {"total": len(results), "by_status": counts,
            "blocking": sum(1 for r in results if r.blocks_publish)}


#: How the LIVE `ops.dq_result` (Session 14) maps onto this contract. Recorded rather than
#: applied: rewriting the engine's output schema is a DRP5 action behind its own gate, and
#: `spark/ops/dq_engine.py` currently blocks publishes. Keeping the mapping here means the
#: migration is a table to read, not an archaeology exercise.
LEGACY_DQ_RESULT_MAPPING: dict[str, str] = {
    "run_id": "run_id",
    "business_date": "interval.cob_date",
    "dataset": "dataset_id (needs the `kind:` prefix added)",
    "check_name": "check_id (v1 names are human-typed; v2 ids are derived)",
    "check_type": "check_type",
    "verdict": "status (PASS->PASS, FAIL+ERROR->FAIL, FAIL+WARN->WARN, NOT_EVALUATED->NOT_EVALUATED)",
    "severity": "severity",
    "observed": "observed_value",
    "threshold": "(moves into expected_rule, which states the rule rather than a bare number)",
    "rows_examined": "rows_examined",
    "detail": "detail",
    "context": "context",
    "evaluated_at": "finished_at",
}

#: Fields the v1 table has no source for. Listed so a migration cannot quietly backfill
#: them with defaults that look like real values.
LEGACY_DQ_RESULT_MISSING = ("dq_run_id", "expected_rule", "failed_count",
                            "sample_reference", "engine", "config_version",
                            "started_at", "interval.start", "interval.end")
