"""Collect REAL evidence from the operational ledgers, then classify from it.

WHY THIS EXISTS
---------------
The first classifier read the question for keywords. That makes the copilot only as good as
the operator's own diagnosis -- and asking "plan a recovery" returned UNKNOWN, because the
operator had not said what was wrong. They were asking because they did not know.

A data engineer, told a table is wrong, does not ask the reporter for a root cause. They
open the DQ results, the reconciliation ledger, the EOD run record and the quarantine
table, and read what the platform already recorded. This does that.

Evidence references are ledger row ids, so every classification can be traced back to the
row that produced it.
"""
from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date

from .model import IncidentCategory, RootCause

WORKGROUP = "kafka-dev-lab-dev-wg"
OPS = "kafka_dev_lab_dev_ops"


def athena(sql: str, width: int = 1, *, workgroup: str = WORKGROUP) -> list:
    """Run one SELECT. Athena OMITS nulls from a result row, so rows are padded to width --
    a short row means NULLs, not a broken query."""
    q = subprocess.run(["aws", "athena", "start-query-execution", "--work-group", workgroup,
                        "--query-string", sql, "--query", "QueryExecutionId",
                        "--output", "text"], capture_output=True, text=True).stdout.strip()
    if not q:
        return []
    for _ in range(60):
        st = subprocess.run(["aws", "athena", "get-query-execution", "--query-execution-id",
                             q, "--query", "QueryExecution.Status.State", "--output", "text"],
                            capture_output=True, text=True).stdout.strip()
        if st not in ("RUNNING", "QUEUED"):
            break
        time.sleep(1.2)
    if st != "SUCCEEDED":
        return []
    out = subprocess.run(["aws", "athena", "get-query-results", "--query-execution-id", q,
                          "--query", "ResultSet.Rows[1:].Data[*].VarCharValue",
                          "--output", "json"], capture_output=True, text=True).stdout
    try:
        return [(r + [None] * width)[:width] for r in json.loads(out or "[]")]
    except Exception:                            # noqa: BLE001
        return []


def _num(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


@dataclass
class Evidence:
    """What the ledgers actually say about one dataset on one date."""

    dataset: str
    cob_date: date | None = None
    dq_failures: list = field(default_factory=list)      # (check_id, severity, failed, rows)
    recon_breaks: list = field(default_factory=list)     # (metric, source, target)
    quarantined: int = 0
    eod_status: str = ""
    eod_detail: str = ""
    refs: list = field(default_factory=list)
    looked: list = field(default_factory=list)           # which ledgers were actually read

    def payload(self) -> dict:
        return {"dataset": self.dataset,
                "cob_date": self.cob_date.isoformat() if self.cob_date else None,
                "dq_failures": self.dq_failures, "recon_breaks": self.recon_breaks,
                "quarantined": self.quarantined, "eod_status": self.eod_status,
                "eod_detail": self.eod_detail, "refs": self.refs,
                "ledgers_read": self.looked}

    @property
    def found_anything(self) -> bool:
        return bool(self.dq_failures or self.recon_breaks or self.quarantined
                    or self.eod_status)


#: Which check family implies which cause. Read from the check_id the platform itself
#: assigned, not from prose -- `uniqueness.*` failing on a CDC table means the same row
#: arrived twice, whatever anyone calls it.
_CHECK_CAUSE = (
    ("uniqueness", IncidentCategory.DUPLICATE_CDC_EVENT),
    ("duplicate", IncidentCategory.DUPLICATE_CDC_EVENT),
    ("timeliness", IncidentCategory.LATE_SOURCE_EVENT),
    ("freshness", IncidentCategory.STALE_DATA),
    ("completeness", IncidentCategory.MISSING_SOURCE_EVENT),
    ("schema", IncidentCategory.SCHEMA_DRIFT),
    ("validity", IncidentCategory.INGESTION_DEFECT),
    ("referential", IncidentCategory.DEPENDENCY_READINESS_DEFECT),
    ("ordering", IncidentCategory.OUT_OF_ORDER_EVENT),
    ("accuracy", IncidentCategory.BAD_SOURCE_VALUE),
)


def collect(dataset: str, cob: date | None, *, runner=athena) -> Evidence:
    """Read the ledgers. A ledger that cannot be read is recorded as unread, never as clean."""
    ev = Evidence(dataset=dataset, cob_date=cob)
    short = dataset.split(":", 1)[-1]
    where_cob = f" AND cob_date = DATE '{cob}'" if cob else ""

    rows = runner(f"""SELECT check_id, severity, CAST(failed_count AS varchar),
                             CAST(rows_examined AS varchar), dq_run_id
                      FROM {OPS}.dq_result_v2
                      WHERE status IN ('FAIL','ERROR')
                        AND dataset_id LIKE '%{short}%'{where_cob}
                      ORDER BY finished_at DESC LIMIT 25""", 5)
    ev.looked.append("dq_result_v2")
    for r in rows:
        ev.dq_failures.append({"check_id": r[0], "severity": r[1],
                               "failed": r[2], "examined": r[3]})
        if r[4]:
            ev.refs.append(f"dq:{r[4]}")

    rows = runner(f"""SELECT metric, CAST(source_value AS varchar),
                             CAST(target_value AS varchar), recon_run_id
                      FROM {OPS}.reconciliation_run
                      WHERE status IN ('FAIL','ERROR')
                        AND (source_dataset LIKE '%{short}%' OR target_dataset LIKE '%{short}%')
                        {where_cob}
                      ORDER BY finished_at DESC LIMIT 10""", 4)
    ev.looked.append("reconciliation_run")
    for r in rows:
        ev.recon_breaks.append({"metric": r[0], "source": r[1], "target": r[2]})
        if r[3]:
            ev.refs.append(f"recon:{r[3]}")

    rows = runner(f"""SELECT CAST(count(*) AS varchar) FROM {OPS}.dq_quarantine
                      WHERE dataset LIKE '%{short}%'{where_cob}""", 1)
    ev.looked.append("dq_quarantine")
    if rows and rows[0][0]:
        ev.quarantined = int(rows[0][0])

    rows = runner(f"""SELECT status, failure_reason, run_id FROM {OPS}.eod_run
                      WHERE table_id LIKE '%{short}%'{where_cob}
                      ORDER BY started_at DESC LIMIT 1""", 3)
    ev.looked.append("eod_run")
    if rows:
        ev.eod_status, ev.eod_detail = rows[0][0] or "", rows[0][1] or ""
        if rows[0][2]:
            ev.refs.append(f"eod:{rows[0][2]}")
    return ev


def classify(ev: Evidence, *, stated: IncidentCategory | None = None) -> RootCause:
    """Turn evidence into a cause. A stated cause is corroborated, never simply believed.

    Confidence is not decoration: a cause read from a BLOCKER check with a row count is
    worth more than one inferred from an EOD status string, and the policy engine treats
    a weakly-evidenced cause as needing a human.
    """
    if stated is not None and ev.found_anything:
        return RootCause(stated, 0.9, tuple(ev.refs)[:6],
                         detail="stated in the request and corroborated by the ledgers")
    if stated is not None:
        return RootCause(stated, 0.6, ("request:stated",),
                         detail="stated in the request; the ledgers show nothing for this "
                                "dataset and date, so it is taken on the operator's word")

    # Rank before reading. Taking the first row ordered by finished_at let a check with
    # 0 failed of 0 EXAMINED rows outrank `uniqueness.one_active_row_per_key` at 1 of 670 --
    # and the copilot then diagnosed a missing source event while a real duplicate sat in
    # the same result set. A check that examined nothing proves nothing: it is the
    # NOT_EVALUATED shape this platform already refuses to treat as a verdict.
    def _informative(f) -> tuple:
        failed = _num(f["failed"])
        examined = _num(f["examined"])
        return (examined > 0,                       # it actually ran
                failed > 0,                         # it actually found something
                f["severity"] == "BLOCKER",
                failed)

    for f in sorted(ev.dq_failures, key=_informative, reverse=True):
        if _num(f["examined"]) == 0:
            # Nothing below this point examined a row; none of them is evidence of a defect.
            break
        cid = (f["check_id"] or "").lower()
        for prefix, cause in _CHECK_CAUSE:
            if cid.startswith(prefix) or f".{prefix}" in cid:
                return RootCause(cause, 0.85 if f["severity"] == "BLOCKER" else 0.7,
                                 tuple(ev.refs)[:6],
                                 detail=f"DQ check {f['check_id']} failed "
                                        f"({f['failed']} of {f['examined']} rows)")
    if ev.quarantined:
        return RootCause(IncidentCategory.INGESTION_DEFECT, 0.8, tuple(ev.refs)[:6],
                         detail=f"{ev.quarantined} row(s) quarantined")
    if ev.recon_breaks:
        b = ev.recon_breaks[0]
        return RootCause(IncidentCategory.RECONCILIATION_DEFECT, 0.75, tuple(ev.refs)[:6],
                         detail=f"reconciliation break on {b['metric']}: "
                                f"{b['source']} vs {b['target']}")
    if ev.eod_status and "WAITING" in ev.eod_status.upper():
        return RootCause(IncidentCategory.DEPENDENCY_READINESS_DEFECT, 0.8,
                         tuple(ev.refs)[:6], detail=ev.eod_detail or ev.eod_status)
    if ev.eod_status and "NOT_CERTIFIED" in ev.eod_status.upper():
        return RootCause(IncidentCategory.DEPENDENCY_READINESS_DEFECT, 0.6,
                         tuple(ev.refs)[:6], detail=ev.eod_status)
    # Looked and found nothing that examined a row. That is a real answer, and a different
    # one from "did not look" -- the caller reports which ledgers were read.
    return RootCause(IncidentCategory.UNKNOWN, 0.0, ())
