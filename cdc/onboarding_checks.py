"""The acceptance checks a newly onboarded table must pass (section F). Pure.

Separated from the CLI so the JUDGEMENT is testable without a platform. Each check knows
what it asserts and how an operator obtains the number; nothing here fetches anything.
"""
from __future__ import annotations

from dataclasses import dataclass


#: What KIND of value a check's evidence must be. Not decoration: the judge used to accept
#: anything truthy, so `"FAILED"`, the string `"0"`, a list, or a `{"observed": ...}` wrapper
#: all PASSED. This is the LAST gate before a table becomes ACTIVE and downstream is told it
#: may be trusted, and a gate that accepts malformed evidence is a formality.
COUNT = "count"          # a positive integer -- how many rows/records were seen
FLAG = "flag"            # a real boolean True
LITERAL = "literal"      # a specific string, given in `expect`


@dataclass(frozen=True)
class Check:
    name: str
    what: str
    how: object          # callable(entry) -> str
    required: bool = True
    kind: str = FLAG
    expect: str = ""     # for LITERAL: the only accepted value


def _connector_how(entry):
    src = entry.get("source") or {}
    return (f"scripts/cdc-runtime.sh connector-status {src.get('connector')} "
            f"-> connector.state and every task RUNNING")


def _snapshot_how(entry):
    mode = (entry.get("onboarding") or {}).get("mode", "changes_only")
    if mode == "changes_only":
        return ("n/a for changes_only -- there is no snapshot. Supply "
                "{\"snapshot_complete\": true} to acknowledge that history starts now")
    return ("the connector log reports snapshot completion for this table, or the signal "
            "table shows the execute-snapshot finished")


def _topic_how(entry):
    return (f"kafka-run-class org.apache.kafka.tools.GetOffsetShell --topic "
            f"{(entry.get('capture') or {}).get('topic')} -> sum of end offsets > 0")


def _full_cdc_how(entry):
    t = (entry.get("targets") or {}).get("full_cdc_identifier")
    return f"SELECT COUNT(*) FROM {t}  (Athena, after one ingest window)"


def _lag_how(entry):
    sla = (entry.get("dq") or {}).get("freshness_sla_minutes", 60)
    return (f"max(source_commit_ts) in FULL_CDC vs now: must be within the table's "
            f"freshness SLA of {sla} minutes")


def _realtime_how(entry):
    rt = (entry.get("realtime_policy") or {}).get("enabled", True)
    if not rt:
        return "n/a -- realtime disabled for this table; supply {\"realtime_rows\": null}"
    return ("spark-submit spark/jobs/realtime/realtime_engine.py --table <id> ... then "
            "SELECT COUNT(*) from the REALTIME target")


def _eod_how(entry):
    eod = (entry.get("eod_policy") or {}).get("enabled", True)
    if not eod:
        return "n/a -- eod disabled for this table; supply {\"eod_status\": null}"
    return ("spark-submit spark/jobs/eod/eod_engine.py --table <id> --cob-date <D> ... "
            "then read ops.eod_run.status for that COB")


def _recon_how(entry):
    t = (entry.get("targets") or {}).get("full_cdc_identifier")
    return (f"an INDEPENDENT Athena count: distinct primary keys in {t} below the EOD "
            f"cutoff, compared against the EOD row_count. Computed in Athena, NOT by the "
            f"job that wrote the data")


CHECKS = (
    Check("connector_running", "the connector and all its tasks are RUNNING",
          _connector_how, kind=LITERAL, expect="RUNNING"),
    Check("snapshot_complete", "the requested snapshot finished (or there was none)",
          _snapshot_how, kind=FLAG),
    Check("topic_has_events", "the table's topic actually carries records", _topic_how,
          kind=COUNT),
    Check("full_cdc_rows", "FULL_CDC received rows for this table", _full_cdc_how,
          kind=COUNT),
    Check("lag_within_sla", "ingest lag is inside the table's freshness SLA", _lag_how,
          kind=FLAG),
    Check("realtime_rows", "a REALTIME materialisation produced rows", _realtime_how,
          required=False, kind=COUNT),
    Check("eod_status", "an EOD close CERTIFIED", _eod_how, required=False,
          kind=LITERAL, expect="CERTIFIED"),
    Check("reconciliation", "an independent count agrees with the close", _recon_how,
          kind=FLAG),
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    detail: str


@dataclass(frozen=True)
class SmokeOutcome:
    table_id: str
    results: tuple

    @property
    def passed(self) -> bool:
        return all(r.status != "FAIL" for r in self.results)

    def payload(self) -> dict:
        return {"table_id": self.table_id, "passed": self.passed,
                "results": [{"name": r.name, "status": r.status, "detail": r.detail}
                            for r in self.results]}


def judge(entry: dict, observed: dict) -> SmokeOutcome:
    """Turn observed values into an accept/reject decision.

    A MISSING required value is a FAIL, never a skip. "We did not measure it" and "it was
    fine" must not reach the same conclusion -- that is the whole difference between an
    acceptance gate and a formality.
    """
    rt_enabled = (entry.get("realtime_policy") or {}).get("enabled", True)
    eod_enabled = (entry.get("eod_policy") or {}).get("enabled", True)
    results = []
    for check in CHECKS:
        if check.name == "realtime_rows" and not rt_enabled:
            results.append(CheckResult(check.name, "SKIP", "realtime disabled for this table"))
            continue
        if check.name == "eod_status" and not eod_enabled:
            results.append(CheckResult(check.name, "SKIP", "eod disabled for this table"))
            continue
        if check.name not in observed:
            results.append(CheckResult(
                check.name, "FAIL" if check.required else "SKIP",
                "not measured" + (" -- a required check with no value is a FAIL, not a skip"
                                  if check.required else "")))
            continue
        value = observed[check.name]
        # An operator may supply either the bare value or `{"observed": v, "note": ...}`.
        # The wrapper is accepted EXPLICITLY rather than falling through as a truthy object:
        # it used to PASS every check that only tested truthiness, so a results file in the
        # wrong shape was indistinguishable from a healthy table.
        note = ""
        if isinstance(value, dict):
            if "observed" not in value:
                results.append(CheckResult(
                    check.name, "FAIL",
                    f"evidence is an object with no 'observed' key: {value!r}. Supply the "
                    f"value itself, or {{\"observed\": <value>}}"))
                continue
            note = str(value.get("note") or "")
            value = value["observed"]

        detail = f"observed {value!r}" + (f" -- {note}" if note else "")
        if value is None:
            results.append(CheckResult(check.name, "SKIP", "reported as not applicable"))
            continue

        # The TYPE is checked, not merely the truthiness. `"FAILED"`, `"0"` and `[]` are all
        # truthy-or-falsy in ways that have nothing to do with what the check asserts.
        if check.kind is COUNT:
            if isinstance(value, bool) or not isinstance(value, int):
                results.append(CheckResult(
                    check.name, "FAIL",
                    f"expected a count (a whole number), got {value!r}. A count is what "
                    f"distinguishes 'measured zero' from 'not measured'"))
            elif value == 0:
                results.append(CheckResult(check.name, "FAIL", f"{detail} -- measured zero"))
            elif value < 0:
                results.append(CheckResult(check.name, "FAIL", f"negative count {value!r}"))
            else:
                results.append(CheckResult(check.name, "PASS", detail))
        elif check.kind is LITERAL:
            if value != check.expect:
                results.append(CheckResult(
                    check.name, "FAIL",
                    f"expected {check.expect!r}, got {value!r}"
                    + (". A close that built but did not certify is not acceptance evidence"
                       if check.name == "eod_status" else "")))
            else:
                results.append(CheckResult(check.name, "PASS", detail))
        else:                                                   # FLAG
            if value is not True:
                results.append(CheckResult(
                    check.name, "FAIL",
                    f"expected true, got {value!r}. A check that did not actually pass must "
                    f"not reach the same conclusion as one that did"))
            else:
                results.append(CheckResult(check.name, "PASS", detail))
    return SmokeOutcome(table_id=entry.get("table_id", ""), results=tuple(results))
