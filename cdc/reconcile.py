"""The cutover gate (sections H and I): what must be true before a table is cut over.

Pure. The checks are declared here and JUDGED here; they are RUN elsewhere, against a live
platform, and their observed values are fed in. That split is the point: the judgement is
the part that must be reviewable and testable, and a module that also fetched would be one
nobody could exercise without the platform.

TWO KINDS OF FAILURE, AND THEY ARE NOT THE SAME
------------------------------------------------
A check that FAILS says the two sides disagree. A check that was NOT MEASURED says nothing at
all -- and the whole history of this repository is failures that looked like successes
because nothing measured them. So an unmeasured required check is a FAIL, never a skip, and
`benchmark_measured` exists specifically so that "we did not benchmark" cannot be reported as
"performance is fine" (section I: do not claim performance improvement without measurement).
"""
from __future__ import annotations

from dataclasses import dataclass

# --------------------------------------------------------------------------- #
# section H -- the reconciliation checks
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class GateCheck:
    name: str
    what: str
    how: str
    required: bool = True


CHECKS: tuple = (
    GateCheck(
        "event_identity_completeness",
        "every dv_event_id in the legacy slice exists in the per-table target, and none "
        "extra",
        "the backfill's own comparison, re-run INDEPENDENTLY in Athena: a full outer join "
        "on dv_event_id between the legacy slice and the per-table target must return zero "
        "rows on both sides"),
    GateCheck(
        "full_cdc_equivalence",
        "the two FULL_CDC sides agree on row count AND on the identity-set hash",
        "SELECT COUNT(*), sha2(concat_ws('|', array_sort(collect_list(dv_event_id))), 256) "
        "from each side"),
    GateCheck(
        "eod_equivalence",
        "an EOD close over the per-table source gives the same business state as over the "
        "legacy source for the same COB",
        "close the same cob_date from both sources, then full-outer-join the two snapshots "
        "on the primary key and compare the payloads. Zero differing rows"),
    GateCheck(
        "realtime_window_equivalence",
        "a REALTIME window over each source holds the same events",
        "materialise both with the same frozen upper bound, then compare dv_event_id sets"),
    GateCheck(
        "dq",
        "the per-table target passes the DQ contract the registry declares for it",
        "the not_null rules and event_date_null_tolerance from the compiled plan"),
    GateCheck(
        "schema_comparison",
        "the per-table target's columns are a superset of what consumers read from legacy",
        "compare the two schemas by NAME and TYPE; a column consumers read that is absent "
        "or retyped breaks them at cutover, not before"),
    GateCheck(
        "consumer_test",
        "every consumer that reads this table produces identical output from both paths",
        "run the reporting flows against each path and diff the mart output"),
    GateCheck(
        "latency",
        "the per-table path is not slower end to end than the legacy path",
        "ingest-to-queryable elapsed time for one window, both paths"),
    GateCheck(
        "benchmark_measured",
        "the performance comparison was actually RUN -- not that it was favourable",
        "cdc/benchmark.py, both paths, all nine metrics present"),
)

REQUIRED = tuple(c.name for c in CHECKS if c.required)


@dataclass(frozen=True)
class CheckOutcome:
    name: str
    status: str          # PASS | FAIL | SKIP
    detail: str


@dataclass(frozen=True)
class GateResult:
    table_id: str
    outcomes: tuple

    @property
    def passed(self) -> bool:
        return all(o.status != "FAIL" for o in self.outcomes)

    @property
    def failures(self) -> tuple:
        return tuple(o for o in self.outcomes if o.status == "FAIL")

    def payload(self) -> dict:
        return {"table_id": self.table_id, "passed": self.passed,
                "outcomes": [{"name": o.name, "status": o.status, "detail": o.detail}
                             for o in self.outcomes]}


def judge_gate(table_id: str, observed: dict) -> GateResult:
    """Decide whether one table may be cut over.

    `observed` maps check name -> value. True/non-zero passes; False/0 fails; None means the
    check does not apply and must SAY so; absent means it was not measured, which for a
    required check is a FAIL.
    """
    outcomes = []
    for check in CHECKS:
        if check.name not in observed:
            outcomes.append(CheckOutcome(
                check.name, "FAIL" if check.required else "SKIP",
                "NOT MEASURED. An unmeasured gate is not a passed gate -- 'we did not "
                "check' and 'it was fine' must not reach the same conclusion"))
            continue
        value = observed[check.name]
        if value is None:
            outcomes.append(CheckOutcome(check.name, "SKIP",
                                         "reported as not applicable"))
        elif value is False or value == 0:
            outcomes.append(CheckOutcome(check.name, "FAIL", f"observed {value!r}"))
        else:
            outcomes.append(CheckOutcome(check.name, "PASS", f"observed {value!r}"))
    return GateResult(table_id=table_id, outcomes=tuple(outcomes))


# --------------------------------------------------------------------------- #
# section I -- the benchmark
# --------------------------------------------------------------------------- #

#: The nine metrics section I names. Recorded per PATH (legacy / per_table) per SCENARIO.
METRICS = ("athena_bytes_scanned", "spark_input_files", "spark_input_bytes",
           "planning_time_ms", "runtime_ms", "file_count", "avg_file_size_bytes",
           "output_rows", "partitions_scanned")

#: The scenarios section I names.
SCENARIOS = ("one_table_one_day", "one_table_seven_days", "eod_source_scan",
             "auto_correct", "fulfill")

PATHS = ("legacy", "per_table")


@dataclass(frozen=True)
class Comparison:
    scenario: str
    metric: str
    legacy: float
    per_table: float

    @property
    def delta_pct(self) -> float:
        """Change from legacy to per-table, as a percentage. NEGATIVE is an improvement for
        every metric here -- all nine are costs (bytes, files, time), none is a benefit, so
        one sign convention is unambiguous."""
        if self.legacy == 0:
            return 0.0
        return (self.per_table - self.legacy) / self.legacy * 100.0

    @property
    def improved(self) -> bool:
        return self.per_table < self.legacy


class BenchmarkReport:
    """Measurements, and a refusal to summarise ones that were not taken.

    `claim()` raises rather than returning a hedged string when a scenario is incomplete.
    A benchmark that quietly reports on three of nine metrics reads exactly like one that
    reported on nine, and section I exists because that is how unmeasured performance claims
    get made.
    """

    def __init__(self):
        self._m: dict = {}

    def record(self, scenario: str, path: str, metric: str, value: float) -> None:
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario {scenario!r}; expected one of "
                             f"{', '.join(SCENARIOS)}")
        if path not in PATHS:
            raise ValueError(f"unknown path {path!r}; expected one of {', '.join(PATHS)}")
        if metric not in METRICS:
            raise ValueError(f"unknown metric {metric!r}; expected one of "
                             f"{', '.join(METRICS)}")
        self._m.setdefault(scenario, {}).setdefault(path, {})[metric] = float(value)

    def missing(self, scenario: str) -> tuple:
        """Which (path, metric) pairs a scenario still lacks."""
        got = self._m.get(scenario, {})
        return tuple(sorted((p, m) for p in PATHS for m in METRICS
                            if m not in got.get(p, {})))

    def complete(self, scenario: str) -> bool:
        return not self.missing(scenario)

    def comparisons(self, scenario: str) -> tuple:
        if not self.complete(scenario):
            raise ValueError(
                f"{scenario} is incomplete: missing "
                f"{', '.join(f'{p}.{m}' for p, m in self.missing(scenario))}. A partial "
                f"benchmark must not be compared -- it reads exactly like a complete one")
        got = self._m[scenario]
        return tuple(Comparison(scenario, m, got["legacy"][m], got["per_table"][m])
                     for m in METRICS)

    def claim(self, scenario: str) -> str:
        """A sentence about this scenario that is TRUE, or an exception.

        Never 'faster' or 'better' in the abstract: the metric and the number are in the
        sentence, because a claim without them is the thing section I forbids.
        """
        comps = self.comparisons(scenario)
        better = [c for c in comps if c.improved]
        worse = [c for c in comps if not c.improved and c.per_table > c.legacy]
        parts = [f"{c.metric} {c.legacy:g} -> {c.per_table:g} ({c.delta_pct:+.1f}%)"
                 for c in comps]
        verdict = ("per-table is cheaper on every metric" if len(better) == len(comps)
                   else f"per-table is cheaper on {len(better)}/{len(comps)} metrics, "
                        f"more expensive on {len(worse)}")
        return f"{scenario}: {verdict}. " + "; ".join(parts)

    def payload(self) -> dict:
        return {"scenarios": {s: self._m.get(s, {}) for s in SCENARIOS},
                "complete": {s: self.complete(s) for s in SCENARIOS}}

    @property
    def any_complete(self) -> bool:
        return any(self.complete(s) for s in SCENARIOS)
