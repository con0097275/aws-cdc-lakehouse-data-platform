"""The REALTIME layer's cost model, as a function of config. R2-I.

Pure. No AWS, no pricing API, no Spark -- and no dollar figure. What it produces is the
thing a decision actually turns on: how many Spark runs a table's cadence buys per day, what
size each one is, and what changes if the table is turned off.

WHY NOT DOLLARS
---------------
A per-run cost depends on how long the run takes, which depends on data this repository does
not have until the layer has been running. A number derived from a guessed duration looks
like a measurement and is not one, and it would be quoted back later as if it were. So this
reports RUNS and SHAPES, which are exact, and `scripts/realtime-benchmark.py` reports
DURATIONS, which are measured. Multiplying the two is the operator's step, with both inputs
visible.
"""
from __future__ import annotations

from dataclasses import dataclass

#: vCPU-minutes per run, from the resource profile. A magnitude for comparing tables, not a
#: bill: EMR Serverless bills driver and executors for their own lifetimes, and an executor
#: that finishes early stops costing.
def _vcpu(profile: dict) -> float:
    return float(profile.get("driver_cores", 0)) + (
        float(profile.get("executor_cores", 0))
        * float(profile.get("executor_instances", 0)))


def runs_per_day(cron: str) -> int:
    """A 5-field cron's runs per day. 0 when it cannot be read as a simple cadence.

    Deliberately narrow: it understands `*/N * * * *`, `* * * * *` and "once a day". A
    general cron expander would answer confidently for expressions nobody in this registry
    writes, and a confident wrong answer in a cost report is worse than an obvious zero.
    """
    fields = (cron or "").split()
    if len(fields) != 5:
        return 0
    minute, hour = fields[0], fields[1]
    if minute.startswith("*/") and minute[2:].isdigit() and hour == "*":
        step = int(minute[2:])
        return (60 // step) * 24 if step else 0
    if minute == "*" and hour == "*":
        return 60 * 24
    if minute.isdigit() and hour.isdigit() and fields[2:] == ["*", "*", "*"]:
        return 1
    return 0


@dataclass(frozen=True)
class TableCost:
    """One table's REALTIME cost shape. Every field is derived from config alone."""
    table_id: str
    enabled: bool
    shape: str
    write_strategy: str
    source_progress: str
    schedule: str
    runs_per_day: int
    profile: str
    vcpu_per_run: float
    rebases_per_day: int
    maintained: bool

    @property
    def vcpu_minutes_class(self) -> float:
        """Runs x vCPU. A COMPARATIVE magnitude between tables, not minutes of anything --
        the per-run duration is measured, not modelled (see the module docstring)."""
        return self.runs_per_day * self.vcpu_per_run


def table_cost(entry: dict) -> TableCost:
    rt = entry.get("realtime_policy") or {}
    profile = rt.get("resource_profile")
    profile = profile if isinstance(profile, dict) else {}
    enabled = bool(rt.get("enabled", True)) and bool(entry.get("enabled", True))
    schedule = rt.get("schedule") or ""
    return TableCost(
        table_id=entry["table_id"],
        enabled=enabled,
        shape=rt.get("shape", ""),
        write_strategy=rt.get("write_strategy", ""),
        source_progress=rt.get("source_progress", ""),
        schedule=schedule,
        # A DISABLED TABLE IS ZERO, not "the cadence it would have had". The R2 brief's
        # acceptance gate is literally that disabled means no scheduled task, no compute
        # and no maintenance -- so the cost model must report zero rather than a
        # counterfactual that looks like spend.
        runs_per_day=runs_per_day(schedule) if enabled else 0,
        profile=str(profile.get("name", "")),
        vcpu_per_run=_vcpu(profile) if enabled else 0.0,
        rebases_per_day=1 if (enabled and rt.get("rebase_on_eod_certified")) else 0,
        maintained=enabled)


def plan_costs(plan: dict) -> list:
    payload = plan.get("plan") or plan
    return [table_cost(e) for e in sorted(payload.get("tables") or (),
                                          key=lambda e: e["table_id"])]


def summarise(costs: list) -> dict:
    """Platform totals. `disabled_tables` is reported because it is the lever."""
    on = [c for c in costs if c.enabled]
    return {
        "tables": len(costs),
        "realtime_enabled": len(on),
        "realtime_disabled": len(costs) - len(on),
        "runs_per_day": sum(c.runs_per_day for c in costs),
        "rebases_per_day": sum(c.rebases_per_day for c in costs),
        "vcpu_minutes_class": round(sum(c.vcpu_minutes_class for c in costs), 1),
        "incremental_tables": sum(1 for c in on
                                  if c.source_progress == "iceberg_snapshot"),
    }
