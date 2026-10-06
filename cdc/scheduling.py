"""Cadence and sizing, shared by every config-driven layer.

Lifted out of `cdc/realtime.py` when EOD needed the same two decisions (Phase D). A second
copy would have been the third instance this month of one rule living in two files: the
source-lab health gate asserting `= "4"` against a five-table registry, `emr-submit.sh`
holding a dead EMR application id, and a writer-schema export whose ids no longer meant what
the file said. All three were "a derived value maintained by hand".

The DEFAULTS differ per layer -- REALTIME refreshes every ten minutes, EOD closes once a day
-- so the default is an argument here and a constant in the layer that owns it.
"""
from __future__ import annotations

from .models import ConfigError

#: Airflow's scheduler cannot fire faster than a minute, and any of these layers holds a
#: Spark submission; a sub-minute cadence queues runs faster than they finish.
_CRON_FIELDS = 5


def normalise_schedule(raw, *, what: str, default: str) -> str:
    """`""`/None -> `default`; anything else must be a 5-field cron expression.

    Validated at COMPILE time. An unparseable schedule discovered by Airflow is a DAG that
    fails to import, and a DAG that fails to import disappears from the UI -- the worst way
    to learn that a registry edit was wrong.
    """
    text = str(raw or "").strip()
    if not text:
        return default
    if text.startswith("@"):
        # `@daily` is valid Airflow, but it hides the cadence behind a word and cannot
        # express "every 10 minutes". One spelling keeps cadence GROUPING honest: two
        # tables asking for the same thing must produce the same string.
        raise ConfigError(
            f"{what}: schedule {text!r} -- use a 5-field cron expression, not an alias, so "
            f"that two tables asking for the same cadence group into one DAG")
    fields = text.split()
    if len(fields) != _CRON_FIELDS:
        raise ConfigError(
            f"{what}: schedule {text!r} must have {_CRON_FIELDS} fields "
            f"(minute hour day month weekday), got {len(fields)}")
    for f in fields:
        if not all(c.isdigit() or c in "*/,-" for c in f):
            raise ConfigError(f"{what}: schedule {text!r} has an unreadable field {f!r}")
    minute = fields[0]
    if minute.startswith("*/"):
        step = minute[2:]
        if not step.isdigit() or int(step) < 1:
            raise ConfigError(f"{what}: schedule {text!r} has a non-positive minute step")
    return text


def schedule_groups(plan: dict, *, policy_key: str, default: str) -> dict:
    """`{schedule: [table_id, ...]}` for every enabled table of one layer, deterministically.

    This is what makes ONE DAG PER CADENCE possible without one DAG per table: tables that
    ask for the same cron share a DAG and are dynamic-mapped inside it.
    """
    payload = plan.get("plan") or plan
    groups: dict[str, list[str]] = {}
    for entry in payload.get("tables") or ():
        policy = entry.get(policy_key) or {}
        if not policy.get("enabled", True):
            continue
        cron = normalise_schedule(policy.get("schedule"), default=default,
                                  what=f"{entry.get('table_id')}: {policy_key}")
        groups.setdefault(cron, []).append(entry["table_id"])
    return {k: sorted(v) for k, v in sorted(groups.items())}


#: What a table's run COSTS, named rather than typed per submission. The pool is here too,
#: because "which queue does this compete in" is the same decision as "how big is it": a
#: large job in the small pool starves the small ones, and a small job in its own pool
#: wastes a slot. Airflow reads `pool`, the submitter reads the rest.
RESOURCE_PROFILES: dict[str, dict] = {
    "small": {"pool": "reporting_jobs", "driver_cores": 1, "driver_memory": "2g",
              "executor_cores": 1, "executor_memory": "2g", "executor_instances": 1,
              "timeout_minutes": 20},
    "medium": {"pool": "reporting_jobs", "driver_cores": 2, "driver_memory": "4g",
               "executor_cores": 2, "executor_memory": "4g", "executor_instances": 2,
               "timeout_minutes": 45},
    "large": {"pool": "spark_jobs", "driver_cores": 2, "driver_memory": "8g",
              "executor_cores": 4, "executor_memory": "8g", "executor_instances": 4,
              "timeout_minutes": 90},
}

#: Most tables here are small; the default must not be the expensive one. A profile nobody
#: chose should never be the one that bills most.
DEFAULT_RESOURCE_PROFILE = "small"


def resolve_resource_profile(name, *, what: str) -> dict:
    """Name -> the resolved sizing, with the name carried so a ledger can record WHICH."""
    chosen = str(name or DEFAULT_RESOURCE_PROFILE).strip().lower()
    if chosen not in RESOURCE_PROFILES:
        raise ConfigError(
            f"{what}: resource_profile {chosen!r} is not one of "
            f"{', '.join(sorted(RESOURCE_PROFILES))}")
    return {"name": chosen, **RESOURCE_PROFILES[chosen]}
