"""Connector capture plan (sections B, C, D): what the connector captures vs what it should.

THE DESIRED LIST IS DERIVED, NEVER HAND-EDITED (section B). `table.include.list` and
`message.key.columns` are two spellings of one fact that already lives in the registry, and
this repository has been bitten by both: a lowercase include-list against UPPERCASE Oracle
topics (connector RUNNING, eight topics at offset 0, every health check green), and a
`message.key.columns` updated in one place and not the other (same key scattered across
partitions, per-key ordering silently broken).

REMOVAL IS A SEPARATE APPROVAL (section B). Adding a table is additive and reversible;
removing one stops capture, and every event that occurs while it is absent is gone from the
source's redo/CDC retention window before anyone notices. So a removal never rides along
with an addition -- it needs its own explicit act.

NO SECRETS IN LOGS (section D). The deployed templates resolve the password through
`${file:...}` (FileConfigProvider), so the literal never exists in the POSTed config, in the
`connect-configs` topic, or in `GET /connectors/<name>/config`. This module still redacts,
because that property is a property of the CURRENT template and not of every future one --
and a redactor that is only correct while nobody changes the template is not a control.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path

from .models import ConfigError

_REPO = Path(__file__).resolve().parents[1]

CONNECTOR_TEMPLATES = {
    "oracle": "connectors/oracle/corebank-source.json.tmpl",
    "sqlserver": "connectors/sqlserver/digital-source.json.tmpl",
}

# --------------------------------------------------------------------------- #
# section C -- onboarding modes
# --------------------------------------------------------------------------- #

#: Capture from the restart point forward. The table's EXISTING rows never appear.
MODE_CHANGES_ONLY = "changes_only"
#: Debezium snapshots the new table while streaming continues. Needs `signal.data.collection`.
MODE_INCREMENTAL = "incremental_snapshot"
#: Re-snapshot on first start against an empty offset. Re-delivers EVERY table.
MODE_INITIAL = "initial_snapshot"
ONBOARDING_MODES = (MODE_CHANGES_ONLY, MODE_INCREMENTAL, MODE_INITIAL)

#: DEFAULT = changes_only, and this is a description of the platform rather than a
#: preference. `docs/CDC_TABLE_ONBOARDING_DESIGN.md` section 5 already records it: both
#: connectors run `snapshot.mode: initial`, which snapshots only on FIRST start against an
#: empty offset, so adding a table to a running connector captures it from the restart point
#: forward and its existing rows are invisible. That is what happens today whether or not
#: anyone chooses it. Defaulting to `incremental_snapshot` -- which the brief prefers and
#: which IS the better answer -- would make every onboarding fail its precheck instead,
#: because neither deployed connector configures `signal.data.collection`. The preference is
#: expressed as a WARNING on the default and a refusal on the unavailable mode, not by
#: picking something the platform cannot do.
DEFAULT_ONBOARDING_MODE = MODE_CHANGES_ONLY


def normalise_mode(raw: str, *, what: str) -> str:
    value = str(raw).strip().lower()
    if value not in ONBOARDING_MODES:
        raise ConfigError(f"{what}: onboarding mode {raw!r} is not one of "
                          f"{', '.join(ONBOARDING_MODES)}")
    return value


@dataclass(frozen=True)
class ModeAssessment:
    mode: str
    available: bool
    detail: str
    remediation: str = ""


def assess_mode(mode: str, connector_config: dict) -> ModeAssessment:
    """Whether the requested onboarding mode is actually available on this connector."""
    if mode == MODE_INCREMENTAL:
        signal = connector_config.get("signal.data.collection")
        if signal:
            return ModeAssessment(mode, True,
                                  f"incremental snapshot available via {signal}")
        return ModeAssessment(
            mode, False,
            "incremental snapshot is NOT available: the connector configures no "
            "`signal.data.collection`, so there is no channel to send an execute-snapshot "
            "signal on. Adding the table and restarting would capture it from the restart "
            "point forward only, which is `changes_only` under a name that promises more.",
            remediation="add signal.data.collection + a signal table, then re-plan")
    if mode == MODE_INITIAL:
        return ModeAssessment(
            mode, True,
            "initial snapshot RE-DELIVERS EVERY CAPTURED TABLE, not just the new one. It "
            "requires clearing the stored offsets, which section C rules out as a normal "
            "onboarding procedure -- the dv_event_id MERGE makes it idempotent, but it is a "
            "full replay of everything to onboard one table.",
            remediation="prefer incremental_snapshot; use this only with explicit approval")
    return ModeAssessment(
        mode, True,
        "changes_only: the table is captured from the restart point forward. Its EXISTING "
        "rows will never appear in FULL_CDC -- backfill them separately or accept that "
        "history starts now.",
        remediation="see docs/CDC_TABLE_ONBOARDING_DESIGN.md section 5")


# --------------------------------------------------------------------------- #
# section D -- config hashing and redaction
# --------------------------------------------------------------------------- #

#: Keys whose VALUE must never be printed. Matched as a substring, case-insensitively, so a
#: future `database.master.password` is covered without editing this list.
SECRET_KEY_HINTS = ("password", "secret", "credential", "token", "sasl.jaas.config",
                    "private.key", "keystore", "truststore")
REDACTED = "***REDACTED***"


def redact(config: dict) -> dict:
    """A loggable copy. Values are replaced, keys are kept: which settings changed is the
    whole point of a diff, and it is answerable without any value at all."""
    out = {}
    for key, value in config.items():
        if any(hint in key.lower() for hint in SECRET_KEY_HINTS):
            out[key] = REDACTED
        elif isinstance(value, str) and re.search(r"\$\{(file|ssm|env|vault):", value):
            # A resolver placeholder is not itself a secret, but it names WHERE one lives.
            # Keeping the provider and dropping the path says enough to debug and nothing
            # useful to an attacker reading a log.
            out[key] = re.sub(r"\$\{(\w+):[^}]*\}", r"${\1:" + REDACTED + "}", value)
        else:
            out[key] = value
    return out


def config_hash(config: dict) -> str:
    """A stable fingerprint of a connector config, computed over the FULL value set.

    Over the full values, not the redacted ones: the hash exists to prove whether the config
    changed, and hashing a redaction would report "unchanged" across a password rotation.
    It is a hash, so it discloses nothing -- which is what makes it the right thing to put in
    a log where the config itself must not go.
    """
    return hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_template(engine: str, *, repo: Path | None = None) -> dict:
    name = CONNECTOR_TEMPLATES.get(engine)
    if not name:
        raise ConfigError(f"no connector template registered for engine {engine!r}")
    path = (repo or _REPO) / name
    if not path.exists():
        raise ConfigError(f"{path}: connector template not found")
    raw = re.sub(r"\$\{[^}]+\}", "PLACEHOLDER", path.read_text())
    try:
        return json.loads(raw)
    except Exception as exc:                                           # noqa: BLE001
        raise ConfigError(f"{path}: not valid JSON after placeholder substitution: "
                          f"{exc}") from None


# --------------------------------------------------------------------------- #
# section B -- the capture diff
# --------------------------------------------------------------------------- #

@dataclass
class CapturePlan:
    engine: str
    connector: str
    current_tables: tuple
    desired_tables: tuple
    current_keys: tuple
    desired_keys: tuple
    old_config_hash: str

    @property
    def add(self) -> tuple:
        return tuple(sorted(set(self.desired_tables) - set(self.current_tables)))

    @property
    def remove(self) -> tuple:
        return tuple(sorted(set(self.current_tables) - set(self.desired_tables)))

    @property
    def key_changes(self) -> tuple:
        """Key-column entries that differ. Tracked separately from the table list because a
        table can be captured correctly and keyed WRONGLY, and only the second one is
        invisible: the topic fills, the health checks pass, and the same PK lands in
        different partitions."""
        return tuple(sorted(set(self.desired_keys) ^ set(self.current_keys)))

    @property
    def changed(self) -> bool:
        return bool(self.add or self.remove or self.key_changes)

    def desired_config(self, template: dict) -> dict:
        """The connector config this plan would apply. Derived; never hand-edited.

        WHEN NOTHING CHANGES, THE TEMPLATE'S OWN STRING IS KEPT. The derived list is sorted
        and the template's is in whatever order it was typed, so rewriting it
        unconditionally produces a config whose SET is identical and whose bytes are not --
        a different hash, an apparent pending change, and a connector restart bought for
        nothing. A restart is not free: it rebalances tasks and re-reads offsets on a live
        capture. So the rewrite happens only when the capture set actually differs.
        """
        cfg = dict(template.get("config") or {})
        if set(self.desired_tables) != set(self.current_tables):
            cfg["table.include.list"] = ",".join(self.desired_tables)
        if self.desired_keys and set(self.desired_keys) != set(self.current_keys):
            cfg["message.key.columns"] = ";".join(self.desired_keys)
        return cfg

    def payload(self) -> dict:
        return {"engine": self.engine, "connector": self.connector,
                "current_tables": list(self.current_tables),
                "desired_tables": list(self.desired_tables),
                "add": list(self.add), "remove": list(self.remove),
                "key_changes": list(self.key_changes),
                "old_config_hash": self.old_config_hash}


def build_capture_plan(plan: dict, engine: str, *, repo: Path | None = None) -> CapturePlan:
    """Diff the connector template on disk against the ENABLED tables in the registry.

    Read from the template on disk, not from the running connector, so this works with no
    AWS and no platform -- which is when onboarding is planned. Confirming that the running
    connector matches the template is a separate, live step.
    """
    payload = plan.get("plan") or plan
    template = load_template(engine, repo=repo)
    cfg = template.get("config") or {}
    current_tables = tuple(sorted(
        t.strip() for t in (cfg.get("table.include.list") or "").split(",") if t.strip()))
    current_keys = tuple(sorted(
        k.strip() for k in (cfg.get("message.key.columns") or "").split(";") if k.strip()))

    desired_tables, desired_keys, connector = [], [], ""
    for entry in payload.get("tables") or []:
        source = entry.get("source") or {}
        if source.get("engine") != engine or not entry.get("enabled", True):
            continue
        connector = connector or source.get("connector", "")
        capture = entry.get("capture") or {}
        if capture.get("include_list_entry"):
            desired_tables.append(capture["include_list_entry"])
        if capture.get("key_columns_entry"):
            desired_keys.append(capture["key_columns_entry"])

    return CapturePlan(
        engine=engine, connector=connector,
        current_tables=current_tables, desired_tables=tuple(sorted(desired_tables)),
        current_keys=current_keys, desired_keys=tuple(sorted(desired_keys)),
        old_config_hash=config_hash(cfg))
