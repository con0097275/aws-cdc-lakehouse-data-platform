"""The metadata plane: mode, environment, outage policy — and the rules that keep it optional.

DRP2. ADR-087 chose DataHub. This module is the boundary that keeps that choice from
becoming a dependency the data plane cannot run without.

THE INVARIANT
-------------
    Business data must never be wrong, lost or blocked because the catalogue is down.

Which produces three design consequences, all enforced here rather than left to callers:

1. **`disabled` is a first-class mode**, not a broken `local`. Every publish path has to
   work with the plane switched off, and the default under `lab_low_cost` is off. A code
   path that only works when DataHub is running is a code path that has taken a dependency
   on a catalogue.
2. **Outage behaviour is DECLARED per flow**, not decided in an exception handler.
   `BEST_EFFORT` records and continues; `REQUIRED` fails the task. Only work whose entire
   purpose is metadata may be `REQUIRED` -- if the ingestion workflow cannot reach DataHub
   it has done nothing, and reporting success would make a stale catalogue look fresh.
3. **Retry is bounded.** Unbounded retry against a service that is down is how a
   "best effort" publish becomes a hang inside a Spark task.

ENVIRONMENT SEPARATION
----------------------
Every DataHub URN carries a `fabric` (DEV / QA / PROD). Two environments may never share
one: a dev lineage edge landing on a prod dataset turns the graph into a confident lie
about production, and nothing downstream can tell. `cdc/urns.py` mints them and the client
refuses any URN whose fabric is not its own.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

import yaml

from .models import ConfigError

DEFAULT_CONFIG = (Path(__file__).resolve().parents[1]
                  / "governance" / "registry" / "metadata_plane.yaml")


class PlaneMode(str, Enum):
    DISABLED = "disabled"
    LOCAL = "local"
    PRODUCTION = "production"


class OutagePolicy(str, Enum):
    BEST_EFFORT = "BEST_EFFORT"
    REQUIRED = "REQUIRED"


class Fabric(str, Enum):
    """DataHub's environment component. A closed set: an unknown fabric is a compile error,
    never a new environment invented by a typo."""

    DEV = "DEV"
    QA = "QA"
    PROD = "PROD"


#: Flows whose purpose IS metadata, and are therefore allowed to be REQUIRED. Everything
#: else is a data flow: it has already committed correct business data by the time it
#: publishes, so failing it on a catalogue outage would convert a metadata problem into a
#: data problem.
METADATA_ONLY_FLOWS = ("metadata_ingestion", "lineage_impact")

#: Anything that looks like a credential sitting in the config file rather than in the
#: environment. CLAUDE.md §3.1: no static credential in Git, ever.
_SECRET_KEYS = ("token", "password", "secret", "api_key", "apikey", "credential")

#: Suffixes that make a key name a POINTER rather than the thing pointed at. `token_env`
#: names an environment variable, `secret_source` names a mechanism, `token_ssm_parameter`
#: names a path -- none of them is a credential, and refusing them would force the config to
#: stop documenting where the secret comes from, which is the opposite of the intent.
_POINTER_SUFFIXES = ("_env", "_ssm_parameter", "_source", "_arn", "_path", "_uri",
                     "_parameter", "_key_id", "_provider")
_SSM_PATH = re.compile(r"^/[A-Za-z0-9._\-/]+$")


@dataclass(frozen=True)
class Environment:
    name: str
    fabric: Fabric
    namespace: str
    aws_environment: str

    def payload(self) -> dict:
        return {"name": self.name, "fabric": self.fabric.value,
                "namespace": self.namespace, "aws_environment": self.aws_environment}


@dataclass(frozen=True)
class ClientSettings:
    timeout_seconds: float = 10.0
    max_attempts: int = 3
    backoff_seconds: float = 0.5
    backoff_multiplier: float = 2.0
    dry_run: bool = True

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ConfigError("client.timeout_seconds must be positive")
        if not 1 <= self.max_attempts <= 10:
            raise ConfigError(
                f"client.max_attempts must be 1-10, got {self.max_attempts}. Retry has to be "
                f"BOUNDED: a best-effort publish that retries forever is a hang inside a "
                f"Spark task, which is worse than the missing metadata it was protecting.")
        if self.backoff_seconds < 0 or self.backoff_multiplier < 1:
            raise ConfigError("client backoff must not shrink between attempts")

    def delays(self) -> tuple[float, ...]:
        """The wait before each retry. Deterministic, so a test can assert the total bound."""
        return tuple(self.backoff_seconds * (self.backoff_multiplier ** i)
                     for i in range(max(0, self.max_attempts - 1)))

    @property
    def worst_case_seconds(self) -> float:
        """What a caller pays if the plane is completely down. Published because a number
        nobody can state is a number nobody has bounded."""
        return self.max_attempts * self.timeout_seconds + sum(self.delays())


@dataclass(frozen=True)
class AuthSettings:
    token_env: str = "DATAHUB_GMS_TOKEN"
    gms_url_env: str = "DATAHUB_GMS_URL"
    token_ssm_parameter: str = ""

    def token(self) -> str:
        """Read at call time, never stored on the instance.

        Holding it on a frozen dataclass would put it into every `repr`, every log line that
        prints the config, and every pytest failure dump.
        """
        return os.environ.get(self.token_env, "")

    def gms_url(self, fallback: str = "") -> str:
        return os.environ.get(self.gms_url_env, "") or fallback


@dataclass(frozen=True)
class MetadataPlaneConfig:
    mode: PlaneMode
    platform: str
    environment: Environment
    environments: dict
    outage_default: OutagePolicy
    outage_by_flow: dict
    client: ClientSettings
    auth: AuthSettings
    server_image_tag: str = ""
    server_image_digests: dict = field(default_factory=dict)
    cli_package: str = "acryl-datahub"
    cli_version: str = ""
    local: dict = field(default_factory=dict)
    production: dict = field(default_factory=dict)

    # -- policy -----------------------------------------------------------
    @property
    def enabled(self) -> bool:
        return self.mode is not PlaneMode.DISABLED

    def policy_for(self, flow: str) -> OutagePolicy:
        return self.outage_by_flow.get(flow, self.outage_default)

    def must_succeed(self, flow: str) -> bool:
        """REQUIRED only counts when the plane is actually on.

        With the plane `disabled` nothing can reach DataHub by definition, so treating a
        REQUIRED flow as failed would make `disabled` mean "everything metadata-shaped is
        broken" instead of "there is no metadata plane". The flow refuses to RUN in that
        case (`refuse_reason`), which is a different and more honest outcome.
        """
        return self.enabled and self.policy_for(flow) is OutagePolicy.REQUIRED

    def refuse_reason(self, flow: str) -> str:
        """Why a metadata-only flow may not run at all, or '' if it may."""
        if self.enabled:
            return ""
        if self.policy_for(flow) is OutagePolicy.REQUIRED:
            return (f"flow {flow!r} is REQUIRED but the metadata plane is {self.mode.value}. "
                    f"Its entire purpose is metadata, so running it would report success for "
                    f"work that could not have happened.")
        return ""

    @property
    def gms_url(self) -> str:
        if self.mode is PlaneMode.LOCAL:
            return self.auth.gms_url(self.local.get("gms_url", ""))
        return self.auth.gms_url()

    def payload(self) -> dict:
        return {"mode": self.mode.value, "platform": self.platform,
                "environment": self.environment.payload(),
                "outage_default": self.outage_default.value,
                "outage_by_flow": {k: self.outage_by_flow[k].value
                                   for k in sorted(self.outage_by_flow)},
                "client": {"timeout_seconds": self.client.timeout_seconds,
                           "max_attempts": self.client.max_attempts,
                           "dry_run": self.client.dry_run,
                           "worst_case_seconds": round(self.client.worst_case_seconds, 2)},
                "server_image_tag": self.server_image_tag,
                "cli": f"{self.cli_package}=={self.cli_version}"}


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #

def _no_inline_secret(node, *, where: str) -> None:
    """Refuse a credential that is sitting in the file rather than in the environment."""
    if isinstance(node, dict):
        for k, v in node.items():
            lowered = str(k).lower()
            if any(s in lowered for s in _SECRET_KEYS) and isinstance(v, str) and v:
                # `*_env` names an environment VARIABLE and `*_ssm_parameter` names a path;
                # neither is the secret itself.
                if lowered.endswith(_POINTER_SUFFIXES):
                    continue
                raise ConfigError(
                    f"{where}.{k}: a credential may not appear in this file. Name the "
                    f"environment variable (`{k}_env`) or the SSM parameter "
                    f"(`{k}_ssm_parameter`) instead. CLAUDE.md §3.1 — no static credential "
                    f"in Git, and a config file is Git.")
            _no_inline_secret(v, where=f"{where}.{k}")
    elif isinstance(node, list):
        for i, v in enumerate(node):
            _no_inline_secret(v, where=f"{where}[{i}]")


def _enum(cls, raw, *, what: str):
    try:
        return cls(raw)
    except ValueError:
        raise ConfigError(f"{what}: {raw!r} is not one of "
                          f"{', '.join(m.value for m in cls)}") from None


def load(path: Path | None = None, *, environment: str | None = None,
         mode: str | None = None) -> MetadataPlaneConfig:
    """`environment` and `mode` override the file, for tests and for an explicit operator
    choice. Neither can widen what the file forbids: `production` still requires the flag."""
    path = path or DEFAULT_CONFIG
    if not path.exists():
        raise ConfigError(
            f"{path}: the metadata plane config is missing. Without it the mode, the fabric "
            f"and the outage policy would all be defaults nobody chose.")
    doc = yaml.safe_load(path.read_text()) or {}
    _no_inline_secret(doc, where=str(path.name))

    plane = doc.get("metadata_plane") or {}
    resolved_mode = _enum(PlaneMode, mode or plane.get("mode", "disabled"),
                          what="metadata_plane.mode")
    platform = plane.get("platform", "datahub")
    if platform != "datahub":
        raise ConfigError(
            f"metadata_plane.platform: {platform!r}. ADR-087 makes DataHub the ONLY primary "
            f"metadata plane, and forbids running a second one beside it. Switching means a "
            f"comparison ADR, not a config edit.")

    envs_raw = doc.get("environments") or {}
    if not envs_raw:
        raise ConfigError("environments: at least one is required")
    environments = {}
    seen_fabric: dict = {}
    for name, body in envs_raw.items():
        body = body or {}
        fabric = _enum(Fabric, body.get("fabric"), what=f"environments.{name}.fabric")
        if fabric in seen_fabric:
            raise ConfigError(
                f"environments.{name}.fabric {fabric.value} is already used by "
                f"{seen_fabric[fabric]!r}. Two environments sharing a fabric means a dev "
                f"lineage edge can land on a prod dataset, and nothing downstream can tell.")
        seen_fabric[fabric] = name
        environments[name] = Environment(
            name=name, fabric=fabric,
            namespace=body.get("namespace") or f"cdc-lakehouse-{name}",
            aws_environment=body.get("aws_environment") or name)

    active = environment or doc.get("active_environment")
    if active not in environments:
        raise ConfigError(
            f"active_environment {active!r} is not declared. Known: "
            f"{', '.join(sorted(environments))}")

    outage = doc.get("outage_policy") or {}
    default = _enum(OutagePolicy, outage.get("default", "BEST_EFFORT"),
                    what="outage_policy.default")
    if default is OutagePolicy.REQUIRED:
        raise ConfigError(
            "outage_policy.default: REQUIRED. The default applies to DATA flows, and a data "
            "flow that fails on a catalogue outage turns a metadata problem into a data "
            "problem. Name the metadata-only flows individually instead.")
    by_flow = {}
    for flow, raw in (outage.get("by_flow") or {}).items():
        policy = _enum(OutagePolicy, raw, what=f"outage_policy.by_flow.{flow}")
        if policy is OutagePolicy.REQUIRED and flow not in METADATA_ONLY_FLOWS:
            raise ConfigError(
                f"outage_policy.by_flow.{flow}: REQUIRED is only allowed for a flow whose "
                f"PURPOSE is metadata ({', '.join(METADATA_ONLY_FLOWS)}). {flow} moves "
                f"business data; its rows are already committed and correct when it "
                f"publishes, so failing it on a DataHub outage loses nothing and breaks "
                f"everything.")
        by_flow[flow] = policy

    c = doc.get("client") or {}
    client = ClientSettings(
        timeout_seconds=float(c.get("timeout_seconds", 10)),
        max_attempts=int(c.get("max_attempts", 3)),
        backoff_seconds=float(c.get("backoff_seconds", 0.5)),
        backoff_multiplier=float(c.get("backoff_multiplier", 2.0)),
        dry_run=bool(c.get("dry_run", True)))

    a = doc.get("auth") or {}
    ssm = a.get("token_ssm_parameter", "")
    if ssm and not _SSM_PATH.match(ssm):
        raise ConfigError(f"auth.token_ssm_parameter {ssm!r} is not an SSM path")
    auth = AuthSettings(token_env=a.get("token_env", "DATAHUB_GMS_TOKEN"),
                        gms_url_env=a.get("gms_url_env", "DATAHUB_GMS_URL"),
                        token_ssm_parameter=ssm)

    versions = doc.get("versions") or {}
    server = versions.get("server") or {}
    cli = versions.get("cli") or {}
    production = doc.get("production") or {}

    cfg = MetadataPlaneConfig(
        mode=resolved_mode, platform=platform, environment=environments[active],
        environments=environments, outage_default=default, outage_by_flow=by_flow,
        client=client, auth=auth,
        server_image_tag=server.get("image_tag", ""),
        server_image_digests=dict(server.get("images") or {}),
        cli_package=cli.get("package", "acryl-datahub"),
        cli_version=str(cli.get("version", "")),
        local=dict(doc.get("local") or {}), production=dict(production))
    validate(cfg)
    return cfg


def validate(cfg: MetadataPlaneConfig) -> None:
    """Rules that apply once the config is assembled."""
    if cfg.mode is PlaneMode.DISABLED:
        return

    for label, value in (("versions.server.image_tag", cfg.server_image_tag),
                         ("versions.cli.version", cfg.cli_version)):
        if not value:
            raise ConfigError(
                f"{label} is required in mode {cfg.mode.value}. CLAUDE.md §3.9 forbids "
                f"`latest`, and an unpinned metadata plane is one that changes underneath "
                f"a lineage graph without anything recording that it did.")
        if value.strip().lower() in ("latest", "head", "nightly", "quickstart"):
            raise ConfigError(f"{label}: {value!r} is a floating tag and is forbidden")

    if not _same_release_line(cfg.server_image_tag, cfg.cli_version):
        raise ConfigError(
            f"server image {cfg.server_image_tag} and CLI {cfg.cli_version} are not on the "
            f"same release line. DataHub publishes the two on independent PATCH cadences, "
            f"so the check is the shared major.minor.patch prefix -- but a CLI a whole "
            f"MINOR ahead of the server writes aspects the server cannot read.")

    if cfg.mode is PlaneMode.PRODUCTION:
        if not cfg.production.get("enable_datahub"):
            raise ConfigError(
                "mode `production` requires `production.enable_datahub: true`, which is "
                "false. CLAUDE.md §4.12 makes every resident component flag-gated, and "
                f"this one is additionally blocked on: "
                f"{cfg.production.get('blocked_until', 'an unmeasured sizing question')}.")
        for key, expected in (("networking", "private_only"), ("tls", "required"),
                              ("authentication", "required"),
                              ("secret_source", "ssm_securestring")):
            if cfg.production.get(key) != expected:
                raise ConfigError(
                    f"production.{key} must be {expected!r} (got "
                    f"{cfg.production.get(key)!r}). CLAUDE.md §3.3/§3.4/§3.6 — a metadata "
                    f"plane holds the map of where every regulated column lives, so it is "
                    f"treated at the classification of the most sensitive thing it "
                    f"describes.")


def _same_release_line(image_tag: str, cli_version: str) -> bool:
    """`v1.7.0.1` and `1.7.0.13` share the line `1.7.0`.

    DataHub tags the server `vMAJOR.MINOR.PATCH[.BUILD]` and ships the CLI
    `MAJOR.MINOR.PATCH[.BUILD]`, and the BUILD components move independently -- on
    2026-09-30 the newest server image was `v1.7.0.1` while no `1.7.0.1` existed on PyPI at
    all. Comparing the full strings would fail a perfectly valid pair.
    """
    def line(v: str) -> tuple:
        parts = v.lstrip("vV").split(".")
        return tuple(parts[:3])
    return line(image_tag) == line(cli_version)
