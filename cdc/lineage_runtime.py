"""OpenLineage runtime emission — job names, facets, and the identity that joins DRP3 to DRP4.

DRP3. Three things this module exists to get right.

1. ONE NAME PER JOB, EVERYWHERE
-------------------------------
`LINEAGE_JOBS` is the only list of job names. Airflow, Spark and every facet use it. A job
whose lineage name is derived from a DAG id in one place and a Spark app name in another
appears in the graph TWICE, and neither copy has the full run history -- which looks like
patchy instrumentation rather than a naming bug, so nobody fixes it.

2. IDENTITY IS CARRIED, NOT INFERRED
-------------------------------------
The Spark listener emits `(namespace, name)` for each dataset; the DataHub Glue ingestor
emits `database.table`. If those two have to be RECONCILED BY INFERENCE, the rule lives in
two codebases and drifts silently -- and the symptom is a lineage graph that splits into
fragments, each of which looks complete.

So every emitted dataset carries its canonical DataHub URN as a facet, minted by
`cdc/urns.py`. A carried identity is checkable; an inferred one is a hope.

3. A FACET IS AN ALLOWLIST
---------------------------
`scrub()` refuses any key outside the allowlist and any key containing a denied substring
(`token`, `password`, `sample`, `payload`, `row`, `before_image`, ...). The failure mode of
a facet builder is not malice -- it is someone adding "just the failing row" while debugging,
into a metadata store that dashboards, alerts and the AI assistant all read.

A facet carries a REFERENCE to evidence (`dq_run_id`, `certification_run_id`), never the
evidence. Heavy operational detail stays in OPS, which is already the operational truth.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

from .assets import AssetId
from .metadata_plane import MetadataPlaneConfig
from .models import ConfigError
from .urns import UrnMinter

DEFAULT_CONFIG = (Path(__file__).resolve().parents[1]
                  / "governance" / "registry" / "openlineage.yaml")

#: OpenLineage run states. START begins a run; the other three end it.
RUN_START = "START"
RUN_COMPLETE = "COMPLETE"
RUN_FAIL = "FAIL"
RUN_ABORT = "ABORT"
TERMINAL_STATES = (RUN_COMPLETE, RUN_FAIL, RUN_ABORT)

#: A UUID-shaped run id. OpenLineage requires one per run; a non-unique id silently merges
#: two runs into one, and the second one's inputs appear to belong to the first.
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)


@dataclass(frozen=True)
class LineageJob:
    name: str
    description: str
    engine: str


@dataclass(frozen=True)
class LineageRuntimeConfig:
    enabled: bool
    jobs: dict
    allow_facets: tuple
    deny_substrings: tuple
    spark_package: str
    spark_jar_location: str
    spark_jar_runtime_path: str
    spark_jar: str
    spark_sha1: str
    listener_class: str
    airflow_provider: str
    airflow_provider_version: str
    airflow_requires: str
    python_version: str
    dbt_version: str
    transport_type: str
    fail_on_error: bool
    timeout_seconds: float
    endpoint_path: str
    url_env: str
    token_env: str
    carry_canonical_urn: bool
    urn_facet: str
    namespaces: dict

    @classmethod
    def load(cls, path: Path | None = None) -> "LineageRuntimeConfig":
        path = path or DEFAULT_CONFIG
        if not path.exists():
            raise ConfigError(f"{path}: the OpenLineage runtime config is missing")
        d = yaml.safe_load(path.read_text()) or {}
        v, t, f, i = (d.get("versions") or {}, d.get("transport") or {},
                      d.get("facets") or {}, d.get("identity") or {})
        spark, air, py, dbt = (v.get("spark") or {}, v.get("airflow") or {},
                               v.get("python") or {}, v.get("dbt") or {})
        dh = t.get("datahub") or {}
        jobs = {k: LineageJob(k, (b or {}).get("description", ""), (b or {}).get("engine", ""))
                for k, b in (d.get("jobs") or {}).items()}
        if not jobs:
            raise ConfigError(f"{path}: `jobs` is required; an unnamed job cannot be one name "
                              f"everywhere, which is the entire point of the list")
        for label, value in (("versions.spark.package", spark.get("package")),
                             ("versions.spark.sha1", spark.get("sha1")),
                             ("versions.airflow.version", air.get("version"))):
            if not value:
                raise ConfigError(f"{path}: {label} is required. CLAUDE.md §3.9 — a floating "
                                  f"lineage agent changes the graph's shape with nothing "
                                  f"recording that it did.")
        if ":latest" in str(spark.get("package")):
            raise ConfigError(f"{path}: versions.spark.package must be an exact version")
        allow = tuple(f.get("allow") or ())
        deny = tuple(f.get("deny_substrings") or ())
        # The deny list guards the ALLOWLIST, checked here rather than only at emit time.
        # Otherwise a data-carrying name could be added to the allowlist and would then be
        # published forever, reviewed-looking and never re-examined.
        poisoned = [a for a in allow if any(bad in a.lower() for bad in deny)]
        if poisoned:
            raise ConfigError(
                f"{path}: allowlisted facet(s) {', '.join(poisoned)} contain a denied "
                f"substring. The allowlist is what a reviewer reads; a data carrier on it "
                f"is published forever and never re-examined.")
        return cls(
            enabled=bool(d.get("enabled", False)), jobs=jobs,
            allow_facets=allow, deny_substrings=deny,
            spark_package=spark["package"], spark_jar=spark.get("jar", ""),
            spark_jar_location=str(spark.get("jar_location", "")),
            spark_jar_runtime_path=str(spark.get("jar_runtime_path", "")),
            spark_sha1=spark["sha1"],
            listener_class=spark.get("listener_class",
                                     "io.openlineage.spark.agent.OpenLineageSparkListener"),
            airflow_provider=air.get("package", "apache-airflow-providers-openlineage"),
            airflow_provider_version=str(air["version"]),
            airflow_requires=str(air.get("requires_airflow", "")),
            python_version=str(py.get("version", "")), dbt_version=str(dbt.get("version", "")),
            transport_type=t.get("type", "console"),
            fail_on_error=bool(t.get("fail_on_error", False)),
            timeout_seconds=float(t.get("timeout_seconds", 10)),
            endpoint_path=dh.get("endpoint_path", ""), url_env=dh.get("url_env", ""),
            token_env=dh.get("token_env", ""),
            carry_canonical_urn=bool(i.get("carry_canonical_urn", True)),
            urn_facet=i.get("urn_facet", "datahub_urn"),
            namespaces=dict(i.get("namespaces") or {}))

    def job(self, name: str) -> LineageJob:
        try:
            return self.jobs[name]
        except KeyError:
            raise ConfigError(
                f"unknown lineage job {name!r}. Declared: {', '.join(sorted(self.jobs))}. A "
                f"job name invented at the call site appears in the graph as a second job "
                f"with no history.") from None


# --------------------------------------------------------------------------- #
# Facets
# --------------------------------------------------------------------------- #

def scrub(cfg: LineageRuntimeConfig, facet: dict, *, where: str = "facet") -> dict:
    """Refuse anything outside the allowlist, or anything whose key looks like data.

    Raises rather than dropping. A silently dropped facet is a facet the author believes is
    being published, which is how a lineage graph acquires a field nobody can find.
    """
    out = {}
    for key, value in facet.items():
        lowered = str(key).lower()
        if key not in cfg.allow_facets:
            hit = next((b for b in cfg.deny_substrings if b in lowered), "")
            reason = (f" It contains {hit!r}: a lineage facet carries REFERENCES to "
                      f"evidence, never the evidence. Failing rows live in the "
                      f"access-controlled quarantine; this graph is read by dashboards, "
                      f"alerts and the AI assistant." if hit else
                      " Add it to `governance/registry/openlineage.yaml` deliberately, or "
                      "it is a field nobody reviewed.")
            raise ConfigError(f"{where}.{key}: not in the facet allowlist.{reason}")
        if value is None:
            continue
        out[key] = value.isoformat() if isinstance(value, (date, datetime)) else value
    return out


@dataclass
class RunContext:
    """Everything a run knows about itself. Assembled once, emitted on START and on the end."""

    job: str
    run_id: str
    environment: str
    flow_mode: str = ""
    table_id: str = ""
    cob_date: date | None = None
    data_interval_start: datetime | None = None
    data_interval_end: datetime | None = None
    config_version: str = ""
    config_hash: str = ""
    spark_app_id: str = ""
    airflow_dag_id: str = ""
    airflow_run_id: str = ""
    airflow_task_id: str = ""
    airflow_try_number: int | None = None
    attempt: int | None = None
    source_snapshot_id: str = ""
    target_snapshot_id: str = ""
    watermark_before: str = ""
    watermark_after: str = ""
    rows_read: int | None = None
    rows_written: int | None = None
    certification_tier: str = ""
    certification_run_id: str = ""
    dq_run_id: str = ""
    recon_run_id: str = ""

    def __post_init__(self) -> None:
        if not _UUID.match(self.run_id):
            raise ConfigError(
                f"run_id {self.run_id!r} is not a UUID. OpenLineage keys a run by this id, "
                f"so a repeated or malformed one merges two runs and the second run's "
                f"inputs appear to belong to the first.")

    def facets(self, cfg: LineageRuntimeConfig) -> dict:
        raw = {"environment": self.environment, "job_id": self.job}
        for name in ("flow_mode", "table_id", "cob_date", "data_interval_start",
                     "data_interval_end", "config_version", "config_hash", "spark_app_id",
                     "airflow_dag_id", "airflow_run_id", "airflow_task_id",
                     "airflow_try_number", "attempt", "source_snapshot_id",
                     "target_snapshot_id", "watermark_before", "watermark_after",
                     "rows_read", "rows_written", "certification_tier",
                     "certification_run_id", "dq_run_id", "recon_run_id"):
            value = getattr(self, name)
            if value not in (None, ""):
                raw[name] = value
        return scrub(cfg, raw, where=f"run[{self.job}]")


@dataclass(frozen=True)
class LineageDataset:
    namespace: str
    name: str
    facets: dict = field(default_factory=dict)

    def payload(self) -> dict:
        return {"namespace": self.namespace, "name": self.name,
                "facets": {k: self.facets[k] for k in sorted(self.facets)}}


class LineageEmitter:
    """Builds OpenLineage events. Does NOT send them -- the Spark listener and the Airflow
    provider do that. This is the shape everything must agree on, and the place a test can
    assert about content rather than about configuration text."""

    def __init__(self, cfg: LineageRuntimeConfig, plane: MetadataPlaneConfig,
                 minter: UrnMinter):
        self.cfg = cfg
        self.plane = plane
        self.minter = minter

    # -- identity ---------------------------------------------------------
    def dataset(self, asset: AssetId, **kw) -> LineageDataset:
        """An OpenLineage dataset that CARRIES its canonical DataHub URN."""
        urn = self.minter.dataset_urn(asset, **kw)
        self.minter.assert_fabric(urn)
        platform = self.minter.platform(asset, source_engine=kw.get("source_engine", ""))
        namespace = self.cfg.namespaces.get(platform)
        if not namespace:
            raise ConfigError(
                f"no OpenLineage namespace declared for platform {platform!r}. Without it "
                f"the listener and the catalogue would each pick their own, and one table "
                f"would become two datasets.")
        facets = {}
        if self.cfg.carry_canonical_urn:
            facets[self.cfg.urn_facet] = urn
        return LineageDataset(namespace, self.minter.dataset_name(asset, **kw),
                              scrub(self.cfg, facets, where=f"dataset[{asset}]"))

    def canonical_urn(self, ds: LineageDataset) -> str:
        try:
            return ds.facets[self.cfg.urn_facet]
        except KeyError:
            raise ConfigError(
                f"dataset {ds.namespace}/{ds.name} carries no {self.cfg.urn_facet} facet, so "
                f"it cannot be joined to the catalogue without inference.") from None

    # -- events -----------------------------------------------------------
    def event(self, ctx: RunContext, state: str, *, inputs=(), outputs=(),
              parent: RunContext | None = None, error: str = "",
              event_time: datetime | None = None) -> dict:
        if state not in (RUN_START, *TERMINAL_STATES):
            raise ConfigError(f"unknown run state {state!r}")
        self.cfg.job(ctx.job)
        if state is RUN_START and (inputs or outputs):
            # Not an error -- START may legitimately declare planned IO -- but the ROW COUNTS
            # cannot be known yet, and a START carrying them would be reporting a result.
            for name in ("rows_read", "rows_written"):
                if getattr(ctx, name) is not None:
                    raise ConfigError(
                        f"{name} on a START event: the run has not read anything yet. A "
                        f"count emitted before the work is a number nobody measured.")
        if state == RUN_FAIL and not error:
            raise ConfigError("a FAIL event must carry an error; a failure with no reason "
                              "is indistinguishable in the graph from a run that vanished")

        run: dict = {"runId": ctx.run_id, "facets": {"platform": ctx.facets(self.cfg)}}
        if parent is not None:
            # Parent-child. Without it, an Airflow-submitted Spark job appears in the graph
            # as an orphan and the run hierarchy has a hole exactly where the orchestration is.
            run["facets"]["parent"] = {
                "run": {"runId": parent.run_id},
                "job": {"namespace": self.plane.environment.namespace, "name": parent.job}}
        if error:
            run["facets"]["errorMessage"] = {"message": error[:2000]}

        return {
            "eventType": state,
            "eventTime": (event_time or datetime.now(timezone.utc)).isoformat(),
            "producer": f"cdc-lakehouse/{self.cfg.spark_package}",
            "job": {"namespace": self.plane.environment.namespace, "name": ctx.job},
            "run": run,
            "inputs": [d.payload() for d in inputs],
            "outputs": [d.payload() for d in outputs],
        }

    # -- runtime configuration --------------------------------------------
    def spark_conf(self, *, parent: RunContext | None = None, app_name: str = "") -> dict:
        """The `--conf` pairs a Spark submit needs. DERIVED, so the pin cannot drift from
        the config that records it.

        `app_name` is not cosmetic. Without it the EMR run emitted
        `job.name = "unknown"` and the listener warned `Failed to obtain the application
        name`: on EMR Serverless `spark.app.name` is not set early enough for the
        APPLICATION START event. Every job would then land on ONE node called `unknown`,
        collapsing the graph exactly the way ADR-090 exists to prevent -- one name per job,
        declared once.
        """
        conf = {
            "spark.extraListeners": self.cfg.listener_class,
            "spark.openlineage.transport.type": self.cfg.transport_type,
            "spark.openlineage.namespace": self.plane.environment.namespace,
            "spark.openlineage.facets.custom_environment_variables": "[]",
        }
        # A STAGED jar beats Maven resolution, and here it is not a preference. EMR
        # Serverless runs in private subnets with no NAT (cost invariant 2), so
        # `spark.jars.packages` cannot reach repo1.maven.org -- Ivy retries and the job
        # dies during resolution having done no work. `spark.jars` over the S3 gateway
        # endpoint is the only form that runs. Maven resolution is kept for a submit from
        # somewhere that HAS egress, so a laptop run still works.
        if self.cfg.spark_jar_location:
            conf["spark.jars"] = self.cfg.spark_jar_location
            # ALSO on the system classpath, or OpenLineage and the Iceberg connector load
            # under different classloaders and the integration cannot read Iceberg's write
            # plans. The symptom is not an error: the APPLICATION event is emitted, every
            # dataset event is missing, and lineage looks enabled while carrying nothing.
            if self.cfg.spark_jar_runtime_path:
                for side in ("driver", "executor"):
                    conf[f"spark.{side}.extraClassPath"] = self.cfg.spark_jar_runtime_path
        else:
            conf["spark.jars.packages"] = self.cfg.spark_package
        if self.cfg.transport_type == "http":
            conf["spark.openlineage.transport.endpoint"] = self.cfg.endpoint_path
            conf["spark.openlineage.transport.timeoutInMillis"] = str(
                int(self.cfg.timeout_seconds * 1000))
        if app_name:
            conf["spark.openlineage.appName"] = app_name
        if parent is not None:
            conf["spark.openlineage.parentJobName"] = parent.job
            conf["spark.openlineage.parentRunId"] = parent.run_id
            conf["spark.openlineage.parentJobNamespace"] = self.plane.environment.namespace
        return conf

    def airflow_transport(self) -> dict:
        """The Airflow provider's transport, as the OBJECT it actually parses.

        Not the same shape as `spark_conf()`. The Spark listener takes a flat
        `spark.openlineage.transport.type=console` string; the Airflow provider reads
        ONE key and parses it as JSON:

            conf.getjson("openlineage", "transport", fallback={})

        so a bare `console` raises `AirflowConfigException: Unable to parse [openlineage]
        'transport' as valid json`. That exception is thrown while the plugin is being
        IMPORTED, so the failure mode is silent in the worst way: the plugin is skipped,
        the listener is never registered, every task runs perfectly, and not one lineage
        event is emitted. Observed 2026-09-30 on Airflow 3.2.2 with provider 2.20.2 --
        `REGISTERED_LISTENERS = []` while the DAG went green.

        Two agreeing sides can still both be wrong: a test asserted that values.yaml
        matched this function, and it passed, because both carried the bare string.
        """
        if self.cfg.transport_type == "http":
            # Deliberately refused rather than rendered. The OpenLineage client does NOT
            # expand `${VAR}` inside this JSON, so an http transport can only be expressed
            # by writing the GMS url and its API token LITERALLY into the value -- which
            # would put a secret into values.yaml and into the Helm release secret, against
            # security invariant 6. Airflow's own answer is the `_CMD` suffix:
            #
            #   AIRFLOW__OPENLINEAGE__TRANSPORT_CMD=/opt/airflow/bin/ol-transport.sh
            #
            # where the script reads DATAHUB_GMS_URL / DATAHUB_GMS_TOKEN (see
            # `transport.datahub.url_env` / `token_env`) at runtime and prints the JSON.
            # Returning a half-built object here would look configured and ship no token.
            raise ConfigError(
                "http transport cannot be expressed as a plain AIRFLOW__OPENLINEAGE__"
                f"TRANSPORT value: the token in {self.cfg.token_env} would have to be "
                "written literally into the config. Use AIRFLOW__OPENLINEAGE__TRANSPORT_CMD "
                "and have the command emit the JSON with the secret read at runtime.")
        return {"type": self.cfg.transport_type}

    def airflow_env(self) -> dict:
        """Environment variables the Airflow provider reads. No secret value appears here --
        only the NAME of the variable that carries one."""
        env = {
            "AIRFLOW__OPENLINEAGE__NAMESPACE": self.plane.environment.namespace,
            # JSON, not a bare string -- see airflow_transport(). `separators` keeps it to
            # one line so it survives a YAML scalar and a `--set-string` without quoting
            # games.
            "AIRFLOW__OPENLINEAGE__TRANSPORT": json.dumps(
                self.airflow_transport(), separators=(",", ":"), sort_keys=True),
            "AIRFLOW__OPENLINEAGE__DISABLED": "false" if self.cfg.enabled else "true",
            # The provider must not do lineage work while the scheduler is PARSING a DAG:
            # parse time is on the critical path of every scheduling loop.
            "AIRFLOW__OPENLINEAGE__DISABLE_SOURCE_CODE": "true",
        }
        return env
