"""The DataHub client — and the rule that it can never take the data plane down with it.

DRP2. One client, four transports, and a publish path whose failure mode is DECLARED rather
than discovered.

    NullTransport       mode `disabled`. Records what it WOULD have sent and sends nothing.
    RecordingTransport  dry run and tests. Same, but inspectable.
    FileTransport       the offline sink from ADR-087 constraint 4: newline-delimited MCPs
                        on disk, validated in CI, loaded when a server exists. This is what
                        lets DRP4-DRP7 make progress at $0 with nothing running.
    RestTransport       a live GMS. urllib only -- no new dependency, because adding one to
                        the Spark wheelhouse to publish metadata would make a metadata
                        outage into a deployment problem.

WHY A PUBLISH CANNOT RAISE
--------------------------
By the time a job publishes metadata, its business data is already committed and correct.
Raising there converts a catalogue outage into a data outage: the table is right, the mart
is right, and the DAG goes red because a web service was restarting.

So `publish()` returns a `PublishResult` and raises only when the flow's declared policy is
`REQUIRED` -- which `metadata_plane.py` permits for exactly two flows, both of whose entire
purpose is metadata. Everything else is BEST_EFFORT by construction, not by convention.

WHY RETRY IS BOUNDED AND SHORT
------------------------------
`ClientSettings.worst_case_seconds` is 31.5s at the shipped settings: 3 attempts x 10s plus
0.5s + 1.0s of backoff. A number nobody can state is a number nobody has bounded, and an
unbounded retry inside a Spark task is a hang wearing a resilience costume.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

from .metadata_plane import MetadataPlaneConfig, OutagePolicy, PlaneMode
from .models import ConfigError
from .urns import UrnMinter, default_catalog

#: Aspect names this client writes. A closed set: an unknown aspect is a typo that DataHub
#: would accept and silently drop, which looks exactly like a successful publish.
KNOWN_ASPECTS = (
    "datasetProperties", "schemaMetadata", "ownership", "globalTags", "glossaryTerms",
    "domains", "datasetProfile", "upstreamLineage", "status", "subTypes",
    "dataFlowInfo", "dataJobInfo", "dataJobInputOutput",
    "dataProcessInstanceProperties", "dataProcessInstanceRunEvent",
    "assertionInfo", "assertionRunEvent",
)


class MetadataPublishError(RuntimeError):
    """Raised ONLY when the flow's policy is REQUIRED. A BEST_EFFORT failure is a returned
    result, never an exception -- see the module docstring."""


@dataclass(frozen=True)
class MetadataChangeProposal:
    """One aspect on one entity. The unit the whole client moves."""

    entity_urn: str
    aspect_name: str
    aspect: dict
    entity_type: str = "dataset"
    change_type: str = "UPSERT"

    def __post_init__(self) -> None:
        if self.aspect_name not in KNOWN_ASPECTS:
            raise ConfigError(
                f"unknown aspect {self.aspect_name!r}. Known: {', '.join(KNOWN_ASPECTS)}. "
                f"DataHub accepts an unrecognised aspect name and drops it, which is "
                f"indistinguishable from a successful publish.")
        if not self.entity_urn.startswith("urn:li:"):
            raise ConfigError(f"{self.entity_urn!r} is not a URN")

    def payload(self) -> dict:
        return {"entityType": self.entity_type, "entityUrn": self.entity_urn,
                "changeType": self.change_type, "aspectName": self.aspect_name,
                "aspect": {"value": json.dumps(self.aspect, sort_keys=True,
                                               separators=(",", ":")),
                           "contentType": "application/json"}}


# --------------------------------------------------------------------------- #
# Transports
# --------------------------------------------------------------------------- #

class Transport:
    name = "base"

    def send(self, mcps: list) -> None:            # pragma: no cover - interface
        raise NotImplementedError


class NullTransport(Transport):
    """Mode `disabled`. Counts, discards, never fails."""

    name = "null"

    def __init__(self) -> None:
        self.count = 0

    def send(self, mcps: list) -> None:
        self.count += len(mcps)


class RecordingTransport(Transport):
    """Dry run and tests. Keeps every proposal so an assertion can be made about CONTENT,
    which is the only kind of assertion that catches a publish that never happened."""

    name = "recording"

    def __init__(self) -> None:
        self.sent: list = []

    def send(self, mcps: list) -> None:
        self.sent.extend(mcps)

    def aspects_for(self, urn: str) -> set:
        return {m.aspect_name for m in self.sent if m.entity_urn == urn}


class FileTransport(Transport):
    """Newline-delimited MCPs. ADR-087's offline path: valid metadata with nothing running."""

    name = "file"

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, mcps: list) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            for m in mcps:
                fh.write(json.dumps(m.payload(), sort_keys=True, separators=(",", ":")) + "\n")


class RestTransport(Transport):
    """A live GMS. **Live-tested 2026-09-30** against DataHub v1.7.0.1: 9/9 smoke aspects,
    373 governance aspects and 87 lineage aspects, all accepted on the first attempt, and
    read back through `get_aspect`.

    That run found three defects 48 offline tests had not, because a recording transport
    accepts any dict and the aspect shape is only validated server-side. See
    `TestLiveSchemaDefects`.

    `urllib` rather than `requests` or the `acryl-datahub` SDK on purpose: this module is
    imported by Spark jobs through `cdc-framework.zip`, and a metadata client that adds a
    dependency to the EMR wheelhouse turns a catalogue feature into a deployment risk.
    """

    name = "rest"

    def __init__(self, gms_url: str, token: str = "", timeout: float = 10.0):
        if not gms_url:
            raise ConfigError(
                "RestTransport needs a GMS url. It comes from the environment "
                "(DATAHUB_GMS_URL) or the mode's default, never from a config literal.")
        self.gms_url = gms_url.rstrip("/")
        self._token = token
        self.timeout = timeout

    def send(self, mcps: list) -> None:
        for m in mcps:
            body = json.dumps({"proposal": m.payload()}).encode()
            req = urllib.request.Request(
                f"{self.gms_url}/aspects?action=ingestProposal", data=body,
                headers={"Content-Type": "application/json",
                         **({"Authorization": f"Bearer {self._token}"} if self._token else {})})
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    if resp.status >= 300:
                        raise MetadataPublishError(
                            f"GMS returned {resp.status} for {m.aspect_name} on "
                            f"{m.entity_urn}")
            except urllib.error.HTTPError as exc:
                # The response BODY is the only thing that says which aspect was rejected
                # and why. Discarding it leaves "HTTP 422" -- true, and useless: nine
                # proposals go out and the error names none of them. Found on the first
                # live publish.
                detail = ""
                try:
                    detail = exc.read().decode("utf-8", "replace")[:600]
                except Exception:                       # pragma: no cover - best effort
                    pass
                raise MetadataPublishError(
                    f"GMS {exc.code} on aspect {m.aspect_name} for {m.entity_urn}: "
                    f"{detail or exc.reason}") from None


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #

@dataclass
class PublishResult:
    flow: str
    attempted: int = 0
    published: int = 0
    attempts: int = 0
    ok: bool = True
    policy: str = OutagePolicy.BEST_EFFORT.value
    error: str = ""
    skipped_reason: str = ""

    @property
    def degraded(self) -> bool:
        """The plane was on, the publish was tried, and it did not land."""
        return not self.ok and not self.skipped_reason

    def payload(self) -> dict:
        return {"flow": self.flow, "attempted": self.attempted,
                "published": self.published, "attempts": self.attempts, "ok": self.ok,
                "policy": self.policy, "error": self.error,
                "skipped_reason": self.skipped_reason}


@dataclass
class DataHubClient:
    config: MetadataPlaneConfig
    transport: Transport = field(default_factory=NullTransport)
    minter: UrnMinter = None                       # type: ignore[assignment]
    metrics: Counter = field(default_factory=Counter)
    _sleep: object = time.sleep

    def __post_init__(self) -> None:
        if self.minter is None:
            self.minter = UrnMinter(self.config.environment, default_catalog())
        if self.minter.environment.name != self.config.environment.name:
            raise ConfigError(
                f"client environment {self.config.environment.name!r} and minter "
                f"environment {self.minter.environment.name!r} disagree. One process "
                f"speaking for two environments is how a dev edge reaches a prod URN.")

    # -- construction -----------------------------------------------------
    @classmethod
    def build(cls, config: MetadataPlaneConfig, *, transport: Transport | None = None,
              file_sink: Path | None = None) -> "DataHubClient":
        """Pick the transport from the MODE, not from the caller's optimism."""
        if transport is None:
            if config.mode is PlaneMode.DISABLED:
                transport = NullTransport()
            elif file_sink is not None:
                transport = FileTransport(file_sink)
            elif config.client.dry_run:
                transport = RecordingTransport()
            else:
                transport = RestTransport(config.gms_url, config.auth.token(),
                                          config.client.timeout_seconds)
        return cls(config=config, transport=transport)

    # -- publish ----------------------------------------------------------
    def publish(self, flow: str, mcps: list) -> PublishResult:
        """Send, with bounded retry. Raises ONLY when the flow's policy is REQUIRED."""
        policy = self.config.policy_for(flow)
        result = PublishResult(flow=flow, attempted=len(mcps), policy=policy.value)

        refusal = self.config.refuse_reason(flow)
        if refusal:
            self.metrics["refused"] += 1
            raise MetadataPublishError(refusal)

        if not self.config.enabled:
            # Not an error and not a success: there is no metadata plane, which is a
            # supported configuration. Counting it as `published` would make a coverage
            # metric report a catalogue that does not exist.
            result.skipped_reason = f"metadata plane is {self.config.mode.value}"
            self.metrics["skipped"] += 1
            self.transport.send(mcps) if isinstance(self.transport, NullTransport) else None
            return result

        for m in mcps:
            self.minter.assert_fabric(m.entity_urn)

        delays = self.config.client.delays()
        last: Exception | None = None
        for attempt in range(1, self.config.client.max_attempts + 1):
            result.attempts = attempt
            try:
                self.transport.send(mcps)
                result.published = len(mcps)
                result.ok = True
                self.metrics["published"] += len(mcps)
                self.metrics["attempts"] += attempt
                return result
            except (urllib.error.URLError, OSError, MetadataPublishError, TimeoutError) as exc:
                last = exc
                self.metrics["transport_error"] += 1
                if attempt < self.config.client.max_attempts:
                    self._sleep(delays[attempt - 1])

        result.ok = False
        result.error = f"{type(last).__name__}: {last}"
        self.metrics["failed"] += 1

        if policy is OutagePolicy.REQUIRED:
            raise MetadataPublishError(
                f"flow {flow!r} is REQUIRED and the metadata plane did not accept "
                f"{len(mcps)} proposal(s) after {result.attempts} attempt(s): "
                f"{result.error}")
        # BEST_EFFORT. The business data is already committed; the catalogue is stale and
        # the counter says so. Failing here would be choosing a data outage over a metadata
        # one.
        self.metrics["degraded"] += 1
        return result

    # -- convenience builders ---------------------------------------------
    def dataset_properties(self, urn: str, *, name: str, description: str = "",
                           custom: dict | None = None) -> MetadataChangeProposal:
        return MetadataChangeProposal(
            urn, "datasetProperties",
            {"name": name, "description": description,
             "customProperties": {k: str(v) for k, v in sorted((custom or {}).items())}})

    def ownership(self, urn: str, owners: list, *,
                  entity_type: str = "dataset") -> MetadataChangeProposal:
        return MetadataChangeProposal(
            urn, "ownership",
            {"owners": [{"owner": self.minter.corp_group_urn(o), "type": "DATAOWNER"}
                        for o in sorted(set(owners))]}, entity_type=entity_type)

    def tags(self, urn: str, names: list, *,
             entity_type: str = "dataset") -> MetadataChangeProposal:
        return MetadataChangeProposal(
            urn, "globalTags",
            {"tags": [{"tag": self.minter.tag_urn(t)} for t in sorted(set(names))]},
            entity_type=entity_type)

    #: The actor recorded on aspects that require an AuditStamp. A service identity, not a
    #: person: these are written by the metadata pipeline, and attributing them to whoever
    #: happened to run it makes the audit trail wrong in a quiet way.
    SYSTEM_ACTOR = "urn:li:corpuser:cdc-lakehouse"

    def glossary_terms(self, urn: str, terms: list,
                       *, at_ms: int | None = None) -> MetadataChangeProposal:
        """`auditStamp` is REQUIRED by DataHub's GlossaryTerms schema and has no default.

        Omitting it returns a 422 that says only "Unprocessable Entity" unless the response
        body is read -- which is how this was found on the first live publish, and why
        `RestTransport` now reports the body.

        `at_ms` is injectable so a test can assert a byte-identical re-emission; a timestamp
        that moves on every call would otherwise make this the one aspect that cannot be
        compared between runs.
        """
        import time as _time

        stamp = int(at_ms if at_ms is not None else _time.time() * 1000)
        return MetadataChangeProposal(
            urn, "glossaryTerms",
            {"terms": [{"urn": self.minter.glossary_term_urn(t)} for t in sorted(set(terms))],
             "auditStamp": {"time": stamp, "actor": self.SYSTEM_ACTOR}})

    def domain(self, urn: str, domain: str) -> MetadataChangeProposal:
        return MetadataChangeProposal(urn, "domains",
                                      {"domains": [self.minter.domain_urn(domain)]})

    def upstream_lineage(self, downstream_urn: str, upstream_urns: list,
                         *, kind: str = "TRANSFORMED") -> MetadataChangeProposal:
        """One edge set. Sorted, so re-emitting identical lineage is byte-identical."""
        return MetadataChangeProposal(
            downstream_urn, "upstreamLineage",
            {"upstreams": [{"dataset": u, "type": kind} for u in sorted(set(upstream_urns))]})

    def dataflow_info(self, flow_id: str, *, description: str = "") -> MetadataChangeProposal:
        return MetadataChangeProposal(
            self.minter.dataflow_urn(flow_id), "dataFlowInfo",
            {"name": flow_id, "description": description,
             "env": self.config.environment.fabric.value},
            entity_type="dataFlow")

    def datajob_info(self, flow_id: str, job_id: str, *, description: str = "",
                     job_type: str = "BATCH_SCHEDULED") -> MetadataChangeProposal:
        """`type` is a UNION in DataHub's DataJobInfo schema, not a string.

        A bare `"BATCH_SCHEDULED"` is rejected with
        `/type :: union type is not backed by a DataMap or null` -- which is invisible in a
        recording transport, because the shape is only validated by the server. Found on the
        first live publish, and the reason the offline smoke test is not sufficient evidence
        on its own.
        """
        return MetadataChangeProposal(
            self.minter.datajob_urn(flow_id, job_id), "dataJobInfo",
            {"name": job_id, "description": description, "type": {"string": job_type}},
            entity_type="dataJob")

    def datajob_io(self, flow_id: str, job_id: str, *, inputs: list,
                   outputs: list) -> MetadataChangeProposal:
        return MetadataChangeProposal(
            self.minter.datajob_urn(flow_id, job_id), "dataJobInputOutput",
            {"inputDatasets": sorted(set(inputs)), "outputDatasets": sorted(set(outputs))},
            entity_type="dataJob")

    # -- read path --------------------------------------------------------
    def health(self) -> dict:
        """Cheap, and honest when it cannot tell.

        `reachable: None` means NOT CHECKED, never `False`. A health check that reports
        "down" when it never looked is the same defect as a DQ check that reports PASS on
        an empty table.
        """
        if not self.config.enabled:
            return {"mode": self.config.mode.value, "reachable": None,
                    "detail": "metadata plane is disabled; nothing to reach"}
        if not isinstance(self.transport, RestTransport):
            return {"mode": self.config.mode.value, "reachable": None,
                    "detail": f"transport is {self.transport.name}; no remote to check"}
        try:
            req = urllib.request.Request(f"{self.transport.gms_url}/health")
            with urllib.request.urlopen(req, timeout=self.config.client.timeout_seconds) as r:
                return {"mode": self.config.mode.value, "reachable": r.status < 300,
                        "detail": f"HTTP {r.status}"}
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            return {"mode": self.config.mode.value, "reachable": False,
                    "detail": f"{type(exc).__name__}: {exc}"}

    # -- retrieval ---------------------------------------------------------
    def _get(self, path: str) -> dict:
        """One GET against GMS. Read-only, and only meaningful on a REST transport."""
        if not isinstance(self.transport, RestTransport):
            raise MetadataPublishError(
                f"transport is {self.transport.name}; there is no remote to read from. This "
                f"is not a failure -- it is what `disabled` and `dry_run` mean.")
        req = urllib.request.Request(
            f"{self.transport.gms_url}{path}",
            headers={"Accept": "application/json",
                     **({"Authorization": f"Bearer {self.transport._token}"}
                        if self.transport._token else {})})
        with urllib.request.urlopen(req, timeout=self.config.client.timeout_seconds) as r:
            return json.loads(r.read().decode())

    def get_aspect(self, urn: str, aspect: str) -> dict | None:
        """The written aspect, read back. `None` when the entity has never carried it.

        Reading back is the half of a smoke test that actually proves something: a publish
        that returns 200 has been ACCEPTED, which is not the same as stored and retrievable.
        """
        quoted = urllib.parse.quote(urn, safe="")
        try:
            doc = self._get(f"/aspects/{quoted}?aspect={aspect}&version=0")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise
        return (doc.get("aspect") or {}) or None

    def relationships(self, urn: str, *, types: str = "DownstreamOf",
                      direction: str = "INCOMING") -> list:
        """Lineage as GMS sees it, which is not necessarily as we sent it."""
        quoted = urllib.parse.quote(urn, safe="")
        doc = self._get(f"/relationships?direction={direction}&urn={quoted}&types={types}")
        return [e.get("entity") for e in (doc.get("relationships") or [])]

    def metrics_payload(self) -> dict:
        return {k: self.metrics[k] for k in sorted(self.metrics)}
