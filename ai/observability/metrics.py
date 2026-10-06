"""AI metrics. CloudWatch EMF, reusing the existing stack -- no new logging system.

METRICS FAIL OPEN. THE AUDIT FAILS CLOSED.
------------------------------------------
Metrics OBSERVE the work, so an outage in the metric sink must not break the agent -- the
same rule `governance/lineage/openlineage.yml` already applies with `fail_on_error: false`.
The AUDIT ROW is part of the control (ADR-058), so an action that cannot be audited must not
proceed. That asymmetry is deliberate and is asserted by test.

NO HIGH-VOLUME TRACES IN ICEBERG
--------------------------------
Per-token timings, prompt bodies and retrieval score vectors go to CloudWatch. An Iceberg
table written per model call is a small-files generator whose compaction costs more than the
telemetry is worth, and `ops.*` is queried for audit, not tailed.

NO SECRET, NO PII, NO FREE TEXT IN A DIMENSION.
-----------------------------------------------
A metric dimension carrying a query string carries whatever was in it. Dimensions here are
low-cardinality enums only; anything variable is a VALUE or a hash.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

NAMESPACE = "AiPlatform"

#: Every metric this platform emits, with its unit. Named centrally so a typo becomes a
#: missing metric at review time rather than a silently absent dashboard line.
METRICS: dict[str, str] = {
    "AgentInvocations":     "Count",
    "AgentLatencyMs":       "Milliseconds",
    "ToolCalls":            "Count",
    "ToolErrors":           "Count",
    "ToolLatencyMs":        "Milliseconds",
    "ModelLatencyMs":       "Milliseconds",
    "TokensIn":             "Count",
    "TokensOut":            "Count",
    "EstimatedModelCostUsd": "None",
    "RetrievalLatencyMs":   "Milliseconds",
    "RetrievedChunks":      "Count",
    "AthenaBytesScanned":   "Bytes",
    "AthenaQueries":        "Count",
    "FeatureLookupLatencyMs": "Milliseconds",
    "InferenceLatencyMs":   "Milliseconds",
    "GuardrailBlocks":      "Count",
    "RoutingDecision":      "Count",
}

#: Two metrics that a generic LLM-observability list would not include, and that matter most
#: here. `RoutingDecision` is the COST signal -- a free lookup silently becoming a paid call
#: is a regression no accuracy metric sees. `GuardrailBlocks` is the SECURITY signal -- a
#: rising count is either an attack or a broken guard.
FIRST_CLASS = ("RoutingDecision", "GuardrailBlocks")

_SECRET = tuple(re.compile(p, re.I) for p in (
    r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b", r"(?i)password\s*[=:]\s*\S+",
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----", r"\bgh[pousr]_[A-Za-z0-9]{30,}",
))
#: Anything not on this list may not become a dimension.
ALLOWED_DIMENSIONS = frozenset({"Tool", "Intent", "Status", "Backend", "Reason",
                                "AuthorizationClass", "Environment"})


class MetricRejected(ValueError):
    """A metric that would leak. Raised at emit time so it fails in test, not in prod."""


def redact(value: str) -> str:
    out = value
    for rx in _SECRET:
        out = rx.sub("[REDACTED]", out)
    return out


def _validate(name: str, dims: dict) -> None:
    if name not in METRICS:
        raise MetricRejected(f"unknown metric {name!r}; add it to METRICS with a unit")
    for k, v in dims.items():
        if k not in ALLOWED_DIMENSIONS:
            raise MetricRejected(
                f"dimension {k!r} is not allow-listed. A dimension carrying free text "
                "carries whatever was in it, and high-cardinality dimensions are also a "
                "CloudWatch cost driver.")
        s = str(v)
        if len(s) > 64:
            raise MetricRejected(f"dimension {k!r} value is too long to be an enum")
        if redact(s) != s:
            raise MetricRejected(f"dimension {k!r} value looks like a secret")


@dataclass
class MetricSink:
    """Collects EMF records. `emit()` NEVER raises on a sink failure -- fail open."""
    records: list = field(default_factory=list)
    failures: int = 0
    _fail: bool = False

    def write(self, payload: dict) -> None:
        if self._fail:
            raise RuntimeError("metric sink unavailable")
        self.records.append(payload)
        # EMF: CloudWatch parses structured log lines from stdout. No extra service.
        if os.environ.get("AI_METRICS_STDOUT", "false").lower() == "true":
            print(json.dumps(payload), file=sys.stdout)


SINK = MetricSink()


def emit(name: str, value: float, *, dimensions: dict | None = None,
         sink: MetricSink | None = None) -> bool:
    """Emit one metric. Returns False if the sink failed; NEVER raises for that reason.

    A validation error DOES raise: an unknown metric or a leaking dimension is a bug in the
    caller, and catching it silently is how a metric quietly stops existing.
    """
    dims = dimensions or {}
    _validate(name, dims)                     # raises -> caller bug, surfaced
    s = sink or SINK
    payload = {
        "_aws": {"Timestamp": int(time.time() * 1000),
                 "CloudWatchMetrics": [{"Namespace": NAMESPACE,
                                        "Dimensions": [sorted(dims)] if dims else [[]],
                                        "Metrics": [{"Name": name,
                                                     "Unit": METRICS[name]}]}]},
        name: value, **dims,
    }
    try:
        s.write(payload)
        return True
    except Exception:                          # noqa: BLE001 -- FAIL OPEN, deliberately
        s.failures += 1
        return False


@contextmanager
def timed(name: str, *, dimensions: dict | None = None, sink: MetricSink | None = None):
    t0 = time.perf_counter()
    try:
        yield
    finally:
        emit(name, round((time.perf_counter() - t0) * 1000, 2),
             dimensions=dimensions, sink=sink)


def emit_agent_run(result: dict, *, sink: MetricSink | None = None) -> int:
    """Emit the full metric set for one agent run. Returns how many were emitted."""
    n = 0
    intent = result.get("intent") or "UNKNOWN"
    n += emit("AgentInvocations", 1, dimensions={"Intent": intent}, sink=sink)
    n += emit("AgentLatencyMs", result.get("duration_ms", 0.0),
              dimensions={"Intent": intent}, sink=sink)
    n += emit("RoutingDecision", 1, dimensions={"Intent": intent}, sink=sink)
    n += emit("ToolCalls", result.get("tool_calls", 0), dimensions={"Intent": intent},
              sink=sink)
    n += emit("ToolErrors", len(result.get("errors", [])), dimensions={"Intent": intent},
              sink=sink)
    n += emit("GuardrailBlocks", len(result.get("safety", [])),
              dimensions={"Intent": intent}, sink=sink)
    n += emit("TokensIn", result.get("tokens_in", 0), sink=sink)
    n += emit("TokensOut", result.get("tokens_out", 0), sink=sink)

    kn = result.get("tool_results", {}).get("retrieve_knowledge")
    if kn:
        n += emit("RetrievedChunks", len(kn.get("chunks", [])),
                  dimensions={"Backend": kn.get("backend", "bm25")}, sink=sink)
    ath = result.get("tool_results", {}).get("query_athena")
    if ath:
        n += emit("AthenaQueries", 1, sink=sink)
        n += emit("AthenaBytesScanned", ath.get("bytes_scanned") or 0, sink=sink)
    return n


def emit_audit(record, *, sink: MetricSink | None = None) -> int:
    """Emit per-tool metrics from an AuditRecord. Dimensions are enums only."""
    dims = {"Tool": record.tool_name, "Status": record.status,
            "AuthorizationClass": record.authorization_class}
    n = emit("ToolLatencyMs", record.duration_ms, dimensions=dims, sink=sink)
    if record.status != "SUCCEEDED":
        n += emit("ToolErrors", 1, dimensions=dims, sink=sink)
    if record.status == "DENIED":
        n += emit("GuardrailBlocks", 1, dimensions={"Tool": record.tool_name}, sink=sink)
    if record.bytes_scanned is not None:
        n += emit("AthenaBytesScanned", record.bytes_scanned,
                  dimensions={"Tool": record.tool_name}, sink=sink)
    return n
