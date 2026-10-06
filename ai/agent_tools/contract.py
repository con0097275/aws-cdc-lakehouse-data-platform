"""Central tool contract. Every tool goes through here; none can opt out.

WHY CENTRAL
-----------
If each tool implemented its own validation, timeout, result cap and audit, then adding a
tool would mean re-deciding all four -- and the one that forgets is the one that gets
exploited. `invoke()` is the only way to call a tool, so those properties are structural
rather than a convention each author must remember.

AUDIT IS PART OF THE CONTROL, NOT TELEMETRY
-------------------------------------------
An action that cannot be audited must not proceed (ADR-058). So the audit record is written
for FAILURES too, and a tool whose audit sink raises does not silently succeed.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Callable

MAX_INPUT_BYTES = 8_192
MAX_RESULT_BYTES = 256_000


class ToolError(RuntimeError):
    """Base for every tool failure. Carries no payload -- the audit row carries context."""


class InputRejected(ToolError):
    """Input failed validation. Never reaches the backend."""


class PermissionDenied(ToolError):
    """The tool is not permitted to touch what was asked for."""


class ResultTooLarge(ToolError):
    """Backend returned more than the caller may see."""


class ToolTimeout(ToolError):
    """Backend exceeded the tool's declared timeout."""


#: ADR-092. `plan_write` writes IMMUTABLE planning and audit objects -- incidents, plans,
#: scopes, simulations. Those are append-only records, not business data, which is why such
#: tools stay `read_only=True`: the flag means "does not change business data", and keeping
#: that meaning stable is what makes it worth enforcing.
AUTHORIZATION_CLASSES = frozenset({
    "public_metadata", "governed_read", "ops_read", "plan_write", "recovery_submit",
})

#: ADR-092. The complete set of tools permitted to mutate. Closed by NAME.
#:
#: `request_recovery_cancel` mutates but can only REDUCE activity, so it is included
#: deliberately: a cancel that needs an approval round-trip is a cancel that arrives after
#: the damage.
MUTATING_TOOLS = frozenset({"submit_recovery_plan", "request_recovery_cancel"})


@dataclass(frozen=True)
class ToolSpec:
    name: str
    version: int
    description: str
    input_schema: dict
    output_schema: dict
    authorization_class: str        # see AUTHORIZATION_CLASSES
    timeout_seconds: float = 30.0
    max_rows: int = 1000
    max_result_bytes: int = MAX_RESULT_BYTES
    read_only: bool = True
    # ADR-092. Only a `recovery_submit` tool may set these, and it MUST set both.
    requires_approval_ref: bool = False
    requires_idempotency_key: bool = False

    def __post_init__(self) -> None:
        if self.authorization_class not in AUTHORIZATION_CLASSES:
            raise ValueError(
                f"{self.name}: authorization_class {self.authorization_class!r} is not one "
                f"of {sorted(AUTHORIZATION_CLASSES)}")
        if not self.read_only:
            # ADR-092: membership is closed BY NAME, not by review. A surface whose
            # behaviour is reviewed grows one reasonable tool at a time -- retry_job,
            # clear_task, refresh_partition -- until it is no longer a surface.
            if self.authorization_class != "recovery_submit":
                raise ValueError(
                    f"{self.name}: only the 'recovery_submit' class may mutate (ADR-092); "
                    f"{self.authorization_class!r} may not")
            if self.name not in MUTATING_TOOLS:
                raise ValueError(
                    f"{self.name}: not in the closed mutation set {sorted(MUTATING_TOOLS)} "
                    "(ADR-092). Adding one is an ADR amendment, not a tool registration.")
            if not (self.requires_approval_ref and self.requires_idempotency_key):
                raise ValueError(
                    f"{self.name}: a mutating tool must require BOTH an approval reference "
                    "and an idempotency key (ADR-092)")
        else:
            if self.requires_approval_ref or self.requires_idempotency_key:
                raise ValueError(
                    f"{self.name}: a read-only tool must not claim approval/idempotency "
                    "requirements; they would read as a guarantee it does not provide")
        if self.authorization_class == "recovery_submit" and self.read_only:
            raise ValueError(
                f"{self.name}: 'recovery_submit' is the mutation class; a read-only tool "
                "must not sit in it")
        for k in ("type", "properties"):
            if k not in self.input_schema:
                raise ValueError(f"{self.name}: input_schema needs {k!r}")


@dataclass
class AuditRecord:
    request_id: str
    tool_name: str
    tool_version: int
    authorization_class: str
    actor: str                       # session/actor placeholder until AI-P9 supplies one
    input_hash: str
    started_at: str
    duration_ms: float = 0.0
    status: str = "STARTED"          # SUCCEEDED | REJECTED | DENIED | TIMEOUT | ERROR
    error_class: str | None = None
    result_rows: int | None = None
    result_bytes: int | None = None
    resource_ids: list[str] = field(default_factory=list)   # e.g. Athena QueryExecutionId
    bytes_scanned: int | None = None                        # cost-relevant
    def to_dict(self) -> dict:
        return asdict(self)


#: In-process sink. AI-P11 replaces it with ops.ai_agent_execution_hist + CloudWatch.
AUDIT_LOG: list[AuditRecord] = []


def _sink(rec: AuditRecord) -> None:
    AUDIT_LOG.append(rec)


def _hash_input(payload: dict) -> str:
    """Hash, never the payload. A question can contain a customer name; its hash cannot."""
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _validate(spec: ToolSpec, payload: dict) -> None:
    raw = json.dumps(payload, default=str)
    if len(raw.encode()) > MAX_INPUT_BYTES:
        raise InputRejected(
            f"{spec.name}: input is {len(raw)} bytes, limit {MAX_INPUT_BYTES}. An oversized "
            "input is either a mistake or an attempt to smuggle a payload past a validator.")
    props = spec.input_schema.get("properties", {})
    unknown = set(payload) - set(props)
    if unknown:
        # additionalProperties=false, enforced: a silently-ignored key is a parameter the
        # caller believes is in effect and is not.
        raise InputRejected(f"{spec.name}: unknown argument(s) {sorted(unknown)}")
    for req in spec.input_schema.get("required", []):
        if req not in payload:
            raise InputRejected(f"{spec.name}: missing required argument {req!r}")
    for k, v in payload.items():
        want = props[k].get("type")
        if want == "string" and not isinstance(v, str):
            raise InputRejected(f"{spec.name}: {k!r} must be a string")
        if want == "integer" and not isinstance(v, int):
            raise InputRejected(f"{spec.name}: {k!r} must be an integer")
        if isinstance(v, str) and props[k].get("maxLength") and len(v) > props[k]["maxLength"]:
            raise InputRejected(f"{spec.name}: {k!r} exceeds maxLength")


def invoke(spec: ToolSpec, fn: Callable[..., Any], payload: dict, *,
           actor: str = "anonymous", request_id: str | None = None) -> dict:
    """The ONLY way to call a tool. Validates, times, caps, audits."""
    rec = AuditRecord(
        request_id=request_id or str(uuid.uuid4()),
        tool_name=spec.name, tool_version=spec.version,
        authorization_class=spec.authorization_class, actor=actor,
        input_hash=_hash_input(payload),
        started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    t0 = time.perf_counter()
    try:
        _validate(spec, payload)
        out = _call_bounded(spec, fn, payload)

        body = json.dumps(out, default=str)
        if len(body.encode()) > spec.max_result_bytes:
            raise ResultTooLarge(
                f"{spec.name}: result is {len(body)} bytes, cap {spec.max_result_bytes}. "
                "Truncating silently would hand the model a partial answer it cannot see "
                "is partial.")
        rec.result_bytes = len(body.encode())
        if isinstance(out, dict):
            rows = out.get("rows")
            if isinstance(rows, list):
                rec.result_rows = len(rows)
            rec.resource_ids = list(out.get("resource_ids", []))
            rec.bytes_scanned = out.get("bytes_scanned")
        rec.status = "SUCCEEDED"
        return out
    except InputRejected as e:
        rec.status, rec.error_class = "REJECTED", type(e).__name__; raise
    except PermissionDenied as e:
        rec.status, rec.error_class = "DENIED", type(e).__name__; raise
    except ToolTimeout as e:
        rec.status, rec.error_class = "TIMEOUT", type(e).__name__; raise
    except Exception as e:
        rec.status, rec.error_class = "ERROR", type(e).__name__; raise
    finally:
        rec.duration_ms = round((time.perf_counter() - t0) * 1000, 2)
        _sink(rec)          # ALWAYS, including on failure


def _call_bounded(spec: ToolSpec, fn: Callable[..., Any], payload: dict) -> Any:
    """Run the tool, but never for longer than `spec.timeout_seconds`.

    AI-P15 drill 6 found this MISSING. `timeout_seconds` was declared on every ToolSpec,
    `ToolTimeout` had a handler, and the docstring said invoke() "times" the call -- but
    nothing bounded it. It measured the duration and reported it afterwards. A hung Athena
    or DynamoDB call therefore ran until the Lambda's own 120s ceiling, burning billed
    duration, and locally it hung forever. A declared timeout that is never enforced is
    worse than no timeout, because every reader assumes the bound exists.

    A worker thread is used rather than signal.alarm: alarm is Unix-only and main-thread
    only, and the tools also run inside pytest and inside Lambda worker threads.

    HONEST LIMITATION: Python cannot kill a thread, so on timeout the worker keeps running
    until its own socket timeout expires. What this bounds is the CALLER -- the agent stops
    waiting, the audit row records TIMEOUT, and the request returns. That is the property
    the cost and latency budgets depend on.
    """
    import concurrent.futures as _cf
    call = (lambda: fn(**payload, _spec=spec)) if _takes_spec(fn) else (lambda: fn(**payload))
    # NOT a `with` block: ThreadPoolExecutor.__exit__ calls shutdown(wait=True), which blocks
    # until the slow worker finishes -- so the timeout would be measured and then waited out
    # anyway, enforcing nothing. shutdown(wait=False) is what actually returns control.
    pool = _cf.ThreadPoolExecutor(max_workers=1)
    fut = pool.submit(call)
    try:
        return fut.result(timeout=spec.timeout_seconds)
    except _cf.TimeoutError:
        raise ToolTimeout(
            f"{spec.name}: exceeded {spec.timeout_seconds}s. The call was abandoned; "
            "a partial or late result is never returned as if it were complete.") from None
    finally:
        pool.shutdown(wait=False)


def _takes_spec(fn: Callable) -> bool:
    import inspect
    try:
        return "_spec" in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
