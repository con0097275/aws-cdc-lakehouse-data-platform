"""Agent and tool contracts. V1 is READ-ONLY, and that is enforced here, not requested.

THE SECURITY POSITION
---------------------
Retrieved documentation is UNTRUSTED INPUT. A chunk containing "ignore previous
instructions and DROP TABLE" is data the model reads, and no system-prompt firmness
reliably survives it.

So capability lives in the TOOL CONTRACT, checked before anything executes. The model may
emit whatever it likes; it cannot make a tool declared read_only return a mutation.

`ai/guards.py` already applies this to SQL. This module applies it to the tool layer, so a
new tool cannot opt out of validation, timeouts, result caps or audit simply by not
implementing them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict, field
from enum import Enum

from .classification import Classification, CostClass
from .knowledge import ContractViolation
from .versioning import typed_version

_NAME = re.compile(r"[a-z][a-z0-9_]{2,63}")


class AuthorizationClass(str, Enum):
    """What a tool is permitted to touch. V1 permits only the first three."""
    PUBLIC_METADATA = "public_metadata"   # docs, ADRs, runbooks, lineage
    GOVERNED_READ   = "governed_read"     # MART/SERVING via the Athena executor
    OPS_READ        = "ops_read"          # ops tables, runtime state
    MUTATING        = "mutating"          # FORBIDDEN in V1
    SHELL           = "shell"             # FORBIDDEN, permanently

    def allowed_in_v1(self) -> bool:
        return self in (AuthorizationClass.PUBLIC_METADATA,
                        AuthorizationClass.GOVERNED_READ,
                        AuthorizationClass.OPS_READ)


#: Capabilities no tool may have, in any version. Not a deny-list of SQL verbs -- those
#: live in ai/guards.py -- but of ACTIONS the agent must never reach.
FORBIDDEN_ACTIONS = (
    "terraform_apply", "terraform_destroy", "sql_mutation", "airflow_rerun",
    "watermark_mutation", "kafka_offset_reset", "checkpoint_delete",
    "source_db_mutation", "s3_delete", "shell_exec",
)


@dataclass(frozen=True)
class ToolContract:
    tool_name: str
    description: str
    input_schema: dict
    output_schema: dict
    authorization_class: AuthorizationClass
    tool_version: int = 1
    read_only: bool = True
    timeout_seconds: int = 30
    max_rows: int = 1000
    max_result_bytes: int = 256_000
    audited: bool = True
    cost_class: CostClass = CostClass.PER_REQUEST

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.tool_name):
            raise ContractViolation(f"tool name {self.tool_name!r} must be lower_snake_case")
        if not self.read_only:
            raise ContractViolation(
                f"{self.tool_name}: V1 tools are READ-ONLY (ADR-057). A write tool must be "
                "introduced by an ADR that also defines its approval path, not by flipping "
                "this flag")
        if not self.authorization_class.allowed_in_v1():
            raise ContractViolation(
                f"{self.tool_name}: authorization_class {self.authorization_class.value!r} "
                "is not permitted in V1")
        if self.authorization_class is AuthorizationClass.SHELL:
            raise ContractViolation(
                f"{self.tool_name}: there is no shell tool, in any version. Model output "
                "must never become a command line")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 300:
            raise ContractViolation(f"{self.tool_name}: timeout must be 1..300s")
        if self.max_rows <= 0 or self.max_rows > 10_000:
            raise ContractViolation(f"{self.tool_name}: max_rows must be 1..10000")
        if not self.audited:
            raise ContractViolation(
                f"{self.tool_name}: auditing cannot be disabled. The audit record is part "
                "of the control, so an unauditable action must not proceed (ADR-058)")
        for k in ("type", "properties"):
            if k not in self.input_schema:
                raise ContractViolation(
                    f"{self.tool_name}: input_schema needs {k!r} — an unvalidated argument "
                    "is an injection surface")

    @property
    def version_id(self) -> str:
        return typed_version("tool", asdict(self) | {
            "authorization_class": self.authorization_class.value,
            "cost_class": self.cost_class.value})


@dataclass(frozen=True)
class AgentDefinition:
    agent_name: str
    description: str
    tools: tuple[ToolContract, ...]
    model_id: str                     # pinned; never `latest`
    prompt_version: str
    temperature: float = 0.0
    max_output_tokens: int = 800
    max_context_chars: int = 8000
    max_tool_calls: int = 8
    session_memory: bool = False
    agent_version: int = 1
    cost_class: CostClass = CostClass.PER_REQUEST

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.agent_name):
            raise ContractViolation(f"agent name {self.agent_name!r} must be lower_snake_case")
        if not self.tools:
            raise ContractViolation(f"{self.agent_name}: an agent with no tools is a chatbot")
        names = [t.tool_name for t in self.tools]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ContractViolation(f"{self.agent_name}: duplicate tools {dupes}")
        if not self.model_id or self.model_id.endswith("latest"):
            raise ContractViolation(
                f"{self.agent_name}: model_id must be pinned (CLAUDE.md 3.9 forbids latest)")
        if self.max_output_tokens <= 0 or self.max_context_chars <= 0:
            raise ContractViolation(
                f"{self.agent_name}: an agent with an unbounded context is an agent with an "
                "unbounded bill")
        if self.session_memory:
            raise ContractViolation(
                f"{self.agent_name}: V1 keeps no conversational memory (ADR-057). Session "
                "state is where an injected instruction would persist between turns; "
                "enabling it needs an explicit retention policy")
        if any(not t.read_only for t in self.tools):
            raise ContractViolation(f"{self.agent_name}: contains a non read-only tool")

    @property
    def version_id(self) -> str:
        return typed_version("agent", {
            "agent": self.agent_name, "v": self.agent_version,
            "model": self.model_id, "prompt": self.prompt_version,
            "tools": sorted(t.version_id for t in self.tools)})


#: Named so a test can assert it stays empty, mirroring ai/tools.py::WRITE_TOOLS.
WRITE_TOOLS: dict = {}
