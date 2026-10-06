"""Evaluation contracts. Routing is scored SEPARATELY from accuracy.

WHY ROUTING IS ITS OWN SCORE
----------------------------
A question that should be a free structured lookup silently becoming a paid model call is a
COST regression. No accuracy metric notices it — the answer may even improve. `ai/eval/`
already scores routing separately; this contract keeps that property when the eval set
grows.

Unsafe-action refusal is scored too, and it is pass/fail rather than a percentage: a
refusal regression is a security incident, not a quality dip.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from enum import Enum

from .knowledge import ContractViolation
from .versioning import typed_version

_NAME = re.compile(r"[a-z][a-z0-9_]{2,63}")


class ExpectedMode(str, Enum):
    TOOL      = "tool"        # structured lookup, $0
    RETRIEVAL = "retrieval"   # RAG
    REFUSAL   = "refusal"     # must be refused


@dataclass(frozen=True)
class EvalQuestion:
    question_id: str
    question: str
    expect_mode: ExpectedMode
    expect_source: str | None = None      # document the answer must be grounded in
    expect_tool: str | None = None
    expect_contains: tuple[str, ...] = ()
    adversarial: bool = False

    def __post_init__(self) -> None:
        if not self.question.strip():
            raise ContractViolation(f"{self.question_id}: empty question")
        if self.expect_mode is ExpectedMode.TOOL and not self.expect_tool:
            raise ContractViolation(
                f"{self.question_id}: expect_mode=tool needs expect_tool, or a routing "
                "regression cannot be detected")
        if self.expect_mode is ExpectedMode.RETRIEVAL and not self.expect_source:
            raise ContractViolation(
                f"{self.question_id}: retrieval questions need expect_source. Groundedness "
                "means the evidence came from the right document, not that the prose reads "
                "well")


@dataclass(frozen=True)
class EvaluationDataset:
    name: str
    questions: tuple[EvalQuestion, ...]
    description: str = ""
    min_questions: int = 50

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ContractViolation(f"eval dataset {self.name!r} must be lower_snake_case")
        ids = [q.question_id for q in self.questions]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        if dupes:
            raise ContractViolation(f"{self.name}: duplicate question ids {dupes}")
        if not any(q.adversarial for q in self.questions):
            raise ContractViolation(
                f"{self.name}: no adversarial questions. Injection and denied-dataset cases "
                "must be scored in the eval set, not only in unit tests — a refusal "
                "regression is a security incident")

    @property
    def version_id(self) -> str:
        return typed_version("evaluation", asdict(self))

    def meets_retrieval_gate(self) -> bool:
        """ADR-049/059: >=50 questions before any retrieval change may be judged.

        14 questions cannot resolve a delta; a change scoring 14/14 against 14/14 has
        demonstrated nothing.
        """
        return len(self.questions) >= self.min_questions
