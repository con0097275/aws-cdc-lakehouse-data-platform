"""Cost classification and budget enforcement.

EVERY AI COMPONENT DECLARES A COST CLASS, AND NONE IS ALWAYS-ON.
----------------------------------------------------------------
ADR-060 makes that a rule rather than an aspiration, so `assert_no_always_on()` fails a test
rather than producing a line in a table nobody re-reads.

RATES ARE NOT HARDCODED
-----------------------
A stale price in source is worse than no price: it looks authoritative and is wrong. Token
counts are recorded; the dollar figure is computed from a rate the caller supplies from
`make pricing` / `docs/PRICE_REFERENCE.md`, and is reported as UNKNOWN when no rate is given.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CostClass(str, Enum):
    ALWAYS_ON = "always_on"
    PER_REQUEST = "per_request"
    PER_JOB = "per_job"
    METERED_WINDOW = "metered_window"


@dataclass(frozen=True)
class Component:
    name: str
    cost_class: CostClass
    idle: str
    marginal: str
    flag: str | None
    destroy: str


#: The full AI platform inventory. If a component is not here it has not been costed.
COMPONENTS: tuple[Component, ...] = (
    Component("knowledge corpus (S3 objects)", CostClass.PER_JOB,
              "~$0 — a few MB under the lake bucket's lifecycle", "one build per commit",
              "enable_ai_rag", "delete the prefix"),
    Component("BM25 retrieval (in-process)", CostClass.PER_REQUEST,
              "$0", "$0 — no service, no tokens", None, "n/a"),
    Component("S3 Vectors index", CostClass.PER_REQUEST,
              "~$0 — storage only, ~6.7 MB at 1,622 chunks", "per query + embedding tokens",
              "enable_ai_vector_index", "delete index + vector bucket"),
    Component("Bedrock Knowledge Base", CostClass.PER_REQUEST,
              "$0 — no standing capacity", "per ingestion + per query",
              "enable_ai_rag", "terraform destroy"),
    Component("Bedrock model invocation", CostClass.PER_REQUEST,
              "$0", "per token in/out", "AI_ENABLE_GENERATION", "disable the flag"),
    Component("Athena (agent queries)", CostClass.PER_REQUEST,
              "$0", "per byte scanned, capped by the workgroup cutoff",
              "enable_athena", "n/a — shared workgroup"),
    Component("offline feature store (Glue DB + Iceberg)", CostClass.PER_JOB,
              "$0 idle — Glue DB is free; storage is MB", "EMR seconds per materialisation",
              "enable_ai_feature_store", "drop tables + prefix"),
    Component("online feature store (DynamoDB)", CostClass.PER_REQUEST,
              "$0 — PAY_PER_REQUEST, and NO TABLE IS CREATED",
              "per read/write when a consumer exists",
              "enable_ai_online_feature_store", "delete table"),
    Component("ML training", CostClass.PER_JOB,
              "$0", "EMR seconds, or $0 when run locally", "enable_ai_ml_training",
              "n/a — no standing resource"),
    Component("batch inference", CostClass.PER_JOB,
              "$0", "EMR seconds or local", None, "n/a"),
    Component("agent runtime (Lambda)", CostClass.PER_REQUEST,
              "$0 — no charge when idle", "per invocation, capped at 2 concurrent",
              "enable_ai_agent_runtime", "terraform destroy"),
    Component("CloudWatch logs + metrics", CostClass.PER_REQUEST,
              "~$0 at this volume", "ingested bytes; 14-day retention",
              "enable_ai_agent_runtime", "delete log group"),
)

#: Never provisioned without a written, operator-approved cost review (ADR-060).
FORBIDDEN_WITHOUT_REVIEW = (
    "OpenSearch Serverless", "always-on SageMaker endpoint", "new EKS cluster",
    "new NAT Gateway", "new persistent EC2 AI runtime", "large online feature stores",
)


def assert_no_always_on() -> None:
    bad = [c.name for c in COMPONENTS if c.cost_class is CostClass.ALWAYS_ON]
    if bad:
        raise AssertionError(
            f"always-on AI components: {bad}. ADR-060 forbids them without a written cost "
            "review; at a bounded monthly budget an idle charge compounds silently.")


@dataclass
class Budget:
    """Per-session ceilings. A ceiling that is not enforced is a wish."""
    max_tokens_in: int = 4000
    max_tokens_out: int = 800
    max_tool_calls: int = 4
    max_athena_bytes: int = 10 * 1024 ** 3          # matches the workgroup cutoff
    max_retrieval_top_k: int = 20

    def check(self, *, tokens_in: int = 0, tokens_out: int = 0, tool_calls: int = 0,
              athena_bytes: int = 0, top_k: int = 0) -> list[str]:
        v = []
        if tokens_in > self.max_tokens_in:
            v.append(f"tokens_in {tokens_in} > {self.max_tokens_in}")
        if tokens_out > self.max_tokens_out:
            v.append(f"tokens_out {tokens_out} > {self.max_tokens_out}")
        if tool_calls > self.max_tool_calls:
            v.append(f"tool_calls {tool_calls} > {self.max_tool_calls}")
        if athena_bytes > self.max_athena_bytes:
            v.append(f"athena_bytes {athena_bytes} > {self.max_athena_bytes}")
        if top_k > self.max_retrieval_top_k:
            v.append(f"top_k {top_k} > {self.max_retrieval_top_k}")
        return v


def estimate_cost_usd(tokens_in: int, tokens_out: int, *,
                      rate_in_per_1k: float | None = None,
                      rate_out_per_1k: float | None = None) -> dict:
    """Cost from a SUPPLIED rate. No rate -> UNKNOWN, never a guess."""
    if rate_in_per_1k is None or rate_out_per_1k is None:
        return {"tokens_in": tokens_in, "tokens_out": tokens_out,
                "cost_usd": None,
                "note": "no rate supplied — run `make pricing` or consult "
                        "docs/PRICE_REFERENCE.md. A hardcoded rate goes stale silently."}
    cost = (tokens_in / 1000.0) * rate_in_per_1k + (tokens_out / 1000.0) * rate_out_per_1k
    return {"tokens_in": tokens_in, "tokens_out": tokens_out,
            "cost_usd": round(cost, 6), "rates": [rate_in_per_1k, rate_out_per_1k]}
