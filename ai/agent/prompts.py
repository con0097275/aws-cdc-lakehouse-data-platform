"""Versioned prompts.

PROMPTS STATE BEHAVIOUR. THEY DO NOT ENFORCE IT.
------------------------------------------------
Every security property here is enforced in `ai/agent_tools/` and in IAM. The prompt says
what the assistant is for; if the model ignores it entirely, the tool contract still refuses
a mutation, the allow-list still refuses a database, and IAM still refuses raw CDC.

That separation is the whole point: a prompt is a request addressed to a component whose job
is producing plausible text, over inputs that may contain an attacker's instructions.

No secrets, no account ids, no endpoints. A prompt is shipped to a third-party model.
"""

from __future__ import annotations

import hashlib

SYSTEM_PROMPT_V = "prompt:system-v1"
ROUTER_PROMPT_V = "prompt:router-v1"
ANSWER_PROMPT_V = "prompt:answer-v1"

SYSTEM_PROMPT = """You are the Data Platform Copilot for an AWS CDC lakehouse.

You answer questions about architecture, data contracts, lineage, pipeline status, features
and models. You are READ-ONLY: you cannot change anything, and no tool available to you can.

Rules you must follow:
- Answer ONLY from tool results. If the tools returned nothing useful, say so plainly.
- Never invent a table name, a metric, a status or a number.
- Cite the source for every factual claim drawn from documentation.
- Operational status comes from tools, never from your own expectation of how the system
  probably behaves.
- If asked to change, delete, reset, rerun or destroy anything, refuse and explain that V1
  is read-only.
"""

ROUTER_PROMPT = """Classify the question into exactly one intent.

KNOWLEDGE        concepts, architecture, "what is", "why", runbook procedure
STRUCTURED_DATA  a number or rows from the warehouse ("how many", "show me", "count")
PIPELINE_OPS     job status, watermarks, staleness, data quality
LINEAGE          what feeds what, upstream/downstream, table schema or ownership
FEATURE          feature definitions, feature groups
PREDICTION       model status, scores, predictions
MIXED            needs more than one of the above
UNSAFE           asks to modify, delete, reset, rerun, destroy or run a command

Answer with the single intent word and nothing else.
"""

ANSWER_PROMPT = """Answer the user's question using ONLY the tool results below.

- Be direct and brief.
- Quote figures exactly as the tools returned them.
- Cite documentation sources inline.
- If the results do not answer the question, say what is missing rather than guessing.
"""


def prompt_version(text: str, label: str) -> str:
    """Content-addressed, so an edited prompt cannot keep its old version."""
    return f"{label}:{hashlib.sha256(text.encode()).hexdigest()[:12]}"


VERSIONS = {
    "system": prompt_version(SYSTEM_PROMPT, SYSTEM_PROMPT_V),
    "router": prompt_version(ROUTER_PROMPT, ROUTER_PROMPT_V),
    "answer": prompt_version(ANSWER_PROMPT, ANSWER_PROMPT_V),
}
