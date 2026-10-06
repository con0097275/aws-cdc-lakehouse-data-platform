"""Runtime entrypoint. Thin on purpose.

The graph lives in `ai/agent/graph.py` and runs identically locally and hosted, so the local
path stays testable without AWS and the runtime stays swappable. If this handler contained
business logic, changing runtime would mean rewriting the agent.

NO CREDENTIALS. The runtime supplies them through the role it assumes; nothing here reads a
key, and `verify_no_credentials_in_package()` asserts it.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent.bedrock import BedrockClient
from agent.graph import AGENT_VERSION, ask
from agent.prompts import VERSIONS

RUNTIME_VERSION = "runtime:v1"
MAX_QUESTION_CHARS = 1000


def _version_metadata() -> dict:
    return {
        "agent_version": AGENT_VERSION,
        "runtime_version": RUNTIME_VERSION,
        "prompt_versions": VERSIONS,
        "model_id": os.environ.get("AI_BEDROCK_MODEL", "(default)"),
        "generation_enabled": os.environ.get("AI_ENABLE_GENERATION", "false"),
    }


def health() -> dict:
    """Readiness WITHOUT calling a model. A health check that costs tokens is a health
    check nobody runs often enough to be useful."""
    import glob
    corpus = sorted(glob.glob(str(Path(__file__).resolve().parents[2] /
                                  "ai" / "knowledge" / "corpus_*")))
    from agent_tools.catalog import CATALOG, WRITE_TOOLS
    ok = bool(corpus) and bool(CATALOG) and WRITE_TOOLS == {}
    return {"status": "ok" if ok else "degraded",
            "corpus_present": bool(corpus),
            "tools": sorted(CATALOG),
            "write_tools_empty": WRITE_TOOLS == {},
            **_version_metadata()}


def handle(event: dict, context=None) -> dict:
    """Lambda / AgentCore entrypoint. Returns a JSON-serialisable envelope."""
    if event.get("action") == "health" or event.get("path") == "/health":
        return {"statusCode": 200, "body": health()}

    question = (event.get("question") or event.get("prompt") or "").strip()
    if not question:
        return {"statusCode": 400,
                "body": {"error": "no question supplied", **_version_metadata()}}
    if len(question) > MAX_QUESTION_CHARS:
        return {"statusCode": 400,
                "body": {"error": f"question exceeds {MAX_QUESTION_CHARS} characters",
                         **_version_metadata()}}

    session_id = (event.get("session_id")
                  or getattr(context, "aws_request_id", None)
                  or "anonymous")

    model = BedrockClient() if os.environ.get(
        "AI_ENABLE_GENERATION", "false").lower() == "true" else None

    result = ask(question, session_id=session_id, model=model)
    return {"statusCode": 200, "body": {
        "request_id": result["request_id"],
        "intent": result["intent"],
        "answer": result["answer"],
        "citations": list(dict.fromkeys(result["citations"])),
        "tool_calls": result["tool_calls"],
        "safety": result["safety"],
        "errors": result["errors"],
        "generated": result["generated"],
        "tokens_in": result["tokens_in"], "tokens_out": result["tokens_out"],
        "duration_ms": result["duration_ms"],
        **_version_metadata()}}


def verify_no_credentials_in_package(root: Path | None = None) -> list[str]:
    """Fail the build if a credential is inside the deployment package."""
    import re
    root = root or Path(__file__).resolve().parents[1]
    pats = [re.compile(p) for p in (
        r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b", r"aws_secret_access_key\s*=",
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----", r"(?i)password\s*=\s*[\"'][^\"'$<]{6,}")]
    hits = []
    for p in root.rglob("*.py"):
        if "__pycache__" in str(p):
            continue
        t = p.read_text(errors="replace")
        for rx in pats:
            if rx.search(t):
                hits.append(f"{p}: {rx.pattern[:30]}")
    return hits


if __name__ == "__main__":
    print(json.dumps(handle({"question": " ".join(sys.argv[1:]) or "What is FULL_CDC?"}),
                     indent=2, default=str)[:2000])
