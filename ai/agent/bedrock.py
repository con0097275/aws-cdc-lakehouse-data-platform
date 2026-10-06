"""Thin Bedrock abstraction. Configurable model id; provider never leaks into the graph.

MODEL IDS AND INFERENCE PROFILES
--------------------------------
Newer Anthropic models on Bedrock cannot be invoked by bare model id -- they require a
cross-region INFERENCE PROFILE (`us.anthropic....`) and reject the plain id with
`ValidationException`. Verified 2026-08-26: `anthropic.claude-haiku-4-5-20251001-v1:0` is
rejected, while `anthropic.claude-3-5-sonnet-20240620-v1:0` and
`anthropic.claude-3-haiku-20240307-v1:0` invoke directly.

The AI-P1 agent config pinned the 4-5 id, which would have failed at first call. The default
below is a model this account can actually invoke, and `verify_access()` exists so the next
phase checks rather than assumes.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

DEFAULT_MODEL = "anthropic.claude-3-5-sonnet-20240620-v1:0"
FALLBACK_MODEL = "anthropic.claude-3-haiku-20240307-v1:0"


class ModelUnavailable(RuntimeError):
    """The model could not be invoked. Callers degrade; they do not fabricate."""


@dataclass
class ModelConfig:
    model_id: str = os.environ.get("AI_BEDROCK_MODEL", DEFAULT_MODEL)
    temperature: float = 0.0
    max_tokens: int = 800
    timeout_seconds: float = 30.0
    fallback_model_id: str | None = FALLBACK_MODEL


class BedrockClient:
    """Generation is OFF unless explicitly enabled -- an unbounded assistant is an
    unbounded bill, and tier 1 must remain free."""

    def __init__(self, config: ModelConfig | None = None, client=None,
                 enabled: bool | None = None):
        self.config = config or ModelConfig()
        self._c = client
        self.enabled = (os.environ.get("AI_ENABLE_GENERATION", "false").lower() == "true"
                        if enabled is None else enabled)
        self.tokens_in = 0
        self.tokens_out = 0

    def _client(self):
        if self._c is None:
            import boto3
            self._c = boto3.client("bedrock-runtime")
        return self._c

    def _invoke(self, model_id: str, system: str, user: str) -> str:
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        })
        r = self._client().invoke_model(modelId=model_id, contentType="application/json",
                                        accept="application/json", body=body)
        d = json.loads(r["body"].read())
        u = d.get("usage", {})
        self.tokens_in += u.get("input_tokens", 0)
        self.tokens_out += u.get("output_tokens", 0)
        return d["content"][0]["text"]

    def complete(self, system: str, user: str) -> str:
        if not self.enabled:
            raise ModelUnavailable("generation disabled (AI_ENABLE_GENERATION is not true)")
        try:
            return self._invoke(self.config.model_id, system, user)
        except Exception as e:
            if not self.config.fallback_model_id:
                raise ModelUnavailable(f"{type(e).__name__}: {e}") from e
            try:
                return self._invoke(self.config.fallback_model_id, system, user)
            except Exception as e2:
                raise ModelUnavailable(f"primary and fallback failed: {e2}") from e2

    def verify_access(self) -> dict:
        """One tiny call. Availability in a listing is not the same as access."""
        try:
            txt = self._invoke(self.config.model_id, "Reply with OK.", "OK?")
            return {"model_id": self.config.model_id, "ok": True, "sample": txt[:40]}
        except Exception as e:
            return {"model_id": self.config.model_id, "ok": False, "error": f"{type(e).__name__}: {e}"[:200]}

    @property
    def usage(self) -> dict:
        # Rates are not hardcoded: a stale price in code is worse than no price. AI-P11
        # attributes cost from the Pricing API against these counts.
        return {"tokens_in": self.tokens_in, "tokens_out": self.tokens_out,
                "model_id": self.config.model_id}
