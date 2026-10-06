"""AI platform contracts — the shared vocabulary every later AI phase compiles against.

WHY THIS IS NOT INSIDE ai/
--------------------------
`ai/` is the assistant, and one of its load-bearing properties is that it is DELETABLE:
no pipeline code imports it, and `test_ai_assistant.py` asserts that. Putting feature or
ops contracts under `ai/` would force `spark/features/*` to import it and quietly destroy
that property.

So contracts live here. `ai/` may import `aiplatform`; `aiplatform` never imports `ai/`.

WHY THIS IS NOT INSIDE reporting/
---------------------------------
`reporting/` is keyed on the five reporting FLOW MODES (EOD, AUTO_CORRECT, FULFILL,
STREAM_BATCH, STREAMING_RT). A knowledge source and an agent tool have no flow mode, and
bolting them onto that schema would either loosen the reporting schema for everyone or
invent a sixth pseudo-mode. AI-P1's brief is explicit: reuse the PATTERN, do not couple to
reporting-only flow modes.

What is reused is the pattern, exactly:

    Git-controlled YAML
        -> JSON Schema validation      (structure)
        -> semantic validation         (cross-references, invariants)
        -> deterministic compilation   (sorted, canonical payload)
        -> sha256 config_version       (content-addressed, never random)

ONE config system. Adding a second place where an AI object can be defined is how two
definitions of the same thing drift apart while both look maintained.
"""

from __future__ import annotations

__all__ = [
    "classification", "versioning", "knowledge", "features",
    "models", "agents", "evaluation", "ops",
]

CONTRACT_VERSION = "1.0.0"
