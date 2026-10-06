# AI DATA RELIABILITY — EVALUATION

Phase **AIGR11**

## 1. What is measured today, and by what

The eval surface is the executable test suite, not a scored LLM benchmark — because the
safety properties this phase delivers are **deterministic**, and a deterministic property
deserves an assertion rather than a score.

| dimension | how | result |
|---|---|---|
| unsafe-action block rate | 52 safety-matrix rows | **52/52** |
| unauthorized mutation | any successful mutation is a P0 | **0** |
| prompt-injection resistance | 5 payloads in the request position | **5/5 blocked** |
| root-cause → disposition | every one of 14 categories | **14/14** |
| scope minimisation | per-job smallest supported granularity | asserted |
| affected-job precision | unrelated branch excluded | asserted |
| excluded-job correctness | every impacted asset planned **or** explained | asserted |
| plan immutability | hash changes on any edit | asserted |
| approval correctness | tamper, expiry, environment, self-approval | 4/4 |
| idempotency | same key converges | asserted |
| graph boundedness | step and tool ceilings terminate | asserted |
| answer labelling | every line labelled | asserted |

## 2. What is NOT measured, and why that matters

| dimension | status |
|---|---|
| intent classification accuracy | **not measured** — no live model is wired; the classifier is injected |
| asset/column resolution accuracy on real phrasing | **not measured** — no live resolver |
| root-cause accuracy against real incidents | **not measured** — the classifier is injected |
| tool-selection accuracy | **not measured** |
| latency / tokens / model cost | **not measured** — no Bedrock invocation has been made |
| recovery success rate | **not measured** — no AI-driven recovery has executed |

These are not oversights to be scored later by estimate. They are the difference between
*"the platform cannot be made to do the wrong thing"* — which is tested — and *"the platform
does the right thing on real requests"* — which needs a live model and a live run.

## 3. The threshold that already holds

> **No unsafe action is reachable, under any input, from any position.**

That is the one an eval cannot soften: it is enforced by construction, and 52 tests say so.

## 4. The next eval to build

A versioned case set of real natural-language requests with known-correct resolutions,
scored for intent, asset/column/date/key resolution, root cause, affected-job precision and
recall, excluded-job correctness, and approval routing — run against a live Bedrock model.
Until that exists, no accuracy number should be quoted for this copilot.
