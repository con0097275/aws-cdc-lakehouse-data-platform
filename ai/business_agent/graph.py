"""The business decision copilot: an explicit, bounded LangGraph.

NOT a ReAct loop. Every path from START to END is a fixed sequence of named nodes, and the
step budget is enforced by the graph rather than by asking a model to stop. An agent that
decides for itself how many Athena queries to run is a cost incident waiting for a busy day.

    classify_intent -> resolve_metric -> resolve_time -> check_data_contract
        -> [knowledge | value | driver | anomaly | forecast | prediction | governance
            | refuse]
        -> build_evidence -> verify_evidence -> compose_answer -> END

`verify_evidence` is the node that matters. If the evidence does not support an answer the
copilot SAYS SO. It never asks the model to fill the gap, because a filled gap is
indistinguishable from a fact in the sentence that comes out.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .tools import CATALOG

MAX_STEPS = 12
MAX_TOOL_CALLS = 6
MAX_TOKENS = 4000
TIMEOUT_SECONDS = 60.0

CONFIDENCE = ("DATA_FACT", "STATISTICAL_OBSERVATION", "MODEL_PREDICTION",
              "LLM_INTERPRETATION")

#: Advisory only. BAI-P6 section 7: the copilot may say WHAT TO LOOK AT, never what to do to
#: a customer or an account.
FORBIDDEN_RECOMMENDATIONS = (
    "close the account", "close these accounts", "block this customer", "block the customer",
    "approve the loan", "approve this loan", "reject the application", "freeze the account",
    "terminate", "suspend the customer",
)


@dataclass
class BusinessState:
    question: str
    as_of: str
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    intent: str = ""
    effective_time_phrase: str = "yesterday"
    memory: dict = field(default_factory=dict)
    metric_id: str | None = None
    metric_definition: dict | None = None
    time_context: dict | None = None
    contract_ok: bool = True
    contract_notes: list[str] = field(default_factory=list)
    tool_results: dict = field(default_factory=dict)
    tool_calls: list[str] = field(default_factory=list)
    evidence: dict = field(default_factory=dict)
    verified: bool = False
    verification: list[str] = field(default_factory=list)
    answer: dict = field(default_factory=dict)
    refused: str | None = None
    steps: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: str = field(default_factory=lambda:
                            datetime.now(timezone.utc).isoformat(timespec="seconds"))
    duration_ms: float = 0.0

    def to_dict(self) -> dict:
        return self.__dict__


# --------------------------------------------------------------------- nodes
def node_classify(state: BusinessState, *, runner=None) -> BusinessState:
    from analytics.request import parse_intent
    from agent.router import Intent as SafetyIntent, route as safety_route
    state.steps += 1
    if safety_route(state.question) is SafetyIntent.UNSAFE:
        state.intent = "UNSAFE"
        state.refused = (
            "I can't do that. This copilot is read-only (ADR-057): it has no tool that "
            "modifies, deletes, reruns, resets or destroys anything, so there is no path "
            "from this request to an action. I can explain what the operation does and "
            "point at the runbook.")
        return state
    q = state.question.lower()
    if any(w in q for w in ("what does", "definition", "mean", "glossary", "how is it "
                            "calculated", "what feeds", "lineage")):
        state.intent = "LINEAGE" if ("feeds" in q or "lineage" in q) else "KNOWLEDGE"
    elif any(w in q for w in ("investigate", "which accounts", "who should")):
        state.intent = "PREDICTION"
    elif any(w in q for w in ("certified", "certification", "data quality", "unavailable",
                              "why is this kpi")):
        state.intent = "GOVERNANCE"
    elif any(w in q for w in ("forecast", "next 7", "tomorrow", "predict")):
        state.intent = "FORECAST"
    elif any(w in q for w in ("abnormal", "anomal", "unusual", "spike")):
        state.intent = "ANOMALY"
    elif any(w in q for w in ("trend", "last 30 days", "last 7 days", "over the last",
                              "daily series", "history")):
        state.intent = "TREND"
    elif any(w in q for w in ("contributed", "which", "driver", "where did")):
        state.intent = "DRIVER"
    else:
        state.intent = parse_intent(state.question).value
    return state


def node_resolve_metric(state: BusinessState) -> BusinessState:
    from analytics.semantic import AmbiguousMetric, UnknownMetric
    state.steps += 1
    if state.intent == "UNSAFE":
        return state
    try:
        state.metric_id = CATALOG["resolve_metric"].fn(state.question)["metric_id"]
        state.metric_definition = CATALOG["get_metric_definition"].fn(state.metric_id)
    except AmbiguousMetric as e:
        state.refused = str(e)
        state.contract_ok = False
    except UnknownMetric as e:
        # Two different questions look identical here, and they need opposite answers:
        #
        #   "what is FULL_CDC"            -> ABOUT something. Answer from the corpus.
        #   "what is gross margin yesterday" -> asks for a NUMBER we cannot define. Refuse.
        #
        # The tell is a quantity or time expression. Sending the first to a refusal made the
        # copilot look broken on an architecture question; sending the second to the corpus
        # would answer a request for a figure with prose and no figure.
        import re as _re
        wants_a_number = _re.search(
            r"\b(yesterday|today|last week|last month|this month|month to date|mtd|"
            r"on \d{4}-\d{2}-\d{2}|how much|how many|total|sum|count|average|avg|"
            r"compare|trend|forecast|top \d+)\b", state.question, _re.I)
        # An investigate/diagnose question IS analytical even when it names no metric.
        # Falling through to the corpus answered it with runbook prose instead of ranking
        # the accounts that actually moved.
        # A FOLLOW-UP inherits the previous subject. A question that NAMES an unknown
        # subject does not: answering "what is gross margin yesterday" with the closing
        # balance is the exact failure this whole layer exists to prevent, and it is
        # invisible in the reply.
        #
        # The tell is a continuation marker or a question with no subject of its own.
        continuation = bool(_re.match(
            r"^\s*(and\b|also\b|what about\b|how about\b|now\b|then\b|ok\b|"
            r"compare\b|break\b|show\b|top \d+|bottom \d+)", state.question, _re.I))
        # ANOMALY, DRIVER, FORECAST and TREND are analytical intents too, and the UI
        # suggests all four as one-click questions -- "Is it abnormal?", "Which account
        # contributed most to the change?". Neither names a metric, so both fell out of
        # metric resolution and were answered from the KNOWLEDGE CORPUS: prose, badged
        # UNKNOWN, in place of the anomaly test or the driver ranking that was asked for.
        # A question the product puts on a button has to reach the tool that answers it.
        #
        # The guard below is what keeps the default honest: a question that NAMES an
        # unrecognised subject ("is gross margin abnormal?") must never be answered about
        # the closing balance instead, which is the failure this whole layer exists to
        # prevent and is invisible in the reply.
        subjectless = state.intent in ("PREDICTION", "DIAGNOSIS", "GOVERNANCE",
                                       "ANOMALY", "DRIVER", "FORECAST", "TREND") and \
            not _re.search(r"\b(margin|revenue|profit|churn|cost|price)\b",
                           state.question, _re.I)
        remembered = (state.memory or {}).get("metric_id")

        if (continuation and remembered) or subjectless:
            default = remembered or "total_closing_balance"
            state.metric_id = default
            state.metric_definition = CATALOG["get_metric_definition"].fn(default)
            state.contract_notes.append(
                (f"continuing with {default} from the previous question"
                 if remembered else
                 f"no metric named; defaulted to {default}. Name one explicitly to change it."))
            return state
        if wants_a_number:
            state.refused = str(e)
            state.contract_ok = False
        else:
            state.intent = "KNOWLEDGE"
            state.contract_notes.append(
                "no governed metric named; answered from the knowledge corpus instead")
    return state


def node_resolve_time(state: BusinessState) -> BusinessState:
    from analytics.timespec import resolve
    state.steps += 1
    if state.refused:
        return state
    # Resolve the phrase the TOOLS will actually use. Resolving the raw question here and
    # defaulting the tools to "yesterday" separately produced a real mismatch -- the
    # verifier correctly refused an otherwise-good answer because the queried window was
    # not the window the state claimed. One resolution, used by both.
    phrase = state.question
    ts = resolve(phrase, as_of=state.as_of)
    # The "closed day" default applies ONLY when the question named no window at all.
    #
    # The test used to be `window == as_of and "today" not in question`, which cannot tell
    # "the user said nothing" from "the user explicitly named this date". Asking
    # `total closing balance on 2026-09-03` with as_of=2026-09-03 satisfied both halves, so
    # the window was silently rewritten to 2026-09-02 and the answer came back labelled
    # "yesterday" -- an answer about a different day than the one asked for. Proven live
    # 2026-09-03; it was only visible because 09-02 held no rows. With data on both dates
    # it would have answered confidently about the wrong day, which is exactly the failure
    # the governed metric layer exists to prevent.
    #
    # `timespec.resolve` already distinguishes the two: an unspecified window is labelled
    # `as of <date>`, while an explicit date is labelled with the date itself and a phrase
    # like "today" carries its own label. So key the default off the LABEL, not off a value
    # comparison that an explicit date happens to satisfy.
    if ts.primary.label == f"as of {state.as_of}":
        phrase = "yesterday"                    # the business default: the closed day
        ts = resolve(phrase, as_of=state.as_of)
    state.effective_time_phrase = phrase
    state.time_context = ts.to_dict()
    return state


def node_check_contract(state: BusinessState) -> BusinessState:
    """Refuse BEFORE querying when the contract already says the answer is impossible."""
    state.steps += 1
    if state.refused or not state.metric_definition:
        return state
    d = state.metric_definition
    if state.intent in ("DRIVER", "BREAKDOWN", "TOP_N", "BOTTOM_N"):
        from analytics.semantic import load_registry
        reg = load_registry()
        # REFUSE A NAMED-BUT-BLOCKED DIMENSION, before any query is planned or billed.
        # Silently substituting an available one answers a different question than the one
        # asked, and the answer looks entirely healthy -- which is precisely the failure the
        # registry's `available: false` + `blocked_reason` pair exists to prevent.
        asked = requested_dimension(state.question, d["allowed_dimensions"])
        if asked and not reg.dimensions[asked].available:
            state.contract_ok = False
            state.contract_notes.append(
                f"{d['metric_id']} cannot be broken down by {asked!r}: "
                f"{reg.dimensions[asked].blocked_reason}. Refusing rather than answering "
                f"by a different dimension.")
        if not asked:
            stray = undeclared_dimension(state.question, d["allowed_dimensions"],
                                         list(reg.dimensions))
            if stray:
                state.contract_ok = False
                state.contract_notes.append(
                    f"{d['metric_id']} has no dimension {stray!r}. Declared dimensions: "
                    f"{', '.join(d['allowed_dimensions'])}. Refusing rather than answering "
                    f"by a different dimension.")
        avail = [x for x in d["allowed_dimensions"] if reg.dimensions[x].available]
        if not avail:
            state.contract_ok = False
            blocked = reg.dimensions[d["allowed_dimensions"][0]].blocked_reason
            state.contract_notes.append(
                f"no dimension of {d['metric_id']} is queryable: {blocked}")
    if state.intent == "FORECAST" and d["time_additivity"] != "additive":
        state.contract_notes.append(
            f"{d['metric_id']} is {d['time_additivity']}; a forecast of it is per-date, "
            "never a rolled-up total")
    return state


def _dimension_aliases(allowed: list[str]) -> dict[str, str]:
    """alias -> dimension name, for spotting the dimension a user actually named.

    Three forms per dimension: the exact name (`product_code`), the spaced form a person
    types (`product code`), and the bare stem (`product`) -- but the stem ONLY when it is
    unambiguous across the metric's dimensions. `account_sk` and `customer_sk` both end in
    `_sk`; their stems (`account`, `customer`) differ, so both are safe. If two dimensions
    ever shared a stem, neither would get one, and the question falls back to the default
    rather than guessing between them.
    """
    stems: dict[str, list[str]] = {}
    for name in allowed:
        stem = name.rsplit("_", 1)[0] if name.rsplit("_", 1)[-1] in ("code", "id", "sk") else name
        stems.setdefault(stem, []).append(name)
    aliases: dict[str, str] = {}
    for name in allowed:
        aliases[name] = name
        aliases[name.replace("_", " ")] = name
    for stem, owners in stems.items():
        if len(owners) == 1 and stem not in aliases:
            aliases[stem] = owners[0]
    return aliases


def requested_dimension(question: str, allowed: list[str]) -> str | None:
    """The dimension the USER named, or None if they named none.

    WHY THIS EXISTS. Both dispatch sites used to pick `next(x for x in allowed if
    available)` -- the first QUERYABLE dimension -- and never looked at the question at all.
    Asking "what drove the change by product_code" (declared, but `available: false` because
    dim_account is not materialised) therefore returned a breakdown by `account_sk`,
    presented as "Top segments -- 200320: 11,665,920 VND". Those are account ids labelled as
    segments: a confident answer to a question nobody asked.

    Returning the named dimension lets the caller REFUSE it when it is blocked, which is
    what the registry's own contract requires -- "a query using it must fail LOUDLY at
    compile time; silently dropping an unavailable dimension would answer a different
    question than the one asked".
    """
    q = " " + " ".join((question or "").lower().replace("_", "_").split()) + " "
    aliases = _dimension_aliases(list(allowed))
    # Longest alias first: `segment_code` must win over the `segment` stem.
    for alias in sorted(aliases, key=len, reverse=True):
        if f" {alias} " in q or f" {alias}?" in q or f" {alias}." in q:
            return aliases[alias]
    return None


def undeclared_dimension(question: str, allowed: list[str],
                         catalog: list[str]) -> str | None:
    """A dimension the user NAMED that this metric does not declare, or None.

    `requested_dimension` only searches the metric's OWN allowed dimensions, so a dimension
    outside that list matched nothing and the caller fell back to the default -- found by a
    web test on 2026-09-17: "break down total closing balance by source_flow_mode" came
    back as "Top segments -- 101: 1,032 VND; 102: …", which are account_sk values presented
    as segments. B2 closed "named but BLOCKED"; this closes "named but NOT DECLARED".

    Two signals, both conservative so an ordinary question is never refused:
      1. the question names a dimension that exists in the registry catalog but is not
         declared for this metric (e.g. `processing_status` on total_closing_balance);
      2. the question says `by <snake_case_identifier>` and that identifier is not an alias
         of any allowed dimension (e.g. `by source_flow_mode`). Requiring an underscore
         keeps "by yesterday" / "by date" from ever tripping it.
    """
    q = " " + " ".join((question or "").lower().split()) + " "
    allowed_aliases = _dimension_aliases(list(allowed))
    for name in sorted(catalog, key=len, reverse=True):
        if name in allowed:
            continue
        for form in (name, name.replace("_", " ")):
            if f" {form} " in q or f" {form}?" in q or f" {form}." in q:
                return name
    import re
    for m in re.finditer(r"\bby\s+([a-z][a-z0-9]*_[a-z0-9_]+)\b", q):
        ident = m.group(1)
        if ident not in allowed_aliases:
            return ident
    return None


def _resolve_dimension(state: BusinessState, reg) -> str | None:
    """The dimension to actually query: the one asked for, else the default."""
    allowed = state.metric_definition["allowed_dimensions"]
    asked = requested_dimension(state.question, allowed)
    if asked and reg.dimensions[asked].available:
        return asked
    if asked:                     # named but blocked -- the contract node already refused
        return None
    if undeclared_dimension(state.question, allowed, list(reg.dimensions)):
        return None               # named but NOT DECLARED -- also refused, never substituted
    return next((x for x in allowed if reg.dimensions[x].available), None)


def _bounded(state: BusinessState, name: str, fn, **kw):
    if len(state.tool_calls) >= MAX_TOOL_CALLS:
        state.errors.append(f"tool budget of {MAX_TOOL_CALLS} reached before {name}")
        return None
    state.tool_calls.append(name)
    try:
        out = fn(**kw)
        state.tool_results[name] = out
        return out
    except Exception as e:                                       # noqa: BLE001
        state.errors.append(f"{name}: {type(e).__name__}: {e}")
        return None


def _fetch_mart_rows(runner, as_of: str, *, lookback_days: int = 30) -> list[dict]:
    """Account-level mart rows for the risk model, typed and bounded.

    Athena returns every cell as a string and so does the demo DuckDB runner, but
    `materialize()` does arithmetic on closing_balance and txn_count. Casting here keeps
    that contract in one place instead of in the model.
    """
    from datetime import date as _date, timedelta as _td
    try:
        end = _date.fromisoformat(as_of)
    except ValueError:
        return []
    start = (end - _td(days=lookback_days)).isoformat()
    sql = ("SELECT account_sk, business_date, closing_balance, debit_amount, "
           "credit_amount, txn_count FROM kafka_dev_lab_dev_mart.mart_account_balance_daily "
           f"WHERE business_date BETWEEN DATE '{start}' AND DATE '{as_of}'")
    try:
        res = runner(sql)
    except Exception:                                            # noqa: BLE001
        return []                       # an optional enrichment must never fail the answer
    out = []
    for r in (res.get("rows") or []):
        try:
            out.append({"account_sk": r["account_sk"],
                        "business_date": str(r["business_date"])[:10],
                        "closing_balance": float(r.get("closing_balance") or 0),
                        "debit_amount": float(r.get("debit_amount") or 0),
                        "credit_amount": float(r.get("credit_amount") or 0),
                        "txn_count": int(float(r.get("txn_count") or 0))})
        except (KeyError, TypeError, ValueError):
            continue
    return out


def node_execute(state: BusinessState, *, runner, mart_rows=None) -> BusinessState:
    state.steps += 1
    if state.refused:
        return state
    m, a = state.metric_id, state.as_of

    if state.intent == "KNOWLEDGE":
        _bounded(state, "retrieve_business_knowledge",
                 CATALOG["retrieve_business_knowledge"].fn,
                 question=state.question, metric_id=m)
        return state
    if state.intent == "LINEAGE" and m:
        _bounded(state, "get_lineage", CATALOG["get_lineage"].fn, metric_id=m)
        return state
    if not m:
        return state

    if state.intent == "GOVERNANCE":
        _bounded(state, "get_certification_status",
                 CATALOG["get_certification_status"].fn, metric_id=m, as_of=a, runner=runner)
        _bounded(state, "get_data_quality", CATALOG["get_data_quality"].fn,
                 metric_id=m, as_of=a, runner=runner)
    elif state.intent == "DRIVER" and state.contract_ok:
        from analytics.semantic import load_registry
        reg = load_registry()
        dim = _resolve_dimension(state, reg)      # the one ASKED FOR, else the default
        if dim:
            _bounded(state, "analyze_metric_drivers",
                     CATALOG["analyze_metric_drivers"].fn, metric_id=m, as_of=a,
                     dimension=dim, runner=runner)
    elif state.intent in ("TOP_N", "BOTTOM_N", "BREAKDOWN"):
        # These fell through to compare_metric_periods, which returns a total and no
        # segments -- the UI showed "tools: compare_metric_periods" for a Top-5 question.
        from analytics.semantic import load_registry
        reg = load_registry()
        dim = _resolve_dimension(state, reg)      # the one ASKED FOR, else the default
        if dim:
            _bounded(state, "breakdown_metric", CATALOG["breakdown_metric"].fn,
                     metric_id=m, as_of=a, dimension=dim, runner=runner,
                     time_phrase=state.effective_time_phrase)
        else:
            # NO TOOL AT ALL was called here, so verification failed with "no tool was
            # called; there is no evidence to answer from" -- a true sentence that answers
            # nothing. Two questions land here: one naming a dimension that is declared but
            # not materialised, and one that is not really a breakdown at all.
            #
            # "What is the average balance per account?" classifies as BREAKDOWN because
            # `per account` looks like a grouping — but it is part of the metric's OWN name,
            # `avg_balance_per_account`. The same shape as reading a column out of an asset
            # name: a word belonging to the subject read as a modifier.
            #
            # Either way the metric total is still a real answer. Give it, and say what
            # could not be broken down, rather than returning nothing.
            state.contract_notes.append(
                "no queryable dimension for this metric; answering the total only")
            _bounded(state, "compare_metric_periods", CATALOG["compare_metric_periods"].fn,
                     metric_id=m, as_of=a, runner=runner,
                     time_phrase=state.effective_time_phrase)
    elif state.intent == "TREND":
        # TREND had no branch: it fell through to compare_metric_periods, which returns two
        # points and no series, so `trend` was always None. The golden set caught it.
        _bounded(state, "detect_metric_anomaly", CATALOG["detect_metric_anomaly"].fn,
                 metric_id=m, as_of=a, runner=runner)
    elif state.intent == "ANOMALY":
        _bounded(state, "detect_metric_anomaly", CATALOG["detect_metric_anomaly"].fn,
                 metric_id=m, as_of=a, runner=runner)
    elif state.intent == "FORECAST":
        _bounded(state, "forecast_metric", CATALOG["forecast_metric"].fn,
                 metric_id=m, as_of=a, runner=runner)
    elif state.intent == "PREDICTION":
        # `mart_rows` is an optional injection and NOTHING passed it -- scripts/ai-ui.py
        # computed a source and then called `ask_business` without it -- so this branch was
        # dead in both modes and "What should I investigate?" fell through to the KPI
        # comparison below. The user got a certified number where they asked which accounts
        # to look at: a different question, answered confidently.
        #
        # Fetched HERE rather than up front, and only for this intent, because it is an
        # extra scan that every other question would pay for and none would read. The SQL
        # goes through the same `prepare()` guard as every other read.
        rows = mart_rows
        if not rows:
            rows = _fetch_mart_rows(runner, a)
        if rows:
            # `materialize()` builds features FOR `as_of`, not for the latest date at or
            # before it, and the UI asks as-of the day AFTER the last business date so that
            # "yesterday" resolves to it. Passing that straight through produced 0 feature
            # rows and an empty, entirely silent prediction. Anchor on the newest date the
            # data actually has, and name it, so the ranking is never attributed to a date
            # the model did not score.
            pred_as_of = max(r["business_date"] for r in rows)
            _bounded(state, "predict_business_risk", CATALOG["predict_business_risk"].fn,
                     as_of=pred_as_of, mart_rows=rows)
            if pred_as_of != a:
                state.contract_notes.append(
                    f"accounts ranked as of {pred_as_of}, the latest business date with "
                    f"account-level rows")
        else:
            state.contract_notes.append(
                "no account-level rows available, so no accounts could be ranked; "
                "answering with the metric movement only")
        _bounded(state, "compare_metric_periods", CATALOG["compare_metric_periods"].fn,
                 metric_id=m, as_of=a, runner=runner)
    else:                                    # VALUE / TREND / PERIOD_COMPARISON / MIXED
        _bounded(state, "compare_metric_periods", CATALOG["compare_metric_periods"].fn,
                 metric_id=m, as_of=a, runner=runner,
                 time_phrase=state.effective_time_phrase)
    return state


def node_build_evidence(state: BusinessState) -> BusinessState:
    state.steps += 1
    state.evidence = {
        "request_id": state.request_id, "intent": state.intent,
        "metric_id": state.metric_id, "metric_definition": state.metric_definition,
        "time_context": state.time_context, "tool_calls": state.tool_calls,
        "tool_results": state.tool_results, "errors": state.errors,
        "contract_notes": state.contract_notes,
        "confidence_by_tool": {t: CATALOG[t].confidence for t in state.tool_calls
                               if t in CATALOG}}
    return state


def node_verify(state: BusinessState) -> BusinessState:
    """BAI-P6 section 6. Insufficient evidence is SAID, never filled."""
    from .verify import verify_evidence
    state.steps += 1
    ok, checks = verify_evidence(state)
    state.verified, state.verification = ok, checks
    return state


def node_compose(state: BusinessState, *, model=None) -> BusinessState:
    from .answer import build_answer
    state.steps += 1
    state.answer = build_answer(state, model=model)
    return state


def node_refuse(state: BusinessState) -> BusinessState:
    state.steps += 1
    state.answer = {"summary": state.refused, "data_status": "N/A",
                    "confidence": "DATA_FACT", "limitations": [state.refused],
                    "evidence": {"tool_calls": []}}
    return state


# --------------------------------------------------------------------- graph
def build_graph(*, runner, mart_rows=None, model=None):
    from langgraph.graph import END, StateGraph
    g = StateGraph(BusinessState)
    g.add_node("classify_intent", node_classify)
    g.add_node("resolve_metric", node_resolve_metric)
    g.add_node("resolve_time", node_resolve_time)
    g.add_node("check_data_contract", node_check_contract)
    g.add_node("execute", lambda s: node_execute(s, runner=runner, mart_rows=mart_rows))
    g.add_node("build_evidence", node_build_evidence)
    g.add_node("verify_evidence", node_verify)
    g.add_node("compose_answer", lambda s: node_compose(s, model=model))
    g.add_node("refuse", node_refuse)

    g.set_entry_point("classify_intent")
    g.add_conditional_edges("classify_intent",
                            lambda s: "refuse" if s.refused else "resolve_metric",
                            {"refuse": "refuse", "resolve_metric": "resolve_metric"})
    g.add_conditional_edges("resolve_metric",
                            lambda s: "refuse" if s.refused else "resolve_time",
                            {"refuse": "refuse", "resolve_time": "resolve_time"})
    g.add_edge("resolve_time", "check_data_contract")
    g.add_edge("check_data_contract", "execute")
    g.add_edge("execute", "build_evidence")
    g.add_edge("build_evidence", "verify_evidence")
    g.add_edge("verify_evidence", "compose_answer")
    g.add_edge("compose_answer", END)
    g.add_edge("refuse", END)
    return g.compile()


def ask_business(question: str, *, as_of: str, runner, mart_rows=None,
                 model=None, memory: dict | None = None, cache=None) -> dict:
    """Entry point. Deterministic, bounded, read-only.

    `memory` carries the LAST resolved metric and window so a follow-up ("and yesterday?",
    "break that down") does not have to name the metric again. It is a hint only: an
    explicit metric in the new question always wins, because silently reusing a stale
    subject is how a user reads an answer about the wrong KPI.

    `cache` is a cross-request QueryCache. Closed business dates are immutable, so
    re-querying them bills twice for a byte-identical answer.
    """
    t0 = time.perf_counter()
    state = BusinessState(question=question, as_of=as_of)
    state.memory = dict(memory or {})
    if cache is not None:
        runner = cache.wrap(runner, as_of)
    try:
        app = build_graph(runner=runner, mart_rows=mart_rows, model=model)
        out = app.invoke(state)
        state = out if isinstance(out, BusinessState) else BusinessState(**dict(out))
    except Exception as e:                                       # noqa: BLE001
        # The framework failing must not take the copilot with it.
        state.errors.append(f"graph: {type(e).__name__}: {e}")
        for node in (node_classify, node_resolve_metric, node_resolve_time,
                     node_check_contract):
            state = node(state)
        state = node_execute(state, runner=runner, mart_rows=mart_rows)
        state = node_compose(node_verify(node_build_evidence(state)), model=model)
    state.duration_ms = round((time.perf_counter() - t0) * 1000, 1)
    if state.steps > MAX_STEPS:
        state.errors.append(f"step budget exceeded: {state.steps} > {MAX_STEPS}")
    out = state.to_dict()
    out["memory"] = {"metric_id": state.metric_id,
                     "time_phrase": state.effective_time_phrase,
                     "as_of": state.as_of}
    if cache is not None:
        out["cache"] = cache.stats()
    return out
