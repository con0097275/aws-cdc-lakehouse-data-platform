"""The entry point behind the CLI and the UI: one English question -> one evidence pack.

WHAT THE MODEL DOES HERE
------------------------
Classifies intent, and (optionally) writes the closing prose. That is all. Asset ids,
columns, dates, keys, lineage, root cause, plan, policy and verification come from tools.

If Bedrock is unavailable the intent falls back to keyword rules. That degrades the
phrasing the copilot understands; it does not change what it is allowed to do.
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from cdc.incidents import CostClass
from .context import (_FILLER, LayerAmbiguity, ResolutionError, concept_for,
                      get_column_lineage,
                      get_dataset_lineage, resolve_asset)
from .evidence import classify as classify_from_evidence
from .evidence import collect as collect_evidence
from .control import RecoveryControlService
from .copilot import KNOWLEDGE_INTENTS, CopilotState, Intent, Label, run
from .model import (IncidentCategory, LineageConfidence, RecoveryScope, RootCause,
                    ScopeSource)
from .planner import AiRecoveryPlanner, JobNode, estimate_cost
from .registry import CAPABILITIES, JOB_LAYER
from .tools import AuthorizationContext

_DATE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
_KEYS = re.compile(r"\b([A-Z]{1,4}\d{2,8})\b")
_QUOTED = re.compile(r"[\"'`]([A-Za-z_][A-Za-z0-9_]*)[\"'`]")

#: Keyword rules used when no model is available. Ordered: the first match wins, and
#: EXECUTE is checked last on purpose -- "investigate and repair" is an investigation the
#: user may then approve, not an instruction to start writing.
#: Precedence, not just a list: **acting beats diagnosing beats describing.**
#:
#: A flat ordered scan put EXPLAIN second, so "repair only the affected DOWNSTREAM data"
#: matched a lineage keyword and a repair request became a lineage question. A descriptive
#: noun appearing inside an instruction does not make it a description.
_ACTION_RULES = (
    (Intent.EXECUTE, ("repair", "fix it", "rerun", "rebuild", "execute", "run the recovery")),
    (Intent.PLAN, ("plan a", "plan the", "dry run", "dry-run", "what would", "simulate",
                   "propose")),
)
_DIAGNOSE_RULES = (
    (Intent.INVESTIGATE, ("investigate", "why is", "why did", "why does", "diagnose",
                          "look into", "is wrong", "is incorrect", "is broken", "failed")),
)
_STATUS_RULES = (
    # `status` alone is too greedy: "what breaks if I drop STATUS" is a lineage question
    # about a COLUMN called STATUS.
    (Intent.STATUS, ("status of", "how is it going", "progress", "still running",
                     "recovery status", "is it done")),
)
_DESCRIBE_RULES = (
    (Intent.EXPLAIN, ("what is", "what does", "explain", "how does", "who owns", "define",
                      "downstream", "upstream", "feed", "feeds", "depends on", "lineage",
                      "impact of", "affected by", "affected downstream", "consumers",
                      "what breaks", "where does", "which tables", "show me")),
)
_INTENT_RULES = _ACTION_RULES + _DIAGNOSE_RULES + _STATUS_RULES + _DESCRIBE_RULES


def classify_intent(question: str, *, model=None) -> Intent:
    if model is not None:
        try:
            return Intent(model(question).strip().upper())
        except Exception:                        # noqa: BLE001 - nonsense is ambiguity
            pass
    q = question.lower()
    for group in (_ACTION_RULES, _DIAGNOSE_RULES, _STATUS_RULES, _DESCRIBE_RULES):
        for intent, words in group:
            if any(w in q for w in words):
                return intent
    return Intent.AMBIGUOUS


@dataclass
class Resolution:
    asset_id: str = ""
    layer_alternates: tuple = ()
    owner: str = ""
    column: str = ""
    column_confidence: str = ""
    cob_dates: tuple = ()
    business_keys: tuple = ()
    impacted: tuple = ()
    notes: list = field(default_factory=list)

    def payload(self) -> dict:
        return {"asset_id": self.asset_id, "owner": self.owner, "column": self.column,
                "column_confidence": self.column_confidence,
                "cob_dates": [d.isoformat() for d in self.cob_dates],
                "business_keys": list(self.business_keys),
                "impacted_count": len(self.impacted), "notes": self.notes,
                "layer_alternates": list(self.layer_alternates)}


def resolve(question: str, *, describing: bool = False) -> Resolution:
    """Deterministic entity extraction, then the REAL resolvers.

    Nothing here guesses an identifier. `resolve_asset` refuses on ambiguity and the caller
    surfaces that refusal rather than picking a candidate.
    """
    r = Resolution()
    r.cob_dates = tuple(date.fromisoformat(d) for d in _DATE.findall(question))
    r.business_keys = tuple(sorted(set(_KEYS.findall(question))))

    # The asset phrase is what remains once dates and keys are removed; the resolver scores
    # it against every governed asset and refuses a tie.
    phrase = _DATE.sub(" ", question)
    phrase = _KEYS.sub(" ", phrase)
    try:
        a = resolve_asset(phrase)
        r.asset_id, r.owner = a.asset_id, a.owner
    except LayerAmbiguity as e:
        # One table at several layers. For a DESCRIPTION all of them are the answer; for a
        # repair the layer decides what gets rewritten, so only a describing caller may
        # proceed -- and it names every layer rather than picking one.
        if not describing:
            r.notes.append(f"asset not resolved: {e}. Name the layer to plan a recovery.")
            return r
        r.layer_alternates = tuple(c.asset_id for c in e.candidates)
        best = next((c for c in e.candidates if c.asset_id.startswith("eod:")),
                    e.candidates[0])
        r.asset_id, r.owner = best.asset_id, best.owner
        r.notes.append(f"one table at {len(e.candidates)} layers: "
                       f"{', '.join(r.layer_alternates)}")
    except ResolutionError as e:
        r.notes.append(f"asset not resolved: {e}")
        return r

    # A column is only accepted if it is really on the asset AND is not simply part of the
    # asset's own name. "EOD ACCOUNT" resolved `..._corebank_account`, and ACCOUNT then
    # matched a column edge, so the copilot reported `column ACCOUNT` for a question that
    # never mentioned a column -- and a spurious column changes what the scope narrows on.
    asset_words = set(re.split(r"[^a-z0-9]+", r.asset_id.lower()))
    explicit = re.findall(r"(?:column|field)\s+[\"'`]?([A-Za-z_][A-Za-z0-9_]*)", question, re.I)
    # `column` OR `field` on this side too. Only the keyword-first pattern accepted both,
    # so "the BALANCE field in EOD ACCOUNT is wrong" captured nothing once "in" was
    # correctly dropped -- a filler fix that turns a wrong answer into no answer has moved
    # the defect, not removed it.
    explicit += re.findall(r"[\"'`]?([A-Za-z_][A-Za-z0-9_]*)[\"'`]?\s+(?:column|field)",
                           question, re.I)
    candidates = [c for c in _QUOTED.findall(question)] + explicit
    candidates += [w for w in re.findall(r"\b([A-Z_]{3,})\b", question)
                   if w.lower() not in ("eod", "cdc", "dq", "sql")]
    # A preposition is not a column. `column|field\s+(\w+)` reads the word AFTER the
    # keyword, but English puts the name BEFORE it as often as after: in "the BALANCE
    # column in EOD ACCOUNT" the word after "column" is "in". That fragment then matched
    # clos*in*g_balance, so the copilot reported `column in` -- and because a non-ABSENT
    # hit stops the search, BALANCE, which was sitting in the very same candidate list,
    # was never tried. Filler is dropped BEFORE the lineage lookup, not after.
    candidates = [c for c in candidates
                  if c.lower() not in asset_words
                  and c.lower() not in _FILLER
                  and len(c) > 2]
    for c in candidates:
        cl = get_column_lineage(r.asset_id, c)
        if cl["confidence"] != LineageConfidence.ABSENT.value:
            r.column, r.column_confidence = c, cl["confidence"]
            break
    try:
        r.impacted = tuple(get_dataset_lineage(r.asset_id)["impacted"])
    except Exception as e:                       # noqa: BLE001
        r.notes.append(f"lineage unavailable: {e}")
    return r


def build_scope(res: Resolution, *, environment: str, request_id: str,
                requested_by: str) -> RecoveryScope:
    """Narrow to a column ONLY on VALIDATED lineage; otherwise say table level and mean it."""
    narrow = res.column and res.column_confidence == LineageConfidence.VALIDATED.value
    return RecoveryScope(
        root_asset_id=res.asset_id, environment=environment,
        root_column=res.column if narrow else "",
        cob_dates=res.cob_dates, business_keys=res.business_keys,
        scope_source=ScopeSource.COLUMN_LINEAGE if narrow else ScopeSource.TABLE_LINEAGE,
        scope_confidence=(LineageConfidence.VALIDATED if narrow
                          else LineageConfidence(res.column_confidence
                                                 or LineageConfidence.ABSENT.value)),
        reason=("narrowed on validated column lineage" if narrow else
                f"column lineage is {res.column_confidence or 'ABSENT'}; table level"),
        requested_by=requested_by, request_id=request_id)


def _planner_for(res: Resolution) -> AiRecoveryPlanner:
    """Map the impacted assets onto registered jobs, by layer.

    An impacted asset whose layer has no registered job simply has no JobNode, and the
    planner records it as excluded WITH a reason rather than dropping it.
    """
    jobs, turns = {}, []
    layer_of = {"snapshot": "eod_build", "eod": "eod_build", "stream": "realtime_materialize",
                "realtime": "realtime_materialize", "curated": "curated_build",
                "mart": "reporting", "full_cdc": "debezium"}
    root_layer = res.asset_id.split(":", 1)[0]
    root_job = layer_of.get(root_layer)
    if root_job:
        jobs[f"{root_job}@{res.asset_id}"] = JobNode(root_job, res.asset_id,
                                                     JOB_LAYER[root_job], CAPABILITIES[root_job])
        turns.append([res.asset_id])
    downstream = [u for u in res.impacted if "mart" in u or "curated" in u][:6]
    if downstream:
        for u in downstream:
            jobs[f"reporting@{u}"] = JobNode("reporting", u, "mart", CAPABILITIES["reporting"])
        turns.append(list(downstream))
    return AiRecoveryPlanner(jobs=jobs, turns=turns)


#: Causes an operator can STATE. Used to corroborate, never to conclude on its own.
_STATED = (
    (("source system", "source value", "upstream sent", "bad value from",
      "source is wrong", "wrong in the source"), IncidentCategory.BAD_SOURCE_VALUE),
    (("transform", "transformation", "logic is", "code is", "bug in the", "buggy"),
     IncidentCategory.TRANSFORM_LOGIC_DEFECT),
    (("dq rule", "quality rule", "the rule is", "check is wrong", "false positive"),
     IncidentCategory.DQ_RULE_DEFECT),
    (("missing event", "never arrived", "no event"), IncidentCategory.MISSING_SOURCE_EVENT),
    (("updated the record", "source update", "restated", "corrected the source",
      "changed the record", "amended"), IncidentCategory.LATE_SOURCE_EVENT),
    (("late", "arrived late", "backdated"), IncidentCategory.LATE_SOURCE_EVENT),
    (("duplicate", "doubled", "counted twice"), IncidentCategory.DUPLICATE_CDC_EVENT),
    (("out of order", "out-of-order"), IncidentCategory.OUT_OF_ORDER_EVENT),
    (("stale", "not refreshed", "out of date"), IncidentCategory.STALE_DATA),
    (("schema change", "column added", "schema drift"), IncidentCategory.SCHEMA_DRIFT),
)


def _stated_cause(question: str) -> IncidentCategory | None:
    q = question.lower()
    for words, category in _STATED:
        if any(w in q for w in words):
            return category
    return None


def ask(question: str, *, principal: str = "operator", roles: tuple = (),
        environment: str = "dev", model=None, composer=None,
        service: RecoveryControlService | None = None) -> dict:
    """One question in, one evidence pack out. Never mutates unless a service is supplied
    AND policy allows AND the intent is EXECUTE."""
    roles = frozenset(roles or ("READ_METADATA", "READ_DATA_SAFE", "PLAN_RECOVERY"))
    ctx = AuthorizationContext(principal, roles, environment)
    started = datetime.now(timezone.utc)
    rid = f"req-{int(started.timestamp())}"

    # Intent first: whether a layer ambiguity may be answered depends on whether the
    # caller is describing or acting.
    intent = classify_intent(question, model=model)
    stated = _stated_cause(question)
    if intent in (Intent.AMBIGUOUS, Intent.EXPLAIN) and stated is not None:
        # The sentence DESCRIBES A DEFECT, whatever its verb suggested. "The source system
        # sent a bad value" states a cause without asking anything; routing it to a
        # governance summary would answer a question nobody asked and skip the disposition
        # that forbids rebuilding for a source defect. EXPLAIN is included because a defect
        # sentence often contains a lineage noun ("the downstream data is wrong").
        intent = Intent.INVESTIGATE
    # A definitional question about a LAYER is not an asset reference. "What is FULL_CDC?"
    # names a layer, every table in that layer scores alike, and the resolver refused a
    # six-way tie -- correct behaviour applied to the wrong kind of question. Answered here,
    # before resolution, so the glossary can never pre-empt an asset question: `concept_for`
    # fires only on definitional phrasing, so "rebuild FULL_CDC for 2026-09-28" is untouched.
    concept_name, concept_text = concept_for(question)
    if concept_name and intent in (Intent.EXPLAIN, Intent.AMBIGUOUS):
        return {"question": question, "request_id": rid,
                "ledger_evidence": None, "ledger_error": None,
                "intent": Intent.EXPLAIN.value,
                # The SAME keys every other answer returns: a caller that renders one must
                # not have to know this branch exists. Building a partial dict here cost
                # three KeyErrors in the CLI before this comment was written.
                "resolution": {"asset_id": "", "owner": "", "column": "",
                               "column_confidence": "", "cob_dates": [],
                               "business_keys": [], "impacted_count": 0,
                               "notes": [f"platform concept: {concept_name}"],
                               "layer_alternates": [], "concept": concept_name},
                "answer": [{"label": Label.FACT.value, "text": concept_text},
                           {"label": Label.LIMITATION.value,
                            "text": ("This is an architecture answer from the platform "
                                     "glossary. No asset was resolved and nothing was "
                                     "diagnosed, because the question named a concept, "
                                     "not a table.")}],
                "terminated": "answered",
                "plan": None, "policy": None, "evidence": None, "safety_events": [],
                "duration_ms": int((datetime.now(timezone.utc) - started).total_seconds() * 1000)}

    res = resolve(question, describing=intent in (Intent.EXPLAIN, Intent.STATUS,
                                                  Intent.AMBIGUOUS))
    evidence_box: dict = {}
    state = CopilotState(request_id=rid, authenticated_user=principal,
                         authorization_context=ctx, raw_request=question)

    def _resolver(_q):
        return {"assets": [res.asset_id] if res.asset_id else [],
                "columns": [res.column] if res.column else [],
                "dates": [d.isoformat() for d in res.cob_dates],
                "keys": list(res.business_keys)}


    def _cause(_s):
        """Read the LEDGERS, then classify. A stated cause is corroborated, not believed.

        The earlier version read only the question, which made the copilot exactly as good
        as the operator's own diagnosis -- and "plan a recovery" returned UNKNOWN, because
        the operator had not said what was wrong. They were asking because they did not
        know. A data engineer in that position opens the DQ results, the reconciliation
        ledger, the EOD run record and the quarantine table. So does this.
        """
        cob = res.cob_dates[0] if res.cob_dates else None
        try:
            ev = collect_evidence(res.asset_id, cob)
        except Exception as e:                   # noqa: BLE001 - unread is not clean
            evidence_box["error"] = f"{type(e).__name__}: {e}"
            return RootCause(stated, 0.6, ("request:stated",),
                             detail="ledgers unreadable; taken on the operator's word") \
                if stated else RootCause(IncidentCategory.UNKNOWN, 0.0, ())
        evidence_box["evidence"] = ev.payload()
        return classify_from_evidence(ev, stated=stated)

    def _plan(s):
        scope = build_scope(res, environment=environment, request_id=rid,
                            requested_by=principal)
        pl = _planner_for(res)
        return pl.plan(incident_id=f"inc-{rid}", root_asset_id=res.asset_id,
                       root_cause=s.root_cause, scope=scope,
                       impacted={res.asset_id, *[j.asset_id for j in pl._jobs.values()]},
                       excluded=[], environment=environment)

    def _describe(st):
        """Facts for a question that is not about a defect. Every line comes from a tool."""
        lines = [(Label.FACT, f"{res.asset_id} is owned by {res.owner or 'nobody recorded'}"
                              f"{', domain ' + dom if (dom := '') else ''}.")]
        if res.column:
            strength = ("VALIDATED, so a recovery may be narrowed to this column"
                        if res.column_confidence == LineageConfidence.VALIDATED.value
                        else f"{res.column_confidence}, which is too weak to narrow a "
                             "recovery on — an impact analysis would fall back to table level")
            lines.append((Label.FACT,
                          f"Column lineage for {res.column} is {strength}."))
        if res.layer_alternates:
            lines.append((Label.FACT,
                          f"This table exists at {len(res.layer_alternates)} layers: "
                          + ", ".join(res.layer_alternates)
                          + f". Described below: {res.asset_id}."))
        if res.impacted:
            shown = [u.split(",")[1] if "," in u else u for u in res.impacted[:8]]
            lines.append((Label.FACT,
                          f"{len(res.impacted)} downstream assets depend on "
                          f"{res.asset_id}: " + ", ".join(shown)
                          + (f", and {len(res.impacted) - 8} more" if len(res.impacted) > 8 else "")))
        else:
            lines.append((Label.LIMITATION,
                          "No downstream lineage was found for this asset."))
        if st.normalized_intent is Intent.AMBIGUOUS:
            lines.append((Label.LIMITATION,
                          "You named an asset but not what you wanted. Ask what it feeds, "
                          "who owns it, why a date looks wrong, or for a recovery plan — "
                          "nothing was diagnosed, because nothing described a defect."))
        else:
            lines.append((Label.LIMITATION,
                          "This is a lineage and governance answer. Nothing was diagnosed "
                          "and no recovery was planned, because the question did not "
                          "describe a defect."))
        return lines

    out = run(state, describe=_describe,
              classifier=(lambda q: intent.value),
              resolver=_resolver if res.asset_id else (lambda _q: {"assets": []}),
              cause_classifier=_cause,
              planner=_plan if res.asset_id else None,
              service=service, composer=composer)

    return {"question": question, "request_id": rid,
            "ledger_evidence": evidence_box.get("evidence"),
            "ledger_error": evidence_box.get("error"),
            "intent": out.normalized_intent.value,
            "resolution": res.payload(),
            "answer": [{"label": l, "text": t} for l, t in out.answer],
            "terminated": out.terminated or "completed",
            "plan": out.recovery_plan.payload() if out.recovery_plan else None,
            "policy": out.policy_decision.payload() if out.policy_decision else None,
            "evidence": out.final_evidence_pack,
            "safety_events": list(out.safety_events),
            "duration_ms": int((datetime.now(timezone.utc) - started).total_seconds() * 1000)}
