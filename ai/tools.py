"""Read-only tools. Structured lookup where a dictionary is better than a language model.

WHY TOOLS AND NOT PURE RAG
--------------------------
"Who owns mart.dim_customer and what is its retention?" has an EXACT answer sitting in
governance/catalog/domains.yml. Retrieval over prose returns documents that discuss the
dimension; a lookup returns the owner.

Measured on this corpus: that question retrieves KIMBALL_MODEL.md sections, which are about
SCD2 mechanics, not ownership — because the registry is one YAML chunk and gets diluted by
the surrounding prose. The retriever is not broken; the question is simply not a retrieval
question.

So the assistant routes: structured questions to a tool, open questions to retrieval. Using
an LLM to paraphrase a dictionary lookup adds cost, latency and a hallucination surface to a
question that has a lookup answer.

EVERY TOOL IS READ-ONLY BY CONSTRUCTION. They read files and return strings. None opens a
network connection, none shells out, none touches AWS.
"""

from __future__ import annotations

import os
import re

import yaml

from guards import (GuardViolation, assert_allowed_tables, assert_no_infrastructure_action,
                    assert_read_only_sql, redact)

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REGISTRY = os.path.join(ROOT, "governance", "catalog", "domains.yml")
LINEAGE = os.path.join(ROOT, "governance", "lineage", "openlineage.yml")
RUNBOOK = os.path.join(ROOT, "docs", "RUNBOOK.md")
ALERTS = os.path.join(ROOT, "observability", "alerts", "slo-alerts.yml")


def _load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


# --------------------------------------------------------------------------- #
# explain_dataset
# --------------------------------------------------------------------------- #

def explain_dataset(name: str) -> str:
    """Owner, grain, classification, PII, retention, SLA and BI access for one dataset."""
    reg = _load(REGISTRY)
    datasets = {d["name"]: d for d in reg["datasets"]}

    if name not in datasets:
        close = [n for n in datasets if name.lower() in n.lower()]
        return (f"'{name}' is not in the registry."
                + (f" Did you mean: {', '.join(sorted(close))}?" if close else
                   f" Known datasets: {', '.join(sorted(datasets))}"))

    d = datasets[name]
    domain = reg["domains"].get(d["domain"], {})
    defaults = reg.get("defaults", {})

    lines = [
        f"{name}  [{d['layer']}]",
        f"  domain         : {d['domain']} — {domain.get('description', '')}",
        f"  owner          : {domain.get('owner', defaults.get('owner'))}",
        f"  grain          : {d['grain']}",
        f"  primary key    : {', '.join(d['primary_key'])}",
        f"  classification : {d['classification']}",
        f"  retention      : {d['retention_days']} days",
        f"  BI access      : {d['bi_access']}",
    ]
    if d.get("freshness_sla_minutes"):
        lines.append(f"  freshness SLA  : {d['freshness_sla_minutes']} min")
    if d.get("pii_columns"):
        lines.append(f"  PII columns    : {', '.join(d['pii_columns'])}"
                     "  <- denied to BI (S14-1)")
    if d.get("masked_table"):
        lines.append(f"  masked copy    : {d['masked_table']}  <- what BI reads instead")
    if d.get("deletion_blocked_until"):
        lines.append(f"  deletion       : BLOCKED until {d['deletion_blocked_until']} — "
                     "retention here is a correctness setting, not storage")
    lines.append(f"\n  source: governance/catalog/domains.yml")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# find_lineage
# --------------------------------------------------------------------------- #

def find_lineage(dataset: str, direction: str = "both") -> str:
    """Upstream and downstream jobs for a dataset, from the lineage graph."""
    graph = _load(LINEAGE)["lineage_graph"]
    up = [j for j in graph if dataset in j["outputs"]]
    down = [j for j in graph if dataset in j["inputs"]]

    if not up and not down:
        known = sorted({x for j in graph for x in j["inputs"] + j["outputs"]})
        return (f"'{dataset}' does not appear in the lineage graph.\n"
                f"  known: {', '.join(known[:12])}...")

    out = [f"Lineage for {dataset}:"]
    if direction in ("both", "upstream"):
        out.append("  produced by:")
        for j in up:
            out.append(f"    {j['job']} ({j['type']}, evidence={j['evidence']})")
            out.append(f"      reads: {', '.join(j['inputs'])}")
        if not up:
            out.append("    (nothing — this is a source)")
    if direction in ("both", "downstream"):
        out.append("  consumed by:")
        for j in down:
            out.append(f"    {j['job']} -> {', '.join(j['outputs'])}")
        if not down:
            out.append("    (nothing — this is a sink)")

    # `declared` edges are asserted from config, not observed telemetry (S14-12). Saying so
    # is the difference between lineage and a diagram.
    if any(j["evidence"] == "declared" for j in up + down):
        out.append("\n  NOTE: some edges are DECLARED from configuration, not observed. "
                   "Debezium emits no OpenLineage.")
    out.append("  source: governance/lineage/openlineage.yml")
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# find_runbook / explain_failure
# --------------------------------------------------------------------------- #

def find_runbook(alert_or_symptom: str) -> str:
    """Map an alert name or symptom to its runbook section."""
    alerts = _load(ALERTS)
    body = open(RUNBOOK).read()

    # Search for the alert name INSIDE the question, not equality against it. A user pastes
    # "the KafkaConsumerLagGrowing alert is firing, what now" — an exact-match comparison
    # against the whole sentence never fires, and the fallback then answers something else
    # entirely, which is worse than saying "not found".
    needle = alert_or_symptom.lower()
    for group in alerts["groups"]:
        for rule in group["rules"]:
            if rule["alert"].lower() in needle or rule["alert"].lower() == needle:
                anchor = rule["annotations"]["runbook"].split("#", 1)[1]
                section = _runbook_section(body, anchor)
                return (f"Alert {rule['alert']} [severity={rule['labels']['severity']}, "
                        f"slo={rule['labels']['slo']}]\n\n{section}\n"
                        f"  source: docs/RUNBOOK.md#{anchor}")

    # Fall back to keyword match over section headings.
    terms = set(re.findall(r"[a-z]+", alert_or_symptom.lower()))
    best, best_score = None, 0
    for heading in re.findall(r"^## (.+)$", body, flags=re.M):
        score = len(terms & set(re.findall(r"[a-z]+", heading.lower())))
        if score > best_score:
            best, best_score = heading, score
    if best:
        return (f"No exact alert match. Closest runbook section: '{best}'\n\n"
                f"{_runbook_section(body, best)}\n  source: docs/RUNBOOK.md")
    return ("No runbook section matches. Available alerts: "
            + ", ".join(r["alert"] for g in alerts["groups"] for r in g["rules"]))


def _runbook_section(body: str, anchor: str) -> str:
    for heading, section in re.findall(r"^## (.+?)$\n(.*?)(?=^## |\Z)", body,
                                       flags=re.M | re.S):
        slug = re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")
        if anchor == heading or anchor in slug or slug in anchor:
            return section.strip()[:1600]
    return "(section not found)"


def suggest_recovery(symptom: str) -> str:
    """Recovery steps for a symptom, drawn from the runbook and DR table."""
    rb = find_runbook(symptom)
    return (f"{rb}\n\n"
            "  The general rule: every recovery in this pipeline is a RERUN, not a repair.\n"
            "  Every job is idempotent by (business_date, run_id).\n"
            "  RPO/RTO for each scenario: docs/DR.md §2")


# --------------------------------------------------------------------------- #
# generate_sql — guarded
# --------------------------------------------------------------------------- #

_TEMPLATES = {
    "count_by_date": "SELECT count(*) AS row_count\nFROM {table}\nWHERE business_date = DATE '{date}'",
    "sample": "SELECT *\nFROM {table}\nWHERE business_date = DATE '{date}'\nLIMIT 20",
    "daily_totals": ("SELECT business_date, count(*) AS rows, sum({measure}) AS total\n"
                     "FROM {table}\nWHERE business_date BETWEEN DATE '{date}' AND DATE '{date}'\n"
                     "GROUP BY business_date\nORDER BY business_date"),
}


def generate_sql(table: str, intent: str = "sample", date: str = "2026-08-14",
                 measure: str = "amount_base") -> str:
    """Emit a read-only Athena query from a TEMPLATE, then re-validate it.

    Templates rather than free-form generation: for a fixed set of analyst questions, a
    template cannot hallucinate a column or a join, and the guard then re-checks the result
    anyway. Free-form SQL generation is where this capability would earn its risk, and it
    has not been shown to be needed here.

    The guard runs on the OUTPUT regardless of how it was produced, so swapping a template
    for a model later does not weaken the boundary.
    """
    reg = _load(REGISTRY)
    if intent not in _TEMPLATES:
        raise GuardViolation(f"unknown intent {intent!r}; known: {sorted(_TEMPLATES)}")

    sql = _TEMPLATES[intent].format(table=table, date=date, measure=measure)
    sql = assert_read_only_sql(sql)
    sql = assert_allowed_tables(sql, reg)
    return (f"{sql}\n\n-- read-only; validated against governance/catalog/domains.yml\n"
            f"-- Athena bills per byte scanned: keep the partition predicate.")


# --------------------------------------------------------------------------- #
# Tool registry
# --------------------------------------------------------------------------- #

TOOLS = {
    "explain_dataset": explain_dataset,
    "find_lineage": find_lineage,
    "find_runbook": find_runbook,
    "suggest_recovery": suggest_recovery,
    "generate_sql": generate_sql,
}

# Named here so a test can assert the assistant never grows one.
WRITE_TOOLS: dict = {}


def call(tool: str, **kwargs) -> str:
    if tool not in TOOLS:
        raise GuardViolation(f"unknown tool {tool!r}")
    result = TOOLS[tool](**kwargs)
    assert_no_infrastructure_action(result)
    return redact(result)
