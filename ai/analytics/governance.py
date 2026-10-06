"""What should I check, and why — data governance triage.

PROPOSES, NEVER APPLIES. Every finding carries a `proposed_fix` that a human runs. The
copilot has no write tool (ADR-057) and this module adds none: it emits a command string,
which is inert until a person executes it deliberately. That boundary is what keeps the AI
from becoming a second, unreviewable source of truth about the lakehouse.

Checks are ordered by what would MISLEAD you fastest, not by how easy they are to compute:

  1. FRESHNESS      stale data answers today's question with last week's numbers, silently
  2. COMPLETENESS   a short partition looks like a business event
  3. WATERMARK      a job reporting SUCCEEDED while producing nothing is the worst case,
                    because every downstream gate believes it
  4. UNIQUENESS     duplicated keys inflate every SUM in the mart
  5. NULLS          a null key silently drops rows from joins
  6. RECONCILIATION layer-to-layer drift

Severity is evidence-based. A check that cannot run returns UNKNOWN and says why; it never
returns PASS by default, because a green board built from checks that did not execute is
worse than no board.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta

from .datasource import DataSource
from .registry import MetricSpec

SEVERITY = ("CRITICAL", "HIGH", "MEDIUM", "LOW", "PASS", "UNKNOWN")
_RANK = {s: i for i, s in enumerate(SEVERITY)}


@dataclass
class Finding:
    check: str
    severity: str
    dataset: str
    summary: str
    evidence: dict
    what_to_check: list[str] = field(default_factory=list)
    proposed_fix: str | None = None       # NEVER executed by this module
    runbook: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class GovernanceReview:
    dataset: str
    as_of: str
    findings: list[Finding]
    verdict: str
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["findings"] = [f.to_dict() for f in self.findings]
        return d


def _window(as_of: str, n: int) -> list[str]:
    y, m, dd = (int(x) for x in as_of.split("-"))
    b = date(y, m, dd)
    return [(b - timedelta(days=i)).isoformat() for i in range(n, 0, -1)]


def _sql_checks(spec: MetricSpec, as_of: str, runner) -> list[Finding]:
    """Checks that need SQL, not a series. Each is refused (UNKNOWN) rather than assumed
    PASS when it cannot run — a green board built from checks that did not execute is worse
    than no board.

    GRAIN UNIQUENESS is first because it is the only one that silently corrupts every other
    number: a duplicated business key inflates every SUM downstream, and the accuracy ladder
    cannot repair it. dbt asserts this at build time; asserting it again at read time catches
    a MERGE that went wrong after the test passed.
    """
    ds = spec.qualified()
    out: list[Finding] = []
    grain_cols = [c.strip() for c in spec.grain.replace("+", ",").split(",")
                  if c.strip() and c.strip().replace("_", "").isalnum()]
    if not grain_cols:
        grain_cols = [spec.date_column]

    def one(sql: str):
        rows = runner(sql)
        return (rows[0] if rows else {})

    # 1. grain uniqueness ------------------------------------------------------------
    try:
        cols = ", ".join(grain_cols)
        r = one(f"SELECT COUNT(*) AS dup FROM (SELECT {cols}, COUNT(*) c FROM {ds} "
                f"WHERE {spec.date_column} = DATE '{as_of}' GROUP BY {cols} HAVING COUNT(*) > 1)")
        dup = int(r.get("dup") or 0)
        out.append(Finding(
            "grain_uniqueness", "PASS" if dup == 0 else "CRITICAL", ds,
            f"{dup} duplicated key(s) on the declared grain ({', '.join(grain_cols)})",
            {"duplicate_keys": dup, "grain": grain_cols},
            [] if dup == 0 else
            ["a duplicated business key inflates every SUM built on this table",
             "the accuracy ladder cannot repair this — fix the MERGE key, then rebuild"],
            None if dup == 0 else
            f"bash scripts/run-flow.sh EOD --business-date {as_of} --execute",
            "docs/REPORTING_TARGET_ARCHITECTURE.md"))
    except Exception as e:                                        # noqa: BLE001
        out.append(Finding("grain_uniqueness", "UNKNOWN", ds,
                           f"could not run: {type(e).__name__}", {},
                           ["a check that did not execute is not a PASS"]))

    # 2. null rate on the grain + measure ---------------------------------------------
    try:
        measure_col = spec.measure.split("(")[-1].rstrip(")").strip()
        checked = [c for c in grain_cols + [measure_col]
                   if c.replace("_", "").isalnum()]
        parts = ", ".join(f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS n_{i}"
                          for i, c in enumerate(checked))
        r = one(f"SELECT COUNT(*) AS total, {parts} FROM {ds} "
                f"WHERE {spec.date_column} = DATE '{as_of}'")
        total = int(r.get("total") or 0)
        nulls = {checked[i]: int(r.get(f"n_{i}") or 0) for i in range(len(checked))}
        worst = max(nulls.values()) if nulls else 0
        rate = (worst / total) if total else 0.0
        sev = "PASS" if worst == 0 else ("CRITICAL" if rate > 0.01 else "MEDIUM")
        out.append(Finding(
            "null_rate", sev, ds,
            f"worst null count {worst} of {total} rows ({rate:.2%})",
            {"total_rows": total, "nulls": nulls},
            [] if worst == 0 else
            ["a NULL join key silently DROPS rows from every downstream join — the total "
             "shrinks and nothing errors"],
            None if worst == 0 else "inspect the source rows before trusting any aggregate"))
    except Exception as e:                                        # noqa: BLE001
        out.append(Finding("null_rate", "UNKNOWN", ds,
                           f"could not run: {type(e).__name__}", {}))
    return out


def _reconciliation(spec: MetricSpec, as_of: str, runner,
                    upstream: str | None) -> Finding | None:
    """Row counts across layers. Drift means the mart is not a faithful projection.

    Compared as a RATIO, not equality: a mart legitimately aggregates, so equal counts are
    not the expectation. A mart with MORE rows than its source is the signal — that is a
    fan-out, and it is how a join silently multiplies money.
    """
    if not upstream:
        return None
    try:
        a = runner(f"SELECT COUNT(*) AS c FROM {spec.qualified()} "
                   f"WHERE {spec.date_column} = DATE '{as_of}'")
        b = runner(f"SELECT COUNT(*) AS c FROM {upstream} "
                   f"WHERE {spec.date_column} = DATE '{as_of}'")
        m = int((a[0] if a else {}).get("c") or 0)
        u = int((b[0] if b else {}).get("c") or 0)
        if u == 0:
            return Finding("reconciliation", "UNKNOWN", spec.qualified(),
                           f"upstream {upstream} has no rows for {as_of}", {"mart": m})
        ratio = m / u
        sev = "PASS" if ratio <= 1.0 else "CRITICAL"
        return Finding(
            "reconciliation", sev, spec.qualified(),
            f"mart {m} rows vs upstream {u} ({ratio:.2f}x)",
            {"mart_rows": m, "upstream_rows": u, "ratio": round(ratio, 4),
             "upstream": upstream},
            [] if sev == "PASS" else
            ["the mart has MORE rows than its source — a join is fanning out and every "
             "amount is being multiplied"],
            None if sev == "PASS" else
            f"bash scripts/run-flow.sh EOD --business-date {as_of} --execute")
    except Exception as e:                                        # noqa: BLE001
        return Finding("reconciliation", "UNKNOWN", spec.qualified(),
                       f"could not run: {type(e).__name__}", {})


def review(spec: MetricSpec, source: DataSource, *, as_of: str, lookback: int = 7,
           watermark: str | None = None, max_staleness_days: int = 1,
           sql_runner=None, upstream: str | None = None) -> GovernanceReview:
    win = _window(as_of, lookback)
    start = win[0]
    findings: list[Finding] = []

    counts = source.row_counts(spec, start, as_of)
    ds = spec.qualified()

    # 1. freshness -----------------------------------------------------------------
    present = sorted(d for d, n in counts.items() if n)
    latest = present[-1] if present else None
    if latest is None:
        findings.append(Finding(
            "freshness", "CRITICAL", ds, f"no data at all in {start}..{as_of}",
            {"window": [start, as_of], "dates_present": 0},
            ["is the EOD job scheduled and running?", "did the Glue table lose its partitions?"],
            "bash scripts/run-flow.sh EOD --execute",
            "docs/runbooks/rebuild-from-scratch.md"))
    else:
        lag = (date(*map(int, as_of.split("-"))) - date(*map(int, latest.split("-")))).days
        sev = "PASS" if lag <= max_staleness_days else ("HIGH" if lag <= 3 else "CRITICAL")
        findings.append(Finding(
            "freshness", sev, ds,
            f"latest partition is {latest} ({lag}d behind {as_of})",
            {"latest": latest, "lag_days": lag, "max_allowed": max_staleness_days},
            [] if sev == "PASS" else
            ["check the EOD job for the missing dates",
             "a stale table answers today's question with old numbers and looks healthy"],
            None if sev == "PASS" else f"bash scripts/run-flow.sh EOD --business-date {as_of} --execute",
            "docs/VERIFY_END_TO_END.md"))

    # 2. completeness --------------------------------------------------------------
    hist = [counts[d] for d in win if counts.get(d)]
    typical = sum(hist) / len(hist) if hist else 0
    today = counts.get(as_of, 0)
    ratio = (today / typical) if typical else None
    if ratio is None:
        findings.append(Finding("completeness", "UNKNOWN", ds,
                                "no baseline days to compare against",
                                {"rows_today": today, "baseline_days": 0},
                                ["load more history before trusting a completeness verdict"]))
    else:
        sev = "PASS" if ratio >= 0.7 else ("HIGH" if ratio >= 0.4 else "CRITICAL")
        findings.append(Finding(
            "completeness", sev, ds,
            f"{today} rows vs typical {typical:.0f} ({ratio:.0%})",
            {"rows_today": today, "typical": round(typical, 1), "ratio": round(ratio, 3)},
            [] if sev == "PASS" else
            ["a short partition looks exactly like a business drop — rule this out FIRST",
             "check the source connector and the EOD job for this date"],
            None if sev == "PASS" else
            f"bash scripts/cdc-runtime.sh status && bash scripts/run-flow.sh EOD "
            f"--business-date {as_of} --execute",
            "docs/runbooks/ai-platform-operations.md"))

    # 3. watermark vs reality ------------------------------------------------------
    if watermark:
        claimed_ok = watermark >= as_of
        produced = counts.get(as_of, 0) > 0
        if claimed_ok and not produced:
            findings.append(Finding(
                "watermark_vs_reality", "CRITICAL", ds,
                f"watermark claims {watermark} but {as_of} has no rows",
                {"watermark": watermark, "rows": 0},
                ["the job reported success and produced nothing",
                 "every downstream gate believes this watermark — treat as an incident"],
                "bash scripts/run-flow.sh EOD --business-date " + as_of + " --execute",
                "docs/VERIFY_END_TO_END.md"))
        else:
            findings.append(Finding(
                "watermark_vs_reality", "PASS", ds,
                f"watermark {watermark} is consistent with the data present",
                {"watermark": watermark, "rows_on_as_of": counts.get(as_of, 0)}))
    else:
        findings.append(Finding("watermark_vs_reality", "UNKNOWN", ds,
                                "no watermark supplied, so the claim could not be checked",
                                {},
                                ["pass the job watermark to check SUCCEEDED-but-empty"]))

    # 4. volume stability ----------------------------------------------------------
    if len(hist) >= 3:
        mean = sum(hist) / len(hist)
        var = sum((h - mean) ** 2 for h in hist) / len(hist)
        sd = var ** 0.5
        if sd == 0:
            # A PERFECTLY STABLE baseline makes the z-score undefined, and returning 0.0
            # marks the check PASS exactly when the baseline is most trustworthy: every
            # prior day was identical, so any deviation is meaningful. Falling back to a
            # relative comparison keeps the check alive instead of silently blind.
            rel = abs(today - mean) / mean if mean else 0.0
            sev = "PASS" if rel < 0.01 else ("MEDIUM" if rel < 0.5 else "HIGH")
            findings.append(Finding(
                "volume_stability", sev, ds,
                f"baseline is perfectly flat at {mean:.0f}; today is {today} "
                f"({rel:.0%} away)",
                {"today": today, "mean": round(mean, 1), "sd": 0.0,
                 "rel_change": round(rel, 3), "note": "z-score undefined; used relative"},
                [] if sev == "PASS" else
                ["every baseline day was identical, so this deviation is not noise"]))
        else:
            z = abs(today - mean) / sd
            sev = "PASS" if z < 3 else "MEDIUM"
            findings.append(Finding(
                "volume_stability", sev, ds,
                f"row count z-score {z:.1f} against the {len(hist)}-day baseline",
                {"today": today, "mean": round(mean, 1), "sd": round(sd, 2), "z": round(z, 2)},
                [] if sev == "PASS" else
                ["volume moved beyond normal variation; confirm whether the source changed"]))

    if sql_runner is not None:
        findings += _sql_checks(spec, as_of, sql_runner)
        rec = _reconciliation(spec, as_of, sql_runner, upstream)
        if rec:
            findings.append(rec)

    order = sorted(findings, key=lambda f: (_RANK[f.severity], f.check))
    worst = order[0].severity if order else "UNKNOWN"
    verdict = ("INVESTIGATE" if worst in ("CRITICAL", "HIGH") else
               "REVIEW" if worst in ("MEDIUM", "LOW") else
               "INCOMPLETE" if worst == "UNKNOWN" else "HEALTHY")
    return GovernanceReview(
        ds, as_of, order, verdict,
        notes=["proposed fixes are NOT executed — this module has no write path (ADR-057)"])
