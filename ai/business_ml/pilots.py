"""Two pilots, both UNSUPERVISED — and that is a decision, not a limitation.

BAI-P0 found four business dates and no honest label. A churn or attrition model here would
need a fabricated target, which is exactly what produced the existing
`account_balance_tier_next_day` artifact: AUC-ROC 1.0, `synthetic_label: true`, and no
predictive meaning. BAI-P5 §7 says prefer unsupervised anomaly over fabricated labels, so:

  PILOT A  account behavioural anomaly  — robust per-feature deviation, explainable
  PILOT B  next-best-investigation      — ranks accounts by how much they account for a
                                          KPI movement AND how unusual they look

Both are INTERPRETABLE BY CONSTRUCTION. Every score decomposes into named per-feature
contributions, so §11 needs no post-hoc explainer and the model never has to be asked why it
said something.

Robust statistics (median / MAD) throughout, not mean / stddev: with a handful of accounts
one extreme balance moves the mean enough to hide itself, which is precisely the account an
anomaly model exists to surface.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

MODEL_A = "model:account_anomaly_v1"
MODEL_B = "model:next_best_investigation_v1"

#: Features the anomaly pilot scores. Chosen because all four are available with ONE day of
#: history, which is what this lakehouse actually has.
ANOMALY_FEATURES = ("balance_close", "net_flow_1d", "txn_count_1d", "avg_txn_value_1d")

MAD_TO_SIGMA = 1.4826          # makes MAD comparable to a standard deviation for normal data


def _median(v: list[float]) -> float | None:
    x = sorted(a for a in v if a is not None)
    if not x:
        return None
    m = len(x) // 2
    return x[m] if len(x) % 2 else (x[m - 1] + x[m]) / 2


def _mad(v: list[float], med: float) -> float | None:
    x = [abs(a - med) for a in v if a is not None]
    return _median(x) if x else None


@dataclass
class FeatureContribution:
    feature_id: str
    value: float | None
    population_median: float | None
    robust_z: float | None
    contribution: float | None       # share of the total score

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AnomalyScore:
    entity: str
    entity_key: str
    score: float | None
    severity: str
    model_version: str
    feature_group_version: str
    feature_event_time: str
    contributions: list[FeatureContribution] = field(default_factory=list)
    refused: str | None = None
    notes: list[str] = field(default_factory=list)
    interpretation: str = (
        "A statistical distance from the population, decomposed per feature. It is not a "
        "diagnosis, not a risk rating, and not evidence of wrongdoing.")

    def to_dict(self) -> dict:
        d = asdict(self)
        d["contributions"] = [c.to_dict() for c in self.contributions]
        return d


def _severity(score: float | None) -> str:
    if score is None:
        return "UNKNOWN"
    for t, s in ((4.0, "EXTREME"), (3.0, "HIGH"), (2.0, "MODERATE")):
        if score >= t:
            return s
    return "NORMAL"


MIN_POPULATION = 5


def score_anomalies(feature_rows, *, features: tuple[str, ...] = ANOMALY_FEATURES,
                    min_population: int = MIN_POPULATION) -> list[AnomalyScore]:
    """Robust per-feature deviation, aggregated to one score per entity.

    Refuses below `min_population`: a median and MAD over three accounts describe the three
    accounts, not a population, and every one of them would look normal.
    """
    if not feature_rows:
        return []
    n = len(feature_rows)
    fg = feature_rows[0].feature_group_version
    fet = feature_rows[0].feature_event_time

    if n < min_population:
        return [AnomalyScore(r.account_sk, "account_sk", None, "UNKNOWN", MODEL_A, fg, fet,
                             refused=f"{n} entities; at least {min_population} are needed "
                                     "for a population baseline",
                             notes=["A median and MAD over a handful of entities describe "
                                    "those entities, not a population."])
                for r in feature_rows]

    stats: dict[str, tuple[float, float]] = {}
    usable: list[str] = []
    for f in features:
        vals = [r.features.get(f) for r in feature_rows]
        vals = [v for v in vals if v is not None]
        if len(vals) < min_population:
            continue
        med = _median(vals)
        mad = _mad(vals, med)
        if not mad:
            continue                       # zero spread: every value identical
        stats[f] = (med, mad * MAD_TO_SIGMA)
        usable.append(f)

    out: list[AnomalyScore] = []
    for r in feature_rows:
        if not usable:
            out.append(AnomalyScore(r.account_sk, "account_sk", None, "UNKNOWN", MODEL_A,
                                    fg, fet,
                                    refused="no feature had usable spread across the "
                                            "population"))
            continue
        contribs: list[FeatureContribution] = []
        for f in usable:
            med, sig = stats[f]
            v = r.features.get(f)
            z = abs(v - med) / sig if v is not None else None
            contribs.append(FeatureContribution(f, v, med, z, None))
        zs = [c.robust_z for c in contribs if c.robust_z is not None]
        score = max(zs) if zs else None    # max, not mean: one extreme feature IS the signal
        total = sum(zs) or 1.0
        for c in contribs:
            c.contribution = (c.robust_z / total) if c.robust_z is not None else None
        contribs.sort(key=lambda c: (c.robust_z or -1), reverse=True)
        out.append(AnomalyScore(r.account_sk, "account_sk",
                                (round(score, 4) if score is not None else None),
                                _severity(score), MODEL_A, fg, fet, contribs,
                                notes=[f"scored on {len(usable)} of {len(features)} "
                                       f"features with usable spread"]))
    out.sort(key=lambda s: (s.score is not None, s.score or 0), reverse=True)
    return out


@dataclass
class Investigation:
    entity: str
    rank: int
    combined_score: float
    contribution_share: float | None
    anomaly_score: float | None
    anomaly_severity: str
    reason: str
    model_version: str = MODEL_B

    def to_dict(self) -> dict:
        return asdict(self)


def rank_investigations(driver_contributions: list[dict],
                        anomalies: list[AnomalyScore], *, top_k: int = 5,
                        weight_contribution: float = 0.6) -> list[Investigation]:
    """Which accounts are worth a human's next hour?

    Combines two INDEPENDENT signals: how much of the KPI movement an account accounts for,
    and how unusual it looks against its peers. Either alone misleads — a large account
    always dominates a movement without being unusual, and an odd small account moves
    nothing. The weight is explicit rather than learned, because there is no label to learn
    it from and a fitted weight here would be a fabricated one.
    """
    amap = {a.entity: a for a in anomalies}
    zs = [a.score for a in anomalies if a.score is not None]
    zmax = max(zs) if zs else None

    rows: list[Investigation] = []
    for d in driver_contributions:
        ent = str(d.get("segment"))
        share = d.get("share_of_delta")
        a = amap.get(ent)
        norm_share = abs(share) if share is not None else 0.0
        norm_anom = ((a.score / zmax) if (a and a.score is not None and zmax) else 0.0)
        combined = (weight_contribution * norm_share
                    + (1 - weight_contribution) * norm_anom)
        bits = []
        if share is not None:
            bits.append(f"contributed {share:.1%} of the observed change")
        if a and a.score is not None:
            top = a.contributions[0].feature_id if a.contributions else "n/a"
            bits.append(f"anomaly {a.severity.lower()} (driven by {top})")
        elif a and a.refused:
            bits.append("anomaly not scored")
        rows.append(Investigation(ent, 0, round(combined, 4), share,
                                  (a.score if a else None),
                                  (a.severity if a else "UNKNOWN"),
                                  "; ".join(bits) or "no signal"))
    rows.sort(key=lambda r: r.combined_score, reverse=True)
    for i, r in enumerate(rows[:top_k], 1):
        r.rank = i
    return rows[:top_k]
