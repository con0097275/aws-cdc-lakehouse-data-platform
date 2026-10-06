"""Batch inference. No always-on endpoint (§9/§10).

Two pilots run over a day's feature rows and emit structured, versioned predictions. Both
carry their own explanation because both are interpretable by construction — nothing here
asks a language model why a score was produced (§11). A model may LATER be asked to phrase a
deterministic explanation in business language; it is never asked to supply one.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from .features import FeatureRow, load_group
from .pilots import (ANOMALY_FEATURES, MODEL_A, AnomalyScore, rank_investigations,
                     score_anomalies)
from .registry import ModelArtifact


class FeatureSchemaError(ValueError):
    pass


@dataclass
class Prediction:
    entity: str
    entity_key: str
    prediction_time: str
    score: float | None
    prediction: str
    model_version: str
    feature_group_version: str
    feature_event_time: str
    explanation: list[dict] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def _check_schema(rows: list[FeatureRow], expected: list[str]) -> list[str]:
    """Missing features and schema drift are REPORTED, never silently imputed.

    Filling a missing feature with a median makes the row scoreable and the score wrong; the
    caller cannot see it happened.
    """
    problems: list[str] = []
    if not rows:
        return ["no feature rows supplied"]
    present = set(rows[0].features)
    missing = [f for f in expected if f not in present]
    if missing:
        problems.append(f"features missing from the rows: {missing}")
    all_null = [f for f in expected if f in present
                and all(r.features.get(f) is None for r in rows)]
    if all_null:
        problems.append(f"features present but entirely NULL: {all_null}")
    return problems


def run_anomaly_batch(rows: list[FeatureRow], artifact: ModelArtifact | None = None, *,
                      features: tuple[str, ...] = ANOMALY_FEATURES) -> list[Prediction]:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    problems = _check_schema(rows, list(features))
    if artifact and artifact.feature_group_version and rows:
        if artifact.feature_group_version != rows[0].feature_group_version:
            raise FeatureSchemaError(
                f"model was trained on feature group {artifact.feature_group_version} but "
                f"the rows are {rows[0].feature_group_version}. Scoring across a feature "
                "definition change produces numbers that are not comparable to anything.")
    scores: list[AnomalyScore] = score_anomalies(rows, features=features)
    out: list[Prediction] = []
    for s in scores:
        out.append(Prediction(
            entity=s.entity, entity_key=s.entity_key, prediction_time=now, score=s.score,
            prediction=s.severity,
            model_version=(artifact.model_version if artifact else MODEL_A),
            feature_group_version=s.feature_group_version,
            feature_event_time=s.feature_event_time,
            explanation=[c.to_dict() for c in s.contributions[:3]],
            notes=list(s.notes) + ([s.refused] if s.refused else []) + problems))
    return out


def run_investigation_batch(driver_contributions: list[dict], rows: list[FeatureRow], *,
                            top_k: int = 5) -> list[dict]:
    scores = score_anomalies(rows)
    return [i.to_dict() for i in rank_investigations(driver_contributions, scores,
                                                     top_k=top_k)]


def fit_artifact(rows: list[FeatureRow], *, training_cutoff: str,
                 features: tuple[str, ...] = ANOMALY_FEATURES) -> ModelArtifact:
    """"Training" here is computing the population median and MAD per feature.

    Calling that a fit is honest: the model IS those statistics, and recording them is what
    makes a score reproducible later. There is no gradient, no label and no held-out set,
    because there is no label to hold out.
    """
    from .pilots import MAD_TO_SIGMA, _mad, _median
    stats: dict = {}
    for f in features:
        vals = [r.features.get(f) for r in rows]
        vals = [v for v in vals if v is not None]
        if len(vals) < 2:
            continue
        med = _median(vals)
        mad = _mad(vals, med)
        if mad:
            stats[f] = {"median": med, "sigma": mad * MAD_TO_SIGMA, "n": len(vals)}
    g = load_group()
    return ModelArtifact(
        model_name="account_anomaly", model_kind="unsupervised_anomaly",
        feature_group_version=(rows[0].feature_group_version if rows else g.version_hash()),
        features=list(features),
        hyperparameters={"statistic": "median/MAD", "mad_to_sigma": MAD_TO_SIGMA,
                         "aggregation": "max robust z across features"},
        population_stats=stats, training_cutoff=training_cutoff, training_rows=len(rows),
        code_version="ml_code:v1", supervised=False, synthetic_label=False,
        notes=["Unsupervised by choice: no honest label exists in four business dates "
               "(BAI-P0). A supervised pilot here would need a fabricated target."]
    ).finalise()
