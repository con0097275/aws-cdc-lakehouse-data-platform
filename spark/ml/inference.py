"""Batch inference. Framework-neutral, so the agent can call it without importing an agent.

NO ALWAYS-ON ENDPOINT. ADR-053/060: nothing here has a latency requirement batch cannot
meet, and an endpoint is an always-on cost against a bounded budget. `score_batch` is a
function; AI-P7 wraps it as a read-only tool.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from ml.model import LogisticRegression


class FeatureSchemaMismatch(ValueError):
    """The features offered do not match the features the model was trained on."""


class ModelVersionMismatch(ValueError):
    """A caller asked for a model version the artifact does not carry."""


@dataclass(frozen=True)
class LoadedModel:
    model_name: str
    model_version: str
    dataset_version: str
    feature_versions: tuple[str, ...]
    feature_names: tuple[str, ...]
    synthetic_label: bool
    model: LogisticRegression


def load_model(path: str | Path, *, expect_version: str | None = None) -> LoadedModel:
    d = json.loads(Path(path).read_text())
    if expect_version and d["model_version"] != expect_version:
        raise ModelVersionMismatch(
            f"artifact is {d['model_version']}, caller asked for {expect_version}. "
            "Scoring with the wrong version silently changes what a prediction means.")
    m = LogisticRegression.from_dict(d["artifact"])
    return LoadedModel(
        model_name=d["model_name"], model_version=d["model_version"],
        dataset_version=d["dataset_version"],
        feature_versions=tuple(d["feature_versions"]),
        feature_names=tuple(m.feature_names),
        synthetic_label=bool(d.get("synthetic_label", False)), model=m)


def score_batch(loaded: LoadedModel, features: pd.DataFrame, *,
                entity_column: str = "entity_id",
                threshold: float = 0.5) -> pd.DataFrame:
    """Score a frame. Every row carries the versions that produced it."""
    if entity_column not in features.columns:
        raise FeatureSchemaMismatch(f"no {entity_column!r} column")

    missing = [c for c in loaded.feature_names if c not in features.columns]
    if missing:
        # REFUSE rather than impute. A silently-imputed feature produces a confident score
        # from data the model never saw, and nothing downstream can tell the difference.
        raise FeatureSchemaMismatch(
            f"missing feature column(s) {missing}. Scoring with defaults would emit a "
            "confident prediction from data that was never supplied.")

    X = features[list(loaded.feature_names)]
    if X.isna().any().any():
        bad = X.columns[X.isna().any()].tolist()
        raise FeatureSchemaMismatch(
            f"null values in {bad}. A null feature is a missing feature.")

    p = loaded.model.predict_proba(X.to_numpy(float))
    return pd.DataFrame({
        entity_column: features[entity_column].values,
        "score": np.round(p, 6),
        "prediction": (p >= threshold).astype(int),
        "model_name": loaded.model_name,
        "model_version": loaded.model_version,
        "dataset_version": loaded.dataset_version,
        "synthetic_label": loaded.synthetic_label,
    })
