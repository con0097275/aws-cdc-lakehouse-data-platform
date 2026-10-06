"""Logistic regression in numpy, plus the artifact/metadata registry contract.

WHY NOT SCIKIT-LEARN
--------------------
It is not installed, and `CLAUDE.md` 3.9 requires every dependency to be pinned. Adding a
large ML stack to train 960 rows would be a real dependency decision -- a version matrix to
maintain and a supply-chain surface -- taken to avoid writing thirty lines. numpy and pandas
are already present and already in use.

If a later model genuinely needs scikit-learn, that is an ADR with a pinned version, not a
side effect of this pilot.

DETERMINISM
-----------
Full-batch gradient descent with a fixed seed, fixed iteration count and no shuffling. The
same inputs produce bit-identical weights, which is what makes "training is reproducible" a
testable claim rather than an aspiration.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone

import numpy as np


@dataclass
class LogisticRegression:
    learning_rate: float = 0.1
    iterations: int = 500
    l2: float = 0.01
    seed: int = 42
    weights: list[float] = field(default_factory=list)
    bias: float = 0.0
    feature_names: list[str] = field(default_factory=list)
    mean: list[float] = field(default_factory=list)
    scale: list[float] = field(default_factory=list)

    @staticmethod
    def _sigmoid(z):
        # Numerically stable: exp on large positive z overflows and silently yields nan
        # weights, which look like a training bug rather than an overflow.
        return np.where(z >= 0, 1.0 / (1.0 + np.exp(-np.clip(z, -500, 500))),
                        np.exp(np.clip(z, -500, 500)) / (1.0 + np.exp(np.clip(z, -500, 500))))

    def fit(self, X: np.ndarray, y: np.ndarray, feature_names: list[str]) -> "LogisticRegression":
        rng = np.random.default_rng(self.seed)
        X = np.asarray(X, dtype=float)
        y = np.asarray(y, dtype=float)

        # Standardise, and remember how: inference must apply the SAME transform or the
        # weights mean something different at scoring time than at training time.
        self.mean = X.mean(axis=0).tolist()
        scale = X.std(axis=0)
        scale[scale == 0] = 1.0          # a constant column carries no signal; do not /0
        self.scale = scale.tolist()
        Xs = (X - np.array(self.mean)) / np.array(self.scale)

        w = rng.normal(0.0, 0.01, size=Xs.shape[1])
        b = 0.0
        n = len(y)
        for _ in range(self.iterations):
            p = self._sigmoid(Xs @ w + b)
            err = p - y
            w -= self.learning_rate * ((Xs.T @ err) / n + self.l2 * w)
            b -= self.learning_rate * err.mean()
        self.weights, self.bias = w.tolist(), float(b)
        self.feature_names = list(feature_names)
        return self

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        Xs = (np.asarray(X, dtype=float) - np.array(self.mean)) / np.array(self.scale)
        return self._sigmoid(Xs @ np.array(self.weights) + self.bias)

    def predict(self, X: np.ndarray, threshold: float = 0.5) -> np.ndarray:
        return (self.predict_proba(X) >= threshold).astype(int)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LogisticRegression":
        m = cls(learning_rate=d["learning_rate"], iterations=d["iterations"],
                l2=d["l2"], seed=d["seed"])
        m.weights, m.bias = d["weights"], d["bias"]
        m.feature_names, m.mean, m.scale = d["feature_names"], d["mean"], d["scale"]
        return m


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #

def auc_roc(y: np.ndarray, p: np.ndarray) -> float:
    y = np.asarray(y); p = np.asarray(p)
    pos, neg = p[y == 1], p[y == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")          # undefined, not 0.5 -- say so rather than invent it
    order = np.argsort(np.concatenate([pos, neg]))
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, len(order) + 1)
    return float((ranks[:len(pos)].sum() - len(pos) * (len(pos) + 1) / 2) /
                 (len(pos) * len(neg)))


def metrics(y: np.ndarray, p: np.ndarray, threshold: float = 0.5) -> dict:
    y = np.asarray(y); yhat = (np.asarray(p) >= threshold).astype(int)
    tp = int(((y == 1) & (yhat == 1)).sum()); tn = int(((y == 0) & (yhat == 0)).sum())
    fp = int(((y == 0) & (yhat == 1)).sum()); fn = int(((y == 1) & (yhat == 0)).sum())
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return {
        "n": int(len(y)), "positives": int(y.sum()),
        "accuracy": round((tp + tn) / len(y), 4) if len(y) else 0.0,
        "precision": round(prec, 4), "recall": round(rec, 4),
        "f1": round(2 * prec * rec / (prec + rec), 4) if prec + rec else 0.0,
        "auc_roc": round(auc_roc(y, p), 4),
        "confusion": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
    }


# --------------------------------------------------------------------------- #
# Registry contract
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class ModelArtifact:
    """The minimal registry record. Everything needed to reproduce or audit a prediction."""
    model_name: str
    model_version: str
    dataset_version: str
    feature_versions: tuple[str, ...]
    code_version: str                 # git commit
    parameters: dict
    metrics: dict
    owner: str
    created_at: str
    synthetic_label: bool
    artifact: dict                    # the serialised model

    def to_json(self) -> str:
        d = asdict(self)
        d["feature_versions"] = list(self.feature_versions)
        return json.dumps(d, indent=2, sort_keys=True)

    @staticmethod
    def make_version(model_name: str, dataset_version: str, params: dict,
                     code_version: str) -> str:
        """Content-addressed: same data + same params + same code -> same version."""
        return "model:" + hashlib.sha256(json.dumps(
            {"m": model_name, "d": dataset_version, "p": params, "c": code_version},
            sort_keys=True).encode()).hexdigest()[:16]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
