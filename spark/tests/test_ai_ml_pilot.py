"""ML pilot tests (AI-P6). Fixture-driven; no AWS, no network."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark"))

from ml.dataset import (FEATURE_COLUMNS, LABEL, LABEL_TIME, FEATURE_TIME, DatasetSpec,
                        InsufficientData, assert_no_future_leakage,
                        assert_no_horizon_leakage, build, build_features, build_labels,
                        point_in_time_join, time_split)
from ml.inference import (FeatureSchemaMismatch, LoadedModel, ModelVersionMismatch,
                          load_model, score_batch)
from ml.model import LogisticRegression, ModelArtifact, auc_roc, metrics

SPEC = DatasetSpec("pilot", "y", FEATURE_COLUMNS, 1, "2026-08-22", "curated.snap")


def _snap(dates=("2026-08-17", "2026-08-18", "2026-08-19"), n=8, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    base = {a: float(rng.integers(1_000_000, 9_000_000)) for a in range(n)}
    for d in dates:
        for a in range(n):
            rows.append({"account_sk": a, "customer_sk": a, "business_date": d,
                         "closing_balance": base[a] + rng.normal(0, 1000),
                         "debit_amount": 0.0, "credit_amount": 0.0, "txn_count": 0})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- dataset / PIT
class TestTrainingDataset:
    def test_dataset_builds_and_is_versioned(self):
        ds, v = build(_snap(), SPEC)
        assert len(ds) and v.startswith("dataset:")

    def test_dataset_version_is_deterministic(self):
        assert build(_snap(), SPEC)[1] == build(_snap(), SPEC)[1]

    def test_dataset_version_changes_with_the_data(self):
        assert build(_snap(seed=0), SPEC)[1] != build(_snap(seed=7), SPEC)[1]

    def test_labels_are_forward_looking_by_one_day(self):
        lab = build_labels(_snap())
        assert set(lab[LABEL].unique()) <= {0, 1}
        # the LAST date can carry no label -- there is nothing after it to look into
        assert lab[LABEL_TIME].max() < pd.Timestamp("2026-08-19")

    def test_single_date_cannot_produce_a_forward_label(self):
        with pytest.raises(InsufficientData):
            build_labels(_snap(dates=("2026-08-17",)))

    def test_point_in_time_never_selects_a_future_feature(self):
        ds, _ = build(_snap(), SPEC)
        assert (ds[FEATURE_TIME] <= ds[LABEL_TIME]).all()

    def test_leakage_guard_catches_a_future_feature(self):
        bad = pd.DataFrame({LABEL_TIME: [pd.Timestamp("2026-08-17")],
                            FEATURE_TIME: [pd.Timestamp("2026-08-18")]})
        with pytest.raises(ValueError):
            assert_no_future_leakage(bad)

    def test_horizon_guard_catches_a_feature_from_the_outcome_window(self):
        df = pd.DataFrame({LABEL_TIME: [pd.Timestamp("2026-08-17")],
                           FEATURE_TIME: [pd.Timestamp("2026-08-17 12:00")]})
        assert_no_horizon_leakage(df, 0)          # no horizon -> no-op
        with pytest.raises(ValueError):
            assert_no_horizon_leakage(df, 1)

    def test_single_class_label_is_refused(self):
        """Training on one class produces a model that predicts the majority and scores
        perfectly. That is not a model."""
        flat = _snap()
        flat["closing_balance"] = 1_000_000.0
        with pytest.raises(InsufficientData, match="single class"):
            build(flat, SPEC)

    def test_split_is_time_based_not_random(self):
        ds, _ = build(_snap(dates=("2026-08-17", "2026-08-18", "2026-08-19", "2026-08-20")),
                      SPEC)
        tr, te = time_split(ds, 1)
        assert tr[LABEL_TIME].max() < te[LABEL_TIME].min()

    def test_split_refuses_when_there_is_nothing_left_to_train_on(self):
        ds, _ = build(_snap(dates=("2026-08-17", "2026-08-18")), SPEC)
        with pytest.raises(InsufficientData):
            time_split(ds, holdout_last_n_dates=1)


# ------------------------------------------------------------------ model
class TestModel:
    def _xy(self):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(200, 3))
        y = (X[:, 0] + 0.3 * rng.normal(size=200) > 0).astype(int)
        return X, y

    def test_training_is_reproducible(self):
        X, y = self._xy()
        a = LogisticRegression(seed=42).fit(X, y, ["a", "b", "c"])
        b = LogisticRegression(seed=42).fit(X, y, ["a", "b", "c"])
        assert a.weights == b.weights and a.bias == b.bias

    def test_a_different_seed_gives_different_weights(self):
        X, y = self._xy()
        assert LogisticRegression(seed=1).fit(X, y, ["a", "b", "c"]).weights != \
               LogisticRegression(seed=2).fit(X, y, ["a", "b", "c"]).weights

    def test_model_learns_a_separable_signal(self):
        X, y = self._xy()
        m = LogisticRegression().fit(X, y, ["a", "b", "c"])
        assert auc_roc(y, m.predict_proba(X)) > 0.9

    def test_constant_column_does_not_divide_by_zero(self):
        X = np.column_stack([np.random.default_rng(0).normal(size=50), np.ones(50)])
        y = (X[:, 0] > 0).astype(int)
        m = LogisticRegression().fit(X, y, ["a", "const"])
        assert np.isfinite(m.predict_proba(X)).all()

    def test_serialization_round_trip_preserves_predictions(self):
        X, y = self._xy()
        m = LogisticRegression().fit(X, y, ["a", "b", "c"])
        back = LogisticRegression.from_dict(m.to_dict())
        assert np.allclose(m.predict_proba(X), back.predict_proba(X))

    def test_auc_is_undefined_not_invented_for_a_single_class(self):
        assert np.isnan(auc_roc(np.ones(10), np.random.default_rng(0).random(10)))

    def test_metrics_report_the_confusion_matrix(self):
        m = metrics(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
        assert m["confusion"] == {"tp": 2, "fp": 0, "tn": 2, "fn": 0}

    def test_model_version_is_content_addressed(self):
        a = ModelArtifact.make_version("m", "dataset:1", {"lr": 0.1}, "abc")
        assert a == ModelArtifact.make_version("m", "dataset:1", {"lr": 0.1}, "abc")
        assert a != ModelArtifact.make_version("m", "dataset:2", {"lr": 0.1}, "abc")


# -------------------------------------------------------------- registry / inference
class TestArtifactAndInference:
    @pytest.fixture()
    def artifact(self, tmp_path):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(80, 4)); y = (X[:, 0] > 0).astype(int)
        m = LogisticRegression().fit(X, y, list(FEATURE_COLUMNS))
        art = ModelArtifact("pilot", "model:abc123", "dataset:xyz", ("feature_group:v1",),
                            "commit1", {"lr": 0.1}, {"test": {"auc_roc": 0.9}}, "risk-data",
                            "2026-08-26T00:00:00+00:00", True, m.to_dict())
        p = tmp_path / "model.json"; p.write_text(art.to_json())
        return p

    def test_artifact_loads(self, artifact):
        lm = load_model(artifact)
        assert lm.model_version == "model:abc123" and lm.synthetic_label is True
        assert lm.feature_names == FEATURE_COLUMNS

    def test_artifact_records_every_version_needed_to_audit_a_prediction(self, artifact):
        d = json.loads(artifact.read_text())
        for k in ("model_version", "dataset_version", "feature_versions", "code_version",
                  "parameters", "metrics", "owner", "created_at", "synthetic_label"):
            assert k in d, k

    def test_version_mismatch_is_refused(self, artifact):
        with pytest.raises(ModelVersionMismatch):
            load_model(artifact, expect_version="model:different")

    def test_batch_inference_scores_and_stamps_versions(self, artifact):
        lm = load_model(artifact)
        f = pd.DataFrame({"entity_id": ["a", "b"],
                          **{c: [1.0, -1.0] for c in FEATURE_COLUMNS}})
        out = score_batch(lm, f)
        assert len(out) == 2
        assert (out["model_version"] == "model:abc123").all()
        assert (out["dataset_version"] == "dataset:xyz").all()
        assert out["prediction"].isin([0, 1]).all()

    def test_missing_feature_is_refused_not_imputed(self, artifact):
        """Imputing produces a confident score from data never supplied, and nothing
        downstream can tell the difference."""
        lm = load_model(artifact)
        f = pd.DataFrame({"entity_id": ["a"], "closing_balance": [1.0]})
        with pytest.raises(FeatureSchemaMismatch, match="missing feature"):
            score_batch(lm, f)

    def test_null_feature_is_refused(self, artifact):
        lm = load_model(artifact)
        f = pd.DataFrame({"entity_id": ["a"],
                          **{c: [None if c == "txn_count" else 1.0]
                             for c in FEATURE_COLUMNS}})
        with pytest.raises(FeatureSchemaMismatch, match="null"):
            score_batch(lm, f)

    def test_missing_entity_column_is_refused(self, artifact):
        lm = load_model(artifact)
        f = pd.DataFrame({c: [1.0] for c in FEATURE_COLUMNS})
        with pytest.raises(FeatureSchemaMismatch):
            score_batch(lm, f)

    def test_inference_is_deterministic(self, artifact):
        lm = load_model(artifact)
        f = pd.DataFrame({"entity_id": ["a"], **{c: [0.5] for c in FEATURE_COLUMNS}})
        assert score_batch(lm, f)["score"][0] == score_batch(lm, f)["score"][0]
