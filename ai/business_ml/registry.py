"""Minimal model artifact + registry, content-addressed.

Follows the pattern already in this repo (`model:eb58e352986ba990`): the version IS a hash
of the definition, so an artifact cannot be silently edited and keep its identity. There is
no external registry service — none is approved, and a hosted registry for two unsupervised
scorers with no always-on inference would be infrastructure bought for its own sake (§9:
"Do not create always-on inference infrastructure by default").

An artifact records what it was built FROM: feature group version, training cutoff, code
version, and the population statistics the scores depend on. Without those a score cannot be
reproduced, and a score that cannot be reproduced cannot be defended.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_DIR = ROOT / "artifacts" / "models"


@dataclass
class ModelArtifact:
    model_name: str
    model_kind: str                     # unsupervised_anomaly | ranking
    feature_group_version: str
    features: list[str]
    hyperparameters: dict
    population_stats: dict              # median/sigma per feature — the fitted state
    training_cutoff: str
    training_rows: int
    code_version: str
    metrics: dict = field(default_factory=dict)
    baseline_metrics: dict = field(default_factory=dict)
    supervised: bool = False
    synthetic_label: bool = False
    owner: str = "risk-data"
    created_at: str = ""
    model_version: str = ""
    notes: list[str] = field(default_factory=list)

    def compute_version(self) -> str:
        payload = json.dumps({
            "name": self.model_name, "kind": self.model_kind,
            "fg": self.feature_group_version, "features": sorted(self.features),
            "hp": self.hyperparameters, "stats": self.population_stats,
            "cutoff": self.training_cutoff, "code": self.code_version}, sort_keys=True)
        return "model:" + hashlib.sha256(payload.encode()).hexdigest()[:16]

    def finalise(self) -> "ModelArtifact":
        self.created_at = self.created_at or datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        self.model_version = self.compute_version()
        return self

    def to_dict(self) -> dict:
        return asdict(self)


def save(artifact: ModelArtifact, root: Path | str | None = None) -> Path:
    a = artifact.finalise()
    d = Path(root or REGISTRY_DIR) / a.model_name / a.model_version.replace(":", "_")
    d.mkdir(parents=True, exist_ok=True)
    p = d / "model.json"
    p.write_text(json.dumps(a.to_dict(), indent=2, default=str))
    return p


def load(path: Path | str) -> ModelArtifact:
    raw = json.loads(Path(path).read_text())
    a = ModelArtifact(**raw)
    if a.model_version != a.compute_version():
        raise ValueError(
            f"{path}: recorded model_version {a.model_version} does not match the hash of "
            f"its own definition ({a.compute_version()}). The artifact was edited after "
            "it was written; it cannot be trusted to reproduce its scores.")
    return a


def latest(model_name: str, root: Path | str | None = None) -> ModelArtifact | None:
    d = Path(root or REGISTRY_DIR) / model_name
    if not d.exists():
        return None
    files = sorted(d.glob("*/model.json"), key=lambda p: p.stat().st_mtime)
    return load(files[-1]) if files else None
