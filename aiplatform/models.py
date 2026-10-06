"""Model and training definitions. Batch inference only; no always-on endpoint.

ADR-053/060: nothing in this project needs sub-second inference, so a real-time endpoint
would be an always-on cost against a bounded budget for a latency nobody asked for. The
contract therefore refuses to express one, rather than allowing it and warning.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, asdict
from enum import Enum

from .classification import CostClass
from .knowledge import ContractViolation
from .versioning import typed_version

_NAME = re.compile(r"[a-z][a-z0-9_]{2,63}")


class InferenceMode(str, Enum):
    BATCH    = "batch"
    REALTIME = "realtime"   # FORBIDDEN without an ADR + cost review


class SplitStrategy(str, Enum):
    TIME_BASED = "time_based"
    RANDOM     = "random"    # FORBIDDEN for time-series features


@dataclass(frozen=True)
class TrainingDataset:
    """A PIT training set: labels joined to features knowable at each label's event time."""
    name: str
    label_name: str
    label_event_time_column: str
    feature_groups: tuple[str, ...]
    train_start: str
    train_end: str
    label_horizon_days: int = 0      # >0 means a FORWARD-looking label
    split: SplitStrategy = SplitStrategy.TIME_BASED

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ContractViolation(f"dataset {self.name!r} must be lower_snake_case")
        if not self.feature_groups:
            raise ContractViolation(f"{self.name}: no feature groups")
        if self.split is SplitStrategy.RANDOM:
            raise ContractViolation(
                f"{self.name}: a random split over time-series features leaks by "
                "construction — rows after the cutoff land in train. Use TIME_BASED, or "
                "every other control here is decoration")
        if self.label_horizon_days < 0:
            raise ContractViolation(f"{self.name}: label_horizon_days must be >= 0")

    @property
    def dataset_version(self) -> str:
        return typed_version("dataset", asdict(self) | {"split": self.split.value})


@dataclass(frozen=True)
class ModelDefinition:
    name: str
    description: str
    owner: str
    dataset: TrainingDataset
    algorithm: str
    inference_mode: InferenceMode = InferenceMode.BATCH
    seed: int = 42
    metrics: tuple[str, ...] = ("auc_roc", "auc_pr")
    model_version: int = 1
    cost_class: CostClass = CostClass.PER_JOB

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ContractViolation(f"model {self.name!r} must be lower_snake_case")
        if self.inference_mode is InferenceMode.REALTIME:
            raise ContractViolation(
                f"{self.name}: realtime inference implies an always-on endpoint, which is "
                "forbidden without a written cost review (ADR-060). Nothing in this project "
                "has a latency requirement that batch cannot meet")
        if self.cost_class is CostClass.ALWAYS_ON:
            raise ContractViolation(f"{self.name}: no AI component may be always-on")
        if not self.metrics:
            raise ContractViolation(
                f"{self.name}: a model with no declared metrics cannot be evaluated or "
                "compared to its predecessor")

    @property
    def version_id(self) -> str:
        return typed_version("model", {
            "model": self.name, "v": self.model_version, "algo": self.algorithm,
            "seed": self.seed, "dataset": self.dataset.dataset_version})
