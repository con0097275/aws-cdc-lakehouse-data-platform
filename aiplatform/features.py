"""Feature contracts, and the point-in-time rule that keeps training sets honest.

THE ONE INVARIANT THIS MODULE EXISTS FOR
----------------------------------------
    feature_event_time <= label_event_time

`feature_event_time` is EVENT time — when the fact became true in the world.
`created_at` is PROCESSING time — when this pipeline happened to write it.

They are different, they diverge under backfill and late arrival, and joining on the wrong
one leaks the future into training. A model trained on leaked features scores beautifully
and is worthless, which is the failure mode that survives longest because nothing looks
broken.

`<=` and not `<`: a feature stamped exactly at T was knowable at T.

This is not a new idea in this repository. `CLAUDE.md` section 5.6 already fixes the same
shape for EOD — an explicit cutoff, dedup by key, last event by `event_order`. The PIT join
is that rule applied per label row, so the semantics are REUSED rather than reinvented.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Iterable, Sequence

from .classification import Classification, CostClass
from .knowledge import ContractViolation
from .versioning import typed_version

FEATURE_TYPES = ("bigint", "int", "double", "decimal", "string", "boolean",
                 "timestamp", "date")

#: Columns every offline feature row carries (ADR-052/054). Ordered as written.
FEATURE_ROW_COLUMNS = (
    "entity_id",           # the join key
    "feature_event_time",  # EVENT time — the only legal PIT join key
    "feature_version",     # which definition produced this value
    # ... feature columns ...
    "source_cob_date",     # the EOD cutoff this row was derived under
    "source_watermark",    # what had arrived when it was computed
    "job_run_id",          # the run that produced it
    "created_at",          # PROCESSING time — audit only, never a join key
)

#: Never legal as a point-in-time join key. Enforced, not documented.
PROCESSING_TIME_COLUMNS = frozenset({"created_at", "ingested_at", "processed_at",
                                     "load_ts", "etl_ts", "_write_ts"})

_NAME = re.compile(r"[a-z][a-z0-9_]{2,63}")


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    dtype: str
    description: str
    owner: str
    version: int = 1
    source_lineage: tuple[str, ...] = ()      # upstream tables/fields
    freshness_sla_minutes: int | None = None

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ContractViolation(f"feature name {self.name!r} must be lower_snake_case")
        if self.dtype not in FEATURE_TYPES:
            raise ContractViolation(
                f"{self.name}: unknown dtype {self.dtype!r}; known: {', '.join(FEATURE_TYPES)}")
        if self.name in PROCESSING_TIME_COLUMNS:
            raise ContractViolation(
                f"{self.name}: processing-time columns must not be features — they are "
                "knowable only after the fact and leak into any join that uses them")
        if self.version < 1:
            raise ContractViolation(f"{self.name}: version starts at 1")
        if not self.description.strip():
            raise ContractViolation(
                f"{self.name}: a feature with no description cannot be reviewed or reused")

    @property
    def feature_version(self) -> str:
        return typed_version("feature", asdict(self))


@dataclass(frozen=True)
class FeatureGroup:
    name: str
    entity_keys: tuple[str, ...]
    event_time_column: str
    owner: str
    domain: str
    features: tuple[FeatureDefinition, ...]
    description: str = ""
    classification: Classification = Classification.INTERNAL
    batch_enabled: bool = True
    streaming_enabled: bool = False
    online_store_enabled: bool = False
    partition_by: tuple[str, ...] = ("source_cob_date",)
    cost_class: CostClass = CostClass.PER_JOB

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ContractViolation(f"feature group {self.name!r} must be lower_snake_case")
        if not self.entity_keys:
            raise ContractViolation(f"{self.name}: at least one entity key required")
        if not self.features:
            raise ContractViolation(f"{self.name}: a group with no features is not a group")

        # EVENT TIME IS MANDATORY AND MUST NOT BE PROCESSING TIME.
        if not self.event_time_column:
            raise ContractViolation(
                f"{self.name}: event_time_column is mandatory — without it there is no "
                "point-in-time boundary and every training set silently leaks")
        if self.event_time_column in PROCESSING_TIME_COLUMNS:
            raise ContractViolation(
                f"{self.name}: event_time_column {self.event_time_column!r} is a "
                "PROCESSING-time column. Point-in-time correctness requires EVENT time; "
                "using processing time makes the leak invisible rather than absent")

        names = [f.name for f in self.features]
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ContractViolation(f"{self.name}: duplicate feature names {dupes}")

        # CLAUDE.md section 6: never partition on a PK / high-cardinality column. The
        # reporting compiler already rejects this for marts; the same rule holds here, and
        # for the same reason — one file per entity per day.
        for p in self.partition_by:
            if p in self.entity_keys:
                raise ContractViolation(
                    f"{self.name}: partition_by includes entity key {p!r}. Partitioning on "
                    "a high-cardinality key produces one file per entity per day "
                    "(CLAUDE.md section 6)")

        if self.streaming_enabled:
            raise ContractViolation(
                f"{self.name}: streaming features are not built (ADR-053). docs/COST.md "
                "records a one-minute cadence as ~7x the monthly budget. Use the existing "
                "STREAM_BATCH mode over REALTIME if a latency requirement appears")
        if self.online_store_enabled and not self.batch_enabled:
            raise ContractViolation(
                f"{self.name}: online store without an offline source has nothing to serve")
        if self.classification is Classification.RESTRICTED:
            raise ContractViolation(
                f"{self.name}: RESTRICTED features are denied to the AI plane at the IAM "
                "layer too (ADR-060); entity_id must be a surrogate key, not a raw identifier")

    @property
    def group_version(self) -> str:
        return typed_version("feature_group", asdict(self))

    def row_columns(self) -> tuple[str, ...]:
        head = FEATURE_ROW_COLUMNS[:3]
        tail = FEATURE_ROW_COLUMNS[3:]
        return head + tuple(f.name for f in self.features) + tail


# --------------------------------------------------------------------------- #
# Point-in-time
# --------------------------------------------------------------------------- #

class FutureLeakage(ContractViolation):
    """A feature that was not knowable at the label's event time.

    Its own class so tests assert the SPECIFIC failure, not merely that something raised.
    """


def assert_no_future_leakage(feature_event_time: datetime,
                             label_event_time: datetime,
                             *, feature_name: str = "<feature>") -> None:
    """The whole contract, in one place, used by every reader.

    One implementation on purpose: two copies of a cutoff rule drift, and the drift is
    silent because both still return rows.
    """
    if feature_event_time > label_event_time:
        raise FutureLeakage(
            f"{feature_name}: feature_event_time {feature_event_time.isoformat()} is AFTER "
            f"label_event_time {label_event_time.isoformat()}. That value was not knowable "
            "when the label was set; joining it trains on the future")


def assert_join_key_is_event_time(column: str) -> None:
    """Reject a processing-time column used as a PIT join key."""
    if column in PROCESSING_TIME_COLUMNS:
        raise FutureLeakage(
            f"{column!r} is processing time, not event time. A join on it is not "
            "point-in-time correct even when every row happens to look right today")


def select_point_in_time(rows: Iterable[dict[str, Any]],
                         label_event_time: datetime,
                         *,
                         event_time_key: str = "feature_event_time",
                         version_key: str = "feature_version") -> dict[str, Any] | None:
    """The last feature row knowable at `label_event_time`, or None.

    Ordering is (event_time DESC, version DESC) — the same shape as the EOD rule in
    CLAUDE.md section 5.6, where the tie-breaker exists because two rows CAN share a
    timestamp and picking arbitrarily makes a rerun non-reproducible.
    """
    assert_join_key_is_event_time(event_time_key)
    eligible = [r for r in rows if r[event_time_key] <= label_event_time]
    if not eligible:
        return None
    return max(eligible, key=lambda r: (r[event_time_key], r.get(version_key, 0)))


def assert_label_horizon_excluded(feature_event_time: datetime,
                                  label_event_time: datetime,
                                  horizon_end: datetime,
                                  *, feature_name: str = "<feature>") -> None:
    """For a FORWARD-looking label over [T, horizon_end], no feature inside that window.

    This is the test that catches real leakage. A label like "no transaction in the next 30
    days" is computed from data in the future relative to T; a feature drawn from the same
    window encodes the answer. `assert_no_future_leakage` alone does NOT catch it, because
    such a feature can still satisfy `feature_event_time <= label_event_time` while being
    derived from the horizon.
    """
    if label_event_time < feature_event_time <= horizon_end:
        raise FutureLeakage(
            f"{feature_name}: feature at {feature_event_time.isoformat()} falls inside the "
            f"label horizon ({label_event_time.isoformat()} .. {horizon_end.isoformat()}). "
            "A forward-looking label must not be predicted from data drawn from its own "
            "outcome window")
