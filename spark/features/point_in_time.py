"""Point-in-time join. ONE implementation, used by training and inference alike.

    for each label row (entity, T): the LAST feature row with feature_event_time <= T

`<=` and not `<`: a value stamped exactly at T was knowable at T.

WHY ONE IMPLEMENTATION
----------------------
Two copies of a cutoff rule drift, and the drift is silent because both still return rows.
Training would then learn on one rule and inference score on another, and the gap shows up
as unexplained production degradation rather than as an error.

This is the same shape `CLAUDE.md` section 5.6 already fixes for EOD -- explicit cutoff,
dedup by key, last event by ordering -- applied per label row instead of per business date.
The tie-breaker exists because two rows CAN share a timestamp, and picking arbitrarily makes
a rerun non-reproducible.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

EVENT_TIME = "feature_event_time"
LABEL_TIME = "label_event_time"
VERSION = "feature_version"

#: Never a legal join key. Mirrors aiplatform.features.PROCESSING_TIME_COLUMNS so the Spark
#: layer and the contract layer cannot disagree about what "event time" means.
PROCESSING_TIME_COLUMNS = frozenset({"created_at", "ingested_at", "processed_at",
                                     "load_ts", "etl_ts", "_write_ts"})


class FutureLeakage(ValueError):
    """A feature that was not knowable at the label's event time."""


def assert_event_time_column(column: str) -> None:
    if column in PROCESSING_TIME_COLUMNS:
        raise FutureLeakage(
            f"{column!r} is processing time, not event time. A join on it is not "
            "point-in-time correct even when every row happens to look right today.")


def point_in_time_join(labels: DataFrame, features: DataFrame, *,
                       entity_keys: list[str],
                       label_time: str = LABEL_TIME,
                       event_time: str = EVENT_TIME,
                       version_col: str = VERSION,
                       label_horizon_days: int = 0) -> DataFrame:
    """Attach, to each label row, the latest feature knowable at its event time.

    `label_horizon_days > 0` marks a FORWARD-looking label. Features drawn from inside
    `[T, T+horizon]` encode the outcome, and `feature_event_time <= T` alone does NOT
    exclude them -- so the horizon is asserted separately by
    `assert_no_horizon_leakage` after the join.
    """
    assert_event_time_column(event_time)
    for k in entity_keys:
        if k not in labels.columns or k not in features.columns:
            raise ValueError(f"entity key {k!r} missing from labels or features")
    if label_time not in labels.columns:
        raise ValueError(f"labels have no {label_time!r}")
    if event_time not in features.columns:
        raise ValueError(f"features have no {event_time!r}")

    cond = [labels[k] == features[k] for k in entity_keys]
    # THE BOUNDARY. Everything else in this function is bookkeeping around this predicate.
    cond.append(features[event_time] <= labels[label_time])

    joined = labels.alias("l").join(features.alias("f"), cond, how="left")

    order = [F.col(f"f.{event_time}").desc()]
    if version_col in features.columns:
        order.append(F.col(f"f.{version_col}").desc())
    w = Window.partitionBy(*[F.col(f"l.{k}") for k in entity_keys],
                           F.col(f"l.{label_time}")).orderBy(*order)

    return (joined.withColumn("_rn", F.row_number().over(w))
                  .filter(F.col("_rn") == 1)
                  .drop("_rn"))


def assert_no_future_leakage(df: DataFrame, *, label_time: str = LABEL_TIME,
                             event_time: str = EVENT_TIME) -> None:
    """Fail if any joined row carries a feature from after its label."""
    bad = df.filter(F.col(event_time).isNotNull() &
                    (F.col(event_time) > F.col(label_time))).count()
    if bad:
        raise FutureLeakage(
            f"{bad} row(s) carry a feature stamped after their label_event_time. The join "
            "is not point-in-time correct.")


def assert_no_horizon_leakage(df: DataFrame, *, horizon_days: int,
                              label_time: str = LABEL_TIME,
                              event_time: str = EVENT_TIME) -> None:
    """Fail if a FORWARD label's own outcome window supplied a feature.

    This is the check that catches real leakage. `feature_event_time <= label_event_time`
    does not: a feature can satisfy it and still be derived from the horizon.
    """
    if horizon_days <= 0:
        return
    end = F.col(label_time) + F.expr(f"INTERVAL {horizon_days} DAYS")
    bad = df.filter(F.col(event_time).isNotNull() &
                    (F.col(event_time) > F.col(label_time)) &
                    (F.col(event_time) <= end)).count()
    if bad:
        raise FutureLeakage(
            f"{bad} row(s) drew a feature from inside the label horizon "
            f"[T, T+{horizon_days}d]. A forward-looking label must not be predicted from "
            "data taken from its own outcome window.")
