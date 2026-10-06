"""Point-in-time training-set builder for the pilot.

THE LABEL IS SYNTHETIC AND SAYS SO
----------------------------------
The available data does not support a real supervised target. Measured 2026-08-26 on the
live catalog:

  business dates                    4   (2026-08-17, 08-20, 08-21, 08-22)
  accounts                        320
  txn_count distinct values         1   (zero for every account, every date)
  debit_amount distinct values      1
  credit_amount distinct values     1
  accounts whose balance changes    2   of 320

So there is no activity, no transaction volume and effectively no temporal movement. A
churn / activity / anomaly label built on this would be degenerate -- one class, or noise
dressed as signal -- and a metric computed from it would be a number with no meaning.

The pilot therefore uses a **clearly documented synthetic target**:

    label = 1 if the account's closing_balance at D+1 is above the
            cross-sectional median at D+1, else 0

`closing_balance` is the ONE field with real variance (323 distinct values,
1.03M..11.67M). The label is FORWARD-LOOKING by one day on purpose: that is what exercises
the horizon-leakage machinery, which is the property this pilot exists to prove.

WHAT THIS PILOT DOES AND DOES NOT DEMONSTRATE
  DOES     point-in-time correctness, leakage rejection, versioning, artifact + metadata
           registry, reproducibility, batch inference -- the pipeline, end to end.
  DOES NOT predictive skill. The label is nearly determined by a feature, so the metrics
           will be high and mean nothing. Reporting them as evidence of a good model would
           be dishonest; they are evidence the plumbing works.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, asdict

import pandas as pd

ENTITY = "entity_id"
FEATURE_TIME = "feature_event_time"
LABEL_TIME = "label_event_time"
LABEL = "label"
LABEL_HORIZON_DAYS = 1

FEATURE_COLUMNS = ("closing_balance", "txn_count", "debit_amount", "credit_amount")


class InsufficientData(RuntimeError):
    """Refuse to train rather than produce a meaningless model."""


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    label_name: str
    feature_columns: tuple[str, ...]
    label_horizon_days: int
    train_end: str
    source_table: str
    synthetic_label: bool = True

    def version(self, row_hash: str) -> str:
        payload = {**asdict(self), "feature_columns": list(self.feature_columns),
                   "rows": row_hash}
        return "dataset:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


def build_labels(snap: pd.DataFrame) -> pd.DataFrame:
    """label at D = f(state at D+1). Forward-looking by one day.

    The label is computed from the NEXT date's median, which is exactly the kind of
    forward-window quantity a leakage check must catch if it ever leaks back into features.
    """
    df = snap.copy()
    df["business_date"] = pd.to_datetime(df["business_date"])
    dates = sorted(df["business_date"].unique())
    if len(dates) < 2:
        raise InsufficientData(
            f"{len(dates)} distinct business date(s). A forward-looking label needs at "
            "least two: one to observe from and one to observe into.")

    out = []
    for i, d in enumerate(dates[:-1]):
        nxt = dates[i + 1]
        fut = df[df["business_date"] == nxt]
        if fut.empty:
            continue
        med = fut["closing_balance"].median()
        lab = fut[["account_sk", "closing_balance"]].copy()
        lab[LABEL] = (lab["closing_balance"] > med).astype(int)
        lab[LABEL_TIME] = d
        lab[ENTITY] = lab["account_sk"].astype(str)
        out.append(lab[[ENTITY, LABEL_TIME, LABEL]])
    if not out:
        raise InsufficientData("no label rows could be formed")
    return pd.concat(out, ignore_index=True)


def build_features(snap: pd.DataFrame) -> pd.DataFrame:
    f = snap.copy()
    f[ENTITY] = f["account_sk"].astype(str)
    f[FEATURE_TIME] = pd.to_datetime(f["business_date"])
    f["feature_version"] = "feature_group:pilot-v1"
    keep = [ENTITY, FEATURE_TIME, "feature_version", *FEATURE_COLUMNS]
    return f[[c for c in keep if c in f.columns]]


def point_in_time_join(labels: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    """The pandas twin of spark/features/point_in_time.py, for the local pilot.

    Same rule, same tie-break: last feature with feature_event_time <= label_event_time,
    ordered (event_time DESC, feature_version DESC). A LEFT join, so a label with no
    feature survives with nulls rather than silently shrinking the set.
    """
    lab = labels.sort_values(LABEL_TIME)
    fea = features.sort_values(FEATURE_TIME)
    merged = pd.merge_asof(
        lab, fea, left_on=LABEL_TIME, right_on=FEATURE_TIME, by=ENTITY,
        direction="backward", allow_exact_matches=True)   # `<=`, not `<`
    return merged


def assert_no_future_leakage(df: pd.DataFrame) -> None:
    bad = df[df[FEATURE_TIME].notna() & (df[FEATURE_TIME] > df[LABEL_TIME])]
    if len(bad):
        raise ValueError(f"{len(bad)} row(s) carry a feature stamped after their label.")


def assert_no_horizon_leakage(df: pd.DataFrame, horizon_days: int) -> None:
    """No feature drawn from inside the forward label's own outcome window."""
    if horizon_days <= 0:
        return
    end = df[LABEL_TIME] + pd.Timedelta(days=horizon_days)
    bad = df[df[FEATURE_TIME].notna() & (df[FEATURE_TIME] > df[LABEL_TIME]) &
             (df[FEATURE_TIME] <= end)]
    if len(bad):
        raise ValueError(
            f"{len(bad)} row(s) drew a feature from inside the label horizon "
            f"[T, T+{horizon_days}d].")


def build(snap: pd.DataFrame, spec: DatasetSpec) -> tuple[pd.DataFrame, str]:
    labels = build_labels(snap)
    features = build_features(snap)
    ds = point_in_time_join(labels, features)

    assert_no_future_leakage(ds)
    assert_no_horizon_leakage(ds, spec.label_horizon_days)

    ds = ds.dropna(subset=[FEATURE_TIME])
    if ds.empty:
        raise InsufficientData("point-in-time join produced no usable rows")

    counts = ds[LABEL].value_counts().to_dict()
    if len(counts) < 2:
        raise InsufficientData(
            f"label has a single class {counts}. Training on it would produce a model that "
            "predicts the majority and scores perfectly, which is not a model.")

    row_hash = hashlib.sha256(
        pd.util.hash_pandas_object(ds.sort_values([ENTITY, LABEL_TIME]),
                                   index=False).values.tobytes()).hexdigest()[:16]
    return ds, spec.version(row_hash)


def time_split(ds: pd.DataFrame, holdout_last_n_dates: int = 1):
    """TIME-BASED split. A random split over time-series features leaks by construction."""
    dates = sorted(ds[LABEL_TIME].unique())
    if len(dates) <= holdout_last_n_dates:
        raise InsufficientData(
            f"{len(dates)} label date(s) — too few to hold out {holdout_last_n_dates} "
            "without leaving nothing to train on.")
    cut = dates[-holdout_last_n_dates]
    return ds[ds[LABEL_TIME] < cut].copy(), ds[ds[LABEL_TIME] >= cut].copy()
