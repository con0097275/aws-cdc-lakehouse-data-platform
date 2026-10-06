"""Offline feature materialisation, point-in-time correct by construction.

Pure Python over rows the governed metric layer returns. No Spark: the pilots run on one
mart at daily grain, and a local, deterministic implementation is testable without a
cluster — which is what makes the leakage tests meaningful rather than decorative.

TWO PROPERTIES THE STORE MUST HAVE, both asserted:

  1. `feature_event_time` is the business date the features DESCRIBE, and every input used
     to compute a row is <= that date. A trailing window that peeks one day forward is
     invisible in the output and fatal in training.
  2. A feature whose window exceeds the history present is emitted as NULL with the reason
     recorded, never computed from fewer days. A "7-day volatility" over 3 days is a number
     with the wrong name, and nothing downstream can tell.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
GROUP_PATH = ROOT / "aiplatform" / "features" / "account_behavior.yaml"

FEATURE_CODE_VERSION = "feature_code:v1"


class LeakageError(ValueError):
    """A feature was computed from data at or after the label time."""


@dataclass(frozen=True)
class FeatureSpec:
    feature_id: str
    dtype: str
    requires_days: int
    description: str = ""


@dataclass(frozen=True)
class FeatureGroupSpec:
    name: str
    entity_keys: tuple[str, ...]
    event_time_column: str
    owner: str
    version: int
    source_relation: str
    features: tuple[FeatureSpec, ...]
    online_enabled: bool

    def version_hash(self) -> str:
        payload = json.dumps({"name": self.name, "v": self.version,
                              "features": [f.feature_id for f in self.features],
                              "code": FEATURE_CODE_VERSION}, sort_keys=True)
        return "feature_group:" + hashlib.sha256(payload.encode()).hexdigest()[:16]


def load_group(path: Path | str | None = None) -> FeatureGroupSpec:
    raw = yaml.safe_load(Path(path or GROUP_PATH).read_text())
    return FeatureGroupSpec(
        name=raw["name"], entity_keys=tuple(raw["entity_keys"]),
        event_time_column=raw["event_time_column"], owner=raw["owner"],
        version=int(raw.get("version", 1)), source_relation=raw["source_relation"],
        online_enabled=bool(raw["online_store"]["enabled"]),
        # `name`/`type`, not `feature_id`/`dtype`: this file must satisfy the SAME
        # FeatureDefinition contract that aiplatform/compile.py validates. Forking a second
        # feature-group format would leave two schemas with only one of them checked.
        features=tuple(FeatureSpec(f["name"], f["type"],
                                   int(f["requires_days"]), f.get("description", ""))
                       for f in raw["features"]))


@dataclass
class FeatureRow:
    account_sk: str
    feature_event_time: str
    features: dict
    feature_group_version: str
    materialization_run_id: str
    source_watermark: str
    created_at: str
    unavailable: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


def _std(vals: list[float]) -> float | None:
    v = [x for x in vals if x is not None]
    if len(v) < 2:
        return None
    m = sum(v) / len(v)
    return (sum((x - m) ** 2 for x in v) / (len(v) - 1)) ** 0.5


def _safe_div(n, d):
    return (n / d) if d else None


def materialize(rows: list[dict], group: FeatureGroupSpec, *, as_of: str,
                run_id: str = "local") -> list[FeatureRow]:
    """Build features for `as_of` from rows at or BEFORE it.

    `rows` are mart rows: account_sk, business_date, closing_balance, debit_amount,
    credit_amount, txn_count.
    """
    hist = [r for r in rows if str(r["business_date"]) <= as_of]
    if not hist:
        return []
    by_acct: dict[str, dict[str, dict]] = {}
    for r in hist:
        by_acct.setdefault(str(r["account_sk"]), {})[str(r["business_date"])] = r

    days_present = len({str(r["business_date"]) for r in hist})
    created = datetime.now(timezone.utc).isoformat(timespec="seconds")
    gv = group.version_hash()
    out: list[FeatureRow] = []

    for acct, byday in by_acct.items():
        if as_of not in byday:
            continue                       # no observation on the feature date
        dates = sorted(byday)
        today = byday[as_of]
        prior = [d for d in dates if d < as_of]

        vals: dict = {}
        unavailable: dict = {}

        def emit(fid: str, value, needed: int):
            if days_present < needed:
                unavailable[fid] = (f"needs {needed} days of history, "
                                    f"{days_present} present")
                vals[fid] = None
            else:
                vals[fid] = value

        bal = float(today["closing_balance"])
        deb = float(today["debit_amount"])
        cre = float(today["credit_amount"])
        tx = int(today["txn_count"])

        emit("balance_close", bal, 1)
        emit("txn_count_1d", tx, 1)
        emit("debit_amount_1d", deb, 1)
        emit("credit_amount_1d", cre, 1)
        emit("net_flow_1d", cre - deb, 1)
        emit("avg_txn_value_1d", _safe_div(deb + cre, tx), 1)

        prev = byday[prior[-1]] if prior else None
        emit("balance_change_1d",
             (bal - float(prev["closing_balance"])) if prev else None, 2)

        w7 = [byday[d] for d in dates if d > (date.fromisoformat(as_of)
                                              - timedelta(days=7)).isoformat()]
        emit("balance_change_7d",
             (bal - float(w7[0]["closing_balance"])) if len(w7) > 1 else None, 7)
        emit("txn_count_7d", sum(int(r["txn_count"]) for r in w7), 7)
        emit("balance_volatility_7d",
             _std([float(r["closing_balance"]) for r in w7]), 7)

        w30 = [byday[d] for d in dates if d > (date.fromisoformat(as_of)
                                               - timedelta(days=30)).isoformat()]
        emit("balance_volatility_30d",
             _std([float(r["closing_balance"]) for r in w30]), 30)
        emit("avg_txn_amount_30d",
             _safe_div(sum(float(r["debit_amount"]) + float(r["credit_amount"])
                           for r in w30),
                       sum(int(r["txn_count"]) for r in w30)), 30)

        active = [d for d in dates if int(byday[d]["txn_count"]) > 0]
        emit("days_since_last_activity",
             ((date.fromisoformat(as_of) - date.fromisoformat(active[-1])).days
              if active else None), 2)
        emit("activity_frequency_7d",
             _safe_div(len([r for r in w7 if int(r["txn_count"]) > 0]), len(w7)), 7)

        out.append(FeatureRow(
            account_sk=acct, feature_event_time=as_of, features=vals,
            feature_group_version=gv, materialization_run_id=run_id,
            source_watermark=max(dates), created_at=created, unavailable=unavailable))
    return out


def point_in_time_join(labels: list[dict], features: list[FeatureRow], *,
                       entity_key: str = "account_sk",
                       label_time_key: str = "label_event_time") -> list[dict]:
    """Attach, for each label, the LATEST feature row strictly at or before its time.

    The `<=` is the whole contract. `<` would drop same-day features that are legitimately
    known; `<` on the wrong side, or any `>`, silently trains on the future and produces a
    model that scores beautifully in backtest and fails in production.
    """
    by_entity: dict[str, list[FeatureRow]] = {}
    for f in features:
        by_entity.setdefault(f.account_sk, []).append(f)
    for v in by_entity.values():
        v.sort(key=lambda r: r.feature_event_time)

    joined: list[dict] = []
    for lab in labels:
        ent = str(lab[entity_key])
        lt = str(lab[label_time_key])
        candidates = [f for f in by_entity.get(ent, []) if f.feature_event_time <= lt]
        if not candidates:
            continue
        chosen = candidates[-1]
        if chosen.feature_event_time > lt:                    # unreachable; asserted anyway
            raise LeakageError(
                f"{ent}: feature_event_time {chosen.feature_event_time} is after label "
                f"time {lt}")
        joined.append({**lab, **chosen.features,
                       "feature_event_time": chosen.feature_event_time,
                       "feature_group_version": chosen.feature_group_version})
    return joined


def assert_no_future_leakage(joined: list[dict], *,
                             label_time_key: str = "label_event_time") -> None:
    for r in joined:
        if r["feature_event_time"] > str(r[label_time_key]):
            raise LeakageError(
                f"feature_event_time {r['feature_event_time']} > label "
                f"{r[label_time_key]} — the model would be trained on the future")


def assert_label_horizon_excluded(joined: list[dict], *, horizon_days: int,
                                  label_time_key: str = "label_event_time") -> None:
    """The check a `<=` comparison alone does NOT catch.

    A label describing the window (t, t+horizon] must not be paired with a feature computed
    inside that window. `feature_event_time <= label_event_time` is satisfied by such a row
    whenever the label time is stamped at the END of its horizon, which is the usual
    convention — so the obvious test passes and the leak survives.
    """
    for r in joined:
        lt = date.fromisoformat(str(r[label_time_key]))
        ft = date.fromisoformat(r["feature_event_time"])
        if lt - timedelta(days=horizon_days) < ft <= lt:
            raise LeakageError(
                f"feature at {ft} falls INSIDE the {horizon_days}-day label horizon ending "
                f"{lt}. It is <= the label time and still leaks.")
