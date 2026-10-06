"""Train the pilot. Local, on-demand, no paid infrastructure.

    python3 spark/ml/train_pilot.py --dry-run     # build the dataset, report, train nothing
    python3 spark/ml/train_pilot.py --train       # train + write the artifact locally
    python3 spark/ml/train_pilot.py --train --publish   # + upload artifact to s3://<lake>/models/

1,280 rows do not need a cluster. Training locally proves the platform without adding an
always-on component, which is what ADR-053/060 ask for.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark"))

from ml.dataset import (FEATURE_COLUMNS, LABEL, LABEL_HORIZON_DAYS, DatasetSpec,
                        InsufficientData, build, time_split)
from ml.model import LogisticRegression, ModelArtifact, metrics, now_iso

MODEL_NAME = "account_balance_tier_next_day"
OWNER = "risk-data"
SOURCE_TABLE = "kafka_dev_lab_dev_curated.fact_account_daily_snapshot"
WORKGROUP = "kafka-dev-lab-dev-wg"


def _git_commit() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT,
                              capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return "unknown"


def load_snapshot(local_csv: str | None = None) -> pd.DataFrame:
    """Read the curated snapshot. Athena unless a local CSV is supplied (tests)."""
    if local_csv:
        return pd.read_csv(local_csv)
    import boto3, time, io
    ath = boto3.client("athena")
    q = (f"SELECT account_sk, customer_sk, business_date, closing_balance, "
         f"debit_amount, credit_amount, txn_count FROM {SOURCE_TABLE}")
    qid = ath.start_query_execution(QueryString=q, WorkGroup=WORKGROUP)["QueryExecutionId"]
    for _ in range(60):
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if st["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena: {st.get('StateChangeReason')}")
    loc = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
        "ResultConfiguration"]["OutputLocation"]
    b, k = loc.replace("s3://", "").split("/", 1)
    body = boto3.client("s3").get_object(Bucket=b, Key=k)["Body"].read()
    return pd.read_csv(io.BytesIO(body))


def run(train: bool, publish: bool, local_csv: str | None) -> int:
    snap = load_snapshot(local_csv)
    spec = DatasetSpec(name="account_balance_tier", label_name=MODEL_NAME,
                       feature_columns=FEATURE_COLUMNS,
                       label_horizon_days=LABEL_HORIZON_DAYS,
                       train_end=str(pd.to_datetime(snap["business_date"]).max().date()),
                       source_table=SOURCE_TABLE, synthetic_label=True)

    print(f"  source rows        {len(snap)}")
    print(f"  business dates     {sorted(str(d)[:10] for d in pd.to_datetime(snap['business_date']).unique())}")

    try:
        ds, dataset_version = build(snap, spec)
    except InsufficientData as e:
        print(f"\n  BLOCKED — insufficient data: {e}", file=sys.stderr)
        return 2

    print(f"  dataset rows       {len(ds)}")
    print(f"  dataset_version    {dataset_version}")
    print(f"  label balance      {ds[LABEL].value_counts().to_dict()}")
    print(f"  label horizon      {LABEL_HORIZON_DAYS} day (forward-looking)")
    print(f"  SYNTHETIC LABEL    yes — see spark/ml/dataset.py for why")

    tr, te = time_split(ds, holdout_last_n_dates=1)
    print(f"  time split         train {len(tr)} / test {len(te)}  (TIME-based, never random)")

    feats = [c for c in FEATURE_COLUMNS if c in ds.columns]
    Xtr, ytr = tr[feats].to_numpy(float), tr[LABEL].to_numpy(int)
    Xte, yte = te[feats].to_numpy(float), te[LABEL].to_numpy(int)

    if not train:
        print("\n  DRY RUN — dataset built and validated; no model trained.")
        return 0

    params = {"learning_rate": 0.1, "iterations": 500, "l2": 0.01, "seed": 42}
    model = LogisticRegression(**params).fit(Xtr, ytr, feats)
    m_tr = metrics(ytr, model.predict_proba(Xtr))
    m_te = metrics(yte, model.predict_proba(Xte))
    print(f"  train metrics      {m_tr}")
    print(f"  test  metrics      {m_te}")

    code_version = _git_commit()
    art = ModelArtifact(
        model_name=MODEL_NAME,
        model_version=ModelArtifact.make_version(MODEL_NAME, dataset_version, params,
                                                 code_version),
        dataset_version=dataset_version,
        feature_versions=tuple(sorted(ds["feature_version"].dropna().unique())),
        code_version=code_version, parameters=params,
        metrics={"train": m_tr, "test": m_te}, owner=OWNER,
        created_at=now_iso(), synthetic_label=True, artifact=model.to_dict())

    out = ROOT / "artifacts" / "models" / art.model_version.replace(":", "_")
    out.mkdir(parents=True, exist_ok=True)
    (out / "model.json").write_text(art.to_json())
    print(f"  model_version      {art.model_version}")
    print(f"  artifact           {out.relative_to(ROOT)}/model.json")

    if publish:
        import boto3
        bucket = os.environ.get("AI_LAKE_BUCKET", "kafka-dev-lab-dev-lake-111122223333")
        key = f"models/artifacts/{MODEL_NAME}/{art.model_version.replace(':', '_')}/model.json"
        boto3.client("s3").put_object(Bucket=bucket, Key=key,
                                      Body=art.to_json().encode(),
                                      ServerSideEncryption="aws:kms")
        print(f"  published          s3://{bucket}/{key}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True)
    g.add_argument("--train", action="store_true")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--local-csv")
    a = ap.parse_args(argv)
    return run(a.train, a.publish, a.local_csv)


if __name__ == "__main__":
    raise SystemExit(main())
