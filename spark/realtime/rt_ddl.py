"""Create the realtime serving layer's tables. Idempotent -- every statement is IF NOT EXISTS.

    spark-submit rt_ddl.py --warehouse ... --ddl s3://.../ddl.sql
"""
from __future__ import annotations

import argparse

from rt_autocorrect import build_spark


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--ddl", required=True)
    args = ap.parse_args()

    spark = build_spark("rt-ddl")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args.warehouse)
    spark.sparkContext.setLogLevel("WARN")

    text = "\n".join(spark.read.text(args.ddl).rdd.map(lambda r: r[0]).collect())
    # Strip comments before splitting: a `--` line containing a semicolon would otherwise
    # split one statement into two invalid halves.
    stripped = "\n".join(ln for ln in text.splitlines()
                         if not ln.strip().startswith("--"))
    for stmt in [s.strip() for s in stripped.split(";") if s.strip()]:
        head = " ".join(stmt.split())[:70]
        spark.sql(stmt)
        print(f"RT_DDL_OK {head}", flush=True)
    print("RT_DDL_COMPLETE", flush=True)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
