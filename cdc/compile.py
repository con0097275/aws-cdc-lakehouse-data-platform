#!/usr/bin/env python3
"""Compile the CDC registry into a deterministic plan.

    python3 -m cdc.compile --check                        validate only, non-zero on defect
    python3 -m cdc.compile --out artifacts/cdc/table-plan.json
    python3 -m cdc.compile --verify artifacts/cdc/table-plan.json

Run as a MODULE (`-m`), not a path. `cdc` is a real package with relative imports precisely
so its module names cannot shadow `spark/reporting/`'s -- running the file directly would
defeat that.

Runs entirely locally. No AWS call, no credential, no cost -- the same deliberate property
`reporting/compile.py` documents: every config defect caught here is one caught in CI rather
than at 02:00 by a coordinator.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent

from .config_loader import load_config                        # noqa: E402
from .models import ConfigError                                # noqa: E402
from .table_plan import build_plan, build_payload, config_version, source_fingerprint  # noqa: E402

DEFAULT_REGISTRY = _HERE / "registry" / "sources.yaml"
DEFAULT_OUT = _HERE.parent / "artifacts" / "cdc" / "table-plan.json"


def compile_plan(registry: Path, *, environment: str = "dev",
                 generated_at: datetime | None = None) -> dict:
    config = load_config(registry, environment=environment)
    return build_plan(config,
                      generated_at=generated_at or datetime.now(timezone.utc),
                      source_sha256=source_fingerprint(registry))


def _report(plan: dict) -> list[str]:
    p = plan["plan"]
    lines = []
    for message in plan.get("deprecations") or []:
        lines.append(f"  DEPRECATED       {message}")
    lines += [f"  config_version   {plan['config_version']}",
             f"  plan_hash        {plan['plan_hash'][:24]}…",
             f"  environment      {p['environment']}",
             f"  sources          {len(p['sources'])}",
             f"  tables           {len(p['tables'])} "
             f"({sum(1 for t in p['tables'] if t['enabled'])} enabled)"]
    for t in p["tables"]:
        flags = []
        if not t["enabled"]:
            flags.append("DISABLED")
        if not t["realtime_policy"]["enabled"]:
            flags.append("no-RT")
        if not t["eod_policy"]["enabled"]:
            flags.append("no-EOD")
        lines.append(f"    {t['table_id']:<34} {t['config_hash']} "
                     f"{t['governance']['classification']:<12} {' '.join(flags)}")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    ap.add_argument("--environment", default="dev")
    ap.add_argument("--check", action="store_true",
                    help="validate and prove determinism; write nothing")
    ap.add_argument("--out", type=Path, help=f"write the plan (default {DEFAULT_OUT})")
    ap.add_argument("--verify", type=Path,
                    help="recompile and compare against an existing plan")
    args = ap.parse_args(argv)

    try:
        plan = compile_plan(args.registry, environment=args.environment)
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        return 2

    if args.verify:
        if not args.verify.exists():
            print(f"{args.verify}: not found", file=sys.stderr)
            return 2
        existing = json.loads(args.verify.read_text())
        if existing.get("plan_hash") != plan["plan_hash"]:
            print(f"DRIFT: {args.verify} has {existing.get('config_version')} but the "
                  f"registry compiles to {plan['config_version']}", file=sys.stderr)
            return 1
        print(f"  {args.verify} matches the registry ({plan['config_version']})")
        return 0

    print("\n".join(_report(plan)))

    if args.check:
        # Determinism is a TESTED property, not an aspiration: recompile and compare.
        again = build_payload(load_config(args.registry, environment=args.environment))
        if config_version(again) != plan["config_version"]:
            print("NON-DETERMINISTIC: two compiles of the same input disagree",
                  file=sys.stderr)
            return 1
        print("  deterministic     yes (recompiled, hashes match)")
        return 0

    out = args.out or DEFAULT_OUT
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(plan, indent=1, sort_keys=True) + "\n")
    print(f"  wrote            {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
