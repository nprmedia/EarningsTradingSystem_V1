from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import yaml

CURR = Path("src/ets/data/signals/sector_weights.yaml")
HIST = Path("out/weights_history.jsonl")
SNAP = Path("out/snapshots")


def load_yaml(p: Path) -> dict[str, Any]:
    if not p.exists():
        return {}
    with p.open("r") as f:
        return yaml.safe_load(f) or {}


def dump_yaml_atomic(p: Path, data: dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=str(p.parent)) as tmp:
        yaml.safe_dump(data, tmp, sort_keys=True)
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, p)


def append_history(changes):
    HIST.parent.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    with HIST.open("a") as f:
        for c in changes:
            rec = dict(timestamp=ts, **c)
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")


def ensure_sector(d: dict[str, Any], sector: str):
    if sector not in d or not isinstance(d[sector], dict):
        d[sector] = {}


def parse_set_arg(s: str):
    # "Technology.M_raw=0.18"
    if "=" not in s or "." not in s.split("=", 1)[0]:
        raise ValueError(f"Bad --set '{s}'. Use Sector.Factor=value")
    left, val = s.split("=", 1)
    sector, factor = left.split(".", 1)
    try:
        w = float(val)
    except Exception:
        raise ValueError(f"Weight must be float in --set '{s}'") from None
    return sector.strip(), factor.strip(), w


def sector_sum(weights: dict[str, float]) -> float:
    return float(sum(float(v) for v in weights.values()))


def normalize_sector(weights: dict[str, float]) -> dict[str, float]:
    s = sector_sum(weights)
    if s == 0:
        return weights
    return {k: float(v) / s for k, v in weights.items()}


def main():  # noqa: C901
    ap = argparse.ArgumentParser(description="Update sector weights and log history.")
    ap.add_argument(
        "--from-yaml",
        help="YAML file with updates: {Sector: {Factor: weight, ...}, ...}",
    )
    ap.add_argument(
        "--set",
        action="append",
        default=[],
        help="Inline updates like: Technology.M_raw=0.18 (repeatable)",
    )
    ap.add_argument("--reason", default="", help="Short free-text reason to log with history")
    ap.add_argument("--source", default="manual", help="Origin tag (manual|tuner|backtest|other)")
    ap.add_argument(
        "--normalize",
        action="store_true",
        help="Normalize each touched sector to sum to 1.0 after updates",
    )
    ap.add_argument(
        "--allow-unequal",
        action="store_true",
        help="Permit sector weight sums != 1.0 (otherwise error)",
    )
    ap.add_argument(
        "--snapshot",
        action="store_true",
        help="Save a snapshot of current YAML before update",
    )
    args = ap.parse_args()

    current = load_yaml(CURR)
    if args.snapshot and CURR.exists():
        SNAP.mkdir(parents=True, exist_ok=True)
        ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        shutil.copy2(CURR, SNAP / f"sector_weights_{ts}.yaml")

    # Build updates structure
    updates: dict[str, dict[str, float]] = {}
    if args.from_yaml:
        upd = load_yaml(Path(args.from_yaml))
        if not isinstance(upd, dict):
            print("[FATAL] --from-yaml must be a mapping", file=sys.stderr)
            sys.exit(2)
        for sec, m in upd.items():
            if not isinstance(m, dict):
                continue
            updates.setdefault(sec, {}).update({k: float(v) for k, v in m.items()})
    for s in args.set:
        sec, fac, w = parse_set_arg(s)
        updates.setdefault(sec, {})[fac] = w

    if not updates:
        print("[INFO] no changes requested")
        return

    changes = []
    touched_sectors = set()

    for sec, m in updates.items():
        ensure_sector(current, sec)
        for fac, new_w in m.items():
            old_w = float(current[sec].get(fac, 0.0))
            if abs(new_w - old_w) < 1e-12:
                continue
            # record history BEFORE applying
            changes.append(
                {
                    "sector": sec,
                    "factor": fac,
                    "old_weight": old_w,
                    "new_weight": float(new_w),
                    "reason": args.reason,
                    "source": args.source,
                }
            )
            current[sec][fac] = float(new_w)
            touched_sectors.add(sec)

    if not changes:
        print("[INFO] nothing changed")
        return

    # Optional normalization
    if args.normalize:
        for sec in touched_sectors:
            current[sec] = normalize_sector(current[sec])

    # Validate sums unless explicitly allowed
    if not args.allow_unequal:
        bad = []
        for sec in touched_sectors:
            s = sector_sum(current[sec])
            if not (abs(s - 1.0) <= 1e-6):
                bad.append((sec, s))
        if bad:
            for sec, s in bad:
                print(
                    f"[ERROR] sector {sec!r} weights sum={s:.6f} (expected 1.0). "
                    "Use --normalize or --allow-unequal.",
                    file=sys.stderr,
                )
            sys.exit(3)

    # Write current YAML atomically, append history
    dump_yaml_atomic(CURR, current)
    append_history(changes)

    print(f"[OK] updated {len(changes)} weights across {len(touched_sectors)} sector(s)")
    for c in changes:
        print(
            f" - {c["sector"]}.{c["factor"]}: {c["old_weight"]} -> {c["new_weight"]} "
            f"[{c["source"]}{" "+c["reason"] if c["reason"] else ""}]"
        )
