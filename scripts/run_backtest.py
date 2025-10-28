#!/usr/bin/env python3
"""
Backtest runner (CI-safe, Ruff/Black-compliant, hardened)
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from pathlib import Path


def _resolve_root() -> Path:
    """Robustly find repo root across CI/local layouts."""
    here = Path(__file__).resolve()

    # 1️⃣ Try 3 levels up (GitHub Actions nested checkout)
    if len(here.parents) > 3:
        cand_gha = here.parents[3]
        if (cand_gha / "src").is_dir():
            return cand_gha

    # 2️⃣ Try 2 levels up (local dev)
    if len(here.parents) > 2:
        cand_local = here.parents[2]
        if (cand_local / "src").is_dir():
            return cand_local

    # 3️⃣ Fallback search upward
    for parent in [here] + list(here.parents):
        if (parent / "src").is_dir():
            return parent

    # 4️⃣ Fallback to script directory
    return here.parent
    """Robustly find repo root across CI/local layouts."""
    here = Path(__file__).resolve()

    # 1️⃣ Try 3 levels up (GitHub Actions nested checkout)
    if len(here.parents) > 3:
        cand_gha = here.parents[3]
        if (cand_gha / "src").is_dir():
            return cand_gha

    # 2️⃣ Try 2 levels up (local dev)
    if len(here.parents) > 2:
        cand_local = here.parents[2]
        if (cand_local / "src").is_dir():
            return cand_local

    # 3️⃣ Fallback search upward
    for parent in [here] + list(here.parents):
        if (parent / "src").is_dir():
            return parent

    # 4️⃣ Fallback to script directory
    return here.parent


def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run ETS backtest with fixtures")
    ap.add_argument(
        "--signals",
        default=os.getenv("BACKTEST_SIGNALS", "tests/fixtures/mock_signals.csv"),
    )
    ap.add_argument(
        "--history",
        default=os.getenv("BACKTEST_HISTORY", "tests/fixtures/mock_history.csv"),
    )
    ap.add_argument("--reports", default="reports")
    ap.add_argument("--metrics", default="metrics")
    return ap.parse_args()


def main() -> int:
    ROOT = _resolve_root()
    os.chdir(ROOT)
    reports, metrics = ROOT / "reports", ROOT / "metrics"
    _ensure_dirs(reports, metrics)
    sys.path.insert(0, str(ROOT))

    try:
        from src.ets.backtest.historical_loader import (
            load_signals,
            load_history,
            make_panel,
        )
        from src.ets.backtest.performance_metrics import (
            compute_metrics,
            save_artifacts,
            save_perf,
        )
    except ModuleNotFoundError as e:
        print("\n[ERROR] Could not import 'src' packages.")
        print(f"  cwd: {Path.cwd()}")
        print(f"  expected src path: {ROOT / 'src'}")
        print(f"  sys.path[0]: {sys.path[0]}")
        print(f"  sys.path: {json.dumps(sys.path[:6], indent=2)}")
        print(
            "  Hint: Ensure that 'src/' exists at the repo root and contains __init__.py in 'src' and 'src/ets'."
        )
        raise e

    args = parse_args()
    print("[INFO] Backtest starting")
    print(f"  ROOT      = {ROOT}")
    print(f"  signals   = {args.signals}")
    print(f"  history   = {args.history}")
    print(f"  reports   = {reports}")
    print(f"  metrics   = {metrics}")

    t0 = time.perf_counter()
    sig, hist = load_signals(args.signals), load_history(args.history)
    panel = make_panel(sig, hist)
    m = compute_metrics(panel)
    save_artifacts(reports, m)
    save_perf(metrics, time.perf_counter() - t0, len(panel))
    print("[OK] Backtest complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
