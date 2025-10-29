#!/usr/bin/env python3
"""
Run a deterministic ETS backtest.

- Works in GitHub Actions and local dev
- No relative-import failures
- Produces /reports + /metrics artifacts
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path


# ---------- Helpers ----------
def _resolve_root() -> Path:
    """Find the repo root containing src/."""
    here = Path(__file__).resolve()
    for parent in [here] + list(here.parents):
        if (parent / "src").is_dir():
            return parent
    raise RuntimeError("Cannot locate repo root containing src/")


def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


# ---------- CLI ----------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run ETS backtest")
    ap.add_argument(
        "--signals",
        default=os.getenv("BACKTEST_SIGNALS", "tests/fixtures/mock_signals.csv"),
        help="Signals CSV",
    )
    ap.add_argument(
        "--history",
        default=os.getenv("BACKTEST_HISTORY", "tests/fixtures/mock_history.csv"),
        help="Historical prices CSV",
    )
    ap.add_argument(
        "--reports",
        default="reports",
        help="Output dir for human-readable reports",
    )
    ap.add_argument(
        "--metrics",
        default="metrics",
        help="Output dir for machine-readable metrics",
    )
    return ap.parse_args()


# ---------- Main ----------
def main() -> int:
    root = _resolve_root()
    os.chdir(root)

    reports_dir = root / "reports"
    metrics_dir = root / "metrics"
    _ensure_dirs(reports_dir, metrics_dir)

    # Make src importable
    sys.path.insert(0, str(root / "src"))

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
        print(f"[FAIL] Import error: {e}")
        print(f"sys.path[0]={sys.path[0]}")
        return 1

    args = parse_args()
    print("[INFO] Backtest starting")
    print(f"  ROOT      = {root}")
    print(f"  signals   = {args.signals}")
    print(f"  history   = {args.history}")
    print(f"  reports   = {reports_dir}")
    print(f"  metrics   = {metrics_dir}")

    t0 = time.perf_counter()
    try:
        sig = load_signals(args.signals)
        hist = load_history(args.history)
        panel = make_panel(sig, hist)

        metrics = compute_metrics(panel)
        save_artifacts(reports_dir, metrics, panel)
        save_perf(metrics_dir, time.perf_counter() - t0, len(panel))
    except Exception as e:
        print(f"[FAIL] {type(e).__name__}: {e}")
        return 1

    print("[OK] Backtest complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
