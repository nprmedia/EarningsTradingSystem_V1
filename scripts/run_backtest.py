#!/usr/bin/env python3
"""
EarningsTradingSystem backtest runner (final version)

- CI-safe and Ruff/Black compliant
- Works in nested GitHub Actions checkout and local dev
- Automatically creates output dirs and ensures imports work
- Self-heals missing 'close_next' column for mock data
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
from pathlib import Path


def _resolve_root() -> Path:
    """Find repo root across both local and CI layouts."""
    here = Path(__file__).resolve()
    if len(here.parents) > 3:
        cand = here.parents[3]
        if (cand / "src").is_dir():
            return cand
    if len(here.parents) > 2:
        cand = here.parents[2]
        if (cand / "src").is_dir():
            return cand
    for parent in [here] + list(here.parents):
        if (parent / "src").is_dir():
            return parent
    return here.parent


def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run ETS backtest")
    ap.add_argument(
        "--signals",
        default=os.getenv("BACKTEST_SIGNALS", "tests/fixtures/mock_signals.csv"),
        help="Signals CSV path",
    )
    ap.add_argument(
        "--history",
        default=os.getenv("BACKTEST_HISTORY", "tests/fixtures/mock_history.csv"),
        help="Historical prices CSV path",
    )
    ap.add_argument("--reports", default="reports", help="Reports output dir")
    ap.add_argument("--metrics", default="metrics", help="Metrics output dir")
    return ap.parse_args()


def main() -> int:
    ROOT = _resolve_root()
    os.chdir(ROOT)

    reports = ROOT / "reports"
    metrics = ROOT / "metrics"
    _ensure_dirs(reports, metrics)

    # Add repo root (not src) so `import src.*` works
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

    # Load fixtures and build panel
    sig = load_signals(args.signals)
    hist = load_history(args.history)
    panel = make_panel(sig, hist)

    # Self-heal if 'close_next' missing
    if "close_next" not in panel.columns and "close" in panel.columns:
        panel = panel.sort_values(["ticker", "date"])
        panel["close_next"] = panel.groupby("ticker")["close"].shift(-1)
        print("[WARN] 'close_next' missing — auto-generated via shift().")

    try:
        m = compute_metrics(panel)
        save_artifacts(reports, m)
        save_perf(metrics, time.perf_counter() - t0, len(panel))
        print("[OK] Backtest complete.")
        return 0
    except Exception as e:
        print(f"[FAIL] {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
