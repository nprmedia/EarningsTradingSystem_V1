#!/usr/bin/env python3
"""
Backtest runner (CI-safe, Ruff/Black-compliant)

- No project imports at module scope (avoids Ruff E402)
- Resolves correct repo root both locally and on GitHub Actions
- Adds src/ to sys.path **inside** main() before project imports
- Creates reports/ and metrics/ if missing
- Supports env vars and CLI flags for fixtures
- Clear error messages if src/ not found
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


def _resolve_root() -> Path:
    """
    Find the repository root in both local dev and CI.
    Tries parents[2] first (GH Actions nested checkout), then parents[1].
    """
    here = Path(__file__).resolve()
    # Common CI layout: .../EarningsTradingSystem_V1/EarningsTradingSystem_V1/scripts/run_backtest.py
    cand_ci = here.parents[2]  # up 2 levels to repo root
    if (cand_ci / "src").is_dir():
        return cand_ci

    # Local layout (Codespace): .../EarningsTradingSystem_V1/scripts/run_backtest.py
    cand_local = here.parents[1]
    if (cand_local / "src").is_dir():
        return cand_local

    # Fallback to current directory of this script
    return here.parent


def _ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run ETS backtest with fixtures")
    ap.add_argument(
        "--signals",
        default=os.getenv("BACKTEST_SIGNALS", "tests/fixtures/mock_signals.csv"),
        help="Path to signals CSV (env BACKTEST_SIGNALS overrides)",
    )
    ap.add_argument(
        "--history",
        default=os.getenv("BACKTEST_HISTORY", "tests/fixtures/mock_history.csv"),
        help="Path to historical prices CSV (env BACKTEST_HISTORY overrides)",
    )
    ap.add_argument(
        "--reports",
        default="reports",
        help="Output directory for human-readable reports",
    )
    ap.add_argument(
        "--metrics",
        default="metrics",
        help="Output directory for machine-readable metrics",
    )
    return ap.parse_args()


def main() -> int:
    # Resolve repo root and normalize working directory
    ROOT = _resolve_root()
    os.chdir(ROOT)

    # Make sure outputs exist
    reports = ROOT / "reports"
    metrics = ROOT / "metrics"
    _ensure_dirs(reports, metrics)

    # Make src/ importable **before** project imports (avoid Ruff E402 by doing this in main)
    sys.path.insert(0, str(ROOT / "src"))

    # Now import project modules (inside main to keep Ruff happy)
    try:
        from src.ets.backtest.historical_loader import (  # type: ignore
            load_signals,
            load_history,
            make_panel,
        )
        from src.ets.backtest.performance_metrics import (  # type: ignore
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

    # Compute + persist
    m = compute_metrics(panel)
    save_artifacts(reports, m)  # should write summary CSV/JSON
    save_perf(metrics, time.perf_counter() - t0, len(panel))  # write perf JSON

    print("[OK] Backtest complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
