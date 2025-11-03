#!/usr/bin/env python3
"""
Run a deterministic ETS backtest.

- Works in GitHub Actions and local dev
- No relative-import failures
- Produces /reports + /metrics artifacts
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from pathlib import Path

import pandas as pd


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


def _sanitize_value(val):
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _sanitize_dict(data):
    return {k: _sanitize_value(v) for k, v in data.items()}


def _compute_trade_metrics(panel: pd.DataFrame, signals: pd.DataFrame):
    merged = panel.merge(
        signals[[c for c in ["ticker", "date", "sector", "rank"] if c in signals.columns]],
        on=["ticker", "date"],
        how="left",
    )
    trades = merged[merged["signal"] != 0].copy()
    if trades.empty:
        cols = ["date", "ticker", "sector", "signal", "ret_next", "hit"]
        return {
            "hit_rate": 0.0,
            "avg_return": 0.0,
            "vol_adj_roi": 0.0,
            "top_precision": 0.0,
        }, pd.DataFrame(columns=cols)

    trades["weighted"] = trades["signal"] * trades["ret_next"]
    trades["hit"] = trades["weighted"] > 0

    avg_return = float(trades["ret_next"].mean())
    std = float(trades["ret_next"].std(ddof=1)) if len(trades) > 1 else 0.0
    vol_adj_roi = avg_return / std if std else 0.0

    min_rank = trades["rank"].min() if "rank" in trades.columns else None
    if min_rank is not None and not pd.isna(min_rank):
        top = trades[trades["rank"] == min_rank]
        top_precision = float(top["hit"].mean()) if len(top) else 0.0
    else:
        top_precision = float(trades["hit"].mean())

    trade_records = trades[["date", "ticker", "sector", "signal", "ret_next", "hit"]].copy()
    trade_records["date"] = pd.to_datetime(trade_records["date"]).dt.strftime("%Y-%m-%d")
    trade_records["hit"] = trade_records["hit"].astype(int)

    trade_metrics = {
        "hit_rate": float(trades["hit"].mean()),
        "avg_return": avg_return,
        "vol_adj_roi": vol_adj_roi,
        "top_precision": top_precision,
    }

    return trade_metrics, trade_records


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
        from ets.backtest.historical_loader import (
            load_signals,
            load_history,
            make_panel,
        )
        from ets.backtest.performance_metrics import (
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
        cleaned_metrics = _sanitize_dict(metrics)
        save_artifacts(reports_dir, cleaned_metrics, panel)
        perf_data, _perf_path = save_perf(metrics_dir, time.perf_counter() - t0, len(panel))
        trade_metrics, trade_records = _compute_trade_metrics(panel, sig)
        trade_metrics = _sanitize_dict(trade_metrics)

        out_dir = root / "out"
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_json = out_dir / "backtest_summary.json"
        summary_payload = {**cleaned_metrics, **trade_metrics, "count": int(len(panel))}
        summary_json.write_text(json.dumps(_sanitize_dict(summary_payload), indent=2))

        summary_csv = reports_dir / "backtest_summary.csv"
        with summary_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(cleaned_metrics.keys()))
            writer.writeheader()
            writer.writerow(cleaned_metrics)

        sector_path = out_dir / "sector_accuracy.csv"
        trade_records.to_csv(sector_path, index=False)

        backtest_perf = metrics_dir / "backtest_perf.json"
        perf_payload = {**perf_data, **trade_metrics}
        perf_payload = _sanitize_dict(perf_payload)
        backtest_perf.write_text(json.dumps(perf_payload, indent=2))
    except Exception as e:
        print(f"[FAIL] {type(e).__name__}: {e}")
        return 1

    print("[OK] Backtest complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
