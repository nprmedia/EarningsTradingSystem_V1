# src/ets/backtest/performance_metrics.py
"""
Performance metrics and artifact saving for ETS backtests.

Design goals
------------
- Deterministic output: no random state, no nondeterministic rounding.
- Works with both real and mock data (NaN-safe).
- Writes CSV + JSON to reports/ and metrics/ directories.
- CI-safe (no absolute paths, no unhandled exceptions).
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd


# --------- Core Metrics ---------
def _safe_mean(x: pd.Series | np.ndarray) -> float:
    """Return mean ignoring NaN and inf."""
    if isinstance(x, pd.Series):
        x = x.to_numpy()
    x = np.array(x, dtype=float)
    x = x[np.isfinite(x)]
    return float(np.mean(x)) if len(x) else float("nan")


def _safe_std(x: pd.Series | np.ndarray) -> float:
    """Return standard deviation ignoring NaN and inf."""
    if isinstance(x, pd.Series):
        x = x.to_numpy()
    x = x[np.isfinite(x)]
    return float(np.std(x, ddof=1)) if len(x) > 1 else float("nan")


def _annualize_daily_return(ret: float, days: int | float) -> float:
    """Convert average daily return to annualized assuming ~252 trading days."""
    if not np.isfinite(ret):
        return float("nan")
    return (1 + ret) ** (252 / max(days, 1)) - 1


def _sharpe_ratio(rets: pd.Series) -> float:
    """Compute daily Sharpe ratio (annualized, rf=0)."""
    mu = _safe_mean(rets)
    sigma = _safe_std(rets)
    if not np.isfinite(mu) or not np.isfinite(sigma) or sigma == 0:
        return float("nan")
    daily_sharpe = mu / sigma
    return daily_sharpe * math.sqrt(252)


def compute_metrics(panel: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute performance metrics from a backtest panel.
    Required columns: ['ticker', 'date', 'signal', 'ret_next']
    """
    if not isinstance(panel, pd.DataFrame):
        raise TypeError("panel must be a pandas DataFrame")

    required = {"ticker", "date", "signal", "ret_next"}
    missing = required - set(panel.columns)
    if missing:
        raise ValueError(f"compute_metrics missing columns: {missing}")

    df = panel.copy()
    df["weighted_ret"] = df["signal"] * df["ret_next"]
    df["weighted_ret"] = pd.to_numeric(df["weighted_ret"], errors="coerce")

    avg_signal = _safe_mean(df["signal"])
    avg_ret = _safe_mean(df["ret_next"])
    avg_weighted = _safe_mean(df["weighted_ret"])
    sharpe = _sharpe_ratio(df["weighted_ret"])
    hit_rate = float(np.nanmean(df["weighted_ret"] > 0)) if len(df) else float("nan")

    total_days = df["date"].nunique()
    ann_ret = _annualize_daily_return(avg_weighted, total_days)

    metrics = {
        "n_rows": int(len(df)),
        "n_tickers": int(df["ticker"].nunique()),
        "days": int(total_days),
        "mean_signal": avg_signal,
        "mean_ret_next": avg_ret,
        "mean_weighted_ret": avg_weighted,
        "annualized_return": ann_ret,
        "sharpe": sharpe,
        "hit_rate": hit_rate,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    return metrics


# --------- Artifact Persistence ---------
def save_artifacts(
    reports_dir: Path, metrics: Dict[str, Any], panel: pd.DataFrame
) -> None:
    """
    Save human-readable outputs (CSV + JSON) to reports/.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)
    csv_path = reports_dir / "panel_sample.csv"
    json_path = reports_dir / "metrics_summary.json"

    # Clip large panel for CI
    sample = panel.head(50).copy()
    sample.to_csv(csv_path, index=False)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

    print(f"[OK] Reports saved: {csv_path.name}, {json_path.name}")


def save_perf(metrics_dir: Path, elapsed_s: float, n_rows: int) -> None:
    """
    Save machine-readable performance timing for CI validation.
    """
    metrics_dir.mkdir(parents=True, exist_ok=True)
    perf_path = metrics_dir / "perf.json"
    data = {"elapsed_s": round(float(elapsed_s), 4), "rows": int(n_rows)}
    with open(perf_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[OK] Performance metrics saved: {perf_path.name}")
