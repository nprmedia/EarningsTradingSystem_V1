# src/ets/backtest/historical_loader.py
"""
Historical + signal loading and panel builder for ETS.

Design goals
------------
- Zero external deps beyond pandas/numpy.
- Robust to fixture variation (extra/missing columns).
- Deterministic merge: (ticker, date) is the key.
- Auto-compute close_next and ret_next when possible.
- Clear, helpful errors with actionable hints.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


# --------- Utilities ---------
def _require_cols(df: pd.DataFrame, required: Iterable[str], name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{name}] missing required columns: {missing}. "
            f"Found columns: {list(df.columns)}"
        )


def _coerce_date(df: pd.DataFrame, col: str = "date") -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.normalize()
    return df


def _normalize_ticker(df: pd.DataFrame, col: str = "ticker") -> pd.DataFrame:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.upper()
    return df


def _safe_read_csv(path: str | Path, name: str) -> pd.DataFrame:
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"[{name}] file not found: {p}")
    try:
        return pd.read_csv(p)
    except Exception as e:
        raise RuntimeError(f"[{name}] failed to read CSV: {p} ({e})") from e


# --------- Public API ---------
@dataclass(frozen=True)
class PanelConfig:
    """Knobs for panel assembly (future-proof)."""

    require_signal: bool = False  # if True, fail when signal is missing
    min_history_rows: int = 1  # fail if fewer than this
    allow_duplicates: bool = False  # if False, de-dup (ticker,date) on load


def load_signals(
    path: str | Path, *, config: PanelConfig | None = None
) -> pd.DataFrame:
    """
    Load signals CSV. Expected columns (flexible):
      - required: ticker, date
      - optional: signal (float), rank, weight, any others
    If 'signal' missing and config.require_signal=True -> error.
    """
    cfg = config or PanelConfig()
    df = _safe_read_csv(path, "signals")
    df = _coerce_date(df, "date")
    df = _normalize_ticker(df, "ticker")

    _require_cols(df, ["ticker", "date"], "signals")

    # If duplicates exist, resolve deterministically (keep last)
    if not cfg.allow_duplicates:
        df = df.sort_values(["ticker", "date"]).drop_duplicates(
            ["ticker", "date"], keep="last"
        )

    # If no 'signal', create neutral zero unless explicitly required
    if "signal" not in df.columns:
        if cfg.require_signal:
            raise ValueError("[signals] 'signal' column required but missing.")
        df["signal"] = 0.0

    # Enforce numeric type for signal
    df["signal"] = pd.to_numeric(df["signal"], errors="coerce").fillna(0.0)

    return df.reset_index(drop=True)


def load_history(
    path: str | Path, *, config: PanelConfig | None = None
) -> pd.DataFrame:
    """
    Load historical prices CSV. Expected columns (flexible):
      - required: ticker, date, close
      - optional: close_next (will be derived if missing)
    """
    cfg = config or PanelConfig()
    df = _safe_read_csv(path, "history")
    df = _coerce_date(df, "date")
    df = _normalize_ticker(df, "ticker")

    _require_cols(df, ["ticker", "date", "close"], "history")

    # Basic hygiene
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["ticker", "date", "close"])

    if len(df) < cfg.min_history_rows:
        raise ValueError(f"[history] too few rows: {len(df)} < {cfg.min_history_rows}")

    # Optional deduping
    if not cfg.allow_duplicates:
        df = df.sort_values(["ticker", "date"]).drop_duplicates(
            ["ticker", "date"], keep="last"
        )

    # Derive close_next when missing
    if "close_next" not in df.columns:
        df = df.sort_values(["ticker", "date"])
        df["close_next"] = df.groupby("ticker", sort=False)["close"].shift(-1)

    return df.reset_index(drop=True)


def make_panel(
    signals: pd.DataFrame, history: pd.DataFrame, *, config: PanelConfig | None = None
) -> pd.DataFrame:
    """
    Build the backtest panel by merging signals onto history on (ticker, date),
    then computing forward return (ret_next) where possible.

    Output columns (at minimum):
      ticker, date, close, close_next, ret_next, signal
    """
    cfg = config or PanelConfig()

    # Validate inputs
    _require_cols(signals, ["ticker", "date", "signal"], "signals")
    _require_cols(history, ["ticker", "date", "close"], "history")

    # Merge signals onto price history (left = history to keep price continuity)
    base = history[["ticker", "date", "close", "close_next"]].copy()
    out = base.merge(
        signals[["ticker", "date", "signal"]],
        on=["ticker", "date"],
        how="left",
        validate="one_to_one",
    )

    # Missing signals become neutral zero unless explicitly disallowed
    if cfg.require_signal and out["signal"].isna().any():
        missing = int(out["signal"].isna().sum())
        raise ValueError(
            f"[panel] {missing} rows missing 'signal' but require_signal=True."
        )
    out["signal"] = out["signal"].fillna(0.0)

    # Compute next-day return where we have close_next
    if "close_next" not in out.columns:
        out["close_next"] = np.nan

    with np.errstate(divide="ignore", invalid="ignore"):
        out["ret_next"] = (out["close_next"] - out["close"]) / out["close"]
    out["ret_next"] = out["ret_next"].replace([np.inf, -np.inf], np.nan)

    # Order + types
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)

    # Helpful stats (not required but nice for logs/metrics)
    out.attrs["n_rows"] = len(out)
    out.attrs["n_tickers"] = out["ticker"].nunique()
    if out["date"].notna().any():
        out.attrs["date_range"] = (
            pd.to_datetime(out["date"]).min(),
            pd.to_datetime(out["date"]).max(),
        )

    return out
