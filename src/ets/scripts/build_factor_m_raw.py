"""
Build M_raw momentum factor across tickers.

Features:
- Pulls OHLCV data (Yahoo → Stooq fallback)
- Computes log-return momentum (M_raw)
- Optional mock data (--mock) for offline runs
- Optional debug mode (--debug) for verbose progress
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

import logging
import math

import numpy as np
import pandas as pd

from ets.data.providers.stooq_client import fetch_daily_ohlc as fetch_stooq
from ets.data.providers.yahoo_direct_client import fetch_daily_ohlc as fetch_yahoo
from ets.utils import ensure_dir

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


# ---------------------------------------------------------------------------
# Data Fetching
# ---------------------------------------------------------------------------
def fetch_daily(
    symbols: list[str], lookback_days: int = 30, mock: bool = False
) -> dict[str, pd.DataFrame]:
    """
    Fetch daily OHLCV bars for given symbols.

    When mock=True, generates synthetic ascending close prices.
    Returns dict[symbol] -> DataFrame(Date index, O,H,L,C,V).
    """
    out: dict[str, pd.DataFrame] = {}

    for sym in symbols:
        df = None
        if mock:
            # Generate fake price data for offline testing
            idx = pd.date_range(end=pd.Timestamp.today(), periods=lookback_days)
            prices = np.linspace(100, 110, lookback_days) + np.random.normal(0, 0.5, lookback_days)
            df = pd.DataFrame(
                {
                    "date": idx,
                    "open": prices - 0.3,
                    "high": prices + 0.3,
                    "low": prices - 0.6,
                    "close": prices,
                    "volume": np.random.randint(1e5, 5e5, lookback_days),
                }
            )
        else:
            try:
                data = fetch_yahoo(sym)
                if data is not None and not pd.DataFrame(data).empty:
                    df = pd.DataFrame(data)
            except Exception as e:
                logger.warning(f"[Yahoo fail] {sym}: {e}")

            if df is None:
                try:
                    data = fetch_stooq(sym)
                    if data is not None and not pd.DataFrame(data).empty:
                        df = pd.DataFrame(data)
                except Exception as e:
                    logger.warning(f"[Stooq fail] {sym}: {e}")

        if df is not None:
            df = df.tail(lookback_days)
            out[sym] = df
        else:
            logger.error(f"[No data] {sym}")

    return out


# ---------------------------------------------------------------------------
# Factor Computation
# ---------------------------------------------------------------------------
def compute_m_raw(close: pd.Series, window: int = 10) -> float:
    """
    Compute M_raw = ln(C_t / C_(t-window)).
    Returns 0.0 if insufficient or invalid data.
    """
    close = close.dropna()
    if len(close) <= window:
        return 0.0
    try:
        c_t = float(close.iloc[-1])
        c_w = float(close.iloc[-(window + 1)])
        return math.log(c_t / c_w) if c_w > 0 else 0.0
    except Exception as e:
        logger.warning(f"[compute_m_raw] {e}")
        return 0.0


def build_factor_m_raw(
    symbols: list[str],
    lookback_days: int = 30,
    window: int = 10,
    mock: bool = False,
    debug: bool = False,
) -> pd.DataFrame:
    """Compute M_raw values for all symbols and return a tidy DataFrame."""
    data_map = fetch_daily(symbols, lookback_days, mock=mock)
    results: dict[str, float] = {}

    for sym, df in data_map.items():
        if "close" not in df:
            logger.warning(f"[Missing close] {sym}")
            continue
        results[sym] = compute_m_raw(df["close"], window)
        if debug:
            logger.info(f"[{sym}] M_raw={results[sym]:.5f}")

    return pd.DataFrame({"symbol": list(results.keys()), "M_raw": list(results.values())})


# ---------------------------------------------------------------------------
# CLI Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    """CLI interface for factor generation."""
    import argparse

    parser = argparse.ArgumentParser(description="Compute M_raw momentum factor.")
    parser.add_argument("--symbols", type=str, default="tickers.csv")
    parser.add_argument("--lookback", type=int, default=30)
    parser.add_argument("--window", type=int, default=10)
    parser.add_argument("--out", type=str, default="out/factors_m_raw.csv")
    parser.add_argument("--mock", action="store_true", help="Use synthetic data for testing.")
    parser.add_argument("--debug", action="store_true", help="Print each computed value.")
    args = parser.parse_args()

    # Load symbol list
    if os.path.exists(args.symbols):
        df = pd.read_csv(args.symbols)
        symbols = df.iloc[:, 0].astype(str).tolist()
    else:
        symbols = ["AAPL", "MSFT", "TSLA"]
        logger.warning(f"[Missing tickers file] Using defaults: {symbols}")

    df_factors = build_factor_m_raw(
        symbols, args.lookback, args.window, mock=args.mock, debug=args.debug
    )

    if args.debug:
        logger.info(f"\n{df_factors}")

    ensure_dir(os.path.dirname(args.out))
    df_factors.to_csv(args.out, index=False)
    logger.info(f"[OK] Saved {args.out} with {len(df_factors)} rows.")


if __name__ == "__main__":
    main()
