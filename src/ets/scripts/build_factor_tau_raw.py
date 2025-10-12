from __future__ import annotations
import argparse
from typing import List, Dict
import numpy as np
import pandas as pd
from ets.factors.cache_utils import (
    read_symbols,
    load_daily_cache,
    save_daily_cache,
    fetch_daily_batch,
    update_factors_csv,
)


def compute_tau_raw(close: pd.Series, window: int = 20) -> float:
    c = close.dropna().astype(float)
    if len(c) < window:
        return 0.0
    sma = float(c.tail(window).mean())
    std = float(c.tail(window).std(ddof=0))
    ct = float(c.iloc[-1])
    denom = max(std, 1e-9)
    z = (ct - sma) / denom
    tau = -z  # stretched up => negative; stretched down => positive (reversion up)
    if np.isfinite(tau):
        return float(tau)
    return 0.0


def main():
    ap = argparse.ArgumentParser(
        description="Build tau_raw (mean reversion stretch) into out/factors_latest.csv"
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    ap.add_argument("--lookback", type=int, default=40, help="days of daily bars")
    ap.add_argument("--window", type=int, default=20, help="window for SMA/stdev")
    args = ap.parse_args()

    symbols: List[str] = read_symbols(args.symbols)
    cached, misses = {}, []
    for s in symbols:
        c = load_daily_cache(s)
        if c is not None and len(c) >= args.window:
            cached[s] = c
        else:
            misses.append(s)
    fetched = {}
    if misses:
        print(f"[INFO] fetching daily bars for {len(misses)} symbols via Yahoo...")
        fetched = fetch_daily_batch(misses, args.lookback)
        for s, df in fetched.items():
            if df is not None and not df.empty:
                save_daily_cache(s, df)

    vals: Dict[str, float] = {}
    for s in symbols:
        df = cached.get(s) or fetched.get(s)
        if df is None or df.empty:
            vals[s] = 0.0
            continue
        vals[s] = compute_tau_raw(df["Close"], window=args.window)

    update_factors_csv(symbols, "tau_raw", vals)
    print("[DONE] tau_raw complete")


if __name__ == "__main__":
    main()
