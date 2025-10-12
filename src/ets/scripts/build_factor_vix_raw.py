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


def vix_delta(series: pd.Series) -> pd.Series:
    s = series.dropna().astype(float)
    return s.diff()


def series_returns(close: pd.Series) -> pd.Series:
    c = close.dropna().astype(float)
    r = np.log(c / c.shift(1))
    return r


def main():
    ap = argparse.ArgumentParser(
        description="Build VIX_raw (corr of symbol returns with ΔVIX over 10d)"
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    ap.add_argument("--lookback", type=int, default=35, help="days of daily bars")
    ap.add_argument("--window", type=int, default=10, help="lookback window")
    args = ap.parse_args()

    symbols: List[str] = read_symbols(args.symbols)
    # Load symbol caches/fetch
    cached, miss = {}, []
    for s in symbols:
        c = load_daily_cache(s)
        if c is not None and len(c) > args.window:
            cached[s] = c
        else:
            miss.append(s)
    fetched = {}
    if miss:
        print(f"[INFO] fetching daily bars for {len(miss)} symbols via Yahoo...")
        fetched = fetch_daily_batch(miss, args.lookback)
        for s, df in fetched.items():
            if df is not None and not df.empty:
                save_daily_cache(s, df)

    # VIX
    vx = "^VIX"
    vix_df = load_daily_cache(vx)
    if vix_df is None or len(vix_df) <= args.window + 1:
        print("[INFO] fetching ^VIX via Yahoo...")
        vfetch = fetch_daily_batch([vx], args.lookback).get(vx)
        if vfetch is not None and not vfetch.empty:
            save_daily_cache(vx, vfetch)
            vix_df = vfetch
    vals: Dict[str, float] = {}
    if vix_df is None or vix_df.empty:
        # no vix → 0
        for s in symbols:
            vals[s] = 0.0
        update_factors_csv(symbols, "VIX_raw", vals)
        print("[DONE] VIX_raw complete (no VIX data, zeros)")
        return

    dvix = vix_delta(vix_df["Close"]).tail(args.window)
    for s in symbols:
        df = cached.get(s) or fetched.get(s)
        if df is None or df.empty:
            vals[s] = 0.0
            continue
        r = series_returns(df["Close"]).tail(args.window)
        if len(r) != len(dvix) or r.isna().all() or dvix.isna().all():
            vals[s] = 0.0
            continue
        corr = r.corr(dvix, method="pearson")
        if np.isnan(corr):
            corr = 0.0
        vals[s] = float(corr)

    update_factors_csv(symbols, "VIX_raw", vals)
    print("[DONE] VIX_raw complete")


if __name__ == "__main__":
    main()
