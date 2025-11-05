from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from ets.factors.cache_utils import (
    fetch_daily_batch,
    load_daily_cache,
    read_symbols,
    save_daily_cache,
    update_factors_csv,
)
from ets.factors.sector_utils import load_sector_etf_map, load_sector_profile


def log_return(close: pd.Series, window: int) -> float:
    c = close.dropna().astype(float)
    if len(c) <= window:
        return 0.0
    return float(np.log(c.iloc[-1] / c.iloc[-(window + 1)]))


def main():  # noqa: C901
    ap = argparse.ArgumentParser(
        description="Build SRM_raw (sector-relative momentum: 10d stock r - sector ETF r)"
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    ap.add_argument("--lookback", type=int, default=35, help="days of daily bars")
    ap.add_argument("--window", type=int, default=10, help="lookback window for momentum")
    args = ap.parse_args()

    symbols: list[str] = read_symbols(args.symbols)
    prof = load_sector_profile()
    m = load_sector_etf_map()

    # figure ETFs required
    sym_to_sector = dict(zip(prof["symbol"], prof["sector"], strict=False))
    etfs_needed = sorted(
        {
            m.get(sym_to_sector.get(s, ""), None)
            for s in symbols
            if m.get(sym_to_sector.get(s, ""), None)
        }
    )
    # load caches / fetch for stocks
    cached, misses = {}, []
    for s in symbols:
        c = load_daily_cache(s)
        if c is not None and len(c) > args.window:
            cached[s] = c
        else:
            misses.append(s)
    fetched = {}
    if misses:
        print(f"[INFO] fetching daily bars for {len(misses)} symbols via Yahoo...")
        fetched.update(fetch_daily_batch(misses, args.lookback))
        for s, df in fetched.items():
            if df is not None and not df.empty:
                save_daily_cache(s, df)

    # fetch ETFs
    etf_cached, etf_miss = {}, []
    for e in etfs_needed:
        c = load_daily_cache(e)  # reuse same cache namespace
        if c is not None and len(c) > args.window:
            etf_cached[e] = c
        else:
            etf_miss.append(e)
    if etf_miss:
        print(f"[INFO] fetching daily bars for {len(etf_miss)} sector ETFs via Yahoo...")
        ff = fetch_daily_batch(etf_miss, args.lookback)
        for e, df in ff.items():
            if df is not None and not df.empty:
                save_daily_cache(e, df)
                etf_cached[e] = df

    vals: dict[str, float] = {}
    for s in symbols:
        df = cached.get(s) or fetched.get(s)
        if df is None or df.empty:
            vals[s] = 0.0
            continue
        sector = sym_to_sector.get(s, "Unknown")
        etf = m.get(sector)
        if not etf:
            vals[s] = 0.0
            continue
        edf = etf_cached.get(etf)
        if edf is None or edf.empty:
            vals[s] = 0.0
            continue
        r_sym = log_return(df["Close"], args.window)
        r_etf = log_return(edf["Close"], args.window)
        vals[s] = float(r_sym - r_etf)

    update_factors_csv(symbols, "SRM_raw", vals)
    print("[DONE] SRM_raw complete")


if __name__ == "__main__":
    main()
