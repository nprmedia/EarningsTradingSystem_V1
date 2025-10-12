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
from ets.factors.sector_utils import load_sector_profile, load_sector_etf_map


def log_rets(close: pd.Series) -> pd.Series:
    c = close.dropna().astype(float)
    return np.log(c / c.shift(1))


def main():
    ap = argparse.ArgumentParser(
        description="Build ETFF_raw (corr of stock returns with sector ETF dollar volume over 20d)"
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    ap.add_argument("--lookback", type=int, default=45, help="days of daily bars")
    ap.add_argument("--window", type=int, default=20, help="lookback window")
    args = ap.parse_args()

    symbols: List[str] = read_symbols(args.symbols)
    prof = load_sector_profile()
    etf_map = load_sector_etf_map()
    sym_to_sector = dict(zip(prof["symbol"], prof["sector"]))

    # Determine ETFs needed
    etfs_needed = sorted(
        {
            etf_map.get(sym_to_sector.get(s, ""), None)
            for s in symbols
            if etf_map.get(sym_to_sector.get(s, ""), None)
        }
    )

    # Load caches/fetch symbols
    cached, miss = {}, []
    for s in symbols:
        c = load_daily_cache(s)
        if c is not None and len(c) > args.window:
            cached[s] = c
        else:
            miss.append(s)
    if miss:
        print(f"[INFO] fetching daily bars for {len(miss)} symbols via Yahoo...")
        fetched = fetch_daily_batch(miss, args.lookback)
        for s, df in fetched.items():
            if df is not None and not df.empty:
                save_daily_cache(s, df)
                cached[s] = df

    # Load caches/fetch ETFs
    etf_cached, etf_miss = {}, []
    for e in etfs_needed:
        c = load_daily_cache(e)
        if c is not None and len(c) > args.window:
            etf_cached[e] = c
        else:
            etf_miss.append(e)
    if etf_miss:
        print(
            f"[INFO] fetching daily bars for {len(etf_miss)} sector ETFs via Yahoo..."
        )
        ff = fetch_daily_batch(etf_miss, args.lookback)
        for e, df in ff.items():
            if df is not None and not df.empty:
                save_daily_cache(e, df)
                etf_cached[e] = df

    vals: Dict[str, float] = {}
    for s in symbols:
        sec = sym_to_sector.get(s, "Unknown")
        etf = etf_map.get(sec)
        if not etf or etf not in etf_cached:
            vals[s] = 0.0
            continue
        df_s = cached.get(s)
        df_e = etf_cached.get(etf)
        if df_s is None or df_s.empty or df_e is None or df_e.empty:
            vals[s] = 0.0
            continue

        r_s = log_rets(df_s["Close"]).tail(args.window)
        # ETF dollar volume
        dv = (
            (df_e["Close"].astype(float) * df_e["Volume"].astype(float))
            .dropna()
            .tail(args.window)
        )
        if len(r_s) != len(dv) or r_s.isna().all() or dv.isna().all():
            vals[s] = 0.0
            continue
        corr = r_s.reset_index(drop=True).corr(
            dv.reset_index(drop=True), method="pearson"
        )
        vals[s] = float(0.0 if np.isnan(corr) else corr)

    update_factors_csv(symbols, "ETFF_raw", vals)
    print("[DONE] ETFF_raw complete")


if __name__ == "__main__":
    main()
