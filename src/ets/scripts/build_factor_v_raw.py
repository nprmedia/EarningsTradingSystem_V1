from __future__ import annotations
import argparse
from typing import List, Dict
import pandas as pd
from ets.factors.cache_utils import (
    read_symbols,
    load_daily_cache,
    save_daily_cache,
    fetch_daily_batch,
    update_factors_csv,
)


def compute_v_raw(volume: pd.Series, baseline_window: int = 20) -> float:
    v = volume.dropna()
    if len(v) < baseline_window + 1:
        return 0.0
    today = float(v.iloc[-1])
    base = float(v.iloc[-(baseline_window + 1) : -1].median())
    if base <= 0:
        return 0.0
    return float((today / base) - 1.0)


def main():
    ap = argparse.ArgumentParser(
        description="Build V_raw (volume surge) into out/factors_latest.csv"
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    ap.add_argument("--lookback", type=int, default=35, help="days of daily bars")
    ap.add_argument(
        "--window", type=int, default=20, help="baseline window for median volume"
    )
    args = ap.parse_args()

    symbols: List[str] = read_symbols(args.symbols)
    cached, misses = {}, []
    for s in symbols:
        c = load_daily_cache(s)
        if c is not None and len(c) >= args.window + 1:
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

    v_vals: Dict[str, float] = {}
    for s in symbols:
        df = cached.get(s) or fetched.get(s)
        if df is None or df.empty or "Volume" not in df.columns:
            v_vals[s] = 0.0
            continue
        v_vals[s] = compute_v_raw(df["Volume"], baseline_window=args.window)

    update_factors_csv(symbols, "V_raw", v_vals)
    print("[DONE] V_raw complete")


if __name__ == "__main__":
    main()
