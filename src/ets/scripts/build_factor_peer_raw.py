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
from ets.factors.sector_utils import load_sector_profile


def log_rets(close: pd.Series) -> pd.Series:
    c = close.dropna().astype(float)
    return np.log(c / c.shift(1))


def main():  # noqa: C901
    ap = argparse.ArgumentParser(
        description="Build PEER_raw (corr with sector peer median returns over 20d)"
    )
    ap.add_argument("--symbols", default="", help="Path to tickers CSV")
    ap.add_argument("--lookback", type=int, default=45, help="days of daily bars")
    ap.add_argument("--window", type=int, default=20, help="lookback window")
    ap.add_argument(
        "--max-peers",
        type=int,
        default=10,
        help="number of peers to include (per sector)",
    )
    args = ap.parse_args()

    symbols: list[str] = read_symbols(args.symbols)
    prof = load_sector_profile()
    if prof.empty:
        # no sector info -> zeros
        update_factors_csv(symbols, "PEER_raw", {s: 0.0 for s in symbols})
        print("[DONE] PEER_raw complete (no sector_profile, zeros)")
        return

    prof = prof[prof["symbol"].isin(symbols)]
    sym_to_sector = dict(zip(prof["symbol"], prof["sector"], strict=False))

    # Build sector->peer list from entire cached profile (not only today's symbols)
    sector_peers: dict[str, list[str]] = {}
    for sec, sdf in prof.groupby("sector"):
        sector_peers[sec] = sorted(sdf["symbol"].unique().tolist())

    # Load caches/fetch for all symbols in union (targets + peers)
    all_syms = sorted(set(symbols) | set(sum(sector_peers.values(), [])))
    cached, miss = {}, []
    for s in all_syms:
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

    vals: dict[str, float] = {}
    for s in symbols:
        sec = sym_to_sector.get(s, "Unknown")
        peers = [p for p in sector_peers.get(sec, []) if p != s][: args.max_peers]
        if len(peers) < 2:
            vals[s] = 0.0
            continue
        r_s = log_rets(cached[s]["Close"]).tail(args.window)
        # build peer median returns series (aligned)
        peer_mat = []
        for p in peers:
            if p not in cached:
                continue
            rr = log_rets(cached[p]["Close"]).tail(args.window)
            peer_mat.append(rr.reset_index(drop=True))
        if len(peer_mat) < 2:
            vals[s] = 0.0
            continue
        peer_df = pd.concat(peer_mat, axis=1)
        med = peer_df.median(axis=1)
        if r_s.isna().all() or med.isna().all():
            vals[s] = 0.0
            continue
        corr = r_s.reset_index(drop=True).corr(med, method="pearson")
        vals[s] = float(0.0 if np.isnan(corr) else corr)

    update_factors_csv(symbols, "PEER_raw", vals)
    print("[DONE] PEER_raw complete")


if __name__ == "__main__":
    main()
