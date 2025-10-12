from __future__ import annotations
import argparse
import sys
from pathlib import Path
from typing import List
import numpy as np
import pandas as pd

# Free data
try:
    import yfinance as yf
except Exception:
    print("[FATAL] yfinance is required (pip install yfinance)", file=sys.stderr)
    raise

CACHE_DIR = Path("out/cache/daily")
OUT_DIR = Path("out")
FACTORS_CSV = OUT_DIR / "factors_latest.csv"


def read_symbols(symbols_arg: str | None) -> List[str]:
    if symbols_arg and Path(symbols_arg).exists():
        # CSV list (first column) or newline list
        raw = Path(symbols_arg).read_text().strip().splitlines()
        syms = []
        for ln in raw:
            s = ln.strip().split(",")[0].upper().lstrip("$")
            if s:
                syms.append(s)
        return sorted(set(syms))
    # Default: try out/quote_results.csv, then tickers.csv
    if (OUT_DIR / "quote_results.csv").exists():
        df = pd.read_csv(OUT_DIR / "quote_results.csv")
        if "symbol" in df.columns:
            return sorted(set(df["symbol"].astype(str).str.upper()))
    if Path("tickers.csv").exists():
        return read_symbols("tickers.csv")
    raise SystemExit(
        "[FATAL] Provide --symbols CSV (or ensure out/quote_results.csv or tickers.csv exists)."
    )


def load_cached(symbol: str) -> pd.DataFrame | None:
    f = CACHE_DIR / f"{symbol}.parquet"
    if f.exists():
        try:
            df = pd.read_parquet(f)
            if isinstance(df.index, pd.DatetimeIndex):
                return df
        except Exception:
            pass
    return None


def save_cache(symbol: str, df: pd.DataFrame) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (CACHE_DIR / f"{symbol}.parquet").unlink(missing_ok=True)
    df.to_parquet(CACHE_DIR / f"{symbol}.parquet")


def fetch_daily(symbols: List[str], lookback_days: int) -> dict[str, pd.DataFrame]:
    """
    Batch-download daily bars; return dict[symbol]->DataFrame(Date index, columns: Open, High, Low, Close, Volume).
    """
    if not symbols:
        return {}
    df = yf.download(
        symbols,
        period=f"{max(lookback_days, 30)}d",
        interval="1d",
        progress=False,
        threads=False,
        group_by="ticker",
        auto_adjust=False,
    )

    out: dict[str, pd.DataFrame] = {}
    if isinstance(df.columns, pd.MultiIndex):
        for sym in symbols:
            sub = df.get(sym)
            if sub is None or sub.empty:
                continue
            sub = sub[["Open", "High", "Low", "Close", "Volume"]].copy()
            sub.index = pd.to_datetime(sub.index)
            out[sym] = sub
    else:
        # Single symbol case
        sub = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        sub.index = pd.to_datetime(sub.index)
        out[symbols[0]] = sub

    return out


def compute_m_raw(close: pd.Series, window: int = 10) -> float:
    """
    M_raw = ln(C_t / C_{t-window})
    Returns 0.0 if insufficient data.
    """
    close = close.dropna()
    if len(close) <= window:
        return 0.0
    c_t = float(close.iloc[-1])
    c_w = float(close.iloc[-(window + 1)])
    if c_t <= 0 or c_w <= 0:
        return 0.0
    return float(np.log(c_t / c_w))


def update_factors_csv(symbols: List[str], m_values: dict[str, float]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if FACTORS_CSV.exists():
        df = pd.read_csv(FACTORS_CSV)
    else:
        df = pd.DataFrame({"symbol": symbols})
    # ensure symbol column normalized
    df["symbol"] = df["symbol"].astype(str).str.upper()
    # left-join M_raw
    upd = pd.DataFrame(
        {"symbol": list(m_values.keys()), "M_raw": list(m_values.values())}
    )
    df = df.merge(upd, on="symbol", how="left")
    # if M_raw arrived as _x/_y due to existing column, resolve to new values
    if "M_raw_x" in df.columns and "M_raw_y" in df.columns:
        df["M_raw"] = df["M_raw_y"].fillna(df["M_raw_x"])
        df = df.drop(columns=["M_raw_x", "M_raw_y"])
    # ensure M_raw exists
    if "M_raw" not in df.columns:
        df["M_raw"] = 0.0
    # write back
    df.to_csv(FACTORS_CSV, index=False)
    print(f"[OK] wrote {FACTORS_CSV} | rows={len(df)}")


def main():
    ap = argparse.ArgumentParser(
        description="Build M_raw (10d log momentum) into out/factors_latest.csv"
    )
    ap.add_argument(
        "--symbols",
        default="",
        help="Path to tickers CSV (first column) or newline list",
    )
    ap.add_argument(
        "--lookback", type=int, default=35, help="Days of daily bars to fetch (min 30)"
    )
    ap.add_argument(
        "--window", type=int, default=10, help="Momentum window (trading days)"
    )
    args = ap.parse_args()

    symbols = read_symbols(args.symbols)
    # Try cache first; fill misses from Yahoo in one batch
    cached: dict[str, pd.DataFrame] = {}
    misses: list[str] = []
    for s in symbols:
        c = load_cached(s)
        if c is not None and (len(c) >= args.window + 1):
            cached[s] = c
        else:
            misses.append(s)

    fetched: dict[str, pd.DataFrame] = {}
    if misses:
        print(f"[INFO] fetching daily bars for {len(misses)} symbols via Yahoo...")
        fetched = fetch_daily(misses, args.lookback)
        # Save to cache
        for s, df in fetched.items():
            if df is not None and not df.empty:
                save_cache(s, df)

    m_vals: dict[str, float] = {}
    for s in symbols:
        df = cached.get(s) or fetched.get(s)
        if df is None or df.empty:
            m_vals[s] = 0.0
            continue
        close = df["Close"].astype(float)
        m_vals[s] = compute_m_raw(close, window=args.window)

    update_factors_csv(symbols, m_vals)
    print("[DONE] M_raw complete")


if __name__ == "__main__":
    main()
