from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Free data
try:
    import yfinance as yf  # type: ignore
except Exception:
    print("[FATAL] yfinance is required (pip install yfinance)", file=sys.stderr)
    raise

OUT_DIR = Path("out")
DAILY_DIR = OUT_DIR / "cache" / "daily"
FACTORS_CSV = OUT_DIR / "factors_latest.csv"


def read_symbols(symbols_arg: str | None) -> list[str]:
    """Read symbols from a CSV (first col) or fallbacks (out/quote_results.csv, tickers.csv)."""

    def _from_file(p: Path) -> list[str]:
        raw = p.read_text().strip().splitlines()
        syms = []
        for ln in raw:
            s = ln.strip().split(",")[0].upper().lstrip("$")
            if s:
                syms.append(s)
        return sorted(set(syms))

    if symbols_arg:
        p = Path(symbols_arg)
        if p.exists():
            return _from_file(p)

    qp = OUT_DIR / "quote_results.csv"
    if qp.exists():
        df = pd.read_csv(qp)
        if "symbol" in df.columns:
            return sorted(set(df["symbol"].astype(str).str.upper()))
    tc = Path("tickers.csv")
    if tc.exists():
        return _from_file(tc)
    raise SystemExit(
        "[FATAL] Provide --symbols CSV (or ensure out/quote_results.csv or tickers.csv exists)."
    )


def load_daily_cache(symbol: str) -> pd.DataFrame | None:
    f = DAILY_DIR / f"{symbol}.parquet"
    if f.exists():
        try:
            df = pd.read_parquet(f)
            if isinstance(df.index, pd.DatetimeIndex) and not df.empty:
                return df
        except Exception:
            return None
    return None


def save_daily_cache(symbol: str, df: pd.DataFrame) -> None:
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    (DAILY_DIR / f"{symbol}.parquet").unlink(missing_ok=True)
    df.to_parquet(DAILY_DIR / f"{symbol}.parquet")


def fetch_daily_batch(symbols: list[str], lookback_days: int = 35) -> dict[str, pd.DataFrame]:
    """Batch-download daily bars from Yahoo; returns dict[symbol] -> df(OHLCV)."""
    if not symbols:
        return {}
    df = yf.download(
        symbols,
        period=f"{max(lookback_days,30)}d",
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
        sub = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        sub.index = pd.to_datetime(sub.index)
        out[symbols[0]] = sub
    return out


def update_factors_csv(symbols: list[str], column: str, values: dict[str, float]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(FACTORS_CSV) if FACTORS_CSV.exists() else pd.DataFrame({"symbol": symbols})
    df["symbol"] = df["symbol"].astype(str).str.upper()
    upd = pd.DataFrame({"symbol": list(values.keys()), column: list(values.values())})
    df = df.merge(upd, on="symbol", how="left")
    # Resolve potential _x/_y if column existed
    if f"{column}_x" in df.columns and f"{column}_y" in df.columns:
        df[column] = df[f"{column}_y"].fillna(df[f"{column}_x"])
        df.drop(columns=[f"{column}_x", f"{column}_y"], inplace=True)
    if column not in df.columns:
        df[column] = 0.0
    df.to_csv(FACTORS_CSV, index=False)
    print(f"[OK] wrote {FACTORS_CSV} (+{column}) | rows={len(df)}")
