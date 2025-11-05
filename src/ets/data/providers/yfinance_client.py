import contextlib
import io
import sys
import time

import requests
import yfinance as yf

# ---- Robust session: browser UA + retry on transient failures ----
_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    }
)
_RETRIES = 2  # keep light to avoid spammy retries
_BACKOFF = 0.8  # seconds


@contextlib.contextmanager
def _suppress_prints():
    """Silence yfinance prints ('Failed to get ticker', JSONDecodeError chatter)."""
    new_out, new_err = io.StringIO(), io.StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield
    finally:
        sys.stdout, sys.stderr = old_out, old_err


def _download_1d_1d(symbol: str) -> dict | None:
    """Use yf.download, but silence prints and handle errors."""
    for i in range(_RETRIES):
        try:
            with _suppress_prints():
                df = yf.download(
                    tickers=symbol,
                    period="5d",  # improve odds of at least 1 full row
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    prepost=False,
                    session=_SESSION,
                    threads=False,
                )
            if df is not None and not df.empty:
                row = df.dropna().iloc[-1]
                return {
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "last": float(row["Close"]),
                    "volume": float(row.get("Volume", 0.0) or 0.0),
                }
        except Exception:
            pass
        time.sleep(_BACKOFF * (i + 1))
    return None


def fetch_quote_basic(ticker: str) -> dict | None:
    """
    Returns dict: last, open, high, low, volume
    Robust against Yahoo quirks (silenced + retries).
    """
    data = _download_1d_1d(ticker)
    if data:
        return data

    # Final fallback: Ticker.history (also silenced)
    for i in range(_RETRIES):
        try:
            with _suppress_prints():
                t = yf.Ticker(ticker, session=_SESSION)
                day = t.history(period="5d", interval="1d", prepost=False)
            if day is not None and not day.empty:
                row = day.dropna().iloc[-1]
                return {
                    "last": float(row["Close"]),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "volume": float(row.get("Volume", 0.0) or 0.0),
                }
        except Exception:
            pass
        time.sleep(_BACKOFF * (i + 1))
    return None
