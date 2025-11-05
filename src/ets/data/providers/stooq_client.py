import csv
import io
import time

import requests

_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/csv,*/*;q=0.1",
        "Connection": "keep-alive",
    }
)
_RETRIES = 3
_BACKOFF = 0.6


def _try_stooq(symbol: str):
    # Stooq tends to use .us suffix for US stocks
    candidates = []
    s = symbol.lower()
    candidates = [s, s.replace(".", "-") + ".us"] if "." in s else [f"{s}.us", s]
    s = symbol.lower()
    candidates = [s, s.replace(".", "-") + ".us"] if "." in s else [f"{s}.us", s]
    candidates = [s, s.replace(".", "-") + ".us"] if "." in s else [f"{s}.us", s]
    for cand in candidates:
        for i in range(_RETRIES):
            try:
                url = f"https://stooq.com/q/d/l/?s={cand}&i=d"
                r = _SESSION.get(url, timeout=8)
                if r.status_code >= 500:
                    time.sleep(_BACKOFF * (i + 1))
                    continue
                r.raise_for_status()
                if not r.text or "404 Not Found" in r.text:
                    break
                f = io.StringIO(r.text.strip())
                reader = list(csv.DictReader(f))
                if not reader:
                    break
                row = reader[-1]
                # Skip if missing values
                if not row.get("Open") or not row.get("Close"):
                    break
                return {
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "last": float(row["Close"]),
                    "volume": float(row.get("Volume") or 0.0),
                }
            except Exception:
                time.sleep(_BACKOFF * (i + 1))
    return None


def fetch_daily_ohlc(symbol: str):
    return _try_stooq(symbol)
