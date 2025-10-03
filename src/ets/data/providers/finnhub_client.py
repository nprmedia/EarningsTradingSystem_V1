import os, time, requests
from typing import Optional

_BASE = "https://finnhub.io/api/v1"
_TOKEN = os.getenv("FINNHUB_TOKEN", "").strip()
_RETRIES = 3
_BACKOFF = 0.8  # seconds

def _get(path: str, params: dict) -> Optional[dict]:
    if not _TOKEN:
        return None
    params = dict(params or {})
    params["token"] = _TOKEN
    for i in range(_RETRIES):
        try:
            r = requests.get(f"{_BASE}{path}", params=params, timeout=8)
            if r.status_code == 429:
                time.sleep(_BACKOFF * (i + 1)); continue
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(_BACKOFF * (i + 1))
    return None

def fetch_quote_basic(symbol: str) -> Optional[dict]:
    """
    Returns dict: last, open, high, low, volume using Finnhub's /quote.
    """
    data = _get("/quote", {"symbol": symbol})
    if not data:
        return None
    c = float(data.get("c") or 0.0)
    o = float(data.get("o") or 0.0)
    h = float(data.get("h") or c)
    l = float(data.get("l") or c)
    v = float(data.get("v") or 0.0)
    return {"last": c, "open": o, "high": h, "low": l, "volume": v}
