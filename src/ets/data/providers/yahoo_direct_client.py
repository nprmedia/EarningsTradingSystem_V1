import time

import requests

_SESSION = requests.Session()
_SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
)

_BASE = "https://query2.finance.yahoo.com/v8/finance/chart"
_RETRIES = 3
_BACKOFF = 0.8


def fetch_daily_ohlc(symbol: str):
    """Yahoo chart endpoint (no crumb). Returns OHLCV dict or None."""
    params = {
        "range": "5d",
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    for i in range(_RETRIES):
        try:
            r = _SESSION.get(f"{_BASE}/{symbol}", params=params, timeout=8)
            if r.status_code >= 500:
                time.sleep(_BACKOFF * (i + 1))
                continue
            r.raise_for_status()
            j = r.json()
            res = (j.get("chart") or {}).get("result") or []
            if not res:
                time.sleep(_BACKOFF * (i + 1))
                continue
            q = res[0].get("indicators", {}).get("quote", [{}])[0]
            o, h, low, c, v = (
                q.get("open"),
                q.get("high"),
                q.get("low"),
                q.get("close"),
                q.get("volume"),
            )
            # find last non-None candle
            if not c:
                return None
            idx = None
            for k in range(len(c) - 1, -1, -1):
                if (
                    c[k] is not None
                    and o[k] is not None
                    and h[k] is not None
                    and low[k] is not None
                ):
                    idx = k
                    break
            if idx is None:
                return None
            return {
                "open": float(o[idx]),
                "high": float(h[idx]),
                "low": float(low[idx]),
                "last": float(c[idx]),
                "volume": float((v[idx] if v else 0) or 0.0),
            }
        except Exception:
            time.sleep(_BACKOFF * (i + 1))
    return None
