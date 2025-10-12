import os
import time
from typing import Optional, Dict, Any
from urllib.parse import urlencode


def _debug(msg: str) -> None:
    if os.getenv("FINNHUB_DEBUG", "0") == "1":
        try:
            print(f"[FH] {msg}")
        except Exception:
            pass


def _get(reg, path, params) -> Optional[Dict[str, Any]]:
    """
    reg: {"session": requests.Session, "base": str, "key": str, "limiter": RateLimiter}
    """
    # rate-limit gate (blocks; not a failure)
    reg["limiter"].acquire()
    sess = reg["session"]
    base = reg.get("base") or os.getenv("FINNHUB_BASE", "https://finnhub.io/api/v1")
    params = dict(params or {})
    params["token"] = reg.get("key") or os.getenv("FINNHUB_API_KEY", "")
    url = f"{base}{path}?{urlencode(params)}"

    r = sess.get(url, timeout=8)
    _debug(f"GET {url} status={r.status_code} bytes={len(r.content)}")
    if r.status_code == 429 or r.status_code >= 500:
        # gentle backoff once, then re-acquire limiter and retry
        time.sleep(0.8)
        reg["limiter"].acquire()
        r = sess.get(url, timeout=8)
        _debug(f"RETRY {url} status={r.status_code} bytes={len(r.content)}")

    if r.status_code != 200:
        _debug(f"body[:240]={r.text[:240]!r}")
        return None

    try:
        out = r.json()
    except Exception as e:
        _debug(f"json_error={e!r} body[:240]={r.text[:240]!r}")
        return None

    # Normalize empty payloads
    if out in (None, {}, []):
        return None
    return out


def quote(reg, symbol: str) -> Optional[Dict[str, Any]]:
    return _get(reg, "/quote", {"symbol": symbol})


def profile2(reg, symbol: str) -> Optional[Dict[str, Any]]:
    return _get(reg, "/stock/profile2", {"symbol": symbol})


def earnings_calendar(reg, _from: str, _to: str) -> Optional[Dict[str, Any]]:
    return _get(reg, "/calendar/earnings", {"from": _from, "to": _to})
