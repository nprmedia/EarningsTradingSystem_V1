import time
from typing import Optional, Dict, Any
from urllib.parse import urlencode


def _get(reg, path, params) -> Optional[Dict[str, Any]]:
    reg["limiter"].acquire()
    sess = reg["session"]
    base = reg["base"]
    params = dict(params or {})
    params["token"] = reg["key"]
    url = f"{base}{path}?{urlencode(params)}"
    r = sess.get(url, timeout=8)
    if r.status_code == 429 or r.status_code >= 500:
        time.sleep(0.8)
        reg["limiter"].acquire()
        r = sess.get(url, timeout=8)
    if r.ok:
        try:
            return r.json()
        except Exception:
            return None
    return None


def candles(reg, symbol: str, resolution: str = "D", count: int = 5) -> Optional[Dict]:
    import time as _t

    now = int(_t.time())
    frm = now - int(60 * 60 * 24 * count * 2)
    out = _get(
        reg,
        "/stock/candle",
        {"symbol": symbol, "resolution": resolution, "from": frm, "to": now},
    )
    if not out or out.get("s") != "ok":
        return None
    return out


def quote(reg, symbol: str) -> Optional[Dict]:
    return _get(reg, "/quote", {"symbol": symbol})


def profile2(reg, symbol: str) -> Optional[Dict]:
    return _get(reg, "/stock/profile2", {"symbol": symbol})


def earnings_calendar(reg, _from: str, _to: str) -> Optional[Dict]:
    return _get(reg, "/calendar/earnings", {"from": _from, "to": _to})
