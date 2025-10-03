from typing import Optional

# Providers
from ets.data.providers.yahoo_direct_client import fetch_daily_ohlc as _yd
from ets.data.providers.yfinance_client import fetch_quote_basic as _yf
from ets.data.providers.stooq_client import fetch_daily_ohlc as _stq
from ets.data.providers.finnhub_client import fetch_quote_basic as _fh  # optional

# Per-run memo to avoid repeat hits
_MEMO: dict[str, dict] = {}

def fetch_quote_basic(symbol: str) -> Optional[dict]:
    s = symbol.upper().strip()
    if s in _MEMO:
        return _MEMO[s]

    # 1) Yahoo direct "chart" first (often bypasses yfinance 429s)
    q = _yd(s)
    if q and q.get("last") and q.get("open"):
        _MEMO[s] = q; return q

    # 2) yfinance
    q = _yf(s)
    if q and q.get("last") and q.get("open"):
        _MEMO[s] = q; return q

    # 3) Stooq
    q = _stq(s)
    if q and q.get("last") and q.get("open"):
        _MEMO[s] = q; return q

    # 4) Finnhub
    q = _fh(s)
    if q and q.get("last") and q.get("open"):
        _MEMO[s] = q; return q

    _MEMO[s] = None
    return None

def pct_change_today(symbol: str) -> float:
    q = fetch_quote_basic(symbol)
    if not q: 
        return 0.0
    o = float(q.get("open") or 0.0)
    c = float(q.get("last") or 0.0)
    if o == 0.0:
        return 0.0
    return (c / o - 1.0) * 100.0
