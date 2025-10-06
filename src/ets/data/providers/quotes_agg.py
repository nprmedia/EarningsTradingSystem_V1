from typing import Optional, List, Dict
import time
from ets.data.providers.yahoo_direct_client import fetch_daily_ohlc as _yd
from ets.data.providers.yfinance_client import fetch_quote_basic as _yf
from ets.data.providers.stooq_client import fetch_daily_ohlc as _stq
from ets.data.providers.finnhub_client import quote as fh_quote
from ets.data.providers.provider_registry import ProviderRegistry

_REG: Optional[ProviderRegistry] = None
def set_registry(reg: ProviderRegistry):
    global _REG; _REG = reg

_MEMO: dict[str, dict] = {}
_PULL_LOG: List[Dict] = []

def _log(symbol: str, provider: str, ok: bool, ms: float, note: str = ""):
    _PULL_LOG.append({"symbol": symbol.upper().strip(), "provider": provider, "ok": int(bool(ok)), "latency_ms": round(ms, 1), "note": note})

def get_pull_log() -> List[Dict]:
    return list(_PULL_LOG)

def _valid_bar(q: dict) -> bool:
    try:
        o = float(q.get("open", 0)); h = float(q.get("high", 0))
        l = float(q.get("low", 0));  c = float(q.get("last", 0))
        if o <= 0 or h <= 0 or l <= 0 or c <= 0: return False
        if l > h: return False
        span = (h - l) / o if o else 0
        return span < 0.2
    except Exception:
        return False

def _from_finnhub(sym: str) -> Optional[dict]:
    if _REG is None: return None
    t0 = time.time()
    q = fh_quote(_REG.finnhub, sym)
    if not q:
        _log(sym, "finnhub", False, (time.time()-t0)*1000); return None
    out = {
        "open": float(q.get("o") or 0.0),
        "high": float(q.get("h") or 0.0),
        "low":  float(q.get("l") or 0.0),
        "last": float(q.get("c") or 0.0),
        "volume": float(q.get("v") or 0.0) if "v" in q else 0.0,
    }
    ok = _valid_bar(out)
    _log(sym, "finnhub", ok, (time.time()-t0)*1000)
    return out if ok else None

def fetch_quote_basic(symbol: str) -> Optional[dict]:
    s = symbol.upper().strip()
    if s in _MEMO: return _MEMO[s]
    q = _from_finnhub(s)
    if q: _MEMO[s] = q; return q
    t0 = time.time(); q = _yd(s); ok = bool(q and _valid_bar(q)); _log(s, "yahoo_direct", ok, (time.time()-t0)*1000)
    if ok: _MEMO[s] = q; return q
    t0 = time.time(); q = _yf(s); ok = bool(q and _valid_bar(q)); _log(s, "yfinance", ok, (time.time()-t0)*1000)
    if ok: _MEMO[s] = q; return q
    t0 = time.time(); q = _stq(s); ok = bool(q and _valid_bar(q)); _log(s, "stooq", ok, (time.time()-t0)*1000)
    if ok: _MEMO[s] = q; return q
    _MEMO[s] = None; return None

def pct_change_today(symbol: str) -> float:
    q = fetch_quote_basic(symbol)
    if not q: return 0.0
    o = float(q.get("open") or 0.0); c = float(q.get("last") or 0.0)
    if o == 0.0: return 0.0
    return (c / o - 1.0) * 100.0
