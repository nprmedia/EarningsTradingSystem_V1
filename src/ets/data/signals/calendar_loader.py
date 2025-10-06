from __future__ import annotations
from typing import List, Dict, Optional
import time

from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.finnhub_client import earnings_calendar

_REG: Optional[ProviderRegistry] = None
_CACHE: dict[str, dict] = {}  # date -> {"ts": time.time(), "events": [..]}

def set_registry(reg: ProviderRegistry):
    global _REG
    _REG = reg

def _fetch_day(date_str: str) -> List[Dict]:
    """
    Fetch earnings for a single YYYY-MM-DD day from Finnhub, cached for 10 minutes.
    """
    now = time.time()
    ent = _CACHE.get(date_str)
    if ent and (now - ent.get("ts", 0)) < 600:
        return ent["events"]

    if _REG is None:
        _CACHE[date_str] = {"ts": now, "events": []}
        return []

    # Finnhub calendar supports a date range; we request just that day
    obj = earnings_calendar(_REG.finnhub, date_str, date_str)
    out: List[Dict] = []
    if obj and "earningsCalendar" in obj and isinstance(obj["earningsCalendar"], list):
        for e in obj["earningsCalendar"]:
            # normalize minimal fields we need
            sym = str(e.get("symbol", "")).upper()
            when = str(e.get("hour", "") or e.get("time", "") or "").lower()  # sometimes "bmo"/"amc"
            out.append({"symbol": sym, "date": date_str, "session": ("amc" if "amc" in when else "bmo" if "bmo" in when else "")})
    _CACHE[date_str] = {"ts": now, "events": out}
    return out

def day_events(date_str: str) -> List[Dict]:
    return list(_fetch_day(date_str))

def same_day_peers(symbol: str, date_str: str, max_peers: int = 10) -> List[str]:
    s = (symbol or "").upper()
    ev = [e.get("symbol","") for e in _fetch_day(date_str)]
    peers = [x for x in ev if x and x != s]
    # cap to avoid too many downstream quote calls
    return peers[:max_peers]
