from __future__ import annotations

import time

from ets.core.run_context import get_run_date
from ets.data.providers.finnhub_client import earnings_calendar
from ets.data.providers.provider_registry import ProviderRegistry

FALLBACK_PEERS: list[str] | None = None


def set_fallback_peers(symbols: list[str] | None):
    global FALLBACK_PEERS
    FALLBACK_PEERS = [s.upper() for s in symbols] if symbols else None


_REG: ProviderRegistry | None = None
_CACHE: dict[str, dict] = {}  # date -> {"ts": time.time(), "events": [..]}


def set_registry(reg: ProviderRegistry):
    global _REG
    _REG = reg


def _fetch_day(date_str: str) -> list[dict]:
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
    out: list[dict] = []
    if obj and "earningsCalendar" in obj and isinstance(obj["earningsCalendar"], list):
        for e in obj["earningsCalendar"]:
            # normalize minimal fields we need
            sym = str(e.get("symbol", "")).upper()
            when = str(
                e.get("hour", "") or e.get("time", "") or ""
            ).lower()  # sometimes "bmo"/"amc"
            out.append(
                {
                    "symbol": sym,
                    "date": date_str,
                    "session": ("amc" if "amc" in when else "bmo" if "bmo" in when else ""),
                }
            )
    # Fallback: if API returned no events for the run date, synthesize from provided peers
    if not out and FALLBACK_PEERS is not None:
        out = [{"symbol": s, "date": date_str, "session": "amc"} for s in FALLBACK_PEERS]
    _CACHE[date_str] = {"ts": now, "events": out}
    return out


def day_events(date_str: str) -> list[dict]:
    return list(_fetch_day(date_str))


def same_day_peers(symbol: str, date_str: str, max_peers: int = 10) -> list[str]:
    s = (symbol or "").upper()
    ev = [e.get("symbol", "") for e in _fetch_day(date_str)]
    peers = [x for x in ev if x and x != s]
    # cap to avoid too many downstream quote calls
    return peers[:max_peers]


def events_for_run_date() -> list[dict]:
    ds = get_run_date("")
    return day_events(ds) if ds else []
