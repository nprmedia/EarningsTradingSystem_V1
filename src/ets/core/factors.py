from __future__ import annotations

import math

from ets.core.run_context import get_run_date

# price providers (Finnhub-first via quotes_agg)
from ets.data.providers.quotes_agg import fetch_quote_basic
from ets.data.signals.calendar_loader import day_events, same_day_peers
from ets.data.signals.macro_features import vix_risk_signal

# new signal helpers
from ets.data.signals.sector_features import etf_flow_proxy, sector_relative_momentum
from ets.data.signals.trends_features import search_interest

# calendar/peer signals are placeholder-safe for now (we’ll wire real calendar next)
# from ets.data.signals.calendar_features import calendar_density  # (optional)

# Simple sector -> ETF map (SPDR). If sector missing, SPY fallback.
_SECTOR_ETF = {
    "Information Technology": "XLK",
    "Consumer Discretionary": "XLY",
    "Financials": "XLF",
    "Industrials": "XLI",
    "Energy": "XLE",
    "Health Care": "XLV",
    "Utilities": "XLU",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}


def _safe(q: dict | None, k: str) -> float:
    try:
        return float(q.get(k) or 0.0)
    except Exception:
        return 0.0


def _pos(x: float) -> float:
    return x if x > 0 else 0.0


def _nz(x: float) -> float:
    return x if x != 0 else 1e-9


# noqa: C901
def compute_raw_factors(  # noqa: C901
    symbol: str, sector_map: dict[str, str] | None = None
) -> dict | None:  # noqa: C901
    """
    Returns a dict with raw features for one symbol.
    Safe: if anything is missing, fields default to 0 and the row is still returned.
    """
    s = (symbol or "").upper().strip()
    if not s:
        return None

    q = fetch_quote_basic(s)
    if not q:
        # still return a row with zeros so downstream doesn’t crash
        return {
            "ticker": s,
            "sector": (sector_map or {}).get(s, "Unknown"),
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "last": 0.0,
            "volume": 0.0,
            "dollar_volume": 0.0,
            # legacy 6
            "M_raw": 0.0,
            "V_raw": 0.0,
            "S_raw": 0.0,
            "A_raw": 0.0,
            "sigma_raw": 0.0,
            "tau_raw": 0.0,
            # new 6
            "CAL_raw": 0.0,
            "SRM_raw": 0.0,
            "PEER_raw": 0.0,
            "ETFF_raw": 0.0,
            "VIX_raw": 0.0,
            "TREND_raw": 0.0,
        }

    o = _safe(q, "open")
    h = _safe(q, "high")
    low = _safe(q, "low")
    c = _safe(q, "last")
    v = _safe(q, "volume")
    dv = c * v

    # --- core 6 raw features (as previously) ---
    M_raw = (c / _nz(o) - 1.0) * 100.0
    V_raw = (h - low) / _nz(o) * 100.0
    mid = (h + low) / 2.0
    S_raw = ((c - mid) / _nz(o)) * 100.0
    A_raw = M_raw * math.sqrt(_pos(V_raw)) if V_raw > 0 else M_raw
    sigma_raw = V_raw * (1.0 / math.sqrt(max(v, 1.0)))  # coarse liquidity-adjusted vol proxy
    tau_raw = (c - o) / _nz(o) * 100.0 / (math.sqrt(_pos(V_raw)) + 1e-6)  # extension vs span

    # --- sector map & ETF selection ---
    sector = (sector_map or {}).get(s, "Unknown")
    etf = _SECTOR_ETF.get(sector, "SPY")

    # --- new features (6) ---
    run_date = get_run_date("")
    if not run_date:
        run_date = sector  # harmless sentinel; yields 0 events
    # Calendar density (count of same-day earnings)
    try:
        _events = day_events(run_date)
        CAL_raw = float(len(_events))
    except Exception:
        CAL_raw = 0.0

    # Sector-relative momentum: stock intraday % – ETF intraday %
    try:
        SRM_raw = sector_relative_momentum(s, etf)
    except Exception:
        SRM_raw = 0.0

    # Peer earnings drift: mean intraday % of other same-day reporters (capped)
    try:
        from ets.data.providers.quotes_agg import pct_change_today

        peers = same_day_peers(s, run_date, max_peers=10)
        if peers:
            vals = [pct_change_today(x) for x in peers]
            PEER_raw = float(sum(vals) / len(vals))
        else:
            PEER_raw = 0.0
    except Exception:
        PEER_raw = 0.0

    # Sector ETF flows proxy (span% * sqrt(volume))
    try:
        ETFF_raw = etf_flow_proxy(etf)
    except Exception:
        ETFF_raw = 0.0

    # Macro risk via VIX intraday move
    try:
        VIX_raw = vix_risk_signal()
    except Exception:
        VIX_raw = 0.0

    # Google Trends search interest (cached weekly)
    try:
        TREND_raw = float(search_interest(s) or 0.0)
    except Exception:
        TREND_raw = 0.0

    # Calendar density (placeholder 0 until we wire events list)
    CAL_raw = 0.0

    # Sector-relative momentum: stock intraday % – ETF intraday %
    try:
        SRM_raw = sector_relative_momentum(s, etf)
    except Exception:
        SRM_raw = 0.0

    # Peer earnings drift (placeholder 0 until calendar peer list is wired)
    PEER_raw = 0.0

    # Sector ETF flows proxy (span% * sqrt(volume))
    try:
        ETFF_raw = etf_flow_proxy(etf)
    except Exception:
        ETFF_raw = 0.0

    # Macro risk via VIX intraday move
    try:
        VIX_raw = vix_risk_signal()
    except Exception:
        VIX_raw = 0.0

    # Google Trends search interest (cached weekly)
    try:
        TREND_raw = float(search_interest(s) or 0.0)
    except Exception:
        TREND_raw = 0.0

    return {
        "ticker": s,
        "sector": sector,
        "open": o,
        "high": h,
        "low": low,
        "last": c,
        "volume": v,
        "dollar_volume": dv,
        # legacy 6
        "M_raw": M_raw,
        "V_raw": V_raw,
        "S_raw": S_raw,
        "A_raw": A_raw,
        "sigma_raw": sigma_raw,
        "tau_raw": tau_raw,
        # new 6
        "CAL_raw": CAL_raw,
        "SRM_raw": SRM_raw,
        "PEER_raw": PEER_raw,
        "ETFF_raw": ETFF_raw,
        "VIX_raw": VIX_raw,
        "TREND_raw": TREND_raw,
    }
