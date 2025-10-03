from ets.data.providers.quotes_agg import pct_change_today

def sector_etf_pct(sector: str, sector_map: dict) -> float:
    symbol = sector_map.get(sector)
    if not symbol: return 0.0
    return pct_change_today(symbol)

def spy_pct(spy_symbol: str = "SPY") -> float:
    return pct_change_today(spy_symbol)

def vix_level(vix_symbol: str = "^VIX") -> float:
    # We only need a level; if blocked, 0 is fine. Try pct source as proxy.
    from ets.data.providers.quotes_agg import fetch_quote_basic
    q = fetch_quote_basic(vix_symbol)
    return float(q.get("last", 15.0)) if q else 15.0
