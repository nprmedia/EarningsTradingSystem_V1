from ets.data.providers.quotes_agg import pct_change_today, fetch_quote_basic

def sector_relative_momentum(symbol: str, sector_etf: str) -> float:
    return pct_change_today(symbol) - pct_change_today(sector_etf)

def etf_flow_proxy(sector_etf: str) -> float:
    q = fetch_quote_basic(sector_etf)
    if not q: return 0.0
    o = float(q.get("open") or 0.0)
    h = float(q.get("high") or 0.0)
    l = float(q.get("low") or 0.0)
    v = float(q.get("volume") or 0.0)
    if o <= 0: return 0.0
    span = (h - l) / o * 100.0
    return span * (v ** 0.5)
