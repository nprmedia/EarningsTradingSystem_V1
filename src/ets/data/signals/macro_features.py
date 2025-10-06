from ets.data.providers.quotes_agg import pct_change_today

def vix_risk_signal() -> float:
    return pct_change_today("^VIX")

def yields_proxy() -> float:
    return 0.0
