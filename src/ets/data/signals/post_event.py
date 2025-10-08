from ets.data.providers.quotes_agg import fetch_quote_basic


def realized_drift_open_to_close(symbol: str) -> float:
    q = fetch_quote_basic(symbol)
    if not q:
        return 0.0
    o = float(q.get("open") or 0.0)
    c = float(q.get("last") or 0.0)
    if o <= 0:
        return 0.0
    return (c / o - 1.0) * 100.0


def prediction_residual(pred_score: float, realized_ret: float) -> float:
    return float(realized_ret - pred_score)
