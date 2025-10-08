from __future__ import annotations
import os
import math
import datetime as dt
from typing import Dict, Optional
import requests
import yfinance as yf

UTC = dt.timezone.utc


def _today_ymd() -> str:
    return dt.datetime.now(UTC).strftime("%Y-%m-%d")


def _days_ago(n: int) -> str:
    return (dt.datetime.now(UTC) - dt.timedelta(days=n)).strftime("%Y-%m-%d")


def _safe_float(x):
    try:
        if x is None:
            return None
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None


def _yf_info(t: yf.Ticker) -> dict:
    # yfinance 'info' can be flaky; prefer fast_info + specific frames, then info
    info = {}
    try:
        info = t.get_info() if hasattr(t, "get_info") else t.info
    except Exception:
        pass
    return info or {}


def _last_price(t: yf.Ticker) -> Optional[float]:
    # fast path
    try:
        fi = getattr(t, "fast_info", None)
        if fi and "last_price" in fi:
            v = _safe_float(fi["last_price"])
            if v is not None:
                return v
    except Exception:
        pass
    # fallback: recent close
    try:
        hist = t.history(period="5d", interval="1d")
        if len(hist) > 0:
            return _safe_float(hist["Close"].iloc[-1])
    except Exception:
        pass
    return None


def factor_fundamentals(sym: str) -> Dict[str, Optional[float]]:
    """
    EPSG, ROE, PEG, DE — from Yahoo free endpoints via yfinance.
    All real values; returns None for any missing -> caller will enforce completeness.
    """
    t = yf.Ticker(sym)
    info = _yf_info(t)
    out: Dict[str, Optional[float]] = {
        "EPSG": None,
        "ROE": None,
        "PEG": None,
        "DE": None,
    }

    # ROE / PEG / DE from info if available
    out["ROE"] = _safe_float(info.get("returnOnEquity"))
    out["PEG"] = _safe_float(info.get("pegRatio"))
    out["DE"] = _safe_float(info.get("debtToEquity"))

    # EPSG: quarterly YoY EPS growth (%)
    try:
        q = getattr(t, "quarterly_earnings", None)
        if q is not None and len(q) >= 5 and "Earnings" in q.columns:
            cur = _safe_float(q["Earnings"].iloc[-1])
            prev = _safe_float(q["Earnings"].iloc[-5])  # YoY comp
            if cur is not None and prev not in (None, 0.0):
                out["EPSG"] = (cur / prev - 1.0) * 100.0
    except Exception:
        pass

    return out


def factor_short_interest(sym: str) -> Dict[str, Optional[float]]:
    """
    SI — Short interest % of float from Yahoo; real number if available.
    """
    t = yf.Ticker(sym)
    info = _yf_info(t)
    si = None
    # Prefer shortPercentOfFloat
    si = _safe_float(info.get("shortPercentOfFloat"))
    if si is None:
        # Compute sharesShort / floatShares if both exist
        ss = _safe_float(info.get("sharesShort"))
        fl = _safe_float(info.get("floatShares"))
        if ss is not None and fl not in (None, 0.0):
            si = 100.0 * (ss / fl)
    return {"SI": si}


def factor_options_skew(sym: str) -> Dict[str, Optional[float]]:
    """
    OPT_SK — Call IV minus Put IV around ATM for nearest expiry using Yahoo options via yfinance.
    """
    t = yf.Ticker(sym)
    try:
        exps = list(getattr(t, "options", []) or [])
        if not exps:
            return {"OPT_SK": None}
        exp = exps[0]
        chain = t.option_chain(exp)
        calls, puts = chain.calls, chain.puts
        last = _last_price(t)
        if last is None or calls.empty or puts.empty:
            return {"OPT_SK": None}
        lo, hi = 0.9 * last, 1.1 * last
        c = calls[(calls["strike"] >= lo) & (calls["strike"] <= hi)]
        p = puts[(puts["strike"] >= lo) & (puts["strike"] <= hi)]
        if (
            c.empty
            or p.empty
            or "impliedVolatility" not in c
            or "impliedVolatility" not in p
        ):
            return {"OPT_SK": None}
        c_iv = _safe_float(c["impliedVolatility"].median())
        p_iv = _safe_float(p["impliedVolatility"].median())
        if c_iv is None or p_iv is None:
            return {"OPT_SK": None}
        return {"OPT_SK": c_iv - p_iv}
    except Exception:
        return {"OPT_SK": None}


def factor_insider(sym: str) -> Dict[str, Optional[float]]:
    """
    INSIDER — Net buy ratio from Finnhub insider transactions (free tier).
    """
    key = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
    if not key:
        return {"INSIDER": None}
    try:
        params = {
            "symbol": sym,
            "from": _days_ago(120),
            "to": _today_ymd(),
            "token": key,
        }
        r = requests.get(
            "https://finnhub.io/api/v1/stock/insider-transactions",
            params=params,
            timeout=10,
        )
        if r.status_code != 200:
            return {"INSIDER": None}
        data = r.json() or {}
        trs = data.get("data") or []
        buys = sum(
            float(x.get("change") or 0.0)
            for x in trs
            if (x.get("transaction") or "").upper() == "BUY"
        )
        sells = -sum(
            float(x.get("change") or 0.0)
            for x in trs
            if (x.get("transaction") or "").upper() == "SELL"
        )
        total = buys + sells
        if total <= 0:
            return {"INSIDER": None}
        return {"INSIDER": buys / total}  # 0..1
    except Exception:
        return {"INSIDER": None}


_POS = {
    "beat",
    "upgraded",
    "accelerates",
    "expands",
    "wins",
    "strong",
    "surge",
    "record",
    "raises",
}
_NEG = {
    "miss",
    "downgraded",
    "cuts",
    "weak",
    "probe",
    "delay",
    "recall",
    "fraud",
    "plunge",
    "warns",
}


def factor_news_sentiment(sym: str) -> Dict[str, Optional[float]]:
    """
    NEWS — very lightweight lexicon score from Finnhub company-news headlines (free).
    """
    key = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
    if not key:
        return {"NEWS": None}
    try:
        params = {"symbol": sym, "from": _days_ago(7), "to": _today_ymd(), "token": key}
        r = requests.get(
            "https://finnhub.io/api/v1/company-news", params=params, timeout=10
        )
        if r.status_code != 200:
            return {"NEWS": None}
        arts = r.json() or []
        if not arts:
            return {"NEWS": None}
        score = 0
        for a in arts:
            h = (a.get("headline") or "").lower()
            score += sum(1 for w in _POS if w in h)
            score -= sum(1 for w in _NEG if w in h)
        return {"NEWS": float(score)}
    except Exception:
        return {"NEWS": None}


def factor_macro() -> Dict[str, Optional[float]]:
    """
    RISK_APP — XLY/XLU relative; LIQ — proxy from spread% * volume using Yahoo.
    RISK_APP is market-wide, not per-symbol.
    """
    try:
        xly = yf.Ticker("XLY").history(period="10d")["Close"].pct_change().iloc[-1]
        xlu = yf.Ticker("XLU").history(period="10d")["Close"].pct_change().iloc[-1]
        risk = float(xly - xlu)
    except Exception:
        risk = None
    return {"RISK_APP": risk}


def factor_liquidity_from_ohlcv(
    open_: float, high: float, low: float, last: float, volume: float
) -> Dict[str, Optional[float]]:
    """
    LIQ — liquidity proxy, real data: intraday range % times sqrt(volume).
    """
    try:
        if last and last > 0 and high and low:
            span = (float(high) - float(low)) / float(last)
            v = float(volume or 0.0)
            return {"LIQ": float(span * (v**0.5))}
    except Exception:
        pass
    return {"LIQ": None}


def factor_eat_from_calendar(sym: str) -> Dict[str, Optional[float]]:
    """
    EAT — Earnings Announcement Timing (BMO/AMC proximity) via Finnhub calendar.
    Uses today's schedule; returns +1 for AMC, -1 for BMO when known, else None.
    """
    key = os.getenv("FINNHUB_API_KEY") or os.getenv("FINNHUB_TOKEN")
    if not key:
        return {"EAT": None}
    try:
        d = _today_ymd()
        url = "https://finnhub.io/api/v1/calendar/earnings"
        r = requests.get(url, params={"from": d, "to": d, "token": key}, timeout=10)
        if r.status_code != 200:
            return {"EAT": None}
        rows = r.json().get("earningsCalendar", []) or []
        for row in rows:
            if (row.get("symbol") or "").upper().strip() == sym.upper():
                # Finnhub sometimes provides "hour": "bmo" or "amc" or times
                sess = (row.get("hour") or row.get("time") or "").lower()
                if "amc" in sess:
                    return {"EAT": 1.0}
                if "bmo" in sess:
                    return {"EAT": -1.0}
                return {"EAT": 0.0}  # same day but unknown time (still real)
        return {"EAT": None}
    except Exception:
        return {"EAT": None}


def compute_extended_factors(
    sym: str, ohlcv: Dict[str, float]
) -> Dict[str, Optional[float]]:
    """
    Returns real values (or None) for: EPSG, ROE, PEG, DE, SI, OPT_SK, INSIDER, NEWS, RISK_APP, LIQ, EAT.
    """
    out: Dict[str, Optional[float]] = {}
    out.update(factor_fundamentals(sym))
    out.update(factor_short_interest(sym))
    out.update(factor_options_skew(sym))
    out.update(factor_insider(sym))
    out.update(factor_news_sentiment(sym))
    out.update(factor_macro())
    out.update(
        factor_liquidity_from_ohlcv(
            ohlcv.get("open"),
            ohlcv.get("high"),
            ohlcv.get("low"),
            ohlcv.get("last"),
            ohlcv.get("volume"),
        )
    )
    out.update(factor_eat_from_calendar(sym))
    return out
