import os
from typing import Optional, Dict, Any, List, Tuple

from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.rate_limiter import retry_with_backoff


# ---------- Provider adapters ----------


def _fh_profile2(reg: dict, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Finnhub company profile (e.g., sector mapping).
    Returns a dict on success or None on failure.
    """
    from ets.data.providers.finnhub_client import profile2

    try:
        return profile2(reg, symbol)
    except Exception:
        return None


def _fh_quote(reg: dict, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Finnhub quote. Returns a dict with keys like c/h/l/o/pc/t or None.
    """
    from ets.data.providers.finnhub_client import quote

    try:
        return quote(reg, symbol)
    except Exception:
        return None


def _yh_quote(reg: dict, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Yahoo quote (native client if present, else yfinance best-effort).
    Returns Finnhub-like keys where possible.
    """
    # Prefer native yahoo client if available
    try:
        from ets.data.providers.yahoo_direct_client import quote as yh_quote

        return yh_quote(reg, symbol)
    except Exception:
        pass

    # Fallback to yfinance
    try:
        import yfinance as yf

        # Try high-frequency snapshot first
        df = yf.download(
            symbol, period="1d", interval="1m", progress=False, threads=False
        )
        if df is not None and not df.empty:
            last = df.iloc[-1]
            return {
                "c": float(last["Close"]),
                "h": float(df["High"].max()),
                "l": float(df["Low"].min()),
                "o": float(df["Open"].iloc[0]),
                "pc": None,
                "t": None,
                "_src": "yfinance.download",
            }

        # Fallback to fast_info
        q = getattr(yf.Ticker(symbol), "fast_info", {}) or {}

        def _to_float(x):
            return float(x) if x is not None else None

        if q:
            return {
                "c": _to_float(q.get("last_price")),
                "h": _to_float(q.get("day_high")),
                "l": _to_float(q.get("day_low")),
                "o": _to_float(q.get("open")),
                "pc": None,
                "t": None,
                "_src": "yfinance.fast_info",
            }
    except Exception:
        return None

    return None


def _stooq_quote(reg: dict, symbol: str) -> Optional[Dict[str, Any]]:
    """
    Optional Stooq adapter if present in your repo.
    """
    try:
        from ets.data.providers.stooq_client import quote as stq

        out = stq(reg, symbol)
        if not out:
            return None
        return {
            "c": out.get("close"),
            "h": out.get("high"),
            "l": out.get("low"),
            "o": out.get("open"),
            "pc": None,
            "t": out.get("date"),
            "_src": "stooq",
        }
    except Exception:
        return None


# ---------- Policy ----------

FACTOR_POLICY: Dict[str, Dict[str, Any]] = {
    # Sector lookup (for weighting/scoring)
    "profile2": {
        "providers": [("finnhub", _fh_profile2)],  # primary only; blocks under limiter
        "cost": 1,
        "retries": 3,
    },
    # Quote example (you can add other factors similarly)
    "quote": {
        "providers": [
            ("yahoo", _yh_quote),  # PRIMARY: fast, broad coverage
            ("finnhub", _fh_quote),  # fallback only on hard failure
            ("stooq", _stooq_quote),
            ("yahoo", _yh_quote),  # PRIMARY: fast, broad coverage
            ("finnhub", _fh_quote),  # fallback only on hard failure
            ("stooq", _stooq_quote),
            (
                "finnhub",
                _fh_quote,
            ),  # PRIMARY (blocks on rate-limit; waiting is NOT failure)
            ("yahoo", _yh_quote),  # failure-only fallback
            ("stooq", _stooq_quote),  # optional last resort
        ],
        "cost": 1,
        "retries": 3,
    },
}


# ---------- Provider resolution (env override) ----------


def _resolve_providers(
    factor: str, default_list: List[Tuple[str, Any]]
) -> List[Tuple[str, Any]]:
    """
    Allow env override of provider chain per factor.
    - ETS_<FACTOR>_PROVIDERS takes precedence (e.g., ETS_QUOTE_PROVIDERS="finnhub,yahoo")
    - ETS_PROVIDERS acts as a global fallback if per-factor is not set.
    Only providers already known for the factor are allowed.
    """
    key = f"ETS_{factor.upper()}_PROVIDERS"
    order = (os.getenv(key) or os.getenv("ETS_PROVIDERS") or "").strip()
    if not order:
        return default_list

    # Build allowed-name map from the default list
    name_to_fn: Dict[str, Tuple[str, Any]] = {
        name: (name, fn) for name, fn in default_list
    }

    out: List[Tuple[str, Any]] = []
    for name in (x.strip().lower() for x in order.split(",") if x.strip()):
        if name in name_to_fn:
            out.append(name_to_fn[name])

    return out or default_list


# ---------- Entry point ----------


def fetch_factor(
    registry: ProviderRegistry, factor: str, symbol: str
) -> Optional[Dict[str, Any]]:
    """
    Blocking, failure-only fallback:
    - Acquire limiter for the provider (BLOCKS until safe; NOT a failure).
    - Execute with bounded retries/backoff.
    - If returns truthy payload -> success; stop.
    - On hard failure (exception/None after retries) -> try next provider.
    - If all fail -> None.
    """
    policy = FACTOR_POLICY.get(factor)
    if not policy:
        raise ValueError(f"Unknown factor '{factor}'")

    providers = _resolve_providers(factor, policy["providers"])
    cost = int(policy.get("cost", 1))
    retries = int(policy.get("retries", 3))

    for name, call_fn in providers:
        reg = getattr(registry, name, None)
        if not reg:
            continue

        lim = reg.get("limiter")
        if lim:
            # This will block to maintain your reserve headroom (e.g., "never within 2").
            lim.acquire(cost=cost)

        def _do():
            return call_fn(reg, symbol)

        try:
            out = retry_with_backoff(_do, attempts=retries, base=0.25, max_sleep=4.0)
            if out:
                return out
        except Exception:
            # Try next provider on hard failure
            continue

    return None
