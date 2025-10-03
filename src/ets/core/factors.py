# src/core/factors.py
# Computes raw factors for a ticker using resilient quote fetching and a tiny on-disk sector cache.
# Inputs: ticker, sector_map (for sector ETF symbol lookup in market_refs)
# Outputs (dict): {ticker, sector, price, dollar_vol, M_raw, V_raw, S_raw, A_raw, sigma_raw, tau_raw}

from typing import Dict, Any
import os, csv

from ets.data.providers.quotes_agg import fetch_quote_basic as _qfetch
from ets.data.market_refs import sector_etf_pct, spy_pct

# --------------------------------------------------------------------
# Tiny on-disk sector cache (avoids Yahoo quoteSummary /info 429s)
# --------------------------------------------------------------------
_SECTOR_CSV = os.path.join("cache", "sectors.csv")
_SECTOR_MAP: Dict[str, str] = {}

def _load_sector_cache():
    if os.path.exists(_SECTOR_CSV):
        try:
            with open(_SECTOR_CSV, "r", newline="", encoding="utf-8") as f:
                for row in csv.reader(f):
                    if not row: 
                        continue
                    sym = (row[0] or "").upper().strip()
                    sec = (row[1] or "Unknown").strip() if len(row) > 1 else "Unknown"
                    if sym:
                        _SECTOR_MAP[sym] = sec
        except Exception:
            # best-effort; ignore cache errors
            pass

def _save_sector_cache():
    os.makedirs(os.path.dirname(_SECTOR_CSV) or ".", exist_ok=True)
    try:
        with open(_SECTOR_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            for sym, sec in sorted(_SECTOR_MAP.items()):
                w.writerow([sym, sec])
    except Exception:
        pass

_load_sector_cache()

def set_sector(symbol: str, sector: str):
    """Optional helper: allow user to seed/override sectors, then persist."""
    s = (symbol or "").upper().strip()
    if not s:
        return
    _SECTOR_MAP[s] = sector or "Unknown"
    _save_sector_cache()

def get_sector(ticker: str) -> str:
    """Return cached sector; defaults to 'Unknown' (uses _default weights)."""
    return _SECTOR_MAP.get((ticker or "").upper().strip(), "Unknown")

# --------------------------------------------------------------------
# Raw factor computation
# --------------------------------------------------------------------
def compute_raw_factors(ticker: str, sector_map: dict) -> Dict[str, Any] | None:
    """
    Returns dict with raw factor inputs (pre-normalization):
      M_raw, V_raw, S_raw, A_raw, sigma_raw, tau_raw, price, dollar_vol, sector
    """
    # 1) Quotes (robust multi-source aggregator; yfinance first)
    q = _qfetch(ticker)
    if not q:
        return None

    last = float(q.get("last", 0.0) or 0.0)
    openp = float(q.get("open", 0.0) or 0.0)
    high  = float(q.get("high", last) or last)
    low   = float(q.get("low", last) or last)
    vol   = float(q.get("volume", 0.0) or 0.0)

    # 2) Basics
    price = last
    dollar_vol = price * vol
    intraday_range = max(high - low, 0.0)
    intraday_vol_pct = (intraday_range / openp) if openp > 0 else 0.0
    pct_chg = (last / openp - 1.0) * 100.0 if openp > 0 else 0.0

    # 3) Sector info (cache-based; if unknown, still fine)
    sector = get_sector(ticker)

    # 4) Market/sector references (for S_raw alignment)
    #    sector_etf_pct uses sector_map to look up the ETF symbol (XLY, XLI, ...)
    sector_ret = sector_etf_pct(sector, sector_map)  # % change close/open today
    market_ret = spy_pct()                           # % change close/open today

    # 5) Raw factors ---------------------------------------------------
    # Momentum (M_raw): today's % change (close vs open), scale-free
    M_raw = pct_chg

    # Liquidity (V_raw): dollar volume
    V_raw = dollar_vol

    # Sector/Peer alignment (S_raw): coarse directional alignment
    def sgn(x: float) -> int:
        return 1 if x > 0 else (-1 if x < 0 else 0)

    align = 0
    if sgn(pct_chg) == sgn(sector_ret): align += 1
    if sgn(pct_chg) == sgn(market_ret): align += 1
    if sgn(sector_ret) == sgn(market_ret): align += 1
    S_raw = align / 3.0  # in {0, 1/3, 2/3, 1}

    # Analyst/estimate trend (A_raw): neutral on free tier (0.5)
    A_raw = 0.5

    # Volatility risk proxy (sigma_raw): intraday range vs open (higher = worse)
    sigma_raw = intraday_vol_pct  # e.g., 0.02 = 2%

    # Tail risk (tau_raw): microcap/penny heuristic
    microcap_flag = 1.0 if dollar_vol < 2_000_000 else 0.0
    penny_flag = 1.0 if price < 3.0 else 0.0
    tau_raw = 0.6 * microcap_flag + 0.4 * penny_flag  # 0, 0.4, 0.6, 1.0

    return {
        "ticker": ticker,
        "sector": sector,
        "price": price,
        "dollar_vol": dollar_vol,
        "M_raw": M_raw,
        "V_raw": V_raw,
        "S_raw": S_raw,
        "A_raw": A_raw,
        "sigma_raw": sigma_raw,
        "tau_raw": tau_raw,
    }
