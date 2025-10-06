import os, time, json
from typing import Optional
from pytrends.request import TrendReq

_CACHE = os.path.join("cache", "trends")
os.makedirs(_CACHE, exist_ok=True)

def _cache_path(keyword: str) -> str:
    return os.path.join(_CACHE, f"{keyword.upper()}.json")

def search_interest(keyword: str, lookback_days: int = 365) -> Optional[float]:
    path = _cache_path(keyword)
    now = time.time()
    if os.path.exists(path) and (now - os.path.getmtime(path) < 7*24*3600):
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f); return float(obj.get("avg_interest", 0.0))
        except Exception: pass
    try:
        pytrends = TrendReq(hl="en-US", tz=360)
        pytrends.build_payload([keyword], timeframe=f"today {lookback_days}-d", geo="US")
        df = pytrends.interest_over_time()
        val = 0.0 if df is None or df.empty else float(df[keyword].tail(52).mean())
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"avg_interest": val, "ts": now}, f)
        return val
    except Exception:
        return 0.0
