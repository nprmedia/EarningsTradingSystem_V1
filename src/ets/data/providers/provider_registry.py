import os

import requests
from dotenv import load_dotenv

from .rate_limiter import RateLimiter

load_dotenv("/workspaces/EarningsTradingSystem_V1/.env", override=True)


class ProviderRegistry:
    """
    Holds per-API sessions and rate limiters from config.
    """

    def __init__(self, cfg: dict):
        api_cfg = cfg.get("api_limits", {})
        self._regs = {}

        # Finnhub
        fh = api_cfg.get("finnhub", {"per_second": 30, "per_minute": 60, "reserve": 2})
        self.finnhub = {
            "key": os.getenv("FINNHUB_API_KEY", ""),
            "session": requests.Session(),
            "limiter": RateLimiter(
                per_second=int(fh.get("per_second", 30)),
                per_minute=int(fh.get("per_minute", 60)),
                reserve=int(fh.get("reserve", 2)),
                name="finnhub",
            ),
            "base": "https://finnhub.io/api/v1",
        }
        self._regs["finnhub"] = self.finnhub
        # inject finnhub env if missing
        try:
            if isinstance(self.finnhub, dict):
                self.finnhub.setdefault(
                    "base", os.environ.get("FINNHUB_BASE", "https://finnhub.io/api/v1")
                )
                if not self.finnhub.get("key"):
                    self.finnhub["key"] = os.environ.get("FINNHUB_API_KEY")
        except Exception:
            pass

        # Yahoo (backup)
        yh = api_cfg.get("yahoo", {"per_second": 5, "per_minute": 20, "reserve": 2})
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        self.yahoo = {
            "session": s,
            "limiter": RateLimiter(
                per_second=int(yh.get("per_second", 5)),
                per_minute=int(yh.get("per_minute", 20)),
                reserve=int(yh.get("reserve", 2)),
                name="yahoo",
            ),
        }
        self._regs["yahoo"] = self.yahoo

        # pytrends (soft heuristic)
        pt = api_cfg.get("pytrends", {"per_second": 2, "per_minute": 10, "reserve": 2})
        self.pytrends = {
            "limiter": RateLimiter(
                per_second=int(pt.get("per_second", 2)),
                per_minute=int(pt.get("per_minute", 10)),
                reserve=int(pt.get("reserve", 2)),
                name="pytrends",
            ),
        }
        self._regs["pytrends"] = self.pytrends

    def stats(self):
        out = []
        for reg in self._regs.values():
            lim = reg.get("limiter")
            if lim:
                out.append(lim.get_stats())
        return out
