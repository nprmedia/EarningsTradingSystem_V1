from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable

import requests
from dotenv import load_dotenv

from .rate_limiter import RateLimiter

_ROOT = Path(__file__).resolve().parents[4]
load_dotenv(_ROOT / ".env", override=True)


class ProviderRegistry:
    """
    Holds per-API sessions and rate limiters from config.
    """

    def __init__(self, cfg: dict):
        api_cfg = cfg.get("api_limits", {})
        self._regs = {}
        self.offline_mode = os.getenv("ETS_OFFLINE", "0") == "1"
        self._offline_prices: dict | None = None
        self._offline_prices_path: Path | None = None

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

    @classmethod
    def from_env(cls) -> "ProviderRegistry":
        """
        Safe constructor honoring env vars.

        - Loads a config file if ETS_CONFIG points to one.
        - Honors ETS_OFFLINE to avoid network calls in CI.
        - Allows overriding the mock prices path via ETS_MOCK_PRICES.
        """

        cfg: dict = {}
        cfg_path = os.getenv("ETS_CONFIG")
        if cfg_path:
            p = Path(cfg_path)
            if p.exists():
                try:
                    import yaml

                    cfg = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
                except Exception:
                    cfg = {}

        reg = cls(cfg)
        mock_prices = os.getenv("ETS_MOCK_PRICES")
        if mock_prices:
            reg._offline_prices_path = Path(mock_prices)
        return reg

    def _load_offline_prices(self) -> dict:
        if self._offline_prices is not None:
            return self._offline_prices

        candidate = self._offline_prices_path
        if not candidate:
            candidate = _ROOT / "tests" / "fixtures" / "mock_prices.json"

        try:
            data = json.loads(Path(candidate).read_text(encoding="utf-8"))
            self._offline_prices = data if isinstance(data, dict) else {}
        except Exception:
            self._offline_prices = {}
        return self._offline_prices

    def get_prices(self, symbols: Iterable[str]) -> dict[str, list]:
        """
        Return price history for symbols.

        In offline mode, serve deterministic fixtures to keep CI stable.
        """

        symbols_list = [s.upper().strip() for s in symbols if s and s.strip()]
        if not symbols_list:
            return {}

        if self.offline_mode:
            prices = self._load_offline_prices()
            return {s: prices.get(s, []) for s in symbols_list}

        out: dict[str, list] = {}
        try:
            import yfinance as yf

            for sym in symbols_list:
                try:
                    df = yf.download(sym, period="1mo", progress=False, threads=False)
                    if df is None or df.empty:
                        out[sym] = []
                        continue
                    out[sym] = [
                        {"date": str(idx.date()), "close": float(row.get("Close", 0.0))}
                        for idx, row in df.iterrows()
                    ]
                except Exception:
                    out[sym] = []
        except Exception:
            return {s: [] for s in symbols_list}

        return out

    def stats(self):
        out = []
        for reg in self._regs.values():
            lim = reg.get("limiter")
            if lim:
                out.append(lim.get_stats())
        return out
