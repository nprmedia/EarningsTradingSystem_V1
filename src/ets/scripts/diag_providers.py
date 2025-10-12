from __future__ import annotations
import json
import yaml
from pathlib import Path


def load_cfg():
    # Try defaults.CONFIG, else src/ets/config/config.yaml
    try:
        from ets.config import defaults

        if hasattr(defaults, "CONFIG"):
            return defaults.CONFIG
    except Exception:
        pass
    p = Path(__file__).resolve().parents[2] / "config" / "config.yaml"
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "ets" / "config" / "config.yaml"
    if not p.exists():
        p = (
            Path(__file__).resolve().parents[3]
            / "src"
            / "ets"
            / "config"
            / "config.yaml"
        )
    if not p.exists():
        raise FileNotFoundError("config.yaml not found")
    return yaml.safe_load(p.read_text())


def main():
    cfg = load_cfg()
    from ets.data.providers.provider_registry import ProviderRegistry

    reg = ProviderRegistry(cfg)
    fh = reg.finnhub
    key = fh.get("key") if isinstance(fh, dict) else None
    print("[CFG] finnhub.base:", fh.get("base"))
    print("[CFG] finnhub.key_present:", bool(key))
    # Try live quote for AAPL
    try:
        from ets.data.providers.finnhub_client import quote

        fh["limiter"].acquire()
        out = quote(fh, "AAPL")
        print("[LIVE] finnhub AAPL:", json.dumps(out)[:200])
    except Exception as e:
        print("[LIVE] finnhub error:", repr(e))
    # yfinance quick sanity
    try:
        import yfinance as yf

        p = yf.Ticker("AAPL").fast_info.get("last_price")
        print("[LIVE] yfinance AAPL last_price:", p)
    except Exception as e:
        print("[LIVE] yfinance error:", repr(e))


if __name__ == "__main__":
    main()
