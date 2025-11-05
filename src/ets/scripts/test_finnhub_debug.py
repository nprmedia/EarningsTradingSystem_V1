from __future__ import annotations

import os
from pathlib import Path

import yaml

from ets.data.providers.finnhub_client import quote
from ets.data.providers.provider_registry import ProviderRegistry

cfg = yaml.safe_load(Path("src/ets/config/config.yaml").read_text())
os.environ["FINNHUB_DEBUG"] = os.getenv("FINNHUB_DEBUG", "1")
reg = ProviderRegistry(cfg)
print("[CFG] base:", reg.finnhub.get("base"), "key_present:", bool(reg.finnhub.get("key")))
reg.finnhub["limiter"].acquire()
print("AAPL:", quote(reg.finnhub, "AAPL"))
