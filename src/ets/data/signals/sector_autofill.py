from __future__ import annotations

import csv
import os
from collections.abc import Iterable

from ets.data.providers.finnhub_client import profile2
from ets.data.providers.provider_registry import ProviderRegistry


def load_sector_cache(cache_dir: str = "cache") -> dict[str, str]:
    path = os.path.join(cache_dir, "sectors.csv")
    out: dict[str, str] = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for r in csv.reader(f):
                if len(r) >= 2:
                    out[str(r[0]).upper()] = r[1]
    return out


def save_sector_cache(mapping: dict[str, str], cache_dir: str = "cache"):
    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, "sectors.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for k, v in sorted(mapping.items()):
            w.writerow([k, v])


def autofill_sectors(
    symbols: Iterable[str], reg: ProviderRegistry, cache_dir: str = "cache"
) -> dict[str, str]:
    cache = load_sector_cache(cache_dir)
    updated = False
    for s in symbols:
        u = str(s).upper().strip()
        if not u or u in cache:
            continue
        prof = profile2(reg.finnhub, u) or {}
        sec = prof.get("finnhubIndustry") or prof.get("sector") or "Unknown"
        cache[u] = sec
        updated = True
    if updated:
        save_sector_cache(cache, cache_dir)
    return cache
