from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ets.data.providers.fetchers import fetch_factor
from ets.data.providers.provider_registry import ProviderRegistry

SYMBOL_CLEAN_RE = re.compile(r"[^A-Za-z0-9\.\-\^]")


def normalize_symbol(s: str) -> str:
    s = s.strip().upper()
    if s.startswith("$"):
        s = s[1:]
    s = SYMBOL_CLEAN_RE.sub("", s)
    return s


def load_config() -> dict[str, Any]:
    # Try defaults.CONFIG
    try:
        from ets.config import defaults

        if hasattr(defaults, "CONFIG"):
            return defaults.CONFIG  # type: ignore[attr-defined]
        # Try YAML beside defaults
        p = Path(defaults.__file__).resolve().parent / "config.yaml"
        if p.exists():
            return yaml.safe_load(p.read_text())
    except Exception:
        pass
    # Common locations relative to repo
    here = Path(__file__).resolve()
    for rel in [
        Path("src/ets/config/config.yaml"),
        Path("ets/config/config.yaml"),
        Path("config/config.yaml"),
    ]:
        cand = here.parents[2] / rel
        if cand.exists():
            return yaml.safe_load(cand.read_text())
    raise FileNotFoundError("Could not locate config.yaml")


def main():
    cfg = load_config()
    out_dir = Path(cfg["app"]["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    tickers_path = sys.argv[1] if len(sys.argv) > 1 else "tickers.csv"
    factor = sys.argv[2] if len(sys.argv) > 2 else "quote"

    df = pd.read_csv(tickers_path)
    col = df.columns[0]
    symbols = [
        normalize_symbol(str(x))
        for x in df[col].dropna().astype(str).tolist()
        if normalize_symbol(str(x))
    ]

    reg = ProviderRegistry(cfg)

    try:
        reserve = reg.finnhub["limiter"].reserve  # type: ignore[index]
    except Exception:
        reserve = "NA"
    print(f"[INFO] pulling {len(symbols)} | factor={factor} | finnhub_reserve={reserve}")

    ok = fail = 0
    t0 = time.time()
    results = []

    for i, s in enumerate(symbols, 1):
        data = fetch_factor(reg, factor, s)
        if data:
            ok += 1
            results.append({"symbol": s, "data": data})
        else:
            fail += 1
            results.append({"symbol": s, "data": None})

        if i % 25 == 0:
            try:
                headroom = reg.finnhub["limiter"].get_stats()["windows"][0]["headroom"]  # type: ignore[index]
            except Exception:
                headroom = "NA"
            print(f"[{i}/{len(symbols)}] ok={ok} fail={fail} headroom={headroom}")

    jsonl = out_dir / f"{factor}_results.jsonl"
    with jsonl.open("w") as f:
        for r in results:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")

    # CSV with minimal/common fields
    rows = []
    for r in results:
        d = r["data"] or {}
        rows.append(
            {
                "symbol": r["symbol"],
                "c": d.get("c"),
                "h": d.get("h"),
                "l": d.get("l"),
                "o": d.get("o"),
                "pc": d.get("pc"),
                "t": d.get("t"),
            }
        )
    (out_dir / f"{factor}_results.csv").write_text(pd.DataFrame(rows).to_csv(index=False))

    dt = time.time() - t0
    print(f"[DONE] {ok}/{len(symbols)} succeeded, {fail} failed in {dt:0.1f}s")
    print(f"[OUT] {out_dir / f'{factor}_results.jsonl'} | {out_dir / f'{factor}_results.csv'}")


if __name__ == "__main__":
    main()
