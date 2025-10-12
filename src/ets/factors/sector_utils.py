from __future__ import annotations
from pathlib import Path
from typing import Dict
import pandas as pd
import yaml

OUT_DIR = Path("out")


def load_sector_profile() -> pd.DataFrame:
    """Loads out/sector_profile.csv (symbol, sector). If missing, returns empty."""
    p = OUT_DIR / "sector_profile.csv"
    if not p.exists():
        return pd.DataFrame(columns=["symbol", "sector"])
    df = pd.read_csv(p)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["sector"] = df["sector"].fillna("Unknown")
    return df


def load_sector_etf_map() -> Dict[str, str]:
    p = Path("src/ets/config/sector_etf_map.yaml")
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text()) or {}
