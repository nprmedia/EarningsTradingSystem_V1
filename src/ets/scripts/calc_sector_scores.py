from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json
import sys
import time
import hashlib
import datetime as dt
import pandas as pd
import numpy as np
import yaml

from ets.data.providers.provider_registry import ProviderRegistry
from ets.data.providers.fetchers import fetch_factor

FACTORS = [
    "M_raw",
    "V_raw",
    "S_raw",
    "A_raw",
    "sigma_raw",
    "tau_raw",
    "CAL_raw",
    "SRM_raw",
    "PEER_raw",
    "ETFF_raw",
    "VIX_raw",
    "TREND_raw",
]


def robust_z(x: pd.Series) -> pd.Series:
    med = x.median(skipna=True)
    mad = (x - med).abs().median(skipna=True)
    if pd.isna(mad) or mad == 0:
        return pd.Series(np.zeros(len(x)), index=x.index)
    return 0.6745 * (x - med) / mad


def winsor(x: pd.Series, p: float = 0.01) -> pd.Series:
    lo, hi = x.quantile(p), x.quantile(1 - p)
    return x.clip(lower=lo, upper=hi)


def normalize_within_sector(
    df: pd.DataFrame, sector_col: str, factors: List[str]
) -> pd.DataFrame:
    out = df.copy()
    for f in factors:
        out[f] = out.groupby(sector_col)[f].transform(winsor)
        out[f] = out.groupby(sector_col)[f].transform(robust_z)
        out[f] = out[f].fillna(0.0)
    return out


def load_cfg() -> Dict[str, Any]:
    here = Path(__file__).resolve()
    for rel in [
        "src/ets/config/config.yaml",
        "ets/config/config.yaml",
        "config/config.yaml",
    ]:
        p = here.parents[2] / rel
        if p.exists():
            import yaml as _y

            return _y.safe_load(p.read_text())
    raise FileNotFoundError("config.yaml not found")


def ensure_sector_map(
    reg: ProviderRegistry, symbols: List[str], cache: Path
) -> pd.DataFrame:
    if cache.exists():
        dm = pd.read_csv(cache)
    else:
        dm = pd.DataFrame(columns=["symbol", "sector"])
    dm["symbol"] = dm["symbol"].astype(str).str.upper()
    have = set(dm["symbol"].tolist())
    need = [s for s in symbols if s not in have]
    rows = []
    for s in need:
        d = fetch_factor(reg, "profile2", s) or {}
        sector = (
            d.get("finnhubIndustry") or d.get("sector") or ""
        ).strip() or "Unknown"
        rows.append({"symbol": s, "sector": sector})
        time.sleep(0.05)  # polite on free tier
    if rows:
        dm = pd.concat([dm, pd.DataFrame(rows)], ignore_index=True)
        dm.drop_duplicates(subset=["symbol"], keep="last", inplace=True)
        dm.to_csv(cache, index=False)
    return dm


def file_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def iso_utc_from_mtime(p: Path) -> str:
    ts = dt.datetime.utcfromtimestamp(p.stat().st_mtime).replace(microsecond=0)
    return ts.isoformat() + "Z"


def main():
    cfg = load_cfg()
    out_dir = Path(cfg["app"]["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    factors_path = out_dir / "factors_latest.csv"
    if not factors_path.exists():
        print(
            f"[FATAL] missing {factors_path}. Build your factor file first.",
            file=sys.stderr,
        )
        sys.exit(2)

    # Load factors
    df = pd.read_csv(factors_path)
    if "symbol" not in df.columns:
        print("[FATAL] 'symbol' column missing", file=sys.stderr)
        sys.exit(3)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    for f in FACTORS:
        if f not in df.columns:
            df[f] = 0.0

    # Sector map via profile2 (cached)
    reg = ProviderRegistry(cfg)
    sec_map = ensure_sector_map(
        reg, df["symbol"].tolist(), out_dir / "sector_profile.csv"
    )
    df = df.merge(sec_map, on="symbol", how="left")
    df["sector"] = df["sector"].fillna("Unknown")

    # Load weights + compute version stamp
    wfile = Path("src/ets/data/signals/sector_weights.yaml")
    weights_by_sector = yaml.safe_load(wfile.read_text()) if wfile.exists() else {}
    if "Default" not in weights_by_sector:
        weights_by_sector["Default"] = {f: 1.0 / len(FACTORS) for f in FACTORS}
    weights_hash = file_sha256(wfile) if wfile.exists() else "absent"
    weights_date = iso_utc_from_mtime(wfile) if wfile.exists() else "absent"

    # Normalize within sector & score
    df = normalize_within_sector(df, sector_col="sector", factors=FACTORS)

    def row_score(r) -> float:
        wmap = weights_by_sector.get(r["sector"], weights_by_sector.get("Default", {}))
        return float(sum(wmap.get(f, 0.0) * r.get(f, 0.0) for f in FACTORS))

    df["Score"] = df.apply(row_score, axis=1)
    df["sector_rank"] = df.groupby("sector")["Score"].rank(
        ascending=False, method="first"
    )

    # Add version stamp columns (duplicated per row for CSV lineage)
    df["weights_hash"] = weights_hash
    df["weights_date"] = weights_date

    # Persist CSV
    cols = [
        "symbol",
        "sector",
        "Score",
        "sector_rank",
        "weights_hash",
        "weights_date",
    ] + FACTORS
    out_csv = out_dir / "scores_latest.csv"
    df[cols].sort_values(["sector", "Score"], ascending=[True, False]).to_csv(
        out_csv, index=False
    )

    # Persist JSONL with _meta block
    out_jsonl = out_dir / "scores_latest.jsonl"
    with open(out_jsonl, "w") as f:
        meta = {
            "weights_hash": weights_hash,
            "weights_date": weights_date,
            "weights_file": str(wfile),
        }
        for _, r in df[
            ["symbol", "sector", "Score", "sector_rank"] + FACTORS
        ].iterrows():
            rec = r.to_dict()
            # replace NaN with None
            for k, v in list(rec.items()):
                if isinstance(v, float) and np.isnan(v):
                    rec[k] = None
            rec["_meta"] = meta
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")

    print(
        f"[DONE] wrote {out_csv} and {out_jsonl} | weights_hash={weights_hash} | n={len(df)}"
    )


if __name__ == "__main__":
    main()
