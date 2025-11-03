from __future__ import annotations
import json
import pathlib
import os
from typing import Dict, Any
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / "out"


def load_latest_output(session: str | None = None) -> pathlib.Path:
    OUT.mkdir(parents=True, exist_ok=True)
    files = sorted(OUT.glob("*.csv"))
    if not files:
        raise FileNotFoundError(
            "No out/*.csv files found. Run scripts/cli_mock_run.py first."
        )
    if session:
        # prioritize files that match session
        session_files = sorted([p for p in files if p.stem.endswith(f"_{session}")])
        if session_files:
            return session_files[-1]
    return files[-1]


def top_n(df: pd.DataFrame, n: int = 5) -> pd.DataFrame:
    n = int(os.getenv("ETS_TOPN", n))
    return df.sort_values("score", ascending=False).head(n).reset_index(drop=True)


def sector_summary(df: pd.DataFrame) -> pd.DataFrame:
    # Requires columns: sector, score, rank
    g = df.groupby("sector", dropna=False)
    out = g.agg(
        mean_score=("score", "mean"),
        std_score=("score", "std"),
        rank_dispersion=("rank", "std"),
        count=("ticker", "count"),
    ).reset_index()
    out["std_score"] = out["std_score"].fillna(0.0)
    out["rank_dispersion"] = out["rank_dispersion"].fillna(0.0)
    return out


def save_json_csv(obj: pd.DataFrame | Dict[str, Any], base: pathlib.Path):
    base.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(obj, pd.DataFrame):
        obj.to_json(str(base.with_suffix(".json")), orient="records", indent=2)
        obj.to_csv(str(base.with_suffix(".csv")), index=False)
    else:
        (base.with_suffix(".json")).write_text(json.dumps(obj, indent=2))


def run_summary(session: str | None = None) -> Dict[str, Any]:
    latest = load_latest_output(session)
    df = pd.read_csv(latest)
    # minimal schema check
    for col in ("ticker", "sector", "score", "rank"):
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in {latest}")
    # Compute artifacts
    sect = sector_summary(df)
    save_json_csv(sect, OUT / "sector_summary")
    tn = top_n(df, n=int(os.getenv("ETS_TOPN", "5")))
    save_json_csv(tn, OUT / "top5")
    return {
        "latest_file": str(latest),
        "top_n": tn["ticker"].tolist(),
        "sectors": sect.to_dict(orient="records"),
    }
