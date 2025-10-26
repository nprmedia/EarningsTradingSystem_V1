from __future__ import annotations
import json
import datetime
from pathlib import Path
import pandas as pd


def compute_metrics(panel: pd.DataFrame) -> dict:
    df = panel.dropna(subset=["ret_next", "signal"]).copy()
    if df.empty:
        return {"count": 0, "hit_rate": None, "avg_ret": None}
    df["hit"] = (df["signal"] * df["ret_next"]) > 0
    hit_rate = float(df["hit"].mean())
    avg_ret = float(df["signal"].mul(df["ret_next"]).mean())
    out = {
        "count": int(len(df)),
        "hit_rate": hit_rate,
        "avg_ret": avg_ret,
    }
    if "sector" in df.columns:
        sect = df.groupby("sector")["hit"].mean().reset_index(name="hit_rate")
        out["by_sector"] = sect.to_dict(orient="records")
    return out


def save_artifacts(metrics: dict, out_dir: Path, panel: pd.DataFrame):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "backtest_summary.json").write_text(json.dumps(metrics, indent=2))
    cols = [
        c
        for c in ["date", "ticker", "sector", "signal", "ret_next"]
        if c in panel.columns
    ]
    panel[cols].to_csv(out_dir / "sector_accuracy.csv", index=False)


def save_perf(meta_dir: Path, elapsed: float, rows: int):
    meta_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    payload = {"timestamp": stamp, "elapsed_sec": elapsed, "rows": rows}
    (meta_dir / f"{stamp}_backtest_perf.json").write_text(json.dumps(payload, indent=2))
