import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
METRICS = ROOT / "metrics"
REPORTS = ROOT / "reports"
METRICS.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)


def compute_perf(df: pd.DataFrame, top_n: int = 5):
    if df.empty:
        return {"hit_rate": 0, "avg_return": 0, "vol_adj_roi": 0}

    total = len(df)
    wins = (df["return"] > 0).sum()
    hit_rate = wins / total
    avg_ret = df["return"].mean()
    vol_adj_roi = avg_ret / (df["return"].std() or 1e-9)

    # Top-N precision
    top_df = df.sort_values("score", ascending=False).head(top_n)
    top_hits = (top_df["return"] > 0).sum()
    top_precision = top_hits / len(top_df)

    # Sector summary
    sec = (
        df.groupby("sector")["return"]
        .agg(["mean", "count"])
        .rename(columns={"mean": "avg_return", "count": "count"})
    )

    result = {
        "timestamp": datetime.utcnow().isoformat(),
        "count": total,
        "hit_rate": round(hit_rate, 4),
        "avg_return": round(avg_ret, 4),
        "vol_adj_roi": round(vol_adj_roi, 4),
        "top_precision": round(top_precision, 4),
        "sector_summary": sec.to_dict(orient="index"),
    }

    # Save outputs
    (METRICS / "backtest_perf.json").write_text(json.dumps(result, indent=2))
    pd.DataFrame([result]).to_csv(REPORTS / "backtest_summary.csv", index=False)
    return result
