from __future__ import annotations
import json
import pathlib
from typing import Dict, Any
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / "out"


def _latest_two() -> tuple[pathlib.Path, pathlib.Path]:
    files = sorted(OUT.glob("*.csv"))
    if len(files) < 2:
        raise FileNotFoundError(
            "Need at least two out/*.csv files for history comparison."
        )
    return files[-2], files[-1]


def compare_latest_two() -> Dict[str, Any]:
    prev_p, cur_p = _latest_two()
    prev = pd.read_csv(prev_p)
    cur = pd.read_csv(cur_p)
    if not {"ticker", "score", "rank"}.issubset(set(prev.columns)) or not {
        "ticker",
        "score",
        "rank",
    }.issubset(set(cur.columns)):
        raise ValueError("Outputs missing required columns for comparison.")
    prev_s = prev.set_index("ticker")
    cur_s = cur.set_index("ticker")
    tickers = sorted(set(prev_s.index).union(cur_s.index))
    diffs = []
    for t in tickers:
        p = prev_s.loc[t] if t in prev_s.index else None
        c = cur_s.loc[t] if t in cur_s.index else None
        entry = {"ticker": t}
        if p is not None:
            entry.update(
                {
                    "prev_score": float(p.get("score", float("nan"))),
                    "prev_rank": int(p.get("rank", 0)),
                }
            )
        else:
            entry.update({"prev_score": None, "prev_rank": None})
        if c is not None:
            entry.update(
                {
                    "cur_score": float(c.get("score", float("nan"))),
                    "cur_rank": int(c.get("rank", 0)),
                }
            )
        else:
            entry.update({"cur_score": None, "cur_rank": None})
        if entry["prev_score"] is not None and entry["cur_score"] is not None:
            entry["delta_score"] = entry["cur_score"] - entry["prev_score"]
            entry["delta_rank"] = (
                (entry["cur_rank"] - entry["prev_rank"])
                if (entry["prev_rank"] and entry["cur_rank"])
                else None
            )
        else:
            entry["delta_score"] = None
            entry["delta_rank"] = None
        diffs.append(entry)
    out = {
        "previous": str(prev_p),
        "current": str(cur_p),
        "count": len(diffs),
        "diffs": diffs,
    }
    (OUT / "history_delta.json").write_text(json.dumps(out, indent=2))
    return out
