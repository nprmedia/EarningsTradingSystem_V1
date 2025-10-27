#!/usr/bin/env python3
# Auto-refactored for Ruff/Black compliance
import sys
import os
import pathlib
import time
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def main():
    ROOT = Path(__file__).resolve().parents[1]
    os.chdir(ROOT)
    (ROOT / "metrics").mkdir(exist_ok=True)
    (ROOT / "reports").mkdir(exist_ok=True)
    import sys
    import json
    import datetime
    import logging
    from typing import Dict, Any, List

    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

    from ets.core.scoring import compute_scores  # real engine

    ROOT = pathlib.Path(__file__).resolve().parents[1]
    OUT = ROOT / "out"
    LOGS = ROOT / "logs"
    FIX = ROOT / "tests" / "fixtures"

    def read_quotes() -> Dict[str, Any]:
        return json.loads((FIX / "mock_quotes.json").read_text())

    def build_df(symbols: List[str]) -> pd.DataFrame:
        q = read_quotes()
        # minimal deterministic norms (tweakable); 12 normalized columns present
        base = {
            "M_norm": 0.60,
            "V_norm": 0.55,
            "S_norm": 0.58,
            "A_norm": 0.57,
            "sigma_norm": 0.52,
            "tau_norm": 0.54,
            "CAL_norm": 0.59,
            "SRM_norm": 0.56,
            "PEER_norm": 0.53,
            "ETFF_norm": 0.52,
            "VIX_norm": 0.50,
            "TREND_norm": 0.56,
        }
        tech_boost = {
            k: (v + 0.06 if k.endswith("_norm") else v) for k, v in base.items()
        }
        rows = []
        for s in symbols:
            last = float(q[s]["last"])
            sector = "Technology" if s in ("AAPL", "MSFT") else "Energy"
            norms = tech_boost if sector == "Technology" and s == "MSFT" else base
            rows.append({"symbol": s, "sector": sector, **norms, "last": last})
        return pd.DataFrame(rows)

    def ensure_dirs():
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / "archive").mkdir(parents=True, exist_ok=True)
        LOGS.mkdir(parents=True, exist_ok=True)

    def dated_output_path(session: str) -> pathlib.Path:
        date_tag = datetime.datetime.utcnow().strftime("%Y%m%d")
        return OUT / f"{date_tag}_{session}.csv"

    def rotate_if_exists(path: pathlib.Path):
        if path.exists():
            ts = int(time.time())
            path.rename(
                path.with_name(path.stem + f"_{ts}.bak" + path.suffix)
                .with_suffix("")
                .with_suffix(".csv")
            )
            # move to archive
            arch = OUT / "archive" / path.name.replace(".csv", f"_{ts}.bak.csv")
            (OUT / "archive").mkdir(parents=True, exist_ok=True)
            arch.write_bytes(path.read_bytes()) if path.exists() else None

    def configure_logging(session: str):
        date_tag = datetime.datetime.utcnow().strftime("%Y%m%d")
        log_path = LOGS / f"{date_tag}_{session}.log"
        logging.basicConfig(
            filename=str(log_path),
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
        return log_path

    def main():
        ensure_dirs()
        session = os.getenv("ETS_SESSION", "amc")
        out_path = dated_output_path(session)
        log_path = configure_logging(session)
        logging.info("Starting CLI mock run session=%s", session)

        symbols = ["AAPL", "MSFT", "XOM"]
        df = build_df(symbols)

        base = {
            "m": 0.12,
            "v": 0.10,
            "s": 0.10,
            "a": 0.10,
            "sigma": 0.08,
            "tau": 0.08,
            "cal": 0.10,
            "srm": 0.10,
            "peer": 0.07,
            "etff": 0.07,
            "vix_risk": 0.04,
            "trend": 0.04,
        }
        mult = {
            "_default": {
                k: 1.0
                for k in [
                    "m",
                    "v",
                    "s",
                    "a",
                    "sigma",
                    "tau",
                    "cal",
                    "srm",
                    "peer",
                    "etff",
                    "vix_risk",
                    "trend",
                ]
            },
            "Energy": {"m": 0.95},
        }
        caps = {}

        scored = compute_scores(df.copy(), base, mult, caps)
        out = scored.copy()
        out["ticker"] = out["symbol"]
        out["price"] = out.get("last", 0.0).astype(float)
        out = (
            out[["ticker", "sector", "price", "score", "score_base_sum"]]
            .sort_values("score", ascending=False)
            .reset_index(drop=True)
        )
        out["rank"] = out.index + 1
        out["recommendation"] = out["score"].apply(
            lambda s: "BUY" if float(s) > 0 else "PASS"
        )
        out = out[
            [
                "ticker",
                "sector",
                "price",
                "score",
                "rank",
                "recommendation",
                "score_base_sum",
            ]
        ]

        rotate_if_exists(out_path)
        out.to_csv(out_path, index=False)
        logging.info("Wrote output to %s", out_path)
        print(f"[OK] wrote {out_path} with {len(out)} rows; log={log_path}")

    if __name__ == "__main__":
        main()


if __name__ == "__main__":
    main()
