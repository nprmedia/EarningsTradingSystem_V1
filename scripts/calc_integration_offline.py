#!/usr/bin/env python3
# Auto-refactored for Ruff/Black compliance
import sys
import os
import pathlib
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def main():
    ROOT = Path(__file__).resolve().parents[1]
    os.chdir(ROOT)
    (ROOT / "metrics").mkdir(exist_ok=True)
    (ROOT / "reports").mkdir(exist_ok=True)

    # Ensure we can import your package from ./src without editable install
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

    from ets.core.scoring import compute_scores  # real engine

    def main():
        # Minimal deterministic universe with all 12 normalized columns your scorer expects
        data = [
            {
                "symbol": "AAPL",
                "sector": "Technology",
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
                "last": 191.23,
            },
            {
                "symbol": "MSFT",
                "sector": "Technology",
                "M_norm": 0.68,
                "V_norm": 0.63,
                "S_norm": 0.64,
                "A_norm": 0.62,
                "sigma_norm": 0.58,
                "tau_norm": 0.61,
                "CAL_norm": 0.66,
                "SRM_norm": 0.62,
                "PEER_norm": 0.60,
                "ETFF_norm": 0.59,
                "VIX_norm": 0.50,
                "TREND_norm": 0.63,
                "last": 419.55,
            },
            {
                "symbol": "XOM",
                "sector": "Energy",
                "M_norm": 0.48,
                "V_norm": 0.47,
                "S_norm": 0.46,
                "A_norm": 0.49,
                "sigma_norm": 0.50,
                "tau_norm": 0.48,
                "CAL_norm": 0.45,
                "SRM_norm": 0.46,
                "PEER_norm": 0.47,
                "ETFF_norm": 0.48,
                "VIX_norm": 0.50,
                "TREND_norm": 0.45,
                "last": 116.02,
            },
        ]
        df = pd.DataFrame(data)

        # Base weights covering your 12 factors (sum not required but included)
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

        # Sector multipliers (default = 1.0; tweakable per-key per-sector)
        mult = {
            "_default": {
                "m": 1.0,
                "v": 1.0,
                "s": 1.0,
                "a": 1.0,
                "sigma": 1.0,
                "tau": 1.0,
                "cal": 1.0,
                "srm": 1.0,
                "peer": 1.0,
                "etff": 1.0,
                "vix_risk": 1.0,
                "trend": 1.0,
            },
            "Energy": {
                # example: slightly downweight momentum in Energy
                "m": 0.95,
                "v": 1.0,
                "s": 1.0,
                "a": 1.0,
                "sigma": 1.0,
                "tau": 1.0,
                "cal": 1.0,
                "srm": 1.0,
                "peer": 1.0,
                "etff": 1.0,
                "vix_risk": 1.0,
                "trend": 1.0,
            },
        }

        # Caps dict (signature requires it; unused in the current compute path)
        caps = {}

        df_scores = compute_scores(df, base, mult, caps)

        # Compose trader-friendly view (ticker/sector/price/score/rank/reco)
        out = df_scores.copy()
        out["ticker"] = out["symbol"]
        out["price"] = out.get("last", 0.0).astype(float)
        out = out[["ticker", "sector", "price", "score", "score_base_sum"]].copy()
        out = out.sort_values("score", ascending=False).reset_index(drop=True)
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

        out_path = (
            pathlib.Path(__file__).resolve().parents[1]
            / "out"
            / "integration_scores.csv"
        )
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(out_path, index=False)
        print(f"[OK] wrote {out_path} with {len(out)} rows")

    if __name__ == "__main__":
        main()


if __name__ == "__main__":
    main()
