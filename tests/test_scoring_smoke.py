import pandas as pd
from ets.core.scoring import compute_scores


def test_scoring_smoke():
    df = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "M_norm": 0,
                "V_norm": 0,
                "S_norm": 0,
                "A_norm": 0,
                "sigma_norm": 0,
                "tau_norm": 0,
                "CAL_norm": 0,
                "SRM_norm": 0,
                "PEER_norm": 0,
                "ETFF_norm": 0,
                "VIX_norm": 0,
                "TREND_norm": 0,
            }
        ]
    )
    base = {
        k: 1.0
        for k in [
            "M",
            "V",
            "S",
            "A",
            "sigma",
            "tau",
            "CAL",
            "SRM",
            "PEER",
            "ETFF",
            "VIX",
            "TREND",
        ]
    }
    out = compute_scores(df, base=base, mult={}, caps={})
    assert "score" in out.columns
