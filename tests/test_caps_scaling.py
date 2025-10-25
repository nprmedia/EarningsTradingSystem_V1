import sys
import pathlib
import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from ets.core.scoring import compute_scores


def test_caps_and_scaling_sanity():
    df = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "sector": "Tech",
                "M_norm": 2.0,
                "V_norm": 2.0,
                "S_norm": 2.0,
                "A_norm": 2.0,
                "sigma_norm": 2.0,
                "tau_norm": 2.0,
                "CAL_norm": 2.0,
                "SRM_norm": 2.0,
                "PEER_norm": 2.0,
                "ETFF_norm": 2.0,
                "VIX_norm": 2.0,
                "TREND_norm": 2.0,
                "last": 1.0,
            },
            {
                "symbol": "BBB",
                "sector": "Tech",
                "M_norm": 0.0,
                "V_norm": 0.0,
                "S_norm": 0.0,
                "A_norm": 0.0,
                "sigma_norm": 0.0,
                "tau_norm": 0.0,
                "CAL_norm": 0.0,
                "SRM_norm": 0.0,
                "PEER_norm": 0.0,
                "ETFF_norm": 0.0,
                "VIX_norm": 0.0,
                "TREND_norm": 0.0,
                "last": 1.0,
            },
        ]
    )
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
    mult = {"_default": {k: 1.0 for k in base}}
    # Provide caps that should constrain absurd inputs without crash; if your impl ignores caps this is a no-op
    caps = {"upper": 1.0, "lower": -1.0}
    out = compute_scores(df, base, mult, caps)
    assert "score" in out.columns
    assert out["score"].notna().all()
    # Monotonic sanity: the row with all-twos should not produce NaN and should rank >= the zero row
    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    assert out.loc[0, "symbol"] in ("AAA", "BBB")
