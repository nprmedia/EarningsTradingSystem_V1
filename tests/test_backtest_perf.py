import json
import pathlib


def test_perf_outputs_exist():
    root = pathlib.Path(__file__).resolve().parents[1]
    m = root / "metrics/backtest_perf.json"
    r = root / "reports/backtest_summary.csv"
    assert m.exists() and r.exists(), "Metrics or reports missing"
    data = json.loads(m.read_text())
    for key in ["hit_rate", "avg_return", "vol_adj_roi", "top_precision"]:
        assert key in data
        assert isinstance(data[key], (int, float))
