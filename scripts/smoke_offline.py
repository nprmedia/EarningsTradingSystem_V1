import argparse
import csv
import json
import os
from datetime import date


def _fallback_compute_scores(rows, prices, fundamentals):
    WEIGHTS = {"rev_qoq": 0.4, "eps_qoq": 0.4, "surprise": 0.2}
    out = []
    for r in rows:
        t = r["ticker"]
        p = prices.get(t, {})
        f = fundamentals.get(t, {})
        s = sum(WEIGHTS[k] * float(f.get(k, 0)) for k in WEIGHTS)
        out.append(
            {
                "ticker": t,
                "sector": r.get("sector", ""),
                "price": float(p.get("last", 0)),
                "score": round(s, 6),
            }
        )
    out.sort(key=lambda d: d["score"], reverse=True)
    for i, row in enumerate(out, 1):
        row["rank"] = i
        row["recommendation"] = "BUY" if row["score"] > 0 else "PASS"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    fxdir = os.environ.get("ETS_OFFLINE_FIXTURES_DIR", "tests/fixtures")
    prices = json.load(open(os.path.join(fxdir, "mock_prices.json")))
    funda = json.load(open(os.path.join(fxdir, "mock_fundamentals.json")))
    rows = list(csv.DictReader(open(a.tickers)))
    recs = _fallback_compute_scores(rows, prices, funda)
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["ticker", "sector", "price", "score", "rank", "recommendation"],
        )
        w.writeheader()
        w.writerows(recs)
    print(f"[OK] wrote {a.out} with {len(recs)} rows on {date.today()}")


if __name__ == "__main__":
    main()
