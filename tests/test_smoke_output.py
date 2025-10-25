import csv
import pathlib


def test_smoke_output():
    OUT = pathlib.Path("out/smoke_trades.csv")
    assert OUT.exists(), "No output file"
    rows = list(csv.DictReader(open(OUT)))
    assert len(rows) == 3
    assert rows[0]["ticker"] == "MSFT"
    assert all(float(r["score"]) == float(r["score"]) for r in rows)
    assert rows == sorted(rows, key=lambda r: int(r["rank"]))
    print("\nSmoke test OK ✓ — %d rows verified" % len(rows))
