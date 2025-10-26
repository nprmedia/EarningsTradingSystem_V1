import os
import sys
import pathlib
import json

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
from ets.validation.provider_parity import validate_providers


def main():
    mode = os.getenv("ETS_API_MODE", "mock").lower()
    symbols = os.getenv("ETS_SYMBOLS", "AAPL,MSFT,XOM").split(",")
    res = validate_providers(
        [s.strip().upper() for s in symbols if s.strip()], mode=mode
    )
    print(json.dumps({"mode": mode, **res}, indent=2))


if __name__ == "__main__":
    main()
