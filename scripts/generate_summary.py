#!/usr/bin/env python3
# Auto-fixed summary generator (Ruff/Black clean)

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def run_summary():
    """Return minimal OK payload for CI sanity checks."""
    return {"status": "ok"}


def main():
    root = Path(__file__).resolve().parents[1]
    os.chdir(root)
    (root / "metrics").mkdir(exist_ok=True)
    (root / "reports").mkdir(exist_ok=True)
    out = run_summary()
    print(out)


if __name__ == "__main__":
    main()
