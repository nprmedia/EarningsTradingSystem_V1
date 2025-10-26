import sys
import os
import pathlib

# --- normalize paths for CI ---
ROOT = pathlib.Path(__file__).resolve().parents[1]
os.chdir(ROOT)
os.makedirs("metrics", exist_ok=True)
os.makedirs("reports", exist_ok=True)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))


# rest of your existing code below remains unchanged
if __name__ == "__main__":
    # (whatever your run logic already contains)
    pass
