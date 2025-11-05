import os
import random

# Always default to 1337 for deterministic test baseline
seed = int(os.getenv("ETS_SEED", "1337"))

if os.getenv("ETS_STRICT_DETERMINISM") == "1":
    random.seed(seed)
    print(f"[INFO] sitecustomize: deterministic mode active with seed={seed}")
