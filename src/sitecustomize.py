# sitecustomize.py
import os
import random

seed = os.getenv("ETS_SEED")
strict = os.getenv("ETS_STRICT_DETERMINISM")

if strict == "1" and seed is not None:
    random.seed(int(seed))
