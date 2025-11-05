import os
import random

# Deterministic seeding must happen on first import, before any other module uses random
if os.getenv("ETS_STRICT_DETERMINISM") == "1":
    seed = int(os.getenv("ETS_SEED", "1337"))
    random.seed(seed)
    # Optional safety print for debugging (can remove later)
    # print(f"[DEBUG] Random seeded deterministically with {seed}")
else:
    # Ensure non-deterministic randomness when not in strict mode
    random.seed()
