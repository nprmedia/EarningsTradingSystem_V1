from __future__ import annotations

import os
import random


def test_env_determinism():
    assert os.getenv("ETS_STRICT_DETERMINISM") == "1"
    random.seed(int(os.getenv("ETS_SEED", "1337")))
    assert [random.random() for _ in range(3)] == [
        0.2620887430333334,
        0.8010217501192073,
        0.17125531413233357,
    ]
