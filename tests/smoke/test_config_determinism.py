from __future__ import annotations

import os
import random


def test_env_determinism():
    assert os.getenv("ETS_STRICT_DETERMINISM") == "1"
    random.seed(int(os.getenv("ETS_SEED", "1337")))
    assert [random.random() for _ in range(3)] == [
        0.6177528569514706,
        0.5332655736050008,
        0.36584835924937553,
    ]
