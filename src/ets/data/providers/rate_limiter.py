from __future__ import annotations

import random
import time
from collections import deque
from typing import Callable, TypeVar

T = TypeVar("T")


class RateLimiter:
    """
    Sliding-window limiter: enforces per-second and per-minute caps.

    Args:
        per_second: allowed calls per rolling second.
        per_minute: allowed calls per rolling minute.
        burst: extra short-term headroom allowed above strict caps.
        reserve: capacity kept unused as safety headroom (e.g., shared quota).
        **_ignore: tolerated for forward compatibility with registry kwargs.
    """

    def __init__(
        self,
        per_second: int = 5,
        per_minute: int = 30,
        burst: int = 2,
        *,
        reserve: int = 0,
        **_ignore,
    ):
        self.per_second = max(1, int(per_second))
        self.per_minute = max(self.per_second, int(per_minute))
        self.burst = max(0, int(burst))
        self.reserve = max(0, int(reserve))

        self._sec = deque()  # timestamps within last 1s
        self._min = deque()  # timestamps within last 60s

        self.stats = {"allowed": 0, "blocked": 0, "sleep_ms": 0.0}

    def _prune(self, now: float) -> None:
        while self._sec and now - self._sec[0] > 1.0:
            self._sec.popleft()
        while self._min and now - self._min[0] > 60.0:
            self._min.popleft()

    def _has_room(self) -> bool:
        sec_cap = max(1, self.per_second + self.burst - self.reserve)
        min_cap = max(1, self.per_minute + self.burst - self.reserve)
        return len(self._sec) < sec_cap and len(self._min) < min_cap

    def acquire(self) -> None:
        """Block until a call is allowed; then record it."""
        now = time.monotonic()
        self._prune(now)

        while not self._has_room():
            self.stats["blocked"] += 1

            now = time.monotonic()
            self._prune(now)

            sec_wait = 0.0 if not self._sec else max(0.0, 1.0 - (now - self._sec[0]))
            min_wait = 0.0 if not self._min else max(0.0, 60.0 - (now - self._min[0]))
            sleep_for = max(0.01, min(sec_wait, min_wait))
            sleep_for += random.uniform(0.0, 0.05)  # jitter

            time.sleep(sleep_for)
            self.stats["sleep_ms"] += sleep_for * 1000.0

            now = time.monotonic()
            self._prune(now)

        self._sec.append(now)
        self._min.append(now)
        self.stats["allowed"] += 1


def with_backoff(
    call: Callable[[], T],
    *,
    tries: int = 4,
    base: float = 0.25,
    max_sleep: float = 4.0,
    jitter: float = 0.25,
) -> T:
    """
    Execute `call` with exponential backoff on exception.

    Args:
        tries: total attempts (>=1)
        base: initial sleep seconds
        max_sleep: upper bound per sleep
        jitter: random [0, jitter] seconds added each sleep
    """
    attempts = max(1, int(tries))
    delay = max(0.0, float(base))
    max_sleep = max(0.0, float(max_sleep))
    jitter = max(0.0, float(jitter))

    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            return call()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if i == attempts - 1:
                break
            sleep_for = min(max_sleep, delay + random.uniform(0.0, jitter))
            time.sleep(sleep_for)
            delay = min(max_sleep, delay * 2.0 if delay else base or 0.25)
    assert last_exc is not None
    raise last_exc
