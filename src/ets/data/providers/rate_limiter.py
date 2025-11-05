from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass
class Window:
    size_sec: float
    capacity: int


class RateLimiter:
    """
    Headroom-aware sliding-window rate limiter with jitterless sleep scheduling.
    Guarantees we never get within `reserve` calls of the max in any window.

    - Supports two windows: per-second and per-minute (configurable).
    - Uses rolling timestamps and computes earliest safe time when headroom is restored.
    - Thread-safe; low contention (single lock).
    - Works as context manager or via .acquire().
    - Optional `cost` to consume >1 token for a single call.
    """

    def __init__(
        self,
        per_second: int | None = None,
        per_minute: int | None = None,
        reserve: int = 2,
        name: str = "limiter",
    ) -> None:
        if per_second is None and per_minute is None:
            raise ValueError("At least one window must be configured")

        self.name = name
        self.reserve = max(0, int(reserve))

        self._windows: list[Window] = []
        if per_second:
            self._windows.append(Window(1.0, int(per_second)))
        if per_minute:
            self._windows.append(Window(60.0, int(per_minute)))

        self._buffers: list[deque[float]] = [deque() for _ in self._windows]
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        for buf, win in zip(self._buffers, self._windows, strict=False):
            cutoff = now - win.size_sec
            while buf and buf[0] <= cutoff:
                buf.popleft()

    def _next_safe_time(self, now: float, cost: int) -> float:
        earliest = now
        for buf, win in zip(self._buffers, self._windows, strict=False):
            used = len(buf)
            allowed = max(0, win.capacity - self.reserve)
            if cost <= 0 or used + cost <= allowed:
                continue
            need_to_expire = used + cost - allowed
            if need_to_expire > len(buf):
                need_to_expire = len(buf)
            idx = need_to_expire - 1
            t_expire = buf[idx] + win.size_sec
            if t_expire > earliest:
                earliest = t_expire
        return earliest

    def acquire(self, cost: int = 1) -> None:
        if cost < 1:
            return
        while True:
            now = time.monotonic()
            with self._lock:
                self._prune(now)
                t_safe = self._next_safe_time(now, cost)
                if t_safe <= now:
                    for buf in self._buffers:
                        for _ in range(cost):
                            buf.append(now)
                    return
            sleep_for = max(0.0, t_safe - time.monotonic())
            time.sleep(min(0.250, sleep_for) if sleep_for > 0 else 0.001)

    def __enter__(self):
        self.acquire(1)
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get_stats(self) -> dict:
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            out = {"name": self.name, "reserve": self.reserve, "windows": []}
            for buf, win in zip(self._buffers, self._windows, strict=False):
                out["windows"].append(
                    {
                        "size_sec": win.size_sec,
                        "capacity": win.capacity,
                        "used": len(buf),
                        "allowed_used": max(0, win.capacity - self.reserve),
                        "headroom": max(0, (win.capacity - self.reserve) - len(buf)),
                    }
                )
            return out


def retry_with_backoff(
    call: Callable[[], T],
    *,
    attempts: int = 5,
    base: float = 0.25,
    max_sleep: float = 8.0,
) -> T:
    """
    Simple exponential backoff helper for transient failures (no jitter).
    """
    delay = base
    last_exc = None
    for i in range(attempts):
        try:
            return call()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if i == attempts - 1:
                break
            time.sleep(delay)
            delay = min(max_sleep, delay * 2.0)
    assert last_exc is not None
    raise last_exc
