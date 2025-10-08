from collections import deque
from time import monotonic, sleep
from typing import Dict


class RateLimiter:
    """
    Rolling-window limiter with per-second and per-minute caps and a 'reserve'
    (we keep at least 'reserve' calls unused in each window).
    """

    def __init__(
        self, per_second: int, per_minute: int, reserve: int = 2, name: str = "api"
    ):
        self.name = name
        self.ps = max(0, per_second - reserve) if per_second else 0
        self.pm = max(0, per_minute - reserve) if per_minute else 0
        self.sec_q = deque()  # timestamps last 1s
        self.min_q = deque()  # timestamps last 60s
        self.stats = {
            "name": name,
            "per_second": per_second,
            "per_minute": per_minute,
            "reserve": reserve,
            "allowed": 0,
            "blocked": 0,
            "sleeps": 0,
        }

    def _prune(self, now: float):
        one_sec_ago = now - 1.0
        one_min_ago = now - 60.0
        while self.sec_q and self.sec_q[0] < one_sec_ago:
            self.sec_q.popleft()
        while self.min_q and self.min_q[0] < one_min_ago:
            self.min_q.popleft()

    def _room(self, now: float) -> bool:
        self._prune(now)
        ok_sec = (self.ps == 0) or (len(self.sec_q) < self.ps)
        ok_min = (self.pm == 0) or (len(self.min_q) < self.pm)
        return ok_sec and ok_min

    def acquire(self, blocking: bool = True, jitter_s: float = 0.03):
        while True:
            now = monotonic()
            if self._room(now):
                self.sec_q.append(now)
                self.min_q.append(now)
                self.stats["allowed"] += 1
                return
            if not blocking:
                self.stats["blocked"] += 1
                raise RuntimeError(f"Rate limited: {self.name}")
            self._prune(now)
            to_sec = (self.sec_q[0] + 1.0 - now) if self.sec_q else 0.01
            to_min = (self.min_q[0] + 60.0 - now) if self.min_q else 0.01
            wait = max(to_sec, to_min) + jitter_s
            self.stats["sleeps"] += 1
            sleep(max(0.01, wait))

    def get_stats(self) -> Dict:
        return dict(self.stats)
