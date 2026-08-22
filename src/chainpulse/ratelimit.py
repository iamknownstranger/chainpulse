"""Async token-bucket rate limiter.

Public keyless APIs are IP-throttled; a client-side token bucket keeps us a
polite citizen and out of 429 territory without any coordination.
"""

from __future__ import annotations

import asyncio
import time


class AsyncTokenBucket:
    def __init__(self, rate_per_sec: float, capacity: float | None = None) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be > 0")
        self.rate = float(rate_per_sec)
        self.capacity = float(capacity if capacity is not None else max(1.0, rate_per_sec))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
        self._updated = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Wait until ``tokens`` are available, then consume them."""
        if tokens > self.capacity:
            raise ValueError("requested tokens exceed bucket capacity")
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
            await asyncio.sleep(deficit / self.rate)
