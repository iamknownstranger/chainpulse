"""Token-bucket behavior tests."""

from __future__ import annotations

import time

import pytest

from chainpulse.ratelimit import AsyncTokenBucket


async def test_burst_up_to_capacity_is_instant() -> None:
    bucket = AsyncTokenBucket(rate_per_sec=1000, capacity=3)
    start = time.monotonic()
    for _ in range(3):
        await bucket.acquire()
    assert time.monotonic() - start < 0.2


async def test_refill_paces_requests() -> None:
    bucket = AsyncTokenBucket(rate_per_sec=50, capacity=1)
    await bucket.acquire()  # drains the single token
    start = time.monotonic()
    await bucket.acquire()  # must wait ~20ms for refill
    elapsed = time.monotonic() - start
    assert 0.005 <= elapsed < 0.5


async def test_invalid_params_rejected() -> None:
    with pytest.raises(ValueError):
        AsyncTokenBucket(rate_per_sec=0)
    bucket = AsyncTokenBucket(rate_per_sec=1, capacity=2)
    with pytest.raises(ValueError):
        await bucket.acquire(tokens=5)
