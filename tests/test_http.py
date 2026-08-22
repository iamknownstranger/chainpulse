"""Retry/backoff unit tests with a fake transport (no network)."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest

from chainpulse.http import HttpClient, backoff_delay


class FakeResponse:
    def __init__(self, status: int, payload: Any = None, headers: dict[str, str] | None = None):
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def json(self) -> Any:
        return self._payload

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str]] = []

    @asynccontextmanager
    async def request(self, method: str, url: str, **kwargs: Any):
        self.calls.append((method, url))
        yield self.responses.pop(0)


def client_with(responses: list[FakeResponse]) -> tuple[HttpClient, FakeSession]:
    client = HttpClient(rate_per_sec=10_000)
    session = FakeSession(responses)
    client._ensure_session = session_ensure(session)  # type: ignore[method-assign]
    return client, session


def session_ensure(session: FakeSession):  # noqa: ANN201
    async def _ensure():
        return session

    return _ensure


async def test_success_first_try() -> None:
    client, session = client_with([FakeResponse(200, {"ok": True})])
    assert await client.get_json("http://x/y") == {"ok": True}
    assert len(session.calls) == 1
    await client.close()


async def test_retries_on_429_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
    client, session = client_with([FakeResponse(429), FakeResponse(200, {"ok": 1})])
    assert await client.get_json("http://x/y") == {"ok": 1}
    assert len(session.calls) == 2
    await client.close()


async def test_gives_up_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "sleep", _instant_sleep)
    client, session = client_with([FakeResponse(503)] * 4)
    client._max_retries = 3
    with pytest.raises(Exception, match="HTTP 503"):
        await client.get_json("http://x/y")
    assert len(session.calls) == 4
    await client.close()


async def test_non_retryable_status_raises_immediately() -> None:
    client, session = client_with([FakeResponse(404)])
    with pytest.raises(Exception, match="HTTP 404"):
        await client.get_json("http://x/y")
    assert len(session.calls) == 1
    await client.close()


def test_backoff_bounds() -> None:
    for attempt in range(6):
        delay = backoff_delay(attempt, base=0.5, cap=8.0)
        ceiling = min(8.0, 0.5 * 2**attempt)
        assert 0 <= delay <= ceiling


async def _instant_sleep(_: float) -> None:
    return None
