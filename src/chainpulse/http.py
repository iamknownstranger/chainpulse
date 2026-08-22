"""Shared async HTTP client: token-bucket throttling + retry with backoff."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import aiohttp

from chainpulse.ratelimit import AsyncTokenBucket

log = logging.getLogger(__name__)

RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class HttpError(Exception):
    def __init__(self, status: int, url: str) -> None:
        self.status = status
        super().__init__(f"HTTP {status} from {url}")


def backoff_delay(attempt: int, base: float = 0.5, cap: float = 8.0) -> float:
    """Exponential backoff with full jitter. ``attempt`` is zero-based."""
    raw = min(cap, base * (2**attempt))
    return random.uniform(0, raw)


class HttpClient:
    def __init__(
        self,
        base_url: str = "",
        rate_per_sec: float = 5.0,
        timeout_s: float = 15.0,
        max_retries: int = 3,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._bucket = AsyncTokenBucket(rate_per_sec)
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._max_retries = max_retries
        self._headers = headers or {}
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout, headers={"User-Agent": "chainpulse/0.1", **self._headers}
            )
        return self._session

    async def _request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            await self._bucket.acquire()
            session = await self._ensure_session()
            try:
                async with session.request(method, url, **kwargs) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    if resp.status in RETRYABLE_STATUSES:
                        retry_after = resp.headers.get("Retry-After")
                        delay = (
                            float(retry_after)
                            if retry_after and retry_after.replace(".", "", 1).isdigit()
                            else backoff_delay(attempt)
                        )
                        log.warning("HTTP %s on %s, retrying in %.2fs", resp.status, url, delay)
                        last_exc = HttpError(resp.status, url)
                        await asyncio.sleep(delay)
                        continue
                    raise HttpError(resp.status, url)
            except (TimeoutError, aiohttp.ClientError) as exc:
                last_exc = exc
                delay = backoff_delay(attempt)
                log.warning("%s on %s, retrying in %.2fs", type(exc).__name__, url, delay)
                await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return await self._request_json("GET", path, params=params)

    async def post_json(self, path: str, payload: dict[str, Any] | None = None) -> Any:
        return await self._request_json("POST", path, json=payload)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self) -> HttpClient:
        await self._ensure_session()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
