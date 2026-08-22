"""Connector base class and registry."""

from __future__ import annotations

from abc import ABC
from typing import ClassVar, Self

from chainpulse.http import HttpClient


class Connector(ABC):
    """A single data venue. Subclasses own their transport details and
    return normalized models; nothing downstream knows the wire format."""

    venue: ClassVar[str]

    def __init__(self, client: HttpClient | None = None) -> None:
        self._owns_client = client is None
        self.client = client or HttpClient()

    async def aclose(self) -> None:
        if self._owns_client:
            await self.client.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()
