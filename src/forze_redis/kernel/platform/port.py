"""Structural protocol for Redis clients (single DSN or tenant-routed)."""

from __future__ import annotations

from datetime import timedelta
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Mapping,
    Protocol,
    Sequence,
)

from redis.asyncio.client import Pipeline

from forze.base.primitives import JsonDict

from .types import RedisPubSubMessage, RedisStreamResponse

# ----------------------- #


class RedisClientPort(Protocol):
    """Operations implemented by :class:`RedisClient` and routed variants."""

    async def close(self) -> None:
        ...  # pragma: no cover

    async def health(self) -> tuple[str, bool]:
        ...  # pragma: no cover

    def pipeline(self, *, transaction: bool = True) -> AsyncContextManager[Pipeline]:
        ...  # pragma: no cover

    async def get(self, key: str) -> bytes | str | None:
        ...  # pragma: no cover

    async def mget(self, keys: Sequence[str]) -> list[bytes | str | None]:
        ...  # pragma: no cover

    async def set(
        self,
        key: str,
        value: bytes | str,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        ...  # pragma: no cover

    async def mset(
        self,
        mapping: Mapping[str, bytes | str],
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        ...  # pragma: no cover

    async def delete(self, *keys: str) -> int:
        ...  # pragma: no cover

    async def unlink(self, *keys: str) -> int:
        ...  # pragma: no cover

    async def expire(self, key: str, seconds: int) -> bool:
        ...  # pragma: no cover

    async def incr(self, key: str, by: int = 1) -> int:
        ...  # pragma: no cover

    async def decr(self, key: str, by: int = 1) -> int:
        ...  # pragma: no cover

    async def reset(self, key: str, value: int) -> int:
        ...  # pragma: no cover

    async def publish(self, channel: str, message: bytes | str) -> int:
        ...  # pragma: no cover

    def subscribe(
        self,
        channels: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[RedisPubSubMessage]:
        """Yield pub/sub messages until cancelled (async iterator / async generator)."""
        ...  # pragma: no cover

    async def xadd(
        self,
        stream: str,
        data: JsonDict,
        *,
        id: str = "*",
        maxlen: int | None = None,
        approx: bool = True,
        nomkstream: bool = False,
        minid: str | None = None,
        limit: int | None = None,
    ) -> str:
        ...  # pragma: no cover

    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block_ms: int | None = None,
    ) -> RedisStreamResponse:
        ...  # pragma: no cover

    async def xdel(self, stream: str, ids: Sequence[str]) -> int:
        ...  # pragma: no cover

    async def xtrim_maxlen(
        self,
        stream: str,
        maxlen: int,
        *,
        approx: bool = True,
        limit: int | None = None,
    ) -> int:
        ...  # pragma: no cover

    async def xtrim_minid(
        self,
        stream: str,
        minid: str,
        *,
        approx: bool = True,
        limit: int | None = None,
    ) -> int:
        ...  # pragma: no cover

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str = "0-0",
        mkstream: bool = True,
    ) -> bool:
        ...  # pragma: no cover

    async def xgroup_read(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block_ms: int | None = None,
        noack: bool = False,
    ) -> RedisStreamResponse:
        ...  # pragma: no cover

    async def xack(self, stream: str, group: str, ids: Sequence[str]) -> int:
        ...  # pragma: no cover
