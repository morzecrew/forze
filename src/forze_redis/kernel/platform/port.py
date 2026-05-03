"""Structural protocol for Redis clients (single DSN or tenant-routed)."""

from datetime import timedelta
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
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

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def pipeline(
        self, *, transaction: bool = True
    ) -> AsyncContextManager[Pipeline]: ...  # pragma: no cover

    def get(self, key: str) -> Awaitable[bytes | str | None]: ...  # pragma: no cover

    def mget(
        self, keys: Sequence[str]
    ) -> Awaitable[list[bytes | str | None]]: ...  # pragma: no cover

    def set(
        self,
        key: str,
        value: bytes | str,
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Awaitable[bool]: ...  # pragma: no cover

    def mset(
        self,
        mapping: Mapping[str, bytes | str],
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> Awaitable[bool]: ...  # pragma: no cover

    def delete(self, *keys: str) -> Awaitable[int]: ...  # pragma: no cover

    def unlink(self, *keys: str) -> Awaitable[int]: ...  # pragma: no cover

    def expire(self, key: str, seconds: int) -> Awaitable[bool]: ...  # pragma: no cover

    def incr(self, key: str, by: int = 1) -> Awaitable[int]: ...  # pragma: no cover

    def decr(self, key: str, by: int = 1) -> Awaitable[int]: ...  # pragma: no cover

    def reset(self, key: str, value: int) -> Awaitable[int]: ...  # pragma: no cover

    def publish(
        self, channel: str, message: bytes | str
    ) -> Awaitable[int]: ...  # pragma: no cover

    def subscribe(
        self,
        channels: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[RedisPubSubMessage]:
        """Yield pub/sub messages until cancelled (async iterator / async generator)."""
        ...  # pragma: no cover

    def xadd(
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
    ) -> Awaitable[str]: ...  # pragma: no cover

    def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block_ms: int | None = None,
    ) -> Awaitable[RedisStreamResponse]: ...  # pragma: no cover

    def xdel(
        self, stream: str, ids: Sequence[str]
    ) -> Awaitable[int]: ...  # pragma: no cover

    def xtrim_maxlen(
        self,
        stream: str,
        maxlen: int,
        *,
        approx: bool = True,
        limit: int | None = None,
    ) -> Awaitable[int]: ...  # pragma: no cover

    def xtrim_minid(
        self,
        stream: str,
        minid: str,
        *,
        approx: bool = True,
        limit: int | None = None,
    ) -> Awaitable[int]: ...  # pragma: no cover

    def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str = "0-0",
        mkstream: bool = True,
    ) -> Awaitable[bool]: ...  # pragma: no cover

    def xgroup_read(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block_ms: int | None = None,
        noack: bool = False,
    ) -> Awaitable[RedisStreamResponse]: ...  # pragma: no cover

    def xack(
        self, stream: str, group: str, ids: Sequence[str]
    ) -> Awaitable[int]: ...  # pragma: no cover
