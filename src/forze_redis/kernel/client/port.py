"""Structural protocol for Redis clients (single DSN or tenant-routed)."""

from collections.abc import AsyncGenerator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import (
    Any,
    Protocol,
)

from redis.asyncio.client import Pipeline

from forze.base.primitives import JsonDict

from .types import (
    RedisAutoClaimResponse,
    RedisPendingEntry,
    RedisPubSubMessage,
    RedisStreamResponse,
)

# ----------------------- #


class RedisClientPort(Protocol):
    """Operations implemented by :class:`RedisClient` and routed variants."""

    def close(self) -> Awaitable[None]: ...  # pragma: no cover

    def health(self) -> Awaitable[tuple[str, bool]]: ...  # pragma: no cover

    def pipeline(
        self,
        *,
        transaction: bool = True,
    ) -> AbstractAsyncContextManager[Pipeline]:
        """Bind a context-local pipeline for **write batching**.

        Value-returning methods called inside the scope raise a precondition
        error (code ``redis_read_in_pipeline``) because pipeline results only
        materialize at ``execute()``.
        """
        ...  # pragma: no cover

    def exists(self, key: str) -> Awaitable[bool]: ...  # pragma: no cover

    def pttl(self, key: str) -> Awaitable[int | None]:
        """Milliseconds until expiry, or ``None`` if missing (``-2``) or no TTL (``-1``)."""
        ...  # pragma: no cover

    def pttl_raw_ms(self, key: str) -> Awaitable[int]:
        """Raw Redis ``PTTL`` in ms: ``>= 0`` time left, ``-1`` persistent, ``-2`` missing."""
        ...  # pragma: no cover

    def run_script(
        self,
        script: str,
        keys: Sequence[str],
        args: Sequence[Any],
    ) -> Awaitable[str]: ...  # pragma: no cover

    def get(self, key: str) -> Awaitable[bytes | None]: ...  # pragma: no cover

    def mget(
        self,
        keys: Sequence[str],
    ) -> Awaitable[list[bytes | None]]: ...  # pragma: no cover

    def scan(
        self,
        cursor: int = 0,
        *,
        match: str | None = None,
        count: int | None = None,
    ) -> Awaitable[tuple[int, list[str]]]:
        """One step of a non-blocking ``SCAN``: returns ``(next_cursor, keys)``.

        Redis' own guarantees, which a caller must honour and cannot infer from the
        signature:

        - **Only a zero cursor means done.** A step may return *no keys* with a non-zero
          cursor (``count`` is a hint about work done, not results returned), so a loop that
          stops on an empty batch silently under-reports. Loop until the cursor comes back
          ``0``.
        - **A key may be returned more than once** across steps. Dedup if it matters.
        - A key present for the whole iteration is returned at least once; one added or
          removed mid-iteration may or may not appear.

        ``match`` is a **glob**, not a literal prefix — escape any ``*?[]\\`` in a value you
        interpolate into it, or the pattern silently matches the wrong keys (or none).
        """
        ...  # pragma: no cover

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

    def expire(
        self,
        key: str,
        seconds: int,
        *,
        gt: bool = False,
    ) -> Awaitable[bool]: ...  # pragma: no cover

    def incr(self, key: str, by: int = 1) -> Awaitable[int]: ...  # pragma: no cover

    def decr(self, key: str, by: int = 1) -> Awaitable[int]: ...  # pragma: no cover

    def reset(self, key: str, value: int) -> Awaitable[int]: ...  # pragma: no cover

    def publish(
        self,
        channel: str,
        message: bytes | str,
    ) -> Awaitable[int]: ...  # pragma: no cover

    def subscribe(
        self,
        channels: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[RedisPubSubMessage]:
        """Yield pub/sub messages until cancelled (async iterator / async generator)."""
        ...  # pragma: no cover

    def track_invalidations(
        self,
        *,
        prefixes: Sequence[str],
        on_keys: Callable[[Sequence[str]], None],
        on_reset: Callable[[], None],
    ) -> Awaitable[Callable[[], Awaitable[None]] | None]:
        """Subscribe to server-side key invalidations (``CLIENT TRACKING`` BCAST).

        Returns an unsubscribe callable, or ``None`` when the client cannot
        provide push (e.g. tenant-routed clients).
        """
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
        self,
        stream: str,
        ids: Sequence[str],
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

    def xlen(self, stream: str) -> Awaitable[int]:
        """``XLEN`` — the stream's entry count (read-only observability)."""
        ...  # pragma: no cover

    def xinfo_groups(self, stream: str) -> Awaitable[list[dict[str, object]]]:
        """``XINFO GROUPS`` rows for *stream* (read-only observability)."""
        ...  # pragma: no cover

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
        self,
        stream: str,
        group: str,
        ids: Sequence[str],
    ) -> Awaitable[int]: ...  # pragma: no cover

    def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int | None = None,
    ) -> Awaitable[RedisAutoClaimResponse]:
        """One ``XAUTOCLAIM`` page; loop on the returned cursor until ``"0-0"``."""
        ...  # pragma: no cover

    def xpending(
        self,
        stream: str,
        group: str,
        *,
        count: int,
        start_id: str = "-",
        end_id: str = "+",
    ) -> Awaitable[list[RedisPendingEntry]]:
        """Extended ``XPENDING`` summary rows for *group*, oldest first."""
        ...  # pragma: no cover
