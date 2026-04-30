"""Redis client that resolves a DSN per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import AsyncContextManager, AsyncIterator, Mapping, Sequence
from uuid import UUID

import attrs
from redis.asyncio.client import Pipeline

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError
from forze.base.primitives import JsonDict

from .client import RedisClient, RedisConfig
from .types import RedisPubSubMessage, RedisStreamResponse

# ----------------------- #


@attrs.define(slots=True)
class RoutedRedisClient:
    """Routes each call to a lazily created :class:`RedisClient` for the current tenant.

    DSN strings are resolved via :meth:`SecretsPort.resolve_str` and
    ``secret_ref_for_tenant``. Use :func:`~forze_redis.execution.lifecycle.routed_redis_lifecycle_step`
    after registering the same instance under :data:`RedisClientDepKey`.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef]
    tenant_provider: Callable[[], UUID | None]
    pool_config: RedisConfig = attrs.field(factory=RedisConfig)
    max_cached_tenants: int = 100

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _clients: OrderedDict[UUID, RedisClient] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise CoreError("max_cached_tenants must be at least 1")

    # ....................... #

    async def startup(self) -> None:
        self._started = True

    # ....................... #

    async def close(self) -> None:
        async with self._lock:
            to_close = list(self._clients.values())
            self._clients.clear()

        for c in to_close:
            await c.close()

        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        async with self._lock:
            client = self._clients.pop(tenant_id, None)

        if client is not None:
            await client.close()

    # ....................... #

    def _require_tenant_id(self) -> UUID:
        tid = self.tenant_provider()

        if tid is None:
            raise CoreError(
                "Tenant ID is required for routed Redis access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    async def _get_client(self) -> RedisClient:
        if not self._started:
            raise InfrastructureError("Routed Redis client is not started")

        tid = self._require_tenant_id()

        async with self._lock:
            if tid in self._clients:
                client = self._clients[tid]
                self._clients.move_to_end(tid)
                return client

            ref = self.secret_ref_for_tenant(tid)

            try:
                dsn = await self.secrets.resolve_str(ref)

            except SecretNotFoundError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve Redis secret for tenant {tid}: {e}",
                ) from e

            client = RedisClient()
            await client.initialize(dsn, config=self.pool_config)
            self._clients[tid] = client
            self._clients.move_to_end(tid)

            while len(self._clients) > self.max_cached_tenants:
                _, old = self._clients.popitem(last=False)
                await old.close()

            return client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    # ....................... #

    def pipeline(self, *, transaction: bool = True) -> AsyncContextManager[Pipeline]:
        @asynccontextmanager
        async def _cm() -> AsyncIterator[Pipeline]:
            inner = await self._get_client()

            async with inner.pipeline(transaction=transaction) as pipe:
                yield pipe

        return _cm()

    # ....................... #

    async def get(self, key: str) -> bytes | str | None:
        inner = await self._get_client()
        return await inner.get(key)

    async def mget(self, keys: Sequence[str]) -> list[bytes | str | None]:
        inner = await self._get_client()
        return await inner.mget(keys)

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
        inner = await self._get_client()
        return await inner.set(key, value, ex=ex, px=px, nx=nx, xx=xx)

    async def mset(
        self,
        mapping: Mapping[str, bytes | str],
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        inner = await self._get_client()
        return await inner.mset(mapping, ex=ex, px=px, nx=nx, xx=xx)

    async def delete(self, *keys: str) -> int:
        inner = await self._get_client()
        return await inner.delete(*keys)

    async def unlink(self, *keys: str) -> int:
        inner = await self._get_client()
        return await inner.unlink(*keys)

    async def expire(self, key: str, seconds: int) -> bool:
        inner = await self._get_client()
        return await inner.expire(key, seconds)

    async def incr(self, key: str, by: int = 1) -> int:
        inner = await self._get_client()
        return await inner.incr(key, by)

    async def decr(self, key: str, by: int = 1) -> int:
        inner = await self._get_client()
        return await inner.decr(key, by)

    async def reset(self, key: str, value: int) -> int:
        inner = await self._get_client()
        return await inner.reset(key, value)

    async def publish(self, channel: str, message: bytes | str) -> int:
        inner = await self._get_client()
        return await inner.publish(channel, message)

    async def subscribe(
        self,
        channels: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[RedisPubSubMessage]:
        inner = await self._get_client()
        async for msg in inner.subscribe(channels, timeout=timeout):
            yield msg

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
        inner = await self._get_client()
        return await inner.xadd(
            stream,
            data,
            id=id,
            maxlen=maxlen,
            approx=approx,
            nomkstream=nomkstream,
            minid=minid,
            limit=limit,
        )

    async def xread(
        self,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block_ms: int | None = None,
    ) -> RedisStreamResponse:
        inner = await self._get_client()
        return await inner.xread(streams, count=count, block_ms=block_ms)

    async def xdel(self, stream: str, ids: Sequence[str]) -> int:
        inner = await self._get_client()
        return await inner.xdel(stream, ids)

    async def xtrim_maxlen(
        self,
        stream: str,
        maxlen: int,
        *,
        approx: bool = True,
        limit: int | None = None,
    ) -> int:
        inner = await self._get_client()
        return await inner.xtrim_maxlen(stream, maxlen, approx=approx, limit=limit)

    async def xtrim_minid(
        self,
        stream: str,
        minid: str,
        *,
        approx: bool = True,
        limit: int | None = None,
    ) -> int:
        inner = await self._get_client()
        return await inner.xtrim_minid(stream, minid, approx=approx, limit=limit)

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str = "0-0",
        mkstream: bool = True,
    ) -> bool:
        inner = await self._get_client()
        return await inner.xgroup_create(stream, group, id=id, mkstream=mkstream)

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
        inner = await self._get_client()
        return await inner.xgroup_read(
            group,
            consumer,
            streams,
            count=count,
            block_ms=block_ms,
            noack=noack,
        )

    async def xack(self, stream: str, group: str, ids: Sequence[str]) -> int:
        inner = await self._get_client()
        return await inner.xack(stream, group, ids)
