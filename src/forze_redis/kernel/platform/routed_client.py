"""Redis client that resolves a DSN per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from datetime import timedelta
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Callable,
    Mapping,
    Sequence,
    final,
)
from uuid import UUID

import attrs
from redis.asyncio.client import Pipeline

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_dsn_fingerprint,
    require_tenant_id,
    resolve_dsn_for_tenant,
)
from forze.base.primitives import JsonDict

from .client import RedisClient
from .port import RedisClientPort
from .types import RedisPubSubMessage, RedisStreamResponse
from .value_objects import RedisConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedRedisClient(RedisClientPort):
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

    # ....................... #

    __pool: TenantClientRegistry[RedisClient, str] = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        self.__pool = TenantClientRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
            guarded=False,
        )

    # ....................... #

    async def startup(self) -> None:
        await self.__pool.startup()

    # ....................... #

    async def close(self) -> None:
        await self.__pool.close()

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        await self.__pool.evict(tenant_id)

    # ....................... #

    async def _create_client(self, tid: UUID) -> RedisClient:
        dsn = await resolve_dsn_for_tenant(
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Redis",
        )

        client = RedisClient()
        await client.initialize(dsn, config=self.pool_config)

        return client

    # ....................... #

    async def _get_client(self) -> RedisClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed Redis access",
        )

        await ensure_dsn_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="Redis",
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()

        return await inner.health()

    # ....................... #

    def pipeline(self, *, transaction: bool = True) -> AsyncContextManager[Pipeline]:
        @asynccontextmanager
        async def _cm() -> AsyncGenerator[Pipeline]:
            inner = await self._get_client()

            async with inner.pipeline(transaction=transaction) as pipe:
                yield pipe

        return _cm()

    # ....................... #

    async def exists(self, key: str) -> bool:
        inner = await self._get_client()
        return await inner.exists(key)

    async def pttl(self, key: str) -> int | None:
        inner = await self._get_client()
        return await inner.pttl(key)

    async def pttl_raw_ms(self, key: str) -> int:
        inner = await self._get_client()
        return await inner.pttl_raw_ms(key)

    async def run_script(
        self,
        script: str,
        keys: Sequence[str],
        args: Sequence[Any],
    ) -> str:
        inner = await self._get_client()
        return await inner.run_script(script, keys, args)

    async def get(self, key: str) -> bytes | None:
        inner = await self._get_client()
        return await inner.get(key)

    async def mget(self, keys: Sequence[str]) -> list[bytes | None]:
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
    ) -> AsyncGenerator[RedisPubSubMessage]:
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
