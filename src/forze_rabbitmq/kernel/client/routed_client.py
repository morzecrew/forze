"""RabbitMQ client that resolves a DSN per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable, Mapping, Sequence, final
from uuid import UUID

import attrs
from aio_pika.abc import AbstractChannel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_dsn_fingerprint,
    require_tenant_id,
    resolve_dsn_for_tenant,
)
from .client import RabbitMQClient
from .port import RabbitMQClientPort
from .types import RabbitMQQueueMessage
from .value_objects import RabbitMQConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedRabbitMQClient(RabbitMQClientPort):
    """Routes each call to a lazily created :class:`RabbitMQClient` for the current tenant.

    DSN strings are resolved via :meth:`SecretsPort.resolve_str` and
    ``secret_ref_for_tenant``. Use
    :func:`~forze_rabbitmq.execution.lifecycle.routed_rabbitmq_lifecycle_step`
    after registering the same instance under :data:`RabbitMQClientDepKey`.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    connection_config: RabbitMQConfig = attrs.field(factory=RabbitMQConfig)
    max_cached_tenants: int = 100

    __pool: TenantClientRegistry[RabbitMQClient, str] = attrs.field(init=False)

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

    async def _create_client(self, tid: UUID) -> RabbitMQClient:
        dsn = await resolve_dsn_for_tenant(
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="RabbitMQ",
        )

        client = RabbitMQClient()
        await client.initialize(dsn, config=self.connection_config)

        return client

    # ....................... #

    async def _get_client(self) -> RabbitMQClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed RabbitMQ access",
        )

        await ensure_dsn_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="RabbitMQ",
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()
        return await inner.health()

    # ....................... #

    @asynccontextmanager
    async def channel(self) -> AsyncGenerator[AbstractChannel]:
        inner = await self._get_client()

        async with inner.channel() as ch:
            yield ch

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        body: bytes,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_id: str | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        delayed_delivery: bool = False,
    ) -> str:
        inner = await self._get_client()
        return await inner.enqueue(
            queue,
            body,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            message_id=message_id,
            delay=delay,
            not_before=not_before,
            delayed_delivery=delayed_delivery,
        )

    async def enqueue_many(
        self,
        queue: str,
        bodies: Sequence[bytes],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        message_ids: Sequence[str] | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
        delayed_delivery: bool = False,
    ) -> list[str]:
        inner = await self._get_client()
        return await inner.enqueue_many(
            queue,
            bodies,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            message_ids=message_ids,
            delay=delay,
            not_before=not_before,
            delayed_delivery=delayed_delivery,
        )

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[RabbitMQQueueMessage]:
        inner = await self._get_client()
        return await inner.receive(queue, limit=limit, timeout=timeout)

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[RabbitMQQueueMessage]:
        inner = await self._get_client()
        async for msg in inner.consume(queue, timeout=timeout):
            yield msg

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        inner = await self._get_client()
        return await inner.ack(queue, ids)

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        inner = await self._get_client()
        return await inner.nack(queue, ids, requeue=requeue)
