"""RabbitMQ client that resolves a DSN per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncIterator, Sequence
from uuid import UUID

import attrs
from aio_pika.abc import AbstractChannel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError

from .client import RabbitMQClient, RabbitMQConfig
from .types import RabbitMQQueueMessage

# ----------------------- #


@attrs.define(slots=True)
class RoutedRabbitMQClient:
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

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _clients: OrderedDict[UUID, RabbitMQClient] = attrs.field(
        factory=OrderedDict,
        init=False,
    )
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise CoreError("max_cached_tenants must be at least 1")

    # ....................... #

    def _get_secret_ref(self, tenant_id: UUID) -> SecretRef:
        if callable(self.secret_ref_for_tenant):
            return self.secret_ref_for_tenant(tenant_id)

        return self.secret_ref_for_tenant[tenant_id]

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
                "Tenant ID is required for routed RabbitMQ access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    async def _get_client(self) -> RabbitMQClient:
        if not self._started:
            raise InfrastructureError("Routed RabbitMQ client is not started")

        tid = self._require_tenant_id()

        async with self._lock:
            if tid in self._clients:
                client = self._clients[tid]
                self._clients.move_to_end(tid)
                return client

            ref = self._get_secret_ref(tid)

            try:
                dsn = await self.secrets.resolve_str(ref)

            except SecretNotFoundError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve RabbitMQ secret for tenant {tid}: {e}",
                ) from e

            client = RabbitMQClient()
            await client.initialize(dsn, config=self.connection_config)
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

    @asynccontextmanager
    async def channel(self) -> AsyncIterator[AbstractChannel]:
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
    ) -> str:
        inner = await self._get_client()
        return await inner.enqueue(
            queue,
            body,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            message_id=message_id,
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
    ) -> list[str]:
        inner = await self._get_client()
        return await inner.enqueue_many(
            queue,
            bodies,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            message_ids=message_ids,
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
    ) -> AsyncIterator[RabbitMQQueueMessage]:
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
