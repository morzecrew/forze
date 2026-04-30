"""SQS client that resolves credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncIterator, Sequence
from uuid import UUID

import attrs
from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsPort,
    resolve_structured,
)
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError

from .client import SQSClient, SQSConfig
from .routing_credentials import SQSRoutingCredentials
from .types import SQSQueueMessage

# ----------------------- #


@attrs.define(slots=True)
class RoutedSQSClient:
    """Routes each operation to a lazily created :class:`SQSClient` for the current tenant.

    Credentials are JSON secrets (see :class:`SQSRoutingCredentials`) resolved via
    :func:`~forze.application.contracts.secrets.resolve_structured`.

    Register this instance under :data:`~forze_sqs.execution.deps.SQSClientDepKey` and
    use :func:`~forze_sqs.execution.lifecycle.routed_sqs_lifecycle_step` for startup/shutdown.

    Do not combine with :func:`~forze_sqs.execution.lifecycle.sqs_lifecycle_step` on the same
    registered instance.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    botocore_config: SQSConfig | None = None
    max_cached_tenants: int = 100

    _lock: asyncio.Lock = attrs.field(factory=asyncio.Lock, init=False)
    _clients: OrderedDict[UUID, SQSClient] = attrs.field(
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
                "Tenant ID is required for routed SQS access",
                code="tenant_required",
            )

        return tid

    # ....................... #

    async def _get_client(self) -> SQSClient:
        if not self._started:
            raise InfrastructureError("Routed SQS client is not started")

        tid = self._require_tenant_id()

        async with self._lock:
            if tid in self._clients:
                client = self._clients[tid]
                self._clients.move_to_end(tid)
                return client

            ref = self._get_secret_ref(tid)

            try:
                creds = await resolve_structured(
                    self.secrets,
                    ref,
                    SQSRoutingCredentials,
                )

            except SecretNotFoundError:
                raise

            except CoreError:
                raise

            except Exception as e:
                raise InfrastructureError(
                    f"Failed to resolve SQS secret for tenant {tid}: {e}",
                ) from e

            client = SQSClient()
            await client.initialize(
                creds.endpoint,
                creds.access_key_id,
                creds.secret_access_key,
                region_name=creds.region_name,
                config=self.botocore_config,
            )
            self._clients[tid] = client
            self._clients.move_to_end(tid)

            while len(self._clients) > self.max_cached_tenants:
                _, old = self._clients.popitem(last=False)
                await old.close()

            return client

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncIterator[AsyncSQSClient]:
        inner = await self._get_client()

        async with inner.client() as c:
            yield c

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.health()

    async def create_queue(
        self,
        queue: str,
        *,
        attributes: dict[str, str] | None = None,
    ) -> str:
        inner = await self._get_client()

        async with inner.client():
            return await inner.create_queue(queue, attributes=attributes)

    async def queue_url(self, queue: str) -> str:
        inner = await self._get_client()

        async with inner.client():
            return await inner.queue_url(queue)

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

        async with inner.client():
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

        async with inner.client():
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
    ) -> list[SQSQueueMessage]:
        inner = await self._get_client()

        async with inner.client():
            return await inner.receive(queue, limit=limit, timeout=timeout)

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[SQSQueueMessage]:
        inner = await self._get_client()

        async with inner.client():
            async for msg in inner.consume(queue, timeout=timeout):
                yield msg

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        inner = await self._get_client()

        async with inner.client():
            return await inner.ack(queue, ids)

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        inner = await self._get_client()

        async with inner.client():
            return await inner.nack(queue, ids, requeue=requeue)
