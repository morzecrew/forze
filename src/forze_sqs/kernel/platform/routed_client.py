"""SQS client that resolves credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable, Mapping, Sequence, final
from uuid import UUID

import attrs
from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy import (
    TenantClientRegistry,
    ensure_structured_fingerprint,
    require_tenant_id,
    resolve_structured_for_tenant,
)
from forze.base.primitives.fingerprint import stable_fingerprint

from .client import SQSClient
from .port import SQSClientPort
from .routing_credentials import SQSRoutingCredentials
from .types import SQSQueueMessage
from .value_objects import SQSConfig

# ----------------------- #


@final
@attrs.define(slots=True)
class RoutedSQSClient(SQSClientPort):
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

    __pool: TenantClientRegistry[SQSClient, str] = attrs.field(init=False)

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

    async def _fingerprint_for(self, tenant_id: UUID) -> str:
        creds = await resolve_structured_for_tenant(
            SQSRoutingCredentials,
            tenant_id=tenant_id,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="SQS",
        )

        return stable_fingerprint(
            creds.endpoint,
            creds.region_name,
            creds.access_key_id,
        )

    # ....................... #

    async def _create_client(self, tid: UUID) -> SQSClient:
        creds = await resolve_structured_for_tenant(
            SQSRoutingCredentials,
            tenant_id=tid,
            secrets=self.secrets,
            ref_for_tenant=self.secret_ref_for_tenant,
            backend="SQS",
        )

        client = SQSClient()

        await client.initialize(
            creds.endpoint,
            creds.access_key_id,
            creds.secret_access_key,
            region_name=creds.region_name,
            config=self.botocore_config,
        )

        return client

    # ....................... #

    async def _get_client(self) -> SQSClient:
        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed SQS access",
        )

        await ensure_structured_fingerprint(
            self.__pool.get_fingerprint,
            self.__pool.set_fingerprint,
            tenant_id=tenant_id,
            fingerprint=lambda: self._fingerprint_for(tenant_id),
        )

        return await self.__pool.get(tenant_id)

    # ....................... #

    @asynccontextmanager
    async def client(self) -> AsyncGenerator[AsyncSQSClient]:
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
        delay: timedelta | None = None,
        not_before: datetime | None = None,
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
                delay=delay,
                not_before=not_before,
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
                delay=delay,
                not_before=not_before,
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
    ) -> AsyncGenerator[SQSQueueMessage]:
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
