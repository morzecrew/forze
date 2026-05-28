"""SQS client that resolves credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import AsyncGenerator, Callable, Mapping, Sequence, final
from uuid import UUID

import attrs
from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from forze.application.contracts.secrets import (
    SecretRef,
    SecretsPort,
    resolve_structured,
    secret_ref_for_tenant,
)
from forze.application.contracts.tenancy import require_tenant_id
from forze.base.exceptions import exc
from forze.base.primitives.fingerprint import stable_fingerprint
from forze.base.primitives.lru_registry import SimpleLruRegistry

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

    _registry: SimpleLruRegistry[UUID, SQSClient] = attrs.field(init=False)
    _fingerprints: dict[UUID, str] = attrs.field(factory=dict, init=False, repr=False)
    _started: bool = attrs.field(default=False, init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.max_cached_tenants < 1:
            raise exc.internal("max_cached_tenants must be at least 1")

        self._registry = SimpleLruRegistry(
            max_entries=self.max_cached_tenants,
            create=self._create_client,
            dispose=lambda client: client.close(),
            dedup_key=lambda tid: self._fingerprints[tid],
        )

    # ....................... #

    async def startup(self) -> None:
        self._started = True

    # ....................... #

    async def close(self) -> None:
        await self._registry.close_all()
        self._started = False

    # ....................... #

    async def evict_tenant(self, tenant_id: UUID) -> None:
        self._fingerprints.pop(tenant_id, None)
        await self._registry.evict(tenant_id)

    # ....................... #

    async def _ensure_fingerprint(self, tenant_id: UUID) -> str:
        cached = self._fingerprints.get(tenant_id)

        if cached is not None:
            return cached

        ref = secret_ref_for_tenant(self.secret_ref_for_tenant, tenant_id)

        try:
            creds = await resolve_structured(
                self.secrets,
                ref,
                SQSRoutingCredentials,
            )

        except exc:
            raise

        except Exception as e:
            raise exc.internal(
                f"Failed to resolve SQS secret for tenant {tenant_id}: {e}",
            ) from e

        fingerprint = stable_fingerprint(
            creds.endpoint,
            creds.region_name,
            creds.access_key_id,
        )
        self._fingerprints[tenant_id] = fingerprint

        return fingerprint

    # ....................... #

    async def _create_client(self, tid: UUID) -> SQSClient:
        ref = secret_ref_for_tenant(self.secret_ref_for_tenant, tid)

        try:
            creds = await resolve_structured(
                self.secrets,
                ref,
                SQSRoutingCredentials,
            )

        except exc:
            raise

        except Exception as e:
            raise exc.internal(
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

        return client

    # ....................... #

    async def _get_client(self) -> SQSClient:
        if not self._started:
            raise exc.internal("Routed SQS client is not started")

        tenant_id = require_tenant_id(
            self.tenant_provider,
            message="Tenant ID is required for routed SQS access",
        )
        await self._ensure_fingerprint(tenant_id)

        return await self._registry.get_or_create(tenant_id)

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
