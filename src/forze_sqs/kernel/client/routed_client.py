"""SQS client that resolves credentials per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

if TYPE_CHECKING:
    # Type-only stub package; kept off the runtime import path.
    from types_aiobotocore_sqs.client import SQSClient as AsyncSQSClient

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.primitives.fingerprint import build_routing_fingerprint

from .client import SQSClient
from .constants import SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES
from .port import SQSClientPort
from .routing_credentials import SQSRoutingCredentials
from .types import SQSQueueMessage
from .value_objects import SQSConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedSQSClient(StructuredSecretRoutedTenantClientBase[SQSClient], SQSClientPort):
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
    creds_type: type[BaseModel] = attrs.field(default=SQSRoutingCredentials, init=False)
    backend: str = attrs.field(default="SQS", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed SQS access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        c = cast(SQSRoutingCredentials, creds)

        return build_routing_fingerprint(
            public=[c.endpoint, c.region_name, c.access_key_id],
            secret=[c.secret_access_key],
        )

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: SQSRoutingCredentials,
    ) -> SQSClient:
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
        headers: Mapping[str, str] | None = None,
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
                headers=headers,
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
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
        max_batch_payload_bytes: int = SQS_DEFAULT_MAX_BATCH_PAYLOAD_BYTES,
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
                headers=headers,
                message_headers=message_headers,
                max_batch_payload_bytes=max_batch_payload_bytes,
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
        count: bool = True,
    ) -> int:
        inner = await self._get_client()

        async with inner.client():
            return await inner.nack(queue, ids, requeue=requeue, count=count)
