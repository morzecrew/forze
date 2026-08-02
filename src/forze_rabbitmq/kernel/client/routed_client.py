"""RabbitMQ client that resolves a DSN per tenant via :class:`~forze.application.contracts.secrets.SecretsPort`."""

import asyncio
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import final
from uuid import UUID

import attrs
from aio_pika.abc import AbstractChannel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import DsnRoutedTenantClientBase

from .client import RabbitMQClient
from .port import RabbitMQClientPort
from .types import RabbitMQQueueMessage
from .value_objects import RabbitMQConfig

# ----------------------- #

_CONSUME_FETCH_WINDOW = timedelta(seconds=5)
"""Wait window of one :meth:`RoutedRabbitMQClient.consume` fetch.

The boundary an AMQP push stream does not have on its own, and the upper bound on how long
a rotation waits: the tenant's client is released at the end of each window, so an eviction
takes effect within one. Long enough that a quiet queue is not re-resolving its secret in a
tight loop. Not caller-visible — the caller's own idle timeout is tracked across fetches and
is the only thing that ends the stream."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedRabbitMQClient(DsnRoutedTenantClientBase[RabbitMQClient], RabbitMQClientPort):
    """Routes each call to a lazily created :class:`RabbitMQClient` for the current tenant.

    DSN strings are resolved via :meth:`SecretsPort.resolve_str` and
    ``secret_ref_for_tenant``. Use
    :func:`~forze_rabbitmq.execution.lifecycle.routed_rabbitmq_lifecycle_step`
    after registering the same instance under :data:`RabbitMQClientDepKey`.

    Every operation runs inside a pool scope (``client_scope``), so ``guarded=True`` is
    fully supported: a rotation eviction drains a client only after in-flight operations
    finish. :meth:`consume` additionally re-acquires the pooled client per fetch, so a
    long-lived consumer follows a rotation onto fresh credentials instead of being torn
    down with the evicted client — and never holds a lease across a yield, which would
    make a guarded eviction wait on the caller's processing.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    connection_config: RabbitMQConfig = attrs.field(factory=RabbitMQConfig)
    max_cached_tenants: int = 100

    # ....................... #

    dsn_backend: str = attrs.field(default="RabbitMQ", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed RabbitMQ access",
        init=False,
    )

    # ....................... #

    async def initialize_client(self, tenant_id: UUID, creds: str) -> RabbitMQClient:
        client = RabbitMQClient()
        await client.initialize(creds, config=self.connection_config)

        return client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        async with self.client_scope() as inner:
            return await inner.health()

    # ....................... #

    @asynccontextmanager
    async def channel(self) -> AsyncGenerator[AbstractChannel]:
        async with self.client_scope() as inner, inner.channel() as ch:
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
        headers: Mapping[str, str] | None = None,
    ) -> str:
        async with self.client_scope() as inner:
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
                headers=headers,
            )

    # ....................... #

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
        headers: Mapping[str, str] | None = None,
        message_headers: Sequence[Mapping[str, str]] | None = None,
    ) -> list[str]:
        async with self.client_scope() as inner:
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
                headers=headers,
                message_headers=message_headers,
            )

    # ....................... #

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[RabbitMQQueueMessage]:
        async with self.client_scope() as inner:
            return await inner.receive(queue, limit=limit, timeout=timeout)

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[RabbitMQQueueMessage]:
        """Yield queue messages continuously, following the tenant across rotation.

        Same idle-timeout semantics as :meth:`RabbitMQClient.consume`, but driven as a
        series of bounded fetches instead of one pinned client: each fetch acquires the
        tenant's *current* pooled client and releases it before the message is yielded, so
        a rotation signal
        (:meth:`~forze.application.contracts.tenancy.routed_client_base.RoutedTenantClientBase.evict_tenant`)
        moves the consumer onto fresh credentials at the next fetch instead of tearing it
        down with the evicted client.

        **Why this polls rather than riding the push consumer.** The pool scope must never
        span a ``yield``. A generator suspended at a yield is not running, so a lease held
        across it lasts as long as the *caller's* processing — which is unbounded, and may
        be forever if the caller stops iterating. A guarded eviction waiting on that lease
        waits with it, and the rotation deadlocks; that is measured, not hypothetical
        (``test_routed_rabbitmq_guarded_registry_full_facade`` hung on exactly this shape
        before the rewrite). An AMQP consume is a push stream with no boundary of its own,
        so the only way to bound the lease is to fetch within a window and let go —
        :meth:`RabbitMQClient.receive` is that fetch, and the loop below is the same
        structure the SQS consumer gets for free from a long poll.

        The cost is a routed consumer that polls instead of holding a subscription, and it
        is paid only by routed (multi-tenant) callers. :meth:`RabbitMQClient.consume` is
        untouched: a single-tenant consumer keeps the push stream and its prefetch.
        """

        idle_seconds = (
            timeout.total_seconds() if timeout is not None and timeout.total_seconds() > 0 else None
        )
        fetch_seconds = _CONSUME_FETCH_WINDOW.total_seconds()
        loop = asyncio.get_running_loop()
        idle_deadline = loop.time() + idle_seconds if idle_seconds is not None else None

        while True:
            if idle_deadline is not None:
                remaining = idle_deadline - loop.time()

                if remaining <= 0:
                    return

                window = min(fetch_seconds, remaining)

            else:
                window = fetch_seconds

            # Scope opened and closed around the fetch alone — see the docstring.
            async with self.client_scope() as inner:
                messages = await inner.receive(
                    queue,
                    limit=1,
                    timeout=timedelta(seconds=window),
                )

            if not messages:
                continue

            if idle_deadline is not None and idle_seconds is not None:
                idle_deadline = loop.time() + idle_seconds

            for message in messages:
                yield message

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        async with self.client_scope() as inner:
            return await inner.ack(queue, ids)

    # ....................... #

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
        count: bool = True,
    ) -> int:
        async with self.client_scope() as inner:
            return await inner.nack(queue, ids, requeue=requeue, count=count)
