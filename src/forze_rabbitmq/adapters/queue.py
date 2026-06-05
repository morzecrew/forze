from forze_rabbitmq._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncGenerator, Sequence, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.queue import (
    QueueCommandPort,
    QueueMessage,
    QueueQueryPort,
)
from forze.application.contracts.resolution import NamedResourceSpec, is_static_named_resource
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import OnceCell

from ..kernel.client import RabbitMQClientPort
from ..kernel.relation import resolve_rabbitmq_namespace
from .codecs import RabbitMQQueueCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueAdapter[M: BaseModel](
    QueueQueryPort[M],
    QueueCommandPort[M],
    TenancyMixin,
):
    """RabbitMQ queue adapter."""

    client: RabbitMQClientPort
    """RabbitMQ client instance."""

    codec: RabbitMQQueueCodec[M]
    """RabbitMQ queue codec instance."""

    namespace: NamedResourceSpec = ""
    """RabbitMQ queue namespace."""

    delayed_delivery: bool = False
    """Whether delayed enqueue uses the DLX delay-queue topology."""

    _namespace_cell: OnceCell[str] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        if tenant is None:
            if self.tenant_aware:
                raise exc.internal("Tenant ID is required for the RabbitMQ queue adapter")

            return None

        return tenant.tenant_id

    # ....................... #

    async def _resolved_namespace(self) -> str:
        async def _factory() -> str:
            return await resolve_rabbitmq_namespace(
                self.namespace,
                self._tenant_id_for_resolve(),
            )

        # Only memoize tenant-independent (static) namespaces; a dynamic resolver
        # depends on the bound tenant and the adapter may be shared across tenants.
        return await self._namespace_cell.resolve(
            _factory,
            cache=is_static_named_resource(self.namespace),
        )

    # ....................... #

    async def __queue_name(self, queue: str) -> str:
        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            tenant_prefix = f"tenant:{tenant_id}"

        else:
            tenant_prefix = ""

        namespace = await self._resolved_namespace()

        if namespace:
            namespaced_queue = f"{namespace}:{queue}"

        else:
            namespaced_queue = queue

        return f"{tenant_prefix}:{namespaced_queue}".lstrip(":")

    # ....................... #

    async def enqueue(
        self,
        queue: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
    ) -> str:
        physical_queue = await self.__queue_name(queue)
        body = self.codec.encode(payload)

        return await self.client.enqueue(
            physical_queue,
            body,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            delay=delay,
            not_before=not_before,
            delayed_delivery=self.delayed_delivery,
        )

    # ....................... #

    async def enqueue_many(
        self,
        queue: str,
        payloads: Sequence[M],
        *,
        type: str | None = None,
        key: str | None = None,
        enqueued_at: datetime | None = None,
        delay: timedelta | None = None,
        not_before: datetime | None = None,
    ) -> list[str]:
        if not payloads:
            return []

        physical_queue = await self.__queue_name(queue)
        bodies = [self.codec.encode(payload) for payload in payloads]

        return await self.client.enqueue_many(
            physical_queue,
            bodies,
            type=type,
            key=key,
            enqueued_at=enqueued_at,
            delay=delay,
            not_before=not_before,
            delayed_delivery=self.delayed_delivery,
        )

    # ....................... #

    async def receive(
        self,
        queue: str,
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[QueueMessage[M]]:
        physical_queue = await self.__queue_name(queue)
        raw = await self.client.receive(physical_queue, limit=limit, timeout=timeout)

        return [self.codec.decode(queue, msg) for msg in raw]

    # ....................... #

    async def consume(
        self,
        queue: str,
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[QueueMessage[M]]:
        physical_queue = await self.__queue_name(queue)

        async for msg in self.client.consume(physical_queue, timeout=timeout):
            yield self.codec.decode(queue, msg)

    # ....................... #

    async def ack(self, queue: str, ids: Sequence[str]) -> int:
        physical_queue = await self.__queue_name(queue)
        return await self.client.ack(physical_queue, ids)

    # ....................... #

    async def nack(
        self,
        queue: str,
        ids: Sequence[str],
        *,
        requeue: bool = True,
    ) -> int:
        physical_queue = await self.__queue_name(queue)
        return await self.client.nack(physical_queue, ids, requeue=requeue)
