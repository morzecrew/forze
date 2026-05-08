"""Integration tests for :class:`~forze_rabbitmq.kernel.platform.RoutedRabbitMQClient`."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.parse import quote
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aio_pika")

from testcontainers.rabbitmq import RabbitMqContainer

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError

from forze_rabbitmq.kernel.platform import RabbitMQClient, RabbitMQConfig, RoutedRabbitMQClient


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/rabbitmq")


def _dsn(container: RabbitMqContainer) -> str:
    host = container.get_container_host_ip()
    port = container.get_exposed_port(container.port)
    vhost = quote(container.vhost, safe="")
    return f"amqp://{container.username}:{container.password}@{host}:{port}/{vhost}"


class _MemSecretsDsn:
    def __init__(
        self,
        paths: dict[str, str],
        *,
        missing_path: str | None = None,
        broken_path: str | None = None,
    ) -> None:
        self._paths = paths
        self._missing_path = missing_path
        self._broken_path = broken_path

    async def resolve_str(self, ref: SecretRef) -> str:
        if self._broken_path is not None and ref.path == self._broken_path:
            raise RuntimeError("vault unavailable")
        if self._missing_path is not None and ref.path == self._missing_path:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )
        try:
            return self._paths[ref.path]
        except KeyError as e:
            raise SecretNotFoundError(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            ) from e

    async def exists(self, ref: SecretRef) -> bool:
        return ref.path in self._paths


class _MemSecretsTenantDsn(_MemSecretsDsn):
    def __init__(
        self,
        dsns: dict[UUID, str],
        *,
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        paths = {f"tenants/{tid}/rabbitmq": d for tid, d in dsns.items()}
        mp = f"tenants/{missing_tenant}/rabbitmq" if missing_tenant else None
        bp = f"tenants/{broken_tenant}/rabbitmq" if broken_tenant else None
        super().__init__(paths, missing_path=mp, broken_path=bp)


def _tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    slot: list[UUID | None] = [None]

    def getter() -> UUID | None:
        return slot[0]

    def setter(value: UUID | None) -> None:
        slot[0] = value

    return getter, setter


async def _receive_until(
    client: RoutedRabbitMQClient,
    queue: str,
    *,
    attempts: int = 8,
) -> list[dict]:
    for _ in range(attempts):
        messages = await client.receive(queue, limit=1, timeout=timedelta(seconds=1))
        if messages:
            return messages
    raise AssertionError("RabbitMQ message was not received in time")


async def _receive_exact(
    client: RoutedRabbitMQClient,
    queue: str,
    expected: int,
    *,
    attempts: int = 8,
) -> list[dict]:
    out: list[dict] = []
    for _ in range(attempts):
        remaining = expected - len(out)
        if remaining <= 0:
            return out
        messages = await client.receive(
            queue,
            limit=remaining,
            timeout=timedelta(seconds=1),
        )
        out.extend(messages)
    if len(out) == expected:
        return out
    raise AssertionError("RabbitMQ batch messages were not received in time")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_rabbitmq_queue_roundtrip(rabbitmq_container: RabbitMqContainer) -> None:
    dsn = _dsn(rabbitmq_container)
    t1 = uuid4()
    secrets = _MemSecretsTenantDsn({t1: dsn})
    tenant_get, tenant_set = _tenant_holder()
    cfg = RabbitMQConfig(
        prefetch_count=20,
        connect_timeout=timedelta(seconds=10.0),
    )

    routed = RoutedRabbitMQClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        connection_config=cfg,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert (await routed.health())[1] is True

        async with routed.channel():
            pass

        queue = f"it:routed-rabbitmq:{uuid4().hex[:12]}"
        ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

        mid = await routed.enqueue(
            queue,
            b'{"value":"hello"}',
            type="created",
            key="partition-1",
            enqueued_at=ts,
        )
        messages = await _receive_until(routed, queue)
        assert messages[0]["id"] == mid
        assert await routed.ack(queue, [mid]) == 1

        ids = await routed.enqueue_many(
            queue,
            [b'{"value":"b1"}', b'{"value":"b2"}'],
            type="created",
            key="batch",
            enqueued_at=ts,
        )
        batch_msgs = await _receive_exact(routed, queue, expected=2)
        assert {m["id"] for m in batch_msgs} == set(ids)
        assert await routed.ack(queue, ids) == 2

        saw = 0
        await routed.enqueue(queue, b'{"value":"stream"}')
        async for _m in routed.consume(queue, timeout=timedelta(seconds=2)):
            saw += 1
            await routed.ack(queue, [_m["id"]])
            break
        assert saw == 1

        await routed.enqueue(queue, b'{"value":"requeue"}')
        first = (await _receive_until(routed, queue))[0]
        assert await routed.nack(queue, [first["id"]], requeue=True) == 1
        second = (await _receive_until(routed, queue))[0]
        assert await routed.ack(queue, [second["id"]]) == 1
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_rabbitmq_mapping_secret_ref(
    rabbitmq_container: RabbitMqContainer,
) -> None:
    dsn = _dsn(rabbitmq_container)
    t1 = uuid4()
    custom = SecretRef(path=f"cfg/amqp/{uuid4().hex[:12]}")
    secrets = _MemSecretsDsn({custom.path: dsn})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedRabbitMQClient(
        secrets=secrets,
        secret_ref_for_tenant={t1: custom},
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert (await routed.health())[1] is True
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_rabbitmq_startup_and_tenant_guards(
    rabbitmq_container: RabbitMqContainer,
) -> None:
    dsn = _dsn(rabbitmq_container)
    t1 = uuid4()
    secrets = _MemSecretsTenantDsn({t1: dsn})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedRabbitMQClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    with pytest.raises(InfrastructureError, match="not started"):
        await routed.health()

    await routed.startup()
    try:
        tenant_set(None)
        with pytest.raises(CoreError, match="Tenant ID"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_rabbitmq_secret_errors(
    rabbitmq_container: RabbitMqContainer,
) -> None:
    dsn = _dsn(rabbitmq_container)
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    tenant_get, tenant_set = _tenant_holder()

    miss = _MemSecretsTenantDsn({t_ok: dsn}, missing_tenant=t_miss)
    r1 = RoutedRabbitMQClient(
        secrets=miss,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r1.startup()
    try:
        tenant_set(t_miss)
        with pytest.raises(SecretNotFoundError):
            await r1.health()
    finally:
        await r1.close()

    br = _MemSecretsTenantDsn({t_ok: dsn}, broken_tenant=t_break)
    r2 = RoutedRabbitMQClient(
        secrets=br,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r2.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(InfrastructureError, match="Failed to resolve RabbitMQ secret"):
            await r2.health()
    finally:
        await r2.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_rabbitmq_lru_and_evict(rabbitmq_container: RabbitMqContainer) -> None:
    dsn = _dsn(rabbitmq_container)
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _MemSecretsTenantDsn({t1: dsn, t2: dsn, t3: dsn})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedRabbitMQClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = RabbitMQClient.close

    async def counting_close(self: RabbitMQClient) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(RabbitMQClient, "close", counting_close):
            tenant_set(t1)
            await routed.health()
            tenant_set(t2)
            await routed.health()
            tenant_set(t1)
            await routed.health()
            tenant_set(t3)
            await routed.health()
            assert sum(closes) == 1

        tenant_set(t1)
        assert (await routed.health())[1] is True
        await routed.evict_tenant(t1)
        await routed.evict_tenant(uuid4())
        tenant_set(t1)
        assert (await routed.health())[1] is True
    finally:
        await routed.close()
