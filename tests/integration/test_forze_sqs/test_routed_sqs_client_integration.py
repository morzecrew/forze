"""Integration tests for :class:`~forze_sqs.kernel.platform.RoutedSQSClient`."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers.localstack")

from testcontainers.localstack import LocalStackContainer

from forze.application.contracts.secrets import SecretRef
from forze.base.errors import CoreError, InfrastructureError, SecretNotFoundError

from forze_sqs.kernel.platform import RoutedSQSClient, SQSClient


def _ref(tid: UUID) -> SecretRef:
    return SecretRef(path=f"tenants/{tid}/sqs")


def _payload(endpoint: str) -> dict[str, str]:
    return {
        "endpoint": endpoint,
        "region_name": "us-east-1",
        "access_key_id": "test",
        "secret_access_key": "test",
    }


class _MemSecretsJson:
    def __init__(
        self,
        path_to_json: dict[str, str],
        *,
        missing_path: str | None = None,
        broken_path: str | None = None,
    ) -> None:
        self._paths = path_to_json
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


class _MemSecretsTenantJson(_MemSecretsJson):
    def __init__(
        self,
        payloads: dict[UUID, dict[str, str]],
        *,
        missing_tenant: UUID | None = None,
        broken_tenant: UUID | None = None,
    ) -> None:
        paths = {
            f"tenants/{tid}/sqs": json.dumps(payload) for tid, payload in payloads.items()
        }
        mp = f"tenants/{missing_tenant}/sqs" if missing_tenant else None
        bp = f"tenants/{broken_tenant}/sqs" if broken_tenant else None
        super().__init__(paths, missing_path=mp, broken_path=bp)


def _tenant_holder() -> tuple[Callable[[], UUID | None], Callable[[UUID | None], None]]:
    slot: list[UUID | None] = [None]

    def getter() -> UUID | None:
        return slot[0]

    def setter(value: UUID | None) -> None:
        slot[0] = value

    return getter, setter


async def _receive_until(
    client: RoutedSQSClient,
    queue: str,
    *,
    attempts: int = 8,
) -> list[dict]:
    for _ in range(attempts):
        messages = await client.receive(queue, limit=1, timeout=timedelta(seconds=1))
        if messages:
            return messages
    raise AssertionError("SQS message was not received in time")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_enqueue_receive_consume_ack(
    localstack_container: LocalStackContainer,
) -> None:
    endpoint = localstack_container.get_url()
    t1 = uuid4()
    secrets = _MemSecretsTenantJson({t1: _payload(endpoint)})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedSQSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert (await routed.health())[1] is True

        qname = f"forze-routed-sqs-{uuid4().hex[:12]}"
        url = await routed.create_queue(qname)
        assert url == await routed.queue_url(qname)

        async with routed.client() as _c:
            _ = _c

        ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        mid = await routed.enqueue(
            url,
            b'{"value":"hello"}',
            type="created",
            key="partition-1",
            enqueued_at=ts,
        )
        assert mid

        ids = await routed.enqueue_many(
            url,
            [b"a", b"b"],
            type="batch",
            key="k",
            enqueued_at=ts,
        )
        assert len(ids) == 2

        msgs = await _receive_until(routed, url)
        assert await routed.ack(url, [msgs[0]["id"]]) == 1

        batch = await routed.receive(url, limit=10, timeout=timedelta(seconds=2))
        assert len(batch) == 2
        await routed.ack(url, [m["id"] for m in batch])

        await routed.enqueue(url, b'{"value":"requeue"}')
        first = (await _receive_until(routed, url))[0]
        assert await routed.nack(url, [first["id"]], requeue=True) == 1
        second = (await _receive_until(routed, url))[0]
        assert second["body"] == b'{"value":"requeue"}'
        assert await routed.ack(url, [second["id"]]) == 1
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_mapping_secret_ref(
    localstack_container: LocalStackContainer,
) -> None:
    endpoint = localstack_container.get_url()
    t1 = uuid4()
    custom = SecretRef(path=f"cfg/sqs/{uuid4().hex[:12]}")
    secrets = _MemSecretsJson({custom.path: json.dumps(_payload(endpoint))})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedSQSClient(
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
async def test_routed_sqs_requires_startup_and_tenant(
    localstack_container: LocalStackContainer,
) -> None:
    endpoint = localstack_container.get_url()
    t1 = uuid4()
    secrets = _MemSecretsTenantJson({t1: _payload(endpoint)})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedSQSClient(
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
async def test_routed_sqs_secret_errors(localstack_container: LocalStackContainer) -> None:
    endpoint = localstack_container.get_url()
    t_ok, t_miss, t_break = uuid4(), uuid4(), uuid4()
    tenant_get, tenant_set = _tenant_holder()

    miss = _MemSecretsTenantJson({t_ok: _payload(endpoint)}, missing_tenant=t_miss)
    r1 = RoutedSQSClient(
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

    br = _MemSecretsTenantJson({t_ok: _payload(endpoint)}, broken_tenant=t_break)
    r2 = RoutedSQSClient(
        secrets=br,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await r2.startup()
    try:
        tenant_set(t_break)
        with pytest.raises(InfrastructureError, match="Failed to resolve SQS secret"):
            await r2.health()
    finally:
        await r2.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_invalid_json_raises_core_error(
    localstack_container: LocalStackContainer,
) -> None:
    endpoint = localstack_container.get_url()
    t1 = uuid4()
    secrets = _MemSecretsJson(
        {f"tenants/{t1}/sqs": "{bad-json"},
    )
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedSQSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        with pytest.raises(CoreError, match="SQSRoutingCredentials"):
            await routed.health()
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_lru_and_evict(localstack_container: LocalStackContainer) -> None:
    endpoint = localstack_container.get_url()
    p = _payload(endpoint)
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _MemSecretsTenantJson({t1: p, t2: p, t3: p})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedSQSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=2,
    )
    await routed.startup()
    closes: list[int] = []
    real_close = SQSClient.close

    async def counting_close(self: SQSClient) -> None:
        closes.append(1)
        await real_close(self)

    try:
        with patch.object(SQSClient, "close", counting_close):
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
