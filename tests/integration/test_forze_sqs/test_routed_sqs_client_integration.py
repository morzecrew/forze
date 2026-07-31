"""Integration tests for :class:`~forze_sqs.kernel.client.RoutedSQSClient`."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from contextlib import aclosing
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

from forze.base.exceptions import CoreException, exc

pytest.importorskip("aioboto3")
pytest.importorskip("testcontainers")

from forze.application.contracts.secrets import SecretRef
from forze_sqs.kernel.client import RoutedSQSClient, SQSClient
from tests.integration._routed_lru_helpers import sqs_payloads_for_lru_eviction
from tests.support.floci import FlociContainer


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
            raise exc.not_found(
                f"No secret for {ref.path!r}",
                details={"ref": ref.path},
            )
        try:
            return self._paths[ref.path]
        except KeyError as e:
            raise exc.not_found(
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
            f"tenants/{tid}/sqs": json.dumps(payload)
            for tid, payload in payloads.items()
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
    floci_container: FlociContainer,
) -> None:
    endpoint = floci_container.get_url()
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

        ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
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
        assert await routed.ack(url, [msgs[0].id]) == 1

        batch = await routed.receive(url, limit=10, timeout=timedelta(seconds=2))
        assert len(batch) == 2
        await routed.ack(url, [m.id for m in batch])

        await routed.enqueue(url, b'{"value":"requeue"}')
        first = (await _receive_until(routed, url))[0]
        assert await routed.nack(url, [first.id], requeue=True) == 1
        second = (await _receive_until(routed, url))[0]
        assert second.body == b'{"value":"requeue"}'
        assert await routed.ack(url, [second.id]) == 1

    finally:
        await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_mapping_secret_ref(
    floci_container: FlociContainer,
) -> None:
    endpoint = floci_container.get_url()
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
    floci_container: FlociContainer,
) -> None:
    endpoint = floci_container.get_url()
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
    with pytest.raises(CoreException, match="not started"):
        await routed.health()

    await routed.startup()
    try:
        tenant_set(None)
        with pytest.raises(CoreException, match="Tenant ID"):
            await routed.health()
    finally:
        await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_secret_errors(
    floci_container: FlociContainer,
) -> None:
    endpoint = floci_container.get_url()
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
        with pytest.raises(CoreException):
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
        with pytest.raises(CoreException, match="Failed to resolve SQS secret"):
            await r2.health()
    finally:
        await r2.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_invalid_json_raises_core_error(
    floci_container: FlociContainer,
) -> None:
    # endpoint = floci_container.get_url()
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
        with pytest.raises(CoreException, match="SQSRoutingCredentials"):
            await routed.health()

    finally:
        await routed.close()

@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_consume_survives_rotation_eviction(
    floci_container: FlociContainer,
) -> None:
    """A long-lived consumer follows a rotation eviction onto the rebuilt client instead
    of crashing: ``consume`` re-acquires the tenant's pooled client per long poll, so
    ``evict_tenant`` (the rotation signal) swaps the stream onto fresh credentials."""

    endpoint = floci_container.get_url()
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

    # Keeping the closed instances referenced also keeps their id()s from being
    # reused by freshly built clients (the receiver-identity assertions below).
    closed: list[SQSClient] = []
    real_close = SQSClient.close

    async def counting_close(self: SQSClient) -> None:
        closed.append(self)
        await real_close(self)

    # Record which pooled client instance serves each poll: the rotation contract is
    # that the stream *switches* to the rebuilt client, not that the old one limps on
    # with stale credentials (the emulator cannot revoke them, so bodies alone pass).
    receivers: list[int] = []
    real_receive = SQSClient.receive

    async def recording_receive(self: SQSClient, queue: str, **kwargs: object) -> object:
        receivers.append(id(self))
        return await real_receive(self, queue, **kwargs)  # type: ignore[arg-type]

    try:
        url = await routed.create_queue(f"forze-routed-rot-{uuid4().hex[:12]}")
        await routed.enqueue(url, b"m1")

        with (
            patch.object(SQSClient, "close", counting_close),
            patch.object(SQSClient, "receive", recording_receive),
        ):
            gen = routed.consume(url)
            async with aclosing(gen):
                first = await asyncio.wait_for(anext(gen), timeout=30)
                assert first.body == b"m1"
                first_receiver = receivers[-1]
                await routed.ack(url, [first.id])  # else eviction re-queues it

                # Rotation between polls: the old client is disposed immediately
                # (unguarded default) while the stream is suspended…
                await routed.evict_tenant(t1)
                assert len(closed) == 1  # the stale client was actually torn down

                await routed.enqueue(url, b"m2")  # next access rebuilds
                second = await asyncio.wait_for(anext(gen), timeout=30)
                assert second.body == b"m2"
                assert receivers[-1] != first_receiver  # served by the rebuilt client
                second_receiver = receivers[-1]
                await routed.ack(url, [second.id])

                # …and mid-poll: evict while the consumer is long-polling an empty
                # queue; the poll on the disposed client fails or drains, and the
                # loop retries on the rebuilt client without the stream dying.
                pending = asyncio.ensure_future(anext(gen))
                await asyncio.sleep(0.3)  # let the long poll start on the old client
                await routed.evict_tenant(t1)
                await routed.enqueue(url, b"m3")
                third = await asyncio.wait_for(pending, timeout=40)
                assert third.body == b"m3"
                assert receivers[-1] != second_receiver
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_guarded_registry_full_facade(
    floci_container: FlociContainer,
) -> None:
    """``guarded=True`` works across the whole facade — every operation runs under a
    pool lease — and a rotation eviction mid-stream drains the old client only after
    its in-flight scope exits while the consumer reconnects."""

    endpoint = floci_container.get_url()
    t1 = uuid4()
    secrets = _MemSecretsTenantJson({t1: _payload(endpoint)})
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedSQSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
        guarded=True,
    )
    tenant_set(t1)
    await routed.startup()
    try:
        assert (await routed.health())[1] is True

        url = await routed.create_queue(f"forze-routed-grd-{uuid4().hex[:12]}")
        assert url == await routed.queue_url(url.rsplit("/", 1)[-1])

        await routed.enqueue(url, b"one")
        await routed.enqueue_many(url, [b"two"])

        msgs = await _receive_until(routed, url)
        assert await routed.ack(url, [msgs[0].id]) == 1
        rest = await _receive_until(routed, url)
        assert await routed.nack(url, [rest[0].id], requeue=True) == 1

        gen = routed.consume(url)
        async with aclosing(gen):
            first = await asyncio.wait_for(anext(gen), timeout=30)
            assert first.body == b"two"
            await routed.ack(url, [first.id])  # else eviction re-queues it

            await routed.evict_tenant(t1)  # drains after in-flight leases exit
            await routed.enqueue(url, b"three")
            second = await asyncio.wait_for(anext(gen), timeout=30)
            assert second.body == b"three"
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_consume_retries_transient_receive_failures(
    floci_container: FlociContainer,
) -> None:
    """A CoreException from the *receive* phase keeps the plain client's semantics —
    retried with backoff, never terminal — and the stream still delivers."""

    endpoint = floci_container.get_url()
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

    real_receive = SQSClient.receive
    failures: list[int] = []

    async def flaky_receive(self: SQSClient, queue: str, **kwargs: object) -> object:
        if not failures:
            failures.append(1)
            raise exc.infrastructure("transient SQS hiccup")
        return await real_receive(self, queue, **kwargs)  # type: ignore[arg-type]

    try:
        url = await routed.create_queue(f"forze-routed-flaky-{uuid4().hex[:12]}")
        await routed.enqueue(url, b"survives")

        with patch.object(SQSClient, "receive", flaky_receive):
            gen = routed.consume(url)
            async with aclosing(gen):
                first = await asyncio.wait_for(anext(gen), timeout=30)
                assert first.body == b"survives"
                assert failures  # the transient failure actually happened and was retried
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_consume_raises_terminal_resolution_errors() -> None:
    """A non-retryable resolution failure (no tenant bound, missing secret) raises out
    of ``consume`` instead of spinning the backoff-retry loop on a misconfiguration —
    the pre-rotation-fix contract, where resolution happened before the loop."""

    t_known, t_missing = uuid4(), uuid4()
    secrets = _MemSecretsTenantJson(
        {t_known: _payload("http://sqs.invalid:1")}, missing_tenant=t_missing
    )
    tenant_get, tenant_set = _tenant_holder()

    routed = RoutedSQSClient(
        secrets=secrets,
        secret_ref_for_tenant=_ref,
        tenant_provider=tenant_get,
        max_cached_tenants=4,
    )
    await routed.startup()
    try:
        # No tenant bound: authentication-kind, terminal.
        tenant_set(None)
        gen = routed.consume("some-queue")
        async with aclosing(gen):
            with pytest.raises(CoreException, match="Tenant ID"):
                await asyncio.wait_for(anext(gen), timeout=10)

        # Tenant bound but its secret is absent: not_found, terminal.
        tenant_set(t_missing)
        gen = routed.consume("some-queue")
        async with aclosing(gen):
            with pytest.raises(CoreException, match="No secret"):
                await asyncio.wait_for(anext(gen), timeout=10)
    finally:
        await routed.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_routed_sqs_lru_and_evict(
    floci_container: FlociContainer,
) -> None:
    endpoint = floci_container.get_url()
    p = _payload(endpoint)
    t1, t2, t3 = uuid4(), uuid4(), uuid4()
    secrets = _MemSecretsTenantJson(sqs_payloads_for_lru_eviction(p, t1, t2, t3))
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
