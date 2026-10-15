"""A tenant-aware Kafka route needs a bound tenant, as every other tenant-aware adapter does."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from _kafka_fakes import FakeAdmin, FakeKafkaClient, Msg, make_codec

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kafka.adapters import (
    KafkaCommitStreamGroupAdapter,
    KafkaCommitStreamGroupAdminAdapter,
    KafkaStreamCommandAdapter,
)

# ----------------------- #


class _Namespace:
    """A per-tenant namespace that records every tenant it was resolved for."""

    def __init__(self) -> None:
        self.seen: list[UUID | None] = []

    def __call__(self, tenant_id: UUID | None) -> str:
        self.seen.append(tenant_id)
        return f"t-{tenant_id}"


async def _produce(adapter: Any) -> None:
    await adapter.append("orders", Msg(body="x"))


async def _consume(adapter: Any) -> None:
    await adapter.read("g", "c", ["orders"], timeout=timedelta(milliseconds=1))


async def _administer(adapter: Any) -> None:
    await adapter.ensure_topic("orders", partitions=1)


_Build = Callable[..., Any]

_ADAPTERS: list[tuple[str, _Build, Callable[[Any], Any]]] = [
    (
        "producer",
        lambda **kw: KafkaStreamCommandAdapter(codec=make_codec(), **kw),
        _produce,
    ),
    (
        "consumer",
        lambda **kw: KafkaCommitStreamGroupAdapter(codec=make_codec(), **kw),
        _consume,
    ),
    ("admin", lambda **kw: KafkaCommitStreamGroupAdminAdapter(**kw), _administer),
]


@pytest.mark.parametrize(
    ("build", "call"), [(b, c) for _, b, c in _ADAPTERS], ids=[n for n, *_ in _ADAPTERS]
)
async def test_a_tenant_aware_route_with_no_tenant_bound_is_refused(
    build: _Build, call: Callable[[Any], Any]
) -> None:
    namespace = _Namespace()
    adapter = build(
        client=FakeKafkaClient(admin=FakeAdmin()),
        namespace=namespace,
        tenant_aware=True,
        tenant_provider=lambda: None,
    )

    with pytest.raises(CoreException) as caught:
        await call(adapter)

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert caught.value.code == "tenant_required"
    assert namespace.seen == []  # never resolved for no tenant


@pytest.mark.parametrize(
    ("build", "call"), [(b, c) for _, b, c in _ADAPTERS], ids=[n for n, *_ in _ADAPTERS]
)
async def test_a_tenant_aware_route_resolves_for_the_bound_tenant(
    build: _Build, call: Callable[[Any], Any]
) -> None:
    tenant = uuid4()
    namespace = _Namespace()
    adapter = build(
        client=FakeKafkaClient(admin=FakeAdmin()),
        namespace=namespace,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant),
    )

    await call(adapter)

    assert namespace.seen == [tenant]


# ....................... #


async def test_the_commit_stream_consumer_refuses_a_tenant_aware_kafka_group() -> None:
    from pydantic import BaseModel

    from forze.application.contracts.inbox import InboxSpec
    from forze.application.contracts.stream import StreamSpec
    from forze.application.execution import DepsRegistry, ExecutionRuntime
    from forze.base.serialization import PydanticModelCodec
    from forze_kafka import KafkaDepsModule
    from forze_kafka.execution.deps.configs import KafkaCommitStreamGroupConfig
    from forze_kafka.kernel.client import KafkaClient
    from forze_kits.integrations.consumer import commit_stream_consumer_background_lifecycle_step

    class _Payload(BaseModel):
        body: str

    async def _handler(message: Any) -> None:
        del message

    spec = StreamSpec(name="orders", codec=PydanticModelCodec(_Payload))
    module = KafkaDepsModule(
        client=KafkaClient(),
        commit_groups={
            "orders": KafkaCommitStreamGroupConfig(
                namespace=lambda tenant_id: f"t-{tenant_id}", tenant_aware=True
            )
        },
    )
    step = commit_stream_consumer_background_lifecycle_step(
        topics=["orders"],
        group="g",
        consumer="c",
        stream_spec=spec,
        handler=_handler,
        inbox_spec=InboxSpec(name="events"),
        tx_route="default",
    )
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

    async with runtime.scope():
        with pytest.raises(CoreException) as caught:
            await step.startup(runtime.get_context())

    assert caught.value.code == "tenant_global_required"
