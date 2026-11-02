"""The mock cache's invalidation push: Redis ``CLIENT TRACKING`` BCAST in memory.

Every port over one state hears the versioned writes and deletes in its namespace, its own
included; a plain write broadcasts nothing, as an untracked Redis KV key does. A dropped stream
loses events without a word, and a reset flushes every subscriber.
"""

from uuid import uuid4

import pytest

from forze.application.contracts.cache import CacheInvalidation, SupportsInvalidationPush
from forze.application.contracts.tenancy import TenantIdentity
from forze_mock.adapters import MockCacheAdapter
from forze_mock.state import MockState
from forze_mock.tenancy import MockRoutedStateRegistry

pytestmark = pytest.mark.unit

# ----------------------- #


def _port(state: MockState, namespace: str = "ns", **kwargs: object) -> MockCacheAdapter:
    return MockCacheAdapter(state=state, namespace=namespace, invalidation_push=True, **kwargs)  # type: ignore[arg-type]


async def _heard(port: MockCacheAdapter) -> list[CacheInvalidation]:
    events: list[CacheInvalidation] = []
    unsubscribe = await port.subscribe_invalidations(events.append)
    assert unsubscribe is not None

    return events


class TestSubscribing:
    async def test_the_port_implements_the_capability(self) -> None:
        assert isinstance(_port(MockState()), SupportsInvalidationPush)

    async def test_a_route_without_push_declines(self) -> None:
        port = MockCacheAdapter(state=MockState(), namespace="ns")

        assert await port.subscribe_invalidations(lambda _inv: None) is None

    async def test_a_subscription_starts_with_a_flush(self) -> None:
        assert await _heard(_port(MockState())) == [CacheInvalidation(key=None)]

    async def test_after_unsubscribing_nothing_arrives(self) -> None:
        state = MockState()
        events: list[CacheInvalidation] = []
        unsubscribe = await _port(state).subscribe_invalidations(events.append)
        assert unsubscribe is not None

        await unsubscribe()
        await unsubscribe()  # idempotent
        await _port(state).set_versioned("k", "1", "v")

        assert events == [CacheInvalidation(key=None)]

    async def test_a_reset_of_the_state_flushes_and_keeps_the_subscribers(self) -> None:
        # As FLUSHALL on Redis: every value went, the stream did not.
        state = MockState()
        events = await _heard(_port(state))

        state.clear()
        await _port(state).set_versioned("k", "1", "v")

        assert events == [
            CacheInvalidation(key=None),
            CacheInvalidation(key=None),
            CacheInvalidation(key="k"),
        ]

    async def test_disposing_a_routed_tenant_state_ends_its_subscriptions(self) -> None:
        registry = MockRoutedStateRegistry()
        await registry.startup()

        async with registry.use(uuid4()) as state:
            events = await _heard(_port(state))

        await registry.close()
        await _port(state).set_versioned("k", "1", "v")

        assert events == [CacheInvalidation(key=None), CacheInvalidation(key=None, ended=True)]


class TestWhatBroadcasts:
    async def test_a_versioned_write_by_another_port_arrives(self) -> None:
        state = MockState()
        events = await _heard(_port(state))

        await _port(state).set_versioned("k", "1", "v")
        await _port(state).set_many_versioned({("a", "1"): 1, ("b", "1"): 2})

        assert [event.key for event in events] == [None, "k", "a", "b"]

    async def test_the_writer_hears_its_own_write(self) -> None:
        port = _port(MockState())
        events = await _heard(port)

        await port.set_versioned("k", "1", "v")

        assert events[-1] == CacheInvalidation(key="k")

    async def test_deleting_a_versioned_entry_arrives_and_deleting_nothing_does_not(self) -> None:
        state = MockState()
        await _port(state).set_versioned("k", "1", "v")
        events = await _heard(_port(state))

        await _port(state).delete("k", hard=True)
        await _port(state).delete("missing", hard=False)

        assert [event.key for event in events] == [None, "k"]

    async def test_a_plain_write_broadcasts_nothing(self) -> None:
        state = MockState()
        events = await _heard(_port(state))

        await _port(state).set("k", "v")
        await _port(state).set_many({"a": 1})

        assert events == [CacheInvalidation(key=None)]

    async def test_another_namespace_is_not_heard(self) -> None:
        state = MockState()
        events = await _heard(_port(state, "ns"))

        await _port(state, "other").set_versioned("k", "1", "v")

        assert events == [CacheInvalidation(key=None)]

    async def test_a_tenant_aware_port_hears_every_tenant_with_its_id(self) -> None:
        state = MockState()
        tenant = TenantIdentity(tenant_id=uuid4())
        events = await _heard(_port(state, tenant_aware=True))
        writer = _port(state, tenant_aware=True, tenant_provider=lambda: tenant)
        plain = await _heard(_port(state))

        await writer.set_versioned("k", "1", "v")

        assert events[-1] == CacheInvalidation(key="k", tenant=str(tenant.tenant_id))
        assert plain == [CacheInvalidation(key=None)]  # its keys carry no tenant


class TestABrokenStream:
    async def test_while_dropping_nothing_arrives(self) -> None:
        state = MockState()
        events = await _heard(_port(state))

        state.cache_push.dropping = True
        await _port(state).set_versioned("k", "1", "v")

        assert events == [CacheInvalidation(key=None)]

    async def test_a_reset_flushes_every_subscriber(self) -> None:
        state = MockState()
        first, second = await _heard(_port(state)), await _heard(_port(state, "other"))

        state.cache_push.reset()

        assert first == second == [CacheInvalidation(key=None)] * 2
