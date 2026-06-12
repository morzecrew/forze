"""RedisCacheAdapter invalidation push: prefix registration + key parsing."""

from __future__ import annotations

import uuid
from typing import Awaitable, Callable, Sequence

import attrs

from forze.application.contracts.cache import CacheInvalidation
from forze.application.contracts.tenancy import TenantIdentity
from forze_redis.adapters.cache import RedisCacheAdapter

# ----------------------- #

_TENANT = uuid.UUID("11111111-1111-1111-1111-111111111111")


@attrs.define
class TrackingFakeClient:
    """Captures track_invalidations registrations and replays server keys."""

    prefixes: tuple[str, ...] | None = None
    on_keys: Callable[[Sequence[str]], None] | None = None
    on_reset: Callable[[], None] | None = None

    async def track_invalidations(
        self,
        *,
        prefixes: Sequence[str],
        on_keys: Callable[[Sequence[str]], None],
        on_reset: Callable[[], None],
    ) -> Callable[[], Awaitable[None]] | None:
        self.prefixes = tuple(prefixes)
        self.on_keys = on_keys
        self.on_reset = on_reset

        async def _unsubscribe() -> None:
            return None

        return _unsubscribe


def _adapter(
    *,
    client: TrackingFakeClient,
    tenant_aware: bool = False,
    push: bool = True,
) -> RedisCacheAdapter:
    return RedisCacheAdapter(
        client=client,  # type: ignore[arg-type]
        namespace="app:products",
        tenant_aware=tenant_aware,
        tenant_provider=(
            (lambda: TenantIdentity(tenant_id=_TENANT)) if tenant_aware else None
        ),
        invalidation_push=push,
    )


# ----------------------- #


class TestCapabilityGating:
    async def test_disabled_returns_none(self) -> None:
        adapter = _adapter(client=TrackingFakeClient(), push=False)

        assert await adapter.subscribe_invalidations(lambda _e: None) is None

    async def test_routed_client_none_propagates(self) -> None:
        @attrs.define
        class RoutedFake:
            async def track_invalidations(self, **_kw: object) -> None:
                return None

        adapter = RedisCacheAdapter(
            client=RoutedFake(),  # type: ignore[arg-type]
            namespace="app:products",
            invalidation_push=True,
        )

        assert await adapter.subscribe_invalidations(lambda _e: None) is None


class TestStaticNamespace:
    async def test_registers_pointer_prefix_and_parses_keys(self) -> None:
        client = TrackingFakeClient()
        adapter = _adapter(client=client)
        events: list[CacheInvalidation] = []

        unsub = await adapter.subscribe_invalidations(events.append)

        assert unsub is not None
        assert client.prefixes == ("cache:pointer:app:products:",)
        assert client.on_keys is not None and client.on_reset is not None

        client.on_keys(
            [
                "cache:pointer:app:products:pk-1",  # pointer → event
                "cache:body:app:products:pk-1:3",  # body scope → ignored
                "cache:pointer:other-ns:pk-9",  # other namespace → ignored
                "cache:pointer:app:products:",  # empty logical key → ignored
            ]
        )

        assert events == [CacheInvalidation(key="pk-1", tenant=None)]

    async def test_reset_maps_to_flush_event(self) -> None:
        client = TrackingFakeClient()
        adapter = _adapter(client=client)
        events: list[CacheInvalidation] = []

        await adapter.subscribe_invalidations(events.append)
        assert client.on_reset is not None

        client.on_reset()

        assert events == [CacheInvalidation(key=None, tenant=None)]


class TestTenantAware:
    async def test_registers_tenant_prefix_and_parses_tenant(self) -> None:
        client = TrackingFakeClient()
        adapter = _adapter(client=client, tenant_aware=True)
        events: list[CacheInvalidation] = []

        await adapter.subscribe_invalidations(events.append)

        assert client.prefixes == ("tenant:",)
        assert client.on_keys is not None

        tid = str(_TENANT)
        client.on_keys(
            [
                f"tenant:{tid}:cache:pointer:app:products:pk-1",  # → event
                f"tenant:{tid}:cache:pointer:other-ns:pk-2",  # other ns → ignored
                f"tenant:{tid}:dlock:app:products:pk-3",  # other scope → ignored
                "cache:pointer:app:products:pk-4",  # tenant-less → ignored
            ]
        )

        assert events == [CacheInvalidation(key="pk-1", tenant=tid)]
