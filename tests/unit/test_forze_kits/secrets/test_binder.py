"""Hot-reload binder: targeted eviction, callback isolation, supervised consumption."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from forze.application.contracts.secrets import SecretChanged, SecretRef, SecretVersion
from forze.base.exceptions import CoreException
from forze_kits.integrations.secrets import SecretsHotReloadBinder
from forze_mock import MockDepsModule, MockSecretsChangeSource
from tests.support.execution_context import context_from_modules

# ----------------------- #

_TENANT_A = uuid4()
_TENANT_B = uuid4()


class _FakeRoutedClient:
    """The routed-pool surface the binder touches: cached ids, ref lookup, evict."""

    def __init__(self) -> None:
        self.secret_ref_for_tenant = {
            _TENANT_A: SecretRef(f"tenants/{_TENANT_A}/dsn"),
            _TENANT_B: SecretRef(f"tenants/{_TENANT_B}/dsn"),
        }
        self.evicted: list[object] = []

    def cached_tenant_ids(self) -> tuple[object, ...]:
        return tuple(self.secret_ref_for_tenant)

    async def evict_tenant(self, tenant_id: object) -> None:
        self.evicted.append(tenant_id)


def _change(path: str) -> SecretChanged:
    return SecretChanged(ref=SecretRef(path), version=SecretVersion("v2"))


class TestDispatch:
    async def test_evicts_only_the_affected_tenant(self) -> None:
        client = _FakeRoutedClient()
        binder = SecretsHotReloadBinder(
            sources=(MockSecretsChangeSource(),),
            routed_clients=(client,),  # type: ignore[arg-type]
        )

        await binder.dispatch(_change(f"tenants/{_TENANT_A}/dsn"))

        assert client.evicted == [_TENANT_A]

    async def test_unrelated_change_evicts_nothing(self) -> None:
        client = _FakeRoutedClient()
        binder = SecretsHotReloadBinder(
            sources=(MockSecretsChangeSource(),),
            routed_clients=(client,),  # type: ignore[arg-type]
        )

        await binder.dispatch(_change("unrelated/path"))

        assert client.evicted == []

    async def test_callback_failure_is_contained(self) -> None:
        seen: list[str] = []

        async def _bad(ref: SecretRef) -> None:
            raise RuntimeError("recycle failed")

        async def _good(ref: SecretRef) -> None:
            seen.append(ref.path)

        binder = SecretsHotReloadBinder(
            sources=(MockSecretsChangeSource(),),
            on_change=(_bad, _good),
        )

        await binder.dispatch(_change("db/dsn"))

        assert seen == ["db/dsn"]

    def test_needs_a_source(self) -> None:
        with pytest.raises(CoreException, match="at least one change source"):
            SecretsHotReloadBinder(sources=())


class TestLifecycle:
    async def test_consumes_emitted_changes_until_stopped(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        source = MockSecretsChangeSource()
        client = _FakeRoutedClient()
        binder = SecretsHotReloadBinder(
            sources=(source,),
            routed_clients=(client,),  # type: ignore[arg-type]
        )
        step = binder.lifecycle_step()

        await step.startup(ctx)

        try:
            for _ in range(50):
                # The mock source is live-only: re-emit until the consume loop has
                # attached its subscription (delivery is at-least-once anyway).
                source.emit(_change(f"tenants/{_TENANT_B}/dsn"))
                await asyncio.sleep(0.01)

                if client.evicted:
                    break

            assert client.evicted[:1] == [_TENANT_B]

        finally:
            await step.shutdown(ctx)

        assert step.requires_long_running
