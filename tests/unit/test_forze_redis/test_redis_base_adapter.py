"""Unit tests for :mod:`forze_redis.adapters.base.RedisBaseAdapter`."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_redis.adapters.base import RedisBaseAdapter
from forze_redis.adapters.codecs import RedisKeyCodec


class _Adapter(RedisBaseAdapter):
    """Minimal concrete adapter for base-class behavior."""

    pass


def test_static_namespace_key_codec() -> None:
    adapter = _Adapter(client=None, namespace="static-ns")  # type: ignore[arg-type]
    assert adapter.key_codec == RedisKeyCodec(namespace="static-ns")
    assert adapter.construct_key("scope", "part") == "scope:static-ns:part"


@pytest.mark.asyncio
async def test_dynamic_namespace_resolves_and_caches() -> None:
    tid = uuid4()

    async def resolver(_tenant_id):
        return f"tenant-{tid.hex[:8]}"

    adapter = _Adapter(
        client=None,  # type: ignore[arg-type]
        namespace=resolver,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )
    await adapter._prepare_keys()
    ns = await adapter._resolved_namespace()
    assert ns == f"tenant-{tid.hex[:8]}"
    assert adapter.key_codec.namespace == ns


@pytest.mark.asyncio
async def test_dynamic_namespace_is_task_isolated_across_tenants() -> None:
    """A shared adapter (per-scope port cache) must not leak/clobber namespaces
    between concurrent operations for different tenants."""

    import asyncio
    from contextvars import ContextVar

    current: ContextVar[TenantIdentity | None] = ContextVar("current", default=None)

    async def resolver(tenant_id):
        return f"ns-{tenant_id.hex[:6]}"

    # One shared adapter, as the port cache would share it.
    adapter = _Adapter(
        client=None,  # type: ignore[arg-type]
        namespace=resolver,
        tenant_provider=current.get,
    )

    async def run(tid) -> str:
        current.set(TenantIdentity(tenant_id=tid))
        await adapter._prepare_keys()
        await asyncio.sleep(0)  # yield so the other tenant's op interleaves
        return adapter.key_codec.namespace  # sync read after an await

    ta, tb = uuid4(), uuid4()
    ns_a, ns_b = await asyncio.gather(
        asyncio.create_task(run(ta)),
        asyncio.create_task(run(tb)),
    )

    assert ns_a == f"ns-{ta.hex[:6]}"
    assert ns_b == f"ns-{tb.hex[:6]}"


def test_tenant_required_when_tenant_aware() -> None:
    adapter = _Adapter(
        client=None,  # type: ignore[arg-type]
        namespace="ns",
        tenant_aware=True,
        tenant_provider=lambda: None,
    )
    with pytest.raises(CoreException, match="Tenant ID is required"):
        adapter._tenant_id_for_resolve()


def test_key_codec_requires_resolve_for_dynamic_namespace() -> None:
    async def resolver(_tenant_id):
        return "dyn"

    adapter = _Adapter(
        client=None,  # type: ignore[arg-type]
        namespace=resolver,
    )
    with pytest.raises(CoreException, match="resolved namespace"):
        _ = adapter.key_codec


def test_construct_key_with_tenant_prefix() -> None:
    tid = uuid4()
    adapter = _Adapter(
        client=None,  # type: ignore[arg-type]
        namespace="app",
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )
    key = adapter.construct_key("cache", "item")
    # join order: tenant prefix, scope, namespace, parts
    assert key == f"tenant:{tid}:cache:app:item"
