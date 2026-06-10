"""Unit tests for the shared queue-name resolution mixin."""

from __future__ import annotations

from typing import ClassVar
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.integrations.queue import ScopedQueueNamingMixin
from forze.base.exceptions import CoreException

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Tenant:
    tenant_id: UUID


@attrs.define(slots=True, kw_only=True, frozen=True)
class _ColonAdapter(ScopedQueueNamingMixin):
    queue_name_separator: ClassVar[str] = ":"
    queue_backend_label: ClassVar[str] = "colon queue"


@attrs.define(slots=True, kw_only=True, frozen=True)
class _DashAdapter(ScopedQueueNamingMixin):
    queue_name_separator: ClassVar[str] = "-"
    queue_backend_label: ClassVar[str] = "dash queue"


# ----------------------- #


@pytest.mark.asyncio
async def test_static_name_without_namespace_or_tenant() -> None:
    adapter = _ColonAdapter()

    assert await adapter._scoped_queue_name("jobs") == "jobs"


@pytest.mark.asyncio
async def test_static_namespace_prefixes_queue() -> None:
    colon = _ColonAdapter(namespace="ns")
    dash = _DashAdapter(namespace="ns")

    assert await colon._scoped_queue_name("jobs") == "ns:jobs"
    assert await dash._scoped_queue_name("jobs") == "ns-jobs"


@pytest.mark.asyncio
async def test_tenant_scoped_resolution_per_separator() -> None:
    tid = uuid4()

    colon = _ColonAdapter(
        namespace="ns",
        tenant_aware=True,
        tenant_provider=lambda: _Tenant(tenant_id=tid),
    )
    dash = _DashAdapter(
        namespace="ns",
        tenant_aware=True,
        tenant_provider=lambda: _Tenant(tenant_id=tid),
    )

    assert await colon._scoped_queue_name("jobs") == f"tenant:{tid}:ns:jobs"
    assert await dash._scoped_queue_name("jobs") == f"tenant-{tid}-ns-jobs"


@pytest.mark.asyncio
async def test_dynamic_namespace_resolver_receives_tenant() -> None:
    tid = uuid4()
    seen: list[UUID | None] = []

    async def _resolver(tenant_id: UUID | None) -> str:
        seen.append(tenant_id)
        return f"dyn-{tenant_id}"

    adapter = _DashAdapter(
        namespace=_resolver,
        tenant_aware=True,
        tenant_provider=lambda: _Tenant(tenant_id=tid),
    )

    assert await adapter._scoped_queue_name("jobs") == f"tenant-{tid}-dyn-{tid}-jobs"
    # A dynamic resolver is not memoized: it runs again on the next call.
    await adapter._scoped_queue_name("jobs")
    assert seen == [tid, tid]


@pytest.mark.asyncio
async def test_static_namespace_is_memoized() -> None:
    adapter = _ColonAdapter(namespace="ns")

    await adapter._scoped_queue_name("jobs")

    assert adapter._namespace_cell.peek() == "ns"


def test_tenant_id_for_resolve_requires_tenant_when_aware() -> None:
    adapter = _ColonAdapter(tenant_aware=True, tenant_provider=lambda: None)

    with pytest.raises(CoreException, match="colon queue adapter"):
        adapter._tenant_id_for_resolve()


def test_tenant_id_for_resolve_optional_when_not_aware() -> None:
    adapter = _ColonAdapter(tenant_provider=lambda: None)

    assert adapter._tenant_id_for_resolve() is None


def test_tenant_id_for_resolve_without_provider() -> None:
    adapter = _ColonAdapter()

    assert adapter._tenant_id_for_resolve() is None


@pytest.mark.asyncio
async def test_non_aware_adapter_ignores_tenant_prefix() -> None:
    tid = uuid4()
    adapter = _ColonAdapter(
        namespace="ns",
        tenant_provider=lambda: _Tenant(tenant_id=tid),
    )

    # No tenant prefix without ``tenant_aware``, even when a provider is set.
    assert await adapter._scoped_queue_name("jobs") == "ns:jobs"
