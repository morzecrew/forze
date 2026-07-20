"""Tests for :mod:`forze.application.contracts.resolution`."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.resolution import resolve_value

# ----------------------- #


@pytest.mark.asyncio
async def test_resolve_value_returns_plain_value() -> None:
    assert await resolve_value(("public", "items"), None) == ("public", "items")


@pytest.mark.asyncio
async def test_resolve_value_sync_callable() -> None:
    tid = uuid4()

    def resolver(tenant_id: UUID | None) -> tuple[str, str]:
        assert tenant_id == tid
        return ("tenant_a", "items")

    assert await resolve_value(resolver, tid) == ("tenant_a", "items")


@pytest.mark.asyncio
async def test_resolve_value_async_callable() -> None:
    tid = uuid4()

    async def resolver(tenant_id: UUID | None) -> tuple[str, str]:
        assert tenant_id == tid
        return ("tenant_b", "items")

    assert await resolve_value(resolver, tid) == ("tenant_b", "items")


# ----------------------- #
# resolve_scoped_namespace


@pytest.mark.asyncio
async def test_resolve_scoped_namespace_memoizes_static() -> None:
    from forze.application.contracts.resolution import resolve_scoped_namespace
    from forze.base.primitives import OnceCell

    cell: OnceCell[str] = OnceCell()
    calls = 0

    async def resolver(spec, tenant_id):
        nonlocal calls
        calls += 1
        return f"{spec}"

    # Static spec (str) → resolved once and cached.
    assert await resolve_scoped_namespace("idx", tenant_id=None, cell=cell, resolver=resolver) == "idx"
    assert await resolve_scoped_namespace("idx", tenant_id=None, cell=cell, resolver=resolver) == "idx"
    assert calls == 1


@pytest.mark.asyncio
async def test_resolve_scoped_namespace_reresolves_dynamic() -> None:
    from forze.application.contracts.resolution import resolve_scoped_namespace
    from forze.base.primitives import OnceCell

    cell: OnceCell[str] = OnceCell()
    t1, t2 = uuid4(), uuid4()

    def spec(tenant_id: UUID | None) -> str:
        return f"idx-{tenant_id}"

    # Dynamic resolver → never memoized; each tenant resolves independently.
    assert await resolve_scoped_namespace(spec, tenant_id=t1, cell=cell) == f"idx-{t1}"
    assert await resolve_scoped_namespace(spec, tenant_id=t2, cell=cell) == f"idx-{t2}"
