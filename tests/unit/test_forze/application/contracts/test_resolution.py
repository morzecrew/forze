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
