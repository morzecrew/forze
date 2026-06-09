"""Tests for mock tenancy namespace partitioning."""

from uuid import UUID

from forze_mock.tenancy import partition_namespace, resolve_mock_namespace

# ----------------------- #

_TENANT = UUID("00000000-0000-4000-8000-000000000099")


async def test_partition_namespace_bare_when_no_tenant() -> None:
    assert partition_namespace(None, "docs") == "docs"


async def test_partition_namespace_prefixes_tenant() -> None:
    assert partition_namespace(_TENANT, "docs") == f"{_TENANT}/docs"


async def test_resolve_mock_namespace_static_and_relation() -> None:
    assert await resolve_mock_namespace("cache", None) == "cache"
    assert await resolve_mock_namespace(("public", "orders"), _TENANT) == "public/orders"
