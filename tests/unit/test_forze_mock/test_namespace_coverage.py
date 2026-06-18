"""Coverage for :mod:`forze_mock.tenancy.namespace` resolve helpers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_mock.tenancy.namespace import (
    resolve_mock_namespace,
    resolve_mock_namespace_sync,
)

# ----------------------- #


class TestResolveSync:
    def test_relation_tuple_joins_ns_and_name(self) -> None:
        out = resolve_mock_namespace_sync(
            default="d", relation=("tenant_a", "orders")
        )
        assert out == "tenant_a/orders"

    def test_relation_str_returned_verbatim(self) -> None:
        out = resolve_mock_namespace_sync(default="d", relation="literal")
        assert out == "literal"

    def test_namespace_str_returned(self) -> None:
        out = resolve_mock_namespace_sync(default="d", namespace="ns")
        assert out == "ns"

    def test_falls_back_to_default(self) -> None:
        assert resolve_mock_namespace_sync(default="d") == "d"

    def test_relation_resolver_callable_falls_through_to_default(self) -> None:
        # A dynamic (callable) relation is not static: sync resolution ignores it
        # and falls back to the default (callers must use the async path).
        out = resolve_mock_namespace_sync(
            default="d", relation=lambda _tid: ("x", "y")
        )
        assert out == "d"

    def test_namespace_resolver_callable_falls_through_to_default(self) -> None:
        out = resolve_mock_namespace_sync(
            default="d", namespace=lambda _tid: "resolved"
        )
        assert out == "d"

    def test_tenant_id_is_ignored(self) -> None:
        out = resolve_mock_namespace_sync(
            default="d", namespace="ns", tenant_id=uuid4()
        )
        assert out == "ns"


# ....................... #


class TestResolveAsync:
    @pytest.mark.asyncio
    async def test_relation_tuple(self) -> None:
        out = await resolve_mock_namespace(("tenant_a", "orders"), None)
        assert out == "tenant_a/orders"

    @pytest.mark.asyncio
    async def test_static_str(self) -> None:
        out = await resolve_mock_namespace("plain", None)
        assert out == "plain"

    @pytest.mark.asyncio
    async def test_resolver_callable(self) -> None:
        tid = uuid4()
        out = await resolve_mock_namespace(lambda t: f"ns-{t}", tid)
        assert out == f"ns-{tid}"

    @pytest.mark.asyncio
    async def test_async_resolver_callable(self) -> None:
        async def _resolver(_tid: object) -> str:
            return "awaited"

        out = await resolve_mock_namespace(_resolver, None)
        assert out == "awaited"
