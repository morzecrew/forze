"""Proves the MCP example preserves operations as tools and round-trips over MCP."""

from __future__ import annotations

import pytest

pytest.importorskip("fastmcp")

from fastmcp import Client

from examples.mcp_server import (
    NS,
    build_context_factory,
    build_registry,
    build_server,
    seed,
)
from forze_kits.aggregates.document.operations import DocumentKernelOp

# ----------------------- #


class TestMcpExample:
    async def test_operations_are_exposed_as_tools(self) -> None:
        registry = build_registry()
        ctx_factory, _ = build_context_factory()
        server = build_server(registry, ctx_factory)

        async with Client(server) as client:
            names = {t.name for t in await client.list_tools()}

        assert NS.key(DocumentKernelOp.GET) in names
        assert NS.key(DocumentKernelOp.LIST) in names
        assert NS.key(DocumentKernelOp.CREATE) in names  # include_writes=True

    async def test_seeded_notes_are_listable_over_mcp(self) -> None:
        registry = build_registry()
        ctx_factory, _ = build_context_factory()
        await seed(registry, ctx_factory)
        server = build_server(registry, ctx_factory)

        async with Client(server) as client:
            result = await client.call_tool(NS.key(DocumentKernelOp.LIST), {})

        assert result.structured_content["count"] == 2

    async def test_create_then_list_round_trips(self) -> None:
        registry = build_registry()
        ctx_factory, _ = build_context_factory()  # fresh, empty store
        server = build_server(registry, ctx_factory)

        async with Client(server) as client:
            await client.call_tool(
                NS.key(DocumentKernelOp.CREATE),
                {"title": "fresh", "body": "made over mcp"},
            )
            result = await client.call_tool(NS.key(DocumentKernelOp.LIST), {})

        assert result.structured_content["count"] == 1
        assert result.structured_content["hits"][0]["title"] == "fresh"
