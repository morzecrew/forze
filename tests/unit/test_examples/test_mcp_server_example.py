"""Proves the MCP example preserves operations as tools and round-trips over MCP."""

from __future__ import annotations

import pytest

pytest.importorskip("fastmcp")

from fastmcp import Client

from examples.recipes.mcp_server.app import (
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

    async def test_querying_dsl_prompts_are_exposed(self) -> None:
        registry = build_registry()
        ctx_factory, _ = build_context_factory()
        server = build_server(registry, ctx_factory)

        async with Client(server) as client:
            prompts = {p.name for p in await client.list_prompts()}

        assert {"forze.querying", "forze.aggregates"} <= prompts

    async def test_field_schema_resource_is_exposed(self) -> None:
        registry = build_registry()
        ctx_factory, _ = build_context_factory()
        server = build_server(registry, ctx_factory)

        async with Client(server) as client:
            uris = {str(r.uri) for r in await client.list_resources()}

        assert "schema://notes" in uris

    async def test_get_by_id_resource_template_round_trips(self) -> None:
        import json

        registry = build_registry()
        ctx_factory, _ = build_context_factory()
        await seed(registry, ctx_factory)
        server = build_server(registry, ctx_factory)

        async with Client(server) as client:
            templates = {
                str(t.uriTemplate) for t in await client.list_resource_templates()
            }
            assert "notes://{id}" in templates

            # Find a seeded note id via the list tool, then read it through the template.
            listed = await client.call_tool(NS.key(DocumentKernelOp.LIST), {})
            note_id = listed.structured_content["hits"][0]["id"]
            content = await client.read_resource(f"notes://{note_id}")

        assert json.loads(content[0].text)["id"] == note_id

    async def test_logging_middleware_emits_access_line(self) -> None:
        import structlog

        registry = build_registry()
        ctx_factory, _ = build_context_factory()
        server = build_server(registry, ctx_factory)

        with structlog.testing.capture_logs() as logs:
            async with Client(server) as client:
                await client.call_tool(NS.key(DocumentKernelOp.LIST), {})

        assert any(
            entry.get("event") == "Processed MCP request"
            and entry.get("mcp", {}).get("method") == "tools/call"
            for entry in logs
        )

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
