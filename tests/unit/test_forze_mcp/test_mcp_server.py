"""Tests for forze_mcp: exposure policy, FastMCP registration, dispatch, round-trip."""

from __future__ import annotations

import pytest

pytest.importorskip("fastmcp")

import attrs
from fastmcp import Client, FastMCP
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.contracts.authn import AuthnIdentity
from forze_mcp.dispatch import build_args, invoke_operation
from forze_mcp.identity import DelegatedIdentityResolver, StaticIdentityResolver
from forze_mcp.projection import exposed_operations
from forze_mcp.prompts import register_dsl_query_prompts
from forze_mcp.registration import register_tools
from forze_mcp.server import build_mcp_server

from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _In(BaseModel):
    n: int
    label: str = "x"


class _Out(BaseModel):
    doubled: int


@attrs.define(slots=True)
class _Doubler(Handler[_In, _Out]):
    async def __call__(self, args: _In) -> _Out:
        return _Out(doubled=args.n * 2)


def _registry() -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "calc.double": lambda _c: _Doubler(),
            "calc.write": lambda _c: _Doubler(),  # COMMAND (default), no descriptor
        }
    )
    reg = reg.set_descriptor(
        "calc.double",
        OperationDescriptor(input_type=_In, output_type=_Out, description="double n"),
    )
    reg = reg.set_descriptor(
        "calc.write",
        OperationDescriptor(input_type=_In, output_type=_Out, description="write n"),
    )
    reg = reg.bind("calc.double").as_query().finish()

    return reg.freeze()


def _ctx_factory():
    return context_from_modules(MockDepsModule())


# ....................... #


class TestExposurePolicy:
    def test_only_read_only_exposed_by_default(self) -> None:
        exposed = exposed_operations(_registry().catalog())

        assert "calc.double" in exposed
        assert "calc.write" not in exposed

    def test_include_writes_exposes_commands(self) -> None:
        exposed = exposed_operations(_registry().catalog(), include_writes=True)

        assert "calc.write" in exposed


# ....................... #


class TestDelegatedIdentity:
    async def test_attaches_agent_as_actor(self) -> None:
        from uuid import uuid4

        agent = AuthnIdentity(principal_id=uuid4())
        user = AuthnIdentity(principal_id=uuid4())

        async def _resolve_subject():
            return user, None

        resolver = DelegatedIdentityResolver(
            agent=agent, resolve_subject=_resolve_subject
        )
        authn, tenant = await resolver.resolve()

        assert authn is not None
        assert authn.principal_id == user.principal_id  # effective subject = user
        assert authn.actor == agent  # actor = agent
        assert tenant is None


class TestDispatch:
    def test_build_args_validates_into_dto(self) -> None:
        descriptor = _registry().catalog()["calc.double"].descriptor

        args = build_args(descriptor, {"n": 3})

        assert isinstance(args, _In)
        assert args.n == 3

    async def test_invoke_runs_through_pipeline(self) -> None:
        reg = _registry()

        result = await invoke_operation(
            registry=reg,
            ctx_factory=_ctx_factory,
            identity=StaticIdentityResolver(),
            op="calc.double",
            descriptor=reg.catalog()["calc.double"].descriptor,
            arguments={"n": 21},
        )

        assert result.doubled == 42


# ....................... #


class TestRegistration:
    async def test_registers_flat_top_level_args(self) -> None:
        server = FastMCP("calc")
        names = register_tools(server, _registry(), _ctx_factory)

        assert names == ["calc.double"]  # write op excluded

        async with Client(server) as client:
            tool = {t.name: t for t in await client.list_tools()}["calc.double"]

        # Flat: DTO fields are top-level properties, not nested under "args".
        assert set(tool.inputSchema.get("properties", {})) == {"n", "label"}
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.description == "double n"

    async def test_register_onto_existing_server_is_additive(self) -> None:
        server = FastMCP("calc")

        @server.tool(name="hand.written")
        def _hand_written(x: int) -> int:
            return x

        register_tools(server, _registry(), _ctx_factory)

        async with Client(server) as client:
            names = {t.name for t in await client.list_tools()}

        assert {"hand.written", "calc.double"} <= names


# ....................... #


class TestQueryPrompts:
    async def test_registers_dsl_prompts(self) -> None:
        server = FastMCP("calc")
        names = register_dsl_query_prompts(server)

        assert names == ["forze.querying", "forze.aggregates"]

        async with Client(server) as client:
            listed = {p.name for p in await client.list_prompts()}

        assert {"forze.querying", "forze.aggregates"} <= listed

    async def test_prefix_is_configurable_and_additive(self) -> None:
        server = FastMCP("calc")

        @server.prompt(name="hand.written")
        def _hand() -> str:
            return "hi"

        register_dsl_query_prompts(server, prefix="acme")

        async with Client(server) as client:
            listed = {p.name for p in await client.list_prompts()}

        assert {"hand.written", "acme.querying", "acme.aggregates"} <= listed

    async def test_querying_prompt_renders_grammar_and_goal(self) -> None:
        server = FastMCP("calc")
        register_dsl_query_prompts(server)

        async with Client(server) as client:
            result = await client.get_prompt("forze.querying", {"goal": "active items"})

        text = result.messages[0].content.text
        assert "active items" in text
        # Grounded in the real DSL grammar.
        assert "$values" in text and "$and" in text and '"asc"' in text


class TestRoundTrip:
    async def test_client_lists_and_calls_a_read_only_tool(self) -> None:
        server = build_mcp_server(_registry(), _ctx_factory, name="calc-mcp")

        async with Client(server) as client:
            tools = {t.name for t in await client.list_tools()}
            assert tools == {"calc.double"}

            result = await client.call_tool("calc.double", {"n": 21})
            assert result.structured_content == {"doubled": 42}


# ....................... #


class TestWriteEnablement:
    async def test_write_op_exposed_with_destructive_hints(self) -> None:
        server = build_mcp_server(
            _registry(), _ctx_factory, name="calc-mcp", include_writes=True
        )

        async with Client(server) as client:
            tools = {t.name: t for t in await client.list_tools()}
            assert {"calc.double", "calc.write"} <= set(tools)

            read_tool = tools["calc.double"]
            write_tool = tools["calc.write"]

            assert read_tool.annotations is not None
            assert read_tool.annotations.readOnlyHint is True
            assert read_tool.annotations.destructiveHint is False

            assert write_tool.annotations is not None
            assert write_tool.annotations.readOnlyHint is False
            assert write_tool.annotations.destructiveHint is True
            # Flat arg schema applies to writes too.
            assert set(write_tool.inputSchema.get("properties", {})) == {"n", "label"}

    async def test_client_calls_a_write_tool_end_to_end(self) -> None:
        server = build_mcp_server(
            _registry(), _ctx_factory, name="calc-mcp", include_writes=True
        )

        async with Client(server) as client:
            result = await client.call_tool("calc.write", {"n": 21})
            assert result.structured_content == {"doubled": 42}

    async def test_writes_excluded_by_default(self) -> None:
        server = build_mcp_server(_registry(), _ctx_factory, name="calc-mcp")

        async with Client(server) as client:
            tools = {t.name for t in await client.list_tools()}
            assert "calc.write" not in tools
