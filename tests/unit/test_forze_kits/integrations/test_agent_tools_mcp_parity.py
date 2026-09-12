"""The symmetry, tested rather than asserted: one operation, two agent surfaces.

`forze_mcp` projects operations as tools for an agent on the far end of a transport; this
bridge projects the same operations for an agent inside the process. Both read the
operation's descriptor, so the tool an agent is offered describes the same call either way.
What differs is presentation, not contract: the MCP surface synthesizes a flat callable
signature and lets FastMCP derive the schema from it, so the two schemas are *equivalent*
rather than byte-identical — same fields, same required set, same types. That equivalence
is the thing worth pinning; byte equality would be a test of FastMCP's schema generator.
"""

from __future__ import annotations

from typing import Any

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
from forze.testing import context_from_modules
from forze_kits.integrations.agent_tools import operation_tools
from forze_mcp.registration import register_tools
from forze_mock import MockDepsModule

pytestmark = pytest.mark.unit

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
    reg = OperationRegistry(handlers={"calc.double": lambda _c: _Doubler()})
    reg = reg.set_descriptor(
        "calc.double",
        OperationDescriptor(input_type=_In, output_type=_Out, description="double n"),
    )
    reg = reg.bind("calc.double").as_query().finish()

    return reg.freeze()


def _ctx_factory():
    return context_from_modules(MockDepsModule())


def _fields(schema: dict[str, Any]) -> dict[str, Any]:
    """Property names mapped to their declared type, with presentation stripped."""

    properties = schema.get("properties", {})

    return {
        name: field.get("type") or field.get("anyOf") or field.get("$ref")
        for name, field in properties.items()
    }


# ....................... #


class TestTheTwoSurfacesAgree:
    async def test_the_same_operation_yields_equivalent_schemas(self) -> None:
        server = FastMCP("calc")
        register_tools(server, _registry(), _ctx_factory)

        async with Client(server) as client:
            mcp_tool = {t.name: t for t in await client.list_tools()}["calc.double"]

        bridged = operation_tools(_registry(), include=["calc.double"]).defs[0]

        assert _fields(bridged.input_schema) == _fields(mcp_tool.input_schema)
        assert set(bridged.input_schema.get("required", ())) == set(
            mcp_tool.input_schema.get("required", ())
        )

    async def test_the_tool_names_match(self) -> None:
        # Same name on both surfaces: an operation is one thing with one id, and a durable
        # agent run that journals a tool name stays readable across either.
        server = FastMCP("calc")
        names = register_tools(server, _registry(), _ctx_factory)

        bridged = operation_tools(_registry(), include=["calc.double"])

        assert tuple(names) == bridged.names

    async def test_the_descriptions_come_from_the_same_place(self) -> None:
        server = FastMCP("calc")
        register_tools(server, _registry(), _ctx_factory)

        async with Client(server) as client:
            mcp_tool = {t.name: t for t in await client.list_tools()}["calc.double"]

        bridged = operation_tools(_registry(), include=["calc.double"]).defs[0]

        # The MCP surface appends catalog-derived sentences (auth, deadlines) to its text;
        # both start from the descriptor's own description, which is what must not drift.
        assert bridged.description is not None
        assert mcp_tool.description is not None
        assert bridged.description in mcp_tool.description

    def test_the_read_write_split_is_the_same_predicate(self) -> None:
        from forze_mcp.projection import exposed_operations

        registry = _registry()
        exposed = exposed_operations(registry.catalog())
        bridged = operation_tools(registry, include=["calc.double"])

        assert set(exposed) == set(bridged.names)


# ....................... #


class TestBothSurfacesRejectAHallucinatedArgument:
    async def test_neither_surface_silently_drops_it(self) -> None:
        # The asymmetry this pins: pydantic's default is to ignore an unknown key, so
        # without an explicit refusal the in-process bridge would obey a call the MCP
        # surface rejects — and an agent would be told its invented filter was applied.
        from uuid import uuid4

        from forze.application.contracts.authn import AuthnIdentity
        from forze.application.execution import Deps, InvocationMetadata
        from forze.testing import context_from_deps
        from forze_kits.integrations.agent_tools import ToolUse, dispatch_tool_use

        server = FastMCP("calc")
        register_tools(server, _registry(), _ctx_factory)

        async with Client(server) as client:
            with pytest.raises(Exception) as mcp_refusal:
                await client.call_tool("calc.double", {"n": 21, "invented": 1})

        ctx = context_from_deps(Deps())
        tools = operation_tools(_registry(), include=["calc.double"])

        with ctx.inv_ctx.bind(
            metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
            authn=AuthnIdentity(principal_id=uuid4()),
        ):
            bridged = await dispatch_tool_use(
                ToolUse(id="t", name="calc.double", input={"n": 21, "invented": 1}),
                ctx=ctx,
                tools=tools,
            )

        assert "invented" in str(mcp_refusal.value)
        assert bridged.is_error is True
        assert "invented" in str(bridged.content)
