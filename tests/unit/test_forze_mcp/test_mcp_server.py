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
from forze_mcp.dispatch import build_args, invoke_operation
from forze_mcp.identity import StaticIdentityResolver
from forze_mcp.projection import exposed_operations
from forze_mcp.registration import register_operations
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
        names = register_operations(server, _registry(), _ctx_factory)

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

        register_operations(server, _registry(), _ctx_factory)

        async with Client(server) as client:
            names = {t.name for t in await client.list_tools()}

        assert {"hand.written", "calc.double"} <= names


# ....................... #


class TestRoundTrip:
    async def test_client_lists_and_calls_a_read_only_tool(self) -> None:
        server = build_mcp_server(_registry(), _ctx_factory, name="calc-mcp")

        async with Client(server) as client:
            tools = {t.name for t in await client.list_tools()}
            assert tools == {"calc.double"}

            result = await client.call_tool("calc.double", {"n": 21})
            assert result.structured_content == {"doubled": 42}
