"""Unit tests for handler execution via OperationRegistry."""

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.registry import OperationRegistry
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class EchoHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return f"result:{args}"


class TestHandlerExecution:
    @pytest.fixture
    def stub_ctx(self) -> ExecutionContext:
        return context_from_deps(Deps())

    @pytest.mark.asyncio
    async def test_resolve_and_call_handler(self, stub_ctx: ExecutionContext) -> None:
        reg = OperationRegistry(
            handlers={"echo": lambda _ctx: EchoHandler()},
        ).freeze()
        resolved = reg.resolve("echo", stub_ctx)
        assert await resolved("foo") == "result:foo"
