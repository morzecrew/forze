"""Unit tests for handler execution via OperationRegistry."""

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from tests.support.execution_context import (
    context_from_deps,
    context_from_modules,
    frozen_deps_from_deps,
)

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


class TestResolvedOperationCache:
    def _counting_registry(self) -> tuple[OperationRegistry, list[int]]:
        calls = [0]

        def _factory(_ctx):
            calls[0] += 1
            return EchoHandler()

        return OperationRegistry(handlers={"echo": _factory}).freeze(), calls

    def test_caching_on_reuses_resolved_operation(self) -> None:
        reg, calls = self._counting_registry()
        ctx = ExecutionContext(deps=frozen_deps_from_deps(Deps()))  # default: on

        first = reg.resolve("echo", ctx)
        second = reg.resolve("echo", ctx)

        assert first is second
        assert calls[0] == 1  # handler factory built once for the scope

    def test_caching_off_rebuilds_each_time(self) -> None:
        reg, calls = self._counting_registry()
        ctx = ExecutionContext(
            deps=frozen_deps_from_deps(Deps()),
            cache_operations=False,
        )

        first = reg.resolve("echo", ctx)
        second = reg.resolve("echo", ctx)

        assert first is not second
        assert calls[0] == 2

    def test_cache_is_per_scope(self) -> None:
        reg, _ = self._counting_registry()
        ctx_a = ExecutionContext(deps=frozen_deps_from_deps(Deps()))
        ctx_b = ExecutionContext(deps=frozen_deps_from_deps(Deps()))

        # Different scopes do not share cached operations.
        assert reg.resolve("echo", ctx_a) is not reg.resolve("echo", ctx_b)

    @pytest.mark.asyncio
    async def test_cached_operation_executes_equivalently(self) -> None:
        reg, _ = self._counting_registry()
        ctx = ExecutionContext(deps=frozen_deps_from_deps(Deps()))

        resolved = reg.resolve("echo", ctx)
        again = reg.resolve("echo", ctx)

        assert await resolved("a") == "result:a"
        assert await again("b") == "result:b"
