"""Tests for the task-scoped invocation deadline and its enforcement."""

from __future__ import annotations

import asyncio

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.execution import (
    ExecutionContext,
    bind_deadline,
    current_deadline,
    remaining_time,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


@attrs.define(slots=True, kw_only=True, frozen=True)
class EchoHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return f"handler:{args}"


@attrs.define(slots=True, kw_only=True, frozen=True)
class StallHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        await asyncio.Event().wait()
        return args


# ----------------------- #


class TestDeadlineContext:
    def test_unbound_by_default(self) -> None:
        assert current_deadline() is None
        assert remaining_time() is None

    def test_bind_sets_and_resets(self) -> None:
        with bind_deadline(5.0):
            deadline = current_deadline()
            left = remaining_time()

            assert deadline is not None
            assert left is not None
            assert 0.0 < left <= 5.0

        assert current_deadline() is None
        assert remaining_time() is None

    def test_none_is_passthrough(self) -> None:
        with bind_deadline(None):
            assert current_deadline() is None

        with bind_deadline(5.0):
            outer = current_deadline()

            with bind_deadline(None):
                assert current_deadline() == outer

    def test_nested_bind_tightens(self) -> None:
        with bind_deadline(5.0):
            with bind_deadline(1.0):
                left = remaining_time()
                assert left is not None
                assert left <= 1.0

    def test_nested_bind_never_extends(self) -> None:
        with bind_deadline(1.0):
            outer = current_deadline()

            with bind_deadline(60.0):
                assert current_deadline() == outer

    def test_expired_deadline_clamps_to_zero(self) -> None:
        with bind_deadline(0.0):
            assert remaining_time() == 0.0

    def test_invocation_context_delegates(self, ctx: ExecutionContext) -> None:
        assert ctx.inv_ctx.get_deadline() is None
        assert ctx.inv_ctx.remaining_time() is None

        with ctx.inv_ctx.bind_deadline(5.0):
            assert ctx.inv_ctx.get_deadline() == current_deadline()
            left = ctx.inv_ctx.remaining_time()
            assert left is not None
            assert 0.0 < left <= 5.0


class TestOperationDeadlineEnforcement:
    @pytest.mark.asyncio
    async def test_runs_normally_without_deadline(self, ctx: ExecutionContext) -> None:
        reg = OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        assert await resolved("x") == "handler:x"

    @pytest.mark.asyncio
    async def test_runs_within_deadline(self, ctx: ExecutionContext) -> None:
        reg = OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(5.0):
            assert await resolved("x") == "handler:x"

    @pytest.mark.asyncio
    async def test_expired_deadline_fails_fast(self, ctx: ExecutionContext) -> None:
        called: list[str] = []

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class TrackingHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                called.append(args)
                return args

        reg = OperationRegistry(handlers={"op": lambda _ctx: TrackingHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(0.0):
            with pytest.raises(CoreException) as ei:
                await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"
        assert called == []

    @pytest.mark.asyncio
    async def test_deadline_bounds_running_operation(
        self, ctx: ExecutionContext
    ) -> None:
        reg = OperationRegistry(handlers={"op": lambda _ctx: StallHandler()}).freeze()
        resolved = reg.resolve("op", ctx)

        with bind_deadline(0.05):
            with pytest.raises(CoreException) as ei:
                await resolved("x")

        assert ei.value.kind is ExceptionKind.TIMEOUT
        assert ei.value.code == "deadline_exceeded"
