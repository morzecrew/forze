"""Tests for resolved operation plan execution."""

from __future__ import annotations

import attrs
import pytest

from forze.application.contracts.execution import (
    BeforeStep,
    DispatchStep,
    FinallyStep,
    Handler,
    MiddlewareStep,
    OnFailureStep,
    OnSuccessStep,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.contracts.execution.value_objects import Failure, Success
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze_mock import MockDepsModule

# ----------------------- #


def _before_factory(order: list[str], name: str):
    def _factory(_ctx):
        async def _before(_args) -> None:
            order.append(name)

        return _before

    return _factory


def _on_success_factory(order: list[str], name: str):
    def _factory(_ctx):
        async def _on_success(_args, _result) -> None:
            order.append(name)

        return _on_success

    return _factory


def _on_failure_factory(order: list[str], name: str):
    def _factory(_ctx):
        async def _on_failure(_args, _exc) -> None:
            order.append(name)

        return _on_failure

    return _factory


def _finally_factory(order: list[str], name: str):
    def _factory(_ctx):
        async def _finally(_args, outcome) -> None:
            if isinstance(outcome, Success):
                order.append(f"{name}:success")
            else:
                assert isinstance(outcome, Failure)
                order.append(f"{name}:failure")

        return _finally

    return _factory


def _wrap_factory(order: list[str], name: str):
    def _factory(_ctx):
        async def _wrap(next, args):
            order.append(f"{name}:before")
            result = await next(args)
            order.append(f"{name}:after")
            return result

        return _wrap

    return _factory


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


@attrs.define(slots=True, kw_only=True, frozen=True)
class EchoHandler(Handler[str, str]):
    label: str = "handler"

    async def __call__(self, args: str) -> str:
        return f"{self.label}:{args}"


@attrs.define(slots=True, kw_only=True, frozen=True)
class FailHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        raise RuntimeError(args)


class TestBeforeAndOnSuccess:
    @pytest.mark.asyncio
    async def test_before_runs_before_handler_on_success_after(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()})
            .bind("op")
            .bind_outer()
            .before(BeforeStep(id="b", factory=_before_factory(order, "before")))
            .on_success(
                OnSuccessStep(id="s", factory=_on_success_factory(order, "on_success"))
            )
            .finish(deep=True)
            .freeze()
        )
        resolved = reg.resolve("op", ctx)
        result = await resolved("x")

        assert result == "handler:x"
        assert order == ["before", "on_success"]


class TestOnFailureAndFinally:
    @pytest.mark.asyncio
    async def test_on_failure_on_error_skips_on_success(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: FailHandler()})
            .bind("op")
            .bind_outer()
            .on_success(
                OnSuccessStep(id="s", factory=_on_success_factory(order, "on_success"))
            )
            .on_failure(
                OnFailureStep(id="f", factory=_on_failure_factory(order, "on_failure"))
            )
            .finish(deep=True)
            .freeze()
        )
        resolved = reg.resolve("op", ctx)

        with pytest.raises(RuntimeError, match="boom"):
            await resolved("boom")

        assert order == ["on_failure"]

    @pytest.mark.asyncio
    async def test_finally_runs_on_success_and_failure(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []
        reg = (
            OperationRegistry(
                handlers={
                    "ok": lambda _ctx: EchoHandler(),
                    "fail": lambda _ctx: FailHandler(),
                },
            )
            .bind("ok", "fail")
            .bind_outer()
            .finally_(FinallyStep(id="fin", factory=_finally_factory(order, "finally")))
            .finish(deep=True)
            .freeze()
        )

        await reg.resolve("ok", ctx)("a")
        order.clear()

        with pytest.raises(RuntimeError):
            await reg.resolve("fail", ctx)("b")

        assert order == ["finally:failure"]


class TestWrapOrdering:
    @pytest.mark.asyncio
    async def test_wrap_higher_priority_closer_to_handler(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()})
            .bind("op")
            .bind_outer()
            .wrap(
                MiddlewareStep(
                    id="outer",
                    factory=_wrap_factory(order, "outer"),
                    priority=10,
                ),
                MiddlewareStep(
                    id="inner",
                    factory=_wrap_factory(order, "inner"),
                    priority=100,
                ),
            )
            .finish(deep=True)
            .freeze()
        )
        await reg.resolve("op", ctx)("x")

        assert order == [
            "outer:before",
            "inner:before",
            "inner:after",
            "outer:after",
        ]


class TestTransactionScope:
    @pytest.mark.asyncio
    async def test_tx_before_runs_inside_scope(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []
        depth_at_before: list[int] = []

        def _tx_before_factory(_ctx):
            async def _before(_args) -> None:
                order.append("tx_before")
                depth_at_before.append(ctx.tx_ctx.depth())

            return _before

        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()})
            .bind("op")
            .bind_tx()
            .set_route("mock")
            .before(BeforeStep(id="tb", factory=_tx_before_factory))
            .finish(deep=True)
            .freeze()
        )
        await reg.resolve("op", ctx)("x")

        assert order == ["tx_before"]
        assert depth_at_before == [1]


class TestAfterCommit:
    @pytest.mark.asyncio
    async def test_after_commit_runs_after_handler_before_outer_on_success(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()})
            .bind("op")
            .bind_tx()
            .set_route("mock")
            .after_commit(
                OnSuccessStep(
                    id="ac",
                    factory=_on_success_factory(order, "after_commit"),
                )
            )
            .finish(deep=False)
            .bind_outer()
            .on_success(
                OnSuccessStep(id="os", factory=_on_success_factory(order, "outer_os"))
            )
            .finish(deep=True)
            .freeze()
        )
        await reg.resolve("op", ctx)("x")

        assert order == ["after_commit", "outer_os"]


class TestDispatch:
    @pytest.mark.asyncio
    async def test_dispatch_invokes_target_operation(
        self, ctx: ExecutionContext
    ) -> None:
        calls: list[str] = []

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class TargetHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                calls.append(args)
                return f"target:{args}"

        reg = (
            OperationRegistry(
                handlers={
                    "main": lambda _ctx: EchoHandler(label="main"),
                    "target": lambda _ctx: TargetHandler(),
                },
            )
            .bind("main")
            .bind_outer()
            .dispatch(
                DispatchStep(
                    id="d",
                    target="target",
                    mapper=lambda _args, result: f"mapped:{result}",
                )
            )
            .finish(deep=True)
            .freeze()
        )
        result = await reg.resolve("main", ctx)("in")

        assert result == "main:in"
        assert calls == ["mapped:main:in"]


class TestFullOrdering:
    @pytest.mark.asyncio
    async def test_outer_tx_after_commit_stage_order(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()})
            .bind("op")
            .bind_outer()
            .before(BeforeStep(id="ob", factory=_before_factory(order, "outer_before")))
            .on_success(
                OnSuccessStep(
                    id="oos",
                    factory=_on_success_factory(order, "outer_on_success"),
                )
            )
            .finish(deep=False)
            .bind_tx()
            .set_route("mock")
            .before(BeforeStep(id="tb", factory=_before_factory(order, "tx_before")))
            .on_success(
                OnSuccessStep(
                    id="tos",
                    factory=_on_success_factory(order, "tx_on_success"),
                )
            )
            .after_commit(
                OnSuccessStep(
                    id="ac",
                    factory=_on_success_factory(order, "after_commit"),
                )
            )
            .finish(deep=True)
            .freeze()
        )
        await reg.resolve("op", ctx)("x")

        assert order == [
            "outer_before",
            "tx_before",
            "tx_on_success",
            "after_commit",
            "outer_on_success",
        ]
