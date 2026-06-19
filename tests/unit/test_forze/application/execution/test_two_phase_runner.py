"""Execution of two-phase (prepare/apply) handlers.

Asserts the engine runs ``prepare`` OUTSIDE the transaction (depth 0) under the
read-only flag, threads its payload into ``apply`` running INSIDE the transaction
(depth 1), restores write capability for a COMMAND ``apply`` (and keeps read-only
for a QUERY one), and — on a ``prepare`` failure — never opens the transaction
while still firing the outer failure/finally hooks.
"""

from __future__ import annotations

import asyncio

import attrs
import pytest

from forze.application.contracts.execution import (
    BeforeStep,
    FinallyStep,
    MiddlewareStep,
    OnFailureStep,
    OnSuccessStep,
    TwoPhaseHandler,
)
from forze.application.contracts.execution.value_objects import Failure, Success
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


@attrs.define(slots=True, kw_only=True, frozen=True)
class RecordingTwoPhase(TwoPhaseHandler[str, str, str]):
    """Records (phase, tx-depth, read-only) at each phase; payload = ``payload:<args>``."""

    ctx: ExecutionContext
    order: list[tuple]

    async def prepare(self, args: str) -> str:
        self.order.append(
            ("prepare", self.ctx.tx_ctx.depth(), self.ctx.inv_ctx.is_read_only())
        )
        return f"payload:{args}"

    async def apply(self, args: str, payload: str) -> str:
        self.order.append(
            ("apply", self.ctx.tx_ctx.depth(), self.ctx.inv_ctx.is_read_only(), payload)
        )
        return f"applied:{args}:{payload}"


@attrs.define(slots=True, kw_only=True, frozen=True)
class PrepareFailsTwoPhase(TwoPhaseHandler[str, str, str]):
    ctx: ExecutionContext
    order: list[tuple]

    async def prepare(self, args: str) -> str:
        self.order.append(("prepare", self.ctx.tx_ctx.depth()))
        raise RuntimeError("prepare boom")

    async def apply(self, args: str, payload: str) -> str:  # pragma: no cover
        self.order.append(("apply", self.ctx.tx_ctx.depth()))
        return payload


def _on_failure_factory(order: list[tuple], name: str):
    def _factory(_ctx):
        async def _on_failure(_args, _exc) -> None:
            order.append((name,))

        return _on_failure

    return _factory


def _finally_factory(order: list[tuple], name: str):
    def _factory(_ctx):
        async def _finally(_args, outcome) -> None:
            kind = "success" if isinstance(outcome, Success) else "failure"
            assert isinstance(outcome, (Success, Failure))
            order.append((f"{name}:{kind}",))

        return _finally

    return _factory


def _on_success_factory(order: list[tuple], name: str):
    def _factory(_ctx):
        async def _on_success(_args, _result) -> None:
            order.append((name,))

        return _on_success

    return _factory


# ....................... #


class TestTwoPhaseBoundary:
    @pytest.mark.asyncio
    async def test_prepare_outside_tx_apply_inside_threads_payload(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[tuple] = []
        reg = (
            OperationRegistry(
                handlers={"op": lambda c: RecordingTwoPhase(ctx=c, order=order)}
            )
            .bind("op")
            .two_phase()
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        result = await reg.resolve("op", ctx)("x")

        assert result == "applied:x:payload:x"
        # prepare: outside the tx (depth 0), read-only bound.
        assert order[0] == ("prepare", 0, True)
        # apply: inside the tx (depth 1), write-capable (COMMAND), payload threaded.
        assert order[1] == ("apply", 1, False, "payload:x")

    @pytest.mark.asyncio
    async def test_query_two_phase_stays_read_only_in_apply(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[tuple] = []
        reg = (
            OperationRegistry(
                handlers={"op": lambda c: RecordingTwoPhase(ctx=c, order=order)}
            )
            .bind("op")
            .as_query()
            .two_phase()
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        await reg.resolve("op", ctx)("x")

        # QUERY binds read-only for the whole op; the prepare double-bind restores
        # to read-only (not write-capable), so apply stays read-only too.
        assert order[0] == ("prepare", 0, True)
        assert order[1] == ("apply", 1, True, "payload:x")


class TestPrepareFailure:
    @pytest.mark.asyncio
    async def test_prepare_failure_skips_apply_and_tx_but_runs_outer_hooks(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[tuple] = []
        reg = (
            OperationRegistry(
                handlers={"op": lambda c: PrepareFailsTwoPhase(ctx=c, order=order)}
            )
            .bind("op")
            .two_phase()
            .bind_outer()
            .on_failure(
                OnFailureStep(id="of", factory=_on_failure_factory(order, "on_failure"))
            )
            .finally_(FinallyStep(id="fin", factory=_finally_factory(order, "finally")))
            .finish(deep=False)
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        with pytest.raises(RuntimeError, match="prepare boom"):
            await reg.resolve("op", ctx)("x")

        # prepare ran outside the tx; apply never ran (no ("apply", ...) entry);
        # the tx never opened (prepare saw depth 0). Outer on_failure + finally fire.
        assert order == [("prepare", 0), ("on_failure",), ("finally:failure",)]


class TestTwoPhaseOrdering:
    @pytest.mark.asyncio
    async def test_outer_prepare_apply_after_commit_order(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[tuple] = []

        def _before_factory(_ctx):
            async def _before(_args) -> None:
                order.append(("outer_before",))

            return _before

        reg = (
            OperationRegistry(
                handlers={"op": lambda c: RecordingTwoPhase(ctx=c, order=order)}
            )
            .bind("op")
            .two_phase()
            .bind_outer()
            .before(BeforeStep(id="ob", factory=_before_factory))
            .on_success(
                OnSuccessStep(id="oos", factory=_on_success_factory(order, "outer_os"))
            )
            .finish(deep=False)
            .bind_tx()
            .set_route("mock")
            .after_commit(
                OnSuccessStep(id="ac", factory=_on_success_factory(order, "after_commit"))
            )
            .finish(deep=True)
            .freeze()
        )

        await reg.resolve("op", ctx)("x")

        names = [e[0] for e in order]
        assert names == [
            "outer_before",
            "prepare",
            "apply",
            "after_commit",
            "outer_os",
        ]


# ....................... #


class _Retry(Exception):
    pass


@attrs.define(slots=True, kw_only=True, frozen=True)
class CountingTwoPhase(TwoPhaseHandler[str, str, str]):
    """Counts prepare/apply calls; optionally fails the first apply (to drive a retry)."""

    counts: dict[str, int]
    fail_first_apply: bool = False

    async def prepare(self, args: str) -> str:
        self.counts["prepare"] += 1
        return f"p:{args}"

    async def apply(self, args: str, payload: str) -> str:
        self.counts["apply"] += 1
        if self.fail_first_apply and self.counts["apply"] == 1:
            raise _Retry
        return f"{payload}:applied"


def _retry_once_wrap(_ctx):
    async def _wrap(next, args):
        try:
            return await next(args)
        except _Retry:
            return await next(args)  # re-enter the body once

    return _wrap


def _hedge_two_wrap(_ctx):
    async def _wrap(next, args):
        # Two concurrent attempts (each its own copied context); first result wins.
        results = await asyncio.gather(
            next(args), next(args), return_exceptions=True
        )
        for r in results:
            if not isinstance(r, BaseException):
                return r
        raise results[0]  # pragma: no cover

    return _wrap


class TestPrepareExactlyOnce:
    """prepare runs exactly once per invocation, even when the body re-runs."""

    @pytest.mark.asyncio
    async def test_retry_reuses_prepare(self, ctx: ExecutionContext) -> None:
        counts = {"prepare": 0, "apply": 0}
        reg = (
            OperationRegistry(
                handlers={
                    "op": lambda _c: CountingTwoPhase(
                        counts=counts, fail_first_apply=True
                    )
                }
            )
            .bind("op")
            .two_phase()
            .bind_outer()
            .wrap(MiddlewareStep(id="retry", factory=_retry_once_wrap))
            .finish(deep=False)
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        result = await reg.resolve("op", ctx)("x")

        assert result == "p:x:applied"
        # apply ran twice (failed then succeeded); prepare ran ONCE.
        assert counts == {"prepare": 1, "apply": 2}

    @pytest.mark.asyncio
    async def test_concurrent_hedge_runs_prepare_once(
        self, ctx: ExecutionContext
    ) -> None:
        counts = {"prepare": 0, "apply": 0}
        reg = (
            OperationRegistry(
                handlers={"op": lambda _c: CountingTwoPhase(counts=counts)}
            )
            .bind("op")
            .two_phase()
            .bind_outer()
            .wrap(MiddlewareStep(id="hedge", factory=_hedge_two_wrap))
            .finish(deep=False)
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        result = await reg.resolve("op", ctx)("x")

        assert result == "p:x:applied"
        # Two concurrent attempts each applied, but the shared once-box ran
        # prepare a single time.
        assert counts["prepare"] == 1
        assert counts["apply"] == 2
