"""Tests for the engine-level idempotency wrap hook."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.contracts.idempotency import IdempotencySpec
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.hooks.idempotency import IdempotencyWrap
from forze.base.exceptions import CoreException, ExceptionKind
from tests.support.execution_context import context_from_modules

from forze_mock import MockDepsModule

# ----------------------- #

_SPEC = IdempotencySpec(name="idem")


class _Args(BaseModel):
    n: int


class _Result(BaseModel):
    value: int


def _ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


# ....................... #


class TestIdempotencyWrapDirect:
    async def test_no_key_is_passthrough(self) -> None:
        ctx = _ctx()
        mw = IdempotencyWrap(op="op", spec=_SPEC, result_type=_Result)(ctx)
        calls = 0

        async def handler(args: _Args) -> _Result:
            nonlocal calls
            calls += 1
            return _Result(value=args.n)

        # No idempotency key bound -> wrap is a no-op.
        res = await mw(handler, _Args(n=5))
        assert res.value == 5
        assert calls == 1

    async def test_store_then_typed_replay(self) -> None:
        ctx = _ctx()
        mw = IdempotencyWrap(op="op", spec=_SPEC, result_type=_Result)(ctx)
        calls = 0

        async def handler(args: _Args) -> _Result:
            nonlocal calls
            calls += 1
            return _Result(value=args.n)

        with ctx.inv_ctx.bind_idempotency("key-1"):
            first = await mw(handler, _Args(n=7))
            second = await mw(handler, _Args(n=7))

        assert calls == 1  # handler ran only once
        assert first.value == 7
        assert isinstance(second, _Result)  # replay is decoded to the typed result
        assert second.value == 7

    async def test_same_key_different_args_conflicts(self) -> None:
        ctx = _ctx()
        mw = IdempotencyWrap(op="op", spec=_SPEC, result_type=_Result)(ctx)

        async def handler(args: _Args) -> _Result:
            return _Result(value=args.n)

        with ctx.inv_ctx.bind_idempotency("key-1"):
            await mw(handler, _Args(n=7))

            with pytest.raises(CoreException) as ei:
                await mw(handler, _Args(n=99))

        assert ei.value.kind is ExceptionKind.CONFLICT

    def test_non_model_result_type_is_config_error(self) -> None:
        ctx = _ctx()

        with pytest.raises(CoreException) as ei:
            IdempotencyWrap(op="op", spec=_SPEC, result_type=int)(ctx)

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    async def test_in_progress_duplicate_conflicts(self) -> None:
        ctx = _ctx()
        port = ctx.idempotency(_SPEC)

        assert await port.begin("op", "k", "h") is None  # fresh claim -> pending

        with pytest.raises(CoreException):  # still pending -> in progress
            await port.begin("op", "k", "h")

    async def test_handler_failure_releases_claim_so_retry_reexecutes(self) -> None:
        ctx = _ctx()
        mw = IdempotencyWrap(op="op", spec=_SPEC, result_type=_Result)(ctx)
        calls = 0

        async def handler(args: _Args) -> _Result:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("handler boom")
            return _Result(value=args.n)

        with ctx.inv_ctx.bind_idempotency("key-fail"):
            with pytest.raises(RuntimeError, match="handler boom"):
                await mw(handler, _Args(n=4))

            # Retry of the failed request re-executes (no stuck pending claim).
            result = await mw(handler, _Args(n=4))

        assert calls == 2
        assert result.value == 4

    async def test_fail_error_does_not_mask_handler_error(self) -> None:
        from unittest.mock import AsyncMock, patch

        from forze_mock.adapters.idempotency import MockIdempotencyAdapter

        ctx = _ctx()
        mw = IdempotencyWrap(op="op", spec=_SPEC, result_type=_Result)(ctx)

        async def handler(args: _Args) -> _Result:
            raise RuntimeError("handler boom")

        with patch.object(
            MockIdempotencyAdapter,
            "fail",
            AsyncMock(side_effect=RuntimeError("fail() broke")),
        ):
            with ctx.inv_ctx.bind_idempotency("key-mask"):
                with pytest.raises(RuntimeError, match="handler boom"):
                    await mw(handler, _Args(n=1))


# ....................... #


class TestIdempotencyWrapInRegistry:
    async def test_replay_skips_handler(self) -> None:
        ctx = _ctx()

        class _H(Handler[_Args, _Result]):
            def __init__(self) -> None:
                self.calls = 0

            async def __call__(self, args: _Args) -> _Result:
                self.calls += 1
                return _Result(value=args.n)

        handler = _H()
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: handler})
            .bind("op")
            .bind_outer()
            .wrap(
                IdempotencyWrap(op="op", spec=_SPEC, result_type=_Result).to_step(),
            )
            .finish(deep=True)
            .freeze()
        )

        with ctx.inv_ctx.bind_idempotency("key-9"):
            r1 = await reg.resolve("op", ctx)(_Args(n=3))
            r2 = await reg.resolve("op", ctx)(_Args(n=3))

        assert handler.calls == 1  # second invocation replayed
        assert r1.value == 3
        assert r2.value == 3


# ....................... #


class TestInvocationIdempotencyKey:
    def test_bind_and_get(self) -> None:
        ctx = _ctx()

        assert ctx.inv_ctx.get_idempotency_key() is None

        with ctx.inv_ctx.bind_idempotency("abc"):
            assert ctx.inv_ctx.get_idempotency_key() == "abc"

        assert ctx.inv_ctx.get_idempotency_key() is None
