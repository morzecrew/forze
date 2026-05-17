"""Unit tests for :mod:`forze.application.execution.middleware`."""

import pytest

from forze.application.execution import ExecutionContext
from forze.application.execution.middleware import (
    ConditionalGuard,
    ConditionalSuccessHook,
    Failed,
    FinallyMiddleware,
    GuardMiddleware,
    OnFailureMiddleware,
    Skip,
    Successful,
    SuccessHookMiddleware,
    TxMiddleware,
    WhenGuard,
    WhenSuccessHook,
    mapped_success_hook,
)
from forze.base.errors import CoreError


class TestGuardMiddleware:
    @pytest.mark.asyncio
    async def test_guard_runs_before_next(self) -> None:
        seen: list[str] = []

        async def guard(args: str) -> None:
            seen.append(f"guard:{args}")

        async def next_fn(args: str) -> str:
            seen.append("next")
            return "done"

        result = await GuardMiddleware(guard=guard)(next_fn, "x")

        assert result == "done"
        assert seen == ["guard:x", "next"]

    @pytest.mark.asyncio
    async def test_guard_invalid_return_raises(self) -> None:
        async def guard(_args: str) -> object:
            return "bad"

        async def next_fn(_args: str) -> str:
            return "never"

        with pytest.raises(CoreError, match="Guard must return None or Skip"):
            await GuardMiddleware(guard=guard)(next_fn, "x")


class TestSuccessHookMiddleware:
    @pytest.mark.asyncio
    async def test_success_hook_runs_after_next_and_preserves_result(self) -> None:
        seen: list[str] = []

        async def hook(args: str, result: str) -> None:
            seen.append(f"hook:{args}:{result}")
            return None

        async def next_fn(_args: str) -> str:
            seen.append("next")
            return "done"

        out = await SuccessHookMiddleware(hook=hook)(next_fn, "x")

        assert out == "done"
        assert seen == ["next", "hook:x:done"]

    @pytest.mark.asyncio
    async def test_success_hook_skip_still_preserves_result(self) -> None:
        async def hook(_args: str, _result: str) -> Skip:
            return Skip(reason="noop")

        async def next_fn(_args: str) -> str:
            return "done"

        assert await SuccessHookMiddleware(hook=hook)(next_fn, "x") == "done"

    @pytest.mark.asyncio
    async def test_success_hook_invalid_return_raises(self) -> None:
        async def hook(_args: str, _result: str) -> object:
            return "bad"

        async def next_fn(_args: str) -> str:
            return "done"

        with pytest.raises(CoreError, match="Success hook must return None or Skip"):
            await SuccessHookMiddleware(hook=hook)(next_fn, "x")


class TestFailureAndFinallyMiddleware:
    @pytest.mark.asyncio
    async def test_on_failure_observes_exception_and_reraises(self) -> None:
        seen: list[str] = []

        async def hook(args: str, exc: Exception) -> None:
            seen.append(f"{args}:{exc}")

        async def next_fn(_args: str) -> str:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await OnFailureMiddleware(hook=hook)(next_fn, "x")

        assert seen == ["x:boom"]

    @pytest.mark.asyncio
    async def test_finally_observes_success_and_failure(self) -> None:
        seen: list[object] = []

        async def hook(_args: str, outcome: Successful[str] | Failed) -> None:
            seen.append(outcome)

        async def ok(_args: str) -> str:
            return "done"

        async def boom(_args: str) -> str:
            raise ValueError("boom")

        mw = FinallyMiddleware[str, str](hook=hook)

        assert await mw(ok, "x") == "done"
        assert isinstance(seen[0], Successful)
        assert seen[0].value == "done"

        with pytest.raises(ValueError, match="boom"):
            await mw(boom, "y")

        assert isinstance(seen[1], Failed)
        assert str(seen[1].exc) == "boom"


class TestTxMiddleware:
    @pytest.mark.asyncio
    async def test_invokes_next_inside_transaction_scope(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        async def next_fn(_args: str) -> str:
            return "ok"

        assert await TxMiddleware(ctx=stub_ctx, route="mock")(next_fn, "x") == "ok"

    @pytest.mark.asyncio
    async def test_with_after_commit_runs_hooks_and_preserves_result(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        seen: list[str] = []

        async def first(args: str, result: str) -> None:
            seen.append(f"first:{args}:{result}")
            return None

        async def second(_args: str, _result: str) -> Skip:
            seen.append("second")
            return Skip(reason="skip")

        mw = TxMiddleware(ctx=stub_ctx, route="mock").with_after_commit(first, second)

        async def next_fn(_args: str) -> str:
            return "result"

        assert await mw(next_fn, "x") == "result"
        assert seen == ["first:x:result", "second"]

    @pytest.mark.asyncio
    async def test_after_commit_invalid_return_raises(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        async def bad(_args: str, _result: str) -> object:
            return "bad"

        mw = TxMiddleware(ctx=stub_ctx, route="mock").with_after_commit(bad)

        async def next_fn(_args: str) -> str:
            return "result"

        with pytest.raises(CoreError, match="After-commit hook must return None or Skip"):
            await mw(next_fn, "x")


class TestMappedAndConditionalHooks:
    @pytest.mark.asyncio
    async def test_mapped_success_hook_maps_args_and_result(self) -> None:
        touched: list[tuple[int, int]] = []

        async def inner(args: int, result: int) -> None:
            touched.append((args, result))
            return None

        outer = mapped_success_hook(
            inner,
            args_mapper=lambda s: len(s),
            result_mapper=lambda _s, result: result + 1,
        )

        assert await outer("ab", 10) is None
        assert touched == [(2, 11)]

    @pytest.mark.asyncio
    async def test_conditional_guard_and_when_guard(self) -> None:
        seen: list[str] = []

        class SampleGuard(ConditionalGuard[str]):
            def condition(self, args: str) -> bool:
                return args == "run"

            async def main(self, args: str) -> None:
                seen.append(args)

        await SampleGuard()("skip")
        await SampleGuard()("run")

        async def inner(args: str) -> None:
            seen.append(f"when:{args}")

        await WhenGuard(guard=inner, when=lambda args: args == "go")("stop")
        await WhenGuard(guard=inner, when=lambda args: args == "go")("go")

        assert seen == ["run", "when:go"]

    @pytest.mark.asyncio
    async def test_conditional_success_hook_and_when_success_hook(self) -> None:
        seen: list[str] = []

        class SampleHook(ConditionalSuccessHook[str, int]):
            def condition(self, _args: str, result: int) -> bool:
                return result > 0

            async def main(self, args: str, result: int) -> None:
                seen.append(f"{args}:{result}")

        await SampleHook()("x", -1)
        await SampleHook()("x", 2)

        async def inner(args: str, result: int) -> None:
            seen.append(f"when:{args}:{result}")

        await WhenSuccessHook(hook=inner, when=lambda _a, r: r == 3)("y", 1)
        await WhenSuccessHook(hook=inner, when=lambda _a, r: r == 3)("y", 3)

        assert seen == ["x:2", "when:y:3"]
