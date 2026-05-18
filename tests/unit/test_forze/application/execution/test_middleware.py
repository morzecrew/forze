"""Unit tests for :mod:`forze.application.execution.middlewares`."""

from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest

from forze.application.execution.middlewares import (
    ConditionalGuard,
    ConditionalOnSuccess,
    Failure,
    FinallyMiddleware,
    GuardMiddleware,
    OnFailureMiddleware,
    OnSuccessMiddleware,
    Skip,
    Success,
    TxMiddleware,
)
from forze.base.errors import CoreError
from forze.base.primitives import StrKey


@asynccontextmanager
async def _noop_transaction(_route: StrKey) -> AsyncIterator[None]:
    yield


class TestGuardMiddleware:
    @pytest.mark.asyncio
    async def test_guard_runs_before_next(self) -> None:
        seen: list[str] = []

        async def guard(args: str) -> None:
            seen.append(f"guard:{args}")

        async def next_fn(args: str) -> str:
            seen.append("next")
            return "done"

        result = await GuardMiddleware(inner=guard)(next_fn, "x")

        assert result == "done"
        assert seen == ["guard:x", "next"]

    @pytest.mark.asyncio
    async def test_guard_invalid_return_raises(self) -> None:
        async def guard(_args: str) -> object:
            return "bad"

        async def next_fn(_args: str) -> str:
            return "never"

        with pytest.raises(CoreError, match="Guard must return None or Skip"):
            await GuardMiddleware(inner=guard)(next_fn, "x")


class TestOnSuccessMiddleware:
    @pytest.mark.asyncio
    async def test_success_hook_runs_after_next_and_preserves_result(self) -> None:
        seen: list[str] = []

        async def hook(args: str, result: str) -> None:
            seen.append(f"hook:{args}:{result}")
            return None

        async def next_fn(_args: str) -> str:
            seen.append("next")
            return "done"

        out = await OnSuccessMiddleware(inner=hook)(next_fn, "x")

        assert out == "done"
        assert seen == ["next", "hook:x:done"]

    @pytest.mark.asyncio
    async def test_success_hook_skip_still_preserves_result(self) -> None:
        async def hook(_args: str, _result: str) -> Skip:
            return Skip(reason="noop")

        async def next_fn(_args: str) -> str:
            return "done"

        assert await OnSuccessMiddleware(inner=hook)(next_fn, "x") == "done"

    @pytest.mark.asyncio
    async def test_success_hook_invalid_return_raises(self) -> None:
        async def hook(_args: str, _result: str) -> object:
            return "bad"

        async def next_fn(_args: str) -> str:
            return "done"

        with pytest.raises(CoreError, match="Success hook must return None or Skip"):
            await OnSuccessMiddleware(inner=hook)(next_fn, "x")


class TestFailureAndFinallyMiddleware:
    @pytest.mark.asyncio
    async def test_on_failure_observes_exception_and_reraises(self) -> None:
        seen: list[str] = []

        async def hook(args: str, exc: Exception) -> None:
            seen.append(f"{args}:{exc}")

        async def next_fn(_args: str) -> str:
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            await OnFailureMiddleware(inner=hook)(next_fn, "x")

        assert seen == ["x:boom"]

    @pytest.mark.asyncio
    async def test_finally_observes_success_and_failure(self) -> None:
        seen: list[object] = []

        async def hook(_args: str, outcome: Success[str] | Failure) -> None:
            seen.append(outcome)

        async def ok(_args: str) -> str:
            return "done"

        async def boom(_args: str) -> str:
            raise ValueError("boom")

        mw = FinallyMiddleware[str, str](inner=hook)

        assert await mw(ok, "x") == "done"
        assert isinstance(seen[0], Success)
        assert seen[0].value == "done"

        with pytest.raises(ValueError, match="boom"):
            await mw(boom, "y")

        assert isinstance(seen[1], Failure)
        assert str(seen[1].exc) == "boom"


class TestTxMiddleware:
    @pytest.mark.asyncio
    async def test_invokes_next_inside_transaction_scope(self) -> None:
        async def next_fn(_args: str) -> str:
            return "ok"

        assert (
            await TxMiddleware(route="mock", runnable=_noop_transaction)(next_fn, "x")
            == "ok"
        )

    @pytest.mark.asyncio
    async def test_with_after_commit_runs_hooks_and_preserves_result(self) -> None:
        seen: list[str] = []

        async def first(args: str, result: str) -> None:
            seen.append(f"first:{args}:{result}")
            return None

        async def second(_args: str, _result: str) -> Skip:
            seen.append("second")
            return Skip(reason="skip")

        mw = TxMiddleware(
            route="mock",
            runnable=_noop_transaction,
        ).with_after_commit(first, second)

        async def next_fn(_args: str) -> str:
            return "result"

        assert await mw(next_fn, "x") == "result"
        assert seen == ["first:x:result", "second"]

    @pytest.mark.asyncio
    async def test_after_commit_invalid_return_raises(self) -> None:
        async def bad(_args: str, _result: str) -> object:
            return "bad"

        mw = TxMiddleware(
            route="mock",
            runnable=_noop_transaction,
        ).with_after_commit(bad)

        async def next_fn(_args: str) -> str:
            return "result"

        with pytest.raises(
            CoreError, match="After-commit hook must return None or Skip"
        ):
            await mw(next_fn, "x")


class TestConditionalHooks:
    @pytest.mark.asyncio
    async def test_mapped_success_hook_inline(self) -> None:
        touched: list[tuple[int, int]] = []

        async def inner(args: int, result: int) -> None:
            touched.append((args, result))
            return None

        async def outer(s: str, result: int) -> None:
            mapped_args = len(s)
            mapped_result = result + 1
            return await inner(mapped_args, mapped_result)

        assert await outer("ab", 10) is None
        assert touched == [(2, 11)]

    @pytest.mark.asyncio
    async def test_conditional_guard(self) -> None:
        seen: list[str] = []

        async def main(args: str) -> None:
            seen.append(args)

        guard = ConditionalGuard(inner=main, predicate=lambda a: a == "run")

        await guard("skip")
        await guard("run")

        async def inner(args: str) -> None:
            seen.append(f"when:{args}")

        when_guard = ConditionalGuard(inner=inner, predicate=lambda a: a == "go")
        await when_guard("stop")
        await when_guard("go")

        assert seen == ["run", "when:go"]

    @pytest.mark.asyncio
    async def test_conditional_on_success(self) -> None:
        seen: list[str] = []

        async def main(args: str, result: int) -> None:
            seen.append(f"{args}:{result}")

        hook = ConditionalOnSuccess(inner=main, predicate=lambda _a, r: r > 0)

        await hook("x", -1)
        await hook("x", 2)

        async def inner(args: str, result: int) -> None:
            seen.append(f"when:{args}:{result}")

        when_hook = ConditionalOnSuccess(
            inner=inner,
            predicate=lambda _a, r: r == 3,
        )
        await when_hook("y", 1)
        await when_hook("y", 3)

        assert seen == ["x:2", "when:y:3"]
