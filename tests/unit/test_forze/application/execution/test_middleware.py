"""Unit tests for forze.application.execution.middleware."""

import pytest

from forze.application.execution import ExecutionContext, Usecase
from forze.application.execution.middleware import (
    EffectMiddleware,
    Failed,
    FinallyMiddleware,
    GuardMiddleware,
    OnFailureMiddleware,
    Successful,
    TxMiddleware,
)

# ----------------------- #


class StubUsecase(Usecase[str, str]):
    """Minimal usecase for middleware tests."""

    async def main(self, args: str) -> str:
        return f"result:{args}"


class TestGuardMiddleware:
    """Tests for GuardMiddleware."""

    @pytest.mark.asyncio
    async def test_guard_runs_before_next(self) -> None:
        seen: list[str] = []

        async def guard(args: str) -> None:
            seen.append(f"guard:{args}")

        async def next_fn(args: str) -> str:
            seen.append("next")
            return "done"

        mw = GuardMiddleware(guard=guard)
        result = await mw(next_fn, "x")
        assert seen == ["guard:x", "next"]
        assert result == "done"

    @pytest.mark.asyncio
    async def test_guard_raise_aborts_chain(self) -> None:
        async def guard(args: str) -> None:
            raise ValueError("abort")

        async def next_fn(args: str) -> str:
            return "never"

        mw = GuardMiddleware(guard=guard)
        with pytest.raises(ValueError, match="abort"):
            await mw(next_fn, "x")


class TestEffectMiddleware:
    """Tests for EffectMiddleware."""

    @pytest.mark.asyncio
    async def test_effect_runs_after_next(self) -> None:
        seen: list[str] = []

        async def effect(args: str, res: str) -> str:
            seen.append(f"effect:{args}:{res}")
            return res.upper()

        async def next_fn(args: str) -> str:
            seen.append("next")
            return "done"

        mw = EffectMiddleware(effect=effect)
        result = await mw(next_fn, "x")
        assert seen == ["next", "effect:x:done"]
        assert result == "DONE"

    @pytest.mark.asyncio
    async def test_effect_can_transform_result(self) -> None:
        async def effect(args: str, res: str) -> str:
            return f"wrapped:{res}"

        async def next_fn(args: str) -> str:
            return "inner"

        mw = EffectMiddleware(effect=effect)
        result = await mw(next_fn, "x")
        assert result == "wrapped:inner"


class TestTxMiddleware:
    """Tests for TxMiddleware."""

    @pytest.mark.asyncio
    async def test_invokes_next_inside_transaction_scope(
        self, stub_ctx: ExecutionContext
    ) -> None:
        async def next_fn(args: str) -> str:
            return "ok"

        mw = TxMiddleware(ctx=stub_ctx, route="mock")
        result = await mw(next_fn, "x")
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_with_after_commit_appends_effects(
        self, stub_ctx: ExecutionContext
    ) -> None:
        seen: list[str] = []

        async def after_commit(args: str, res: str) -> str:
            seen.append(f"commit:{res}")
            return res

        mw = TxMiddleware(ctx=stub_ctx, route="mock").with_after_commit(after_commit)

        async def next_fn(args: str) -> str:
            return "result"

        result = await mw(next_fn, "x")
        assert result == "result"
        assert seen == ["commit:result"]


class TestOnFailureMiddleware:
    """Tests for OnFailureMiddleware."""

    @pytest.mark.asyncio
    async def test_runs_hook_on_exception_then_reraises(self) -> None:
        seen: list[str] = []

        async def hook(args: str, exc: Exception) -> None:
            seen.append(f"fail:{args}:{type(exc).__name__}")

        async def next_fn(args: str) -> str:
            raise ValueError("boom")

        mw = OnFailureMiddleware(hook=hook)
        with pytest.raises(ValueError, match="boom"):
            await mw(next_fn, "x")

        assert seen == ["fail:x:ValueError"]

    @pytest.mark.asyncio
    async def test_skipped_on_success(self) -> None:
        async def hook(args: str, exc: Exception) -> None:
            raise AssertionError("should not run")

        async def next_fn(args: str) -> str:
            return "ok"

        mw = OnFailureMiddleware(hook=hook)
        assert await mw(next_fn, "x") == "ok"


class TestFinallyMiddleware:
    """Tests for FinallyMiddleware."""

    @pytest.mark.asyncio
    async def test_success_path_passes_successful(self) -> None:
        seen: list[str] = []

        async def hook(args: str, outcome: Successful[str] | Failed) -> None:
            assert isinstance(outcome, Successful)
            seen.append(f"ok:{outcome.value}")

        async def next_fn(args: str) -> str:
            return "inner"

        mw = FinallyMiddleware(hook=hook)
        assert await mw(next_fn, "a") == "inner"
        assert seen == ["ok:inner"]

    @pytest.mark.asyncio
    async def test_failure_path_passes_failed(self) -> None:
        seen: list[str] = []

        async def hook(args: str, outcome: Successful[str] | Failed) -> None:
            assert isinstance(outcome, Failed)
            seen.append(f"err:{type(outcome.exc).__name__}")

        async def next_fn(args: str) -> str:
            raise RuntimeError("x")

        mw = FinallyMiddleware(hook=hook)
        with pytest.raises(RuntimeError, match="x"):
            await mw(next_fn, "a")

        assert seen == ["err:RuntimeError"]
