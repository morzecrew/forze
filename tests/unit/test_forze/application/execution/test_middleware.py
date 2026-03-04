"""Unit tests for forze.application.execution.middleware."""

import pytest

from forze.application.execution import Deps, ExecutionContext, Usecase
from forze.application.execution.middleware import (
    EffectMiddleware,
    GuardMiddleware,
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

        mw = TxMiddleware(ctx=stub_ctx)
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

        mw = TxMiddleware(ctx=stub_ctx).with_after_commit(after_commit)

        async def next_fn(args: str) -> str:
            return "result"

        result = await mw(next_fn, "x")
        assert result == "result"
        assert seen == ["commit:result"]
