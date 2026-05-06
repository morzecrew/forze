"""Unit tests for forze.application.execution.middleware."""

import pytest

from forze.application.execution import ExecutionContext, Usecase
from forze.application.execution.middleware import (
    ConditionalEffect,
    ConditionalGuard,
    EffectMiddleware,
    Failed,
    FinallyMiddleware,
    GuardMiddleware,
    OnFailureMiddleware,
    Successful,
    TxMiddleware,
    WhenEffect,
    WhenGuard,
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


class _SampleConditionalGuard(ConditionalGuard[str]):
    def __init__(self) -> None:
        self.main_ran = False

    def condition(self, args: str) -> bool:
        return args == "yes"

    async def main(self, args: str) -> None:
        self.main_ran = True


class _SampleConditionalEffect(ConditionalEffect[str, int]):
    def condition(self, args: str, res: int) -> bool:
        return res < 10

    async def main(self, args: str, res: int) -> int:
        return res * 2


class TestConditionalGuard:
    """Tests for :class:`ConditionalGuard`."""

    @pytest.mark.asyncio
    async def test_skips_main_when_condition_false(self) -> None:
        g = _SampleConditionalGuard()
        await g("no")
        assert g.main_ran is False

    @pytest.mark.asyncio
    async def test_runs_main_when_condition_true(self) -> None:
        g = _SampleConditionalGuard()
        await g("yes")
        assert g.main_ran is True

    @pytest.mark.asyncio
    async def test_main_may_raise(self) -> None:
        class _Raiser(ConditionalGuard[str]):
            def condition(self, args: str) -> bool:
                return True

            async def main(self, args: str) -> None:
                raise ValueError("nope")

        with pytest.raises(ValueError, match="nope"):
            await _Raiser()("x")


class TestWhenGuard:
    """Tests for :class:`WhenGuard`."""

    @pytest.mark.asyncio
    async def test_skips_inner_when_predicate_false(self) -> None:
        seen: list[str] = []

        async def inner(args: str) -> None:
            seen.append("inner")

        g = WhenGuard(guard=inner, when=lambda a: a == "go")
        await g("stop")
        assert seen == []

    @pytest.mark.asyncio
    async def test_invokes_inner_when_predicate_true(self) -> None:
        seen: list[str] = []

        async def inner(args: str) -> None:
            seen.append(args)

        g = WhenGuard(guard=inner, when=lambda a: a == "go")
        await g("go")
        assert seen == ["go"]

    @pytest.mark.asyncio
    async def test_works_inside_guard_middleware(self) -> None:
        seen: list[str] = []

        async def inner_guard(args: str) -> None:
            seen.append(f"guard:{args}")

        wrapped = WhenGuard(guard=inner_guard, when=lambda a: len(a) > 1)

        async def next_fn(args: str) -> str:
            return "ok"

        mw = GuardMiddleware(guard=wrapped)
        assert await mw(next_fn, "ab") == "ok"
        assert seen == ["guard:ab"]

        seen.clear()
        assert await mw(next_fn, "x") == "ok"
        assert seen == []


class TestConditionalEffect:
    """Tests for :class:`ConditionalEffect`."""

    @pytest.mark.asyncio
    async def test_skips_main_returns_res_when_condition_false(self) -> None:
        e = _SampleConditionalEffect()
        out = await e("x", 20)
        assert out == 20

    @pytest.mark.asyncio
    async def test_runs_main_when_condition_true(self) -> None:
        e = _SampleConditionalEffect()
        out = await e("x", 3)
        assert out == 6


class TestWhenEffect:
    """Tests for :class:`WhenEffect`."""

    @pytest.mark.asyncio
    async def test_skips_effect_returns_res_when_false(self) -> None:
        async def inner(args: str, res: int) -> int:
            return res + 100

        e = WhenEffect(effect=inner, when=lambda a, r: False)
        assert await e("a", 5) == 5

    @pytest.mark.asyncio
    async def test_invokes_effect_when_true(self) -> None:
        async def inner(args: str, res: int) -> int:
            return res + len(args)

        e = WhenEffect(effect=inner, when=lambda a, r: r < 10)
        assert await e("hi", 3) == 5

    @pytest.mark.asyncio
    async def test_works_inside_effect_middleware(self) -> None:
        async def inner(args: str, res: str) -> str:
            return f"{res}!"

        wrapped = WhenEffect(
            effect=inner, when=lambda a, r: a.startswith("x")
        )

        async def next_fn(args: str) -> str:
            return "done"

        mw = EffectMiddleware(effect=wrapped)
        assert await mw(next_fn, "x1") == "done!"
        assert await mw(next_fn, "y") == "done"
