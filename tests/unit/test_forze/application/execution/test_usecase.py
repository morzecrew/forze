"""Unit tests for forze.application.execution.usecase."""

import pytest

from forze.application.execution import Deps, ExecutionContext, Usecase
from forze.application.execution.middleware import EffectMiddleware, GuardMiddleware

# ----------------------- #


class ConcreteUsecase(Usecase[str, str]):
    """Concrete usecase for testing."""

    async def main(self, args: str) -> str:
        return f"result:{args}"


class TestUsecase:
    """Tests for Usecase base class."""

    @pytest.fixture
    def stub_ctx(self) -> ExecutionContext:
        return ExecutionContext(deps=Deps())

    @pytest.mark.asyncio
    async def test_call_invokes_main(self, stub_ctx: ExecutionContext) -> None:
        uc = ConcreteUsecase(ctx=stub_ctx)
        result = await uc("foo")
        assert result == "result:foo"

    @pytest.mark.asyncio
    async def test_with_middlewares_runs_guard_before_main(
        self, stub_ctx: ExecutionContext
    ) -> None:
        seen: list[str] = []

        async def guard(args: str) -> None:
            seen.append(f"guard:{args}")

        uc = ConcreteUsecase(ctx=stub_ctx).with_middlewares(
            GuardMiddleware(guard=guard)
        )
        result = await uc("x")
        assert seen == ["guard:x"]
        assert result == "result:x"

    @pytest.mark.asyncio
    async def test_with_middlewares_runs_effect_after_main(
        self, stub_ctx: ExecutionContext
    ) -> None:
        seen: list[str] = []

        async def effect(args: str, res: str) -> str:
            seen.append(f"effect:{args}:{res}")
            return res.upper()

        uc = ConcreteUsecase(ctx=stub_ctx).with_middlewares(
            EffectMiddleware(effect=effect)
        )
        result = await uc("x")
        assert seen == ["effect:x:result:x"]
        assert result == "RESULT:X"
