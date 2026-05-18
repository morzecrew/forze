"""Unit tests for :mod:`forze.application.execution.usecase`."""

import pytest

from forze.application.execution import Deps, ExecutionContext, Usecase
from forze.application.execution.middlewares import GuardMiddleware, OnSuccessMiddleware


class ConcreteUsecase(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return f"result:{args}"


class TestUsecase:
    @pytest.fixture
    def stub_ctx(self) -> ExecutionContext:
        return ExecutionContext(deps=Deps())

    @pytest.mark.asyncio
    async def test_call_invokes_main(self, stub_ctx: ExecutionContext) -> None:
        uc = ConcreteUsecase(ctx=stub_ctx)
        assert await uc("foo") == "result:foo"

    @pytest.mark.asyncio
    async def test_with_middlewares_runs_guard_before_main(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        seen: list[str] = []

        async def guard(args: str) -> None:
            seen.append(f"guard:{args}")

        uc = ConcreteUsecase(ctx=stub_ctx).with_middlewares(
            GuardMiddleware(inner=guard)
        )

        assert await uc("x") == "result:x"
        assert seen == ["guard:x"]

    @pytest.mark.asyncio
    async def test_with_middlewares_runs_success_hook_after_main_without_replacing_result(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        seen: list[str] = []

        async def hook(args: str, result: str) -> None:
            seen.append(f"hook:{args}:{result}")
            return None

        uc = ConcreteUsecase(ctx=stub_ctx).with_middlewares(
            OnSuccessMiddleware(inner=hook)
        )

        assert await uc("x") == "result:x"
        assert seen == ["hook:x:result:x"]
