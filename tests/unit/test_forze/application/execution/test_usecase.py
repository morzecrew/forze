"""Unit tests for forze.application.execution.usecase."""

import pytest

from forze.application.execution import Usecase

# ----------------------- #


class ConcreteUsecase(Usecase[str, str]):
    """Concrete usecase for testing."""

    async def main(self, args: str) -> str:
        return f"result:{args}"


class TestUsecase:
    """Tests for Usecase base class."""

    @pytest.mark.asyncio
    async def test_call_invokes_main(self) -> None:
        uc = ConcreteUsecase()
        result = await uc("foo")
        assert result == "result:foo"

    @pytest.mark.asyncio
    async def test_with_guards_runs_guard_before_main(self) -> None:
        seen: list[str] = []

        async def guard(args: str) -> None:
            seen.append(f"guard:{args}")

        uc = ConcreteUsecase().with_guards(guard)
        result = await uc("x")
        assert seen == ["guard:x"]
        assert result == "result:x"

    @pytest.mark.asyncio
    async def test_with_effects_runs_effect_after_main(self) -> None:
        seen: list[str] = []

        async def effect(args: str, res: str) -> str:
            seen.append(f"effect:{args}:{res}")
            return res.upper()

        uc = ConcreteUsecase().with_effects(effect)
        result = await uc("x")
        assert seen == ["effect:x:result:x"]
        assert result == "RESULT:X"
