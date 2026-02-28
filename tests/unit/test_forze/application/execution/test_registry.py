"""Unit tests for forze.application.execution.registry."""

import pytest

from forze.application.execution import ExecutionContext, Usecase, UsecaseRegistry

# ----------------------- #


class StubUsecase(Usecase[str, str]):
    """Minimal usecase for registry tests."""

    async def main(self, args: str) -> str:
        return f"ok:{args}"


class TestUsecaseRegistry:
    """Tests for UsecaseRegistry."""

    def test_register_returns_new_instance(self) -> None:
        reg = UsecaseRegistry()
        new = reg.register("get", lambda ctx: StubUsecase())
        assert new is not reg
        assert new.exists("get")
        assert not reg.exists("get")

    def test_register_inplace_mutates(self) -> None:
        reg = UsecaseRegistry()
        reg.register("get", lambda ctx: StubUsecase(), inplace=True)
        assert reg.exists("get")

    def test_register_duplicate_raises(self) -> None:
        from forze.base.errors import CoreError

        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase())
        with pytest.raises(CoreError, match="already registered"):
            reg.register("get", lambda ctx: StubUsecase())

    def test_override_replaces_factory(self) -> None:
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase())
        new = reg.override("get", lambda ctx: StubUsecase())
        assert new is not reg
        assert new.exists("get")

    def test_override_unregistered_raises(self) -> None:
        from forze.base.errors import CoreError

        reg = UsecaseRegistry()
        with pytest.raises(CoreError, match="not registered"):
            reg.override("get", lambda ctx: StubUsecase())

    def test_register_many_adds_multiple(self) -> None:
        reg = UsecaseRegistry()
        new = reg.register_many(
            {
                "get": lambda ctx: StubUsecase(),
                "create": lambda ctx: StubUsecase(),
            }
        )
        assert new.exists("get")
        assert new.exists("create")

    def test_override_many_replaces_multiple(self) -> None:
        reg = (
            UsecaseRegistry()
            .register("get", lambda ctx: StubUsecase())
            .register("create", lambda ctx: StubUsecase())
        )
        new = reg.override_many(
            {
                "get": lambda ctx: StubUsecase(),
                "create": lambda ctx: StubUsecase(),
            }
        )
        assert new.exists("get")
        assert new.exists("create")

    def test_exists_returns_true_for_registered(self) -> None:
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase())
        assert reg.exists("get")

    def test_exists_returns_false_for_unregistered(self) -> None:
        reg = UsecaseRegistry()
        assert not reg.exists("get")

    def test_resolve_returns_usecase(self) -> None:
        from forze.application.execution import Deps

        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase())
        ctx = ExecutionContext(deps=Deps())
        uc = reg.resolve("get", ctx)
        assert isinstance(uc, StubUsecase)

    def test_resolve_unregistered_raises(self) -> None:
        from forze.application.execution import Deps
        from forze.base.errors import CoreError

        reg = UsecaseRegistry()
        ctx = ExecutionContext(deps=Deps())
        with pytest.raises(CoreError, match="not registered"):
            reg.resolve("get", ctx)
