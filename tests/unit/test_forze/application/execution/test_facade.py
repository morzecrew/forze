"""Unit tests for forze.application.execution.facade."""

import pytest

from forze.application.execution import (
    ExecutionContext,
    Usecase,
    UsecaseRegistry,
    UsecasesFacade,
    build_usecases_facade,
    facade_op,
)

# ----------------------- #


class StubUsecase(Usecase[str, str]):
    """Minimal usecase for facade tests."""

    async def main(self, args: str) -> str:
        return f"ok:{args}"


class MinimalFacade(UsecasesFacade):
    """Facade with a single operation."""

    get = facade_op("get", uc=StubUsecase)


class TestUsecasesFacade:
    """Tests for UsecasesFacade."""

    def test_resolve_returns_usecase(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        """resolve returns usecase from registry."""
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        facade = MinimalFacade(ctx=stub_ctx, reg=reg)
        uc = facade.resolve("get")
        assert uc is not None
        assert isinstance(uc, StubUsecase)

    def test_declared_ops_returns_facade_operations(self) -> None:
        """declared_ops returns all facade_op keys."""
        ops = MinimalFacade.declared_ops()
        assert ops == {"get"}

    def test_validate_registry_passes_when_all_ops_registered(self) -> None:
        """validate_registry does not raise when all ops exist."""
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        MinimalFacade.validate_registry(reg)

    def test_validate_registry_raises_when_op_missing(self) -> None:
        """validate_registry raises CoreError when operation is missing."""
        from forze.base.errors import CoreError

        reg = UsecaseRegistry()
        with pytest.raises(CoreError, match="requires missing operations"):
            MinimalFacade.validate_registry(reg)


class TestFacadeOp:
    """Tests for facade_op descriptor."""

    def test_get_returns_usecase_on_instance(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        """Accessing facade_op on instance returns resolved usecase."""
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        facade = MinimalFacade(ctx=stub_ctx, reg=reg)
        uc = facade.get
        assert uc is not None
        assert isinstance(uc, StubUsecase)

    def test_get_on_class_raises(self) -> None:
        """Accessing facade_op on class raises AttributeError."""
        with pytest.raises(AttributeError, match="available only on facade instances"):
            _ = MinimalFacade.get


class TestBuildUsecasesFacade:
    """Tests for build_usecases_facade."""

    def test_returns_facade_instance(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        """build_usecases_facade returns configured facade."""
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        facade = build_usecases_facade(MinimalFacade, reg, stub_ctx)
        assert isinstance(facade, MinimalFacade)
        assert facade.ctx is stub_ctx
        assert facade.reg is reg

    def test_validate_true_raises_when_missing_op(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        """build_usecases_facade with validate=True raises when registry incomplete."""
        from forze.base.errors import CoreError

        reg = UsecaseRegistry()
        with pytest.raises(CoreError, match="requires missing operations"):
            build_usecases_facade(MinimalFacade, reg, stub_ctx)

    def test_validate_false_skips_validation(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        """build_usecases_facade with validate=False skips registry check."""
        from forze.base.errors import CoreError

        reg = UsecaseRegistry()
        facade = build_usecases_facade(MinimalFacade, reg, stub_ctx, validate=False)
        assert isinstance(facade, MinimalFacade)
        with pytest.raises(CoreError, match="not registered"):
            _ = facade.get
