"""Unit tests for forze.application.execution.facade."""

from forze.application.execution import (
    ExecutionContext,
    Usecase,
    UsecaseRegistry,
    UsecasesFacade,
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
        reg.finalize("test")
        facade = MinimalFacade(ctx=stub_ctx, reg=reg)
        uc = facade.resolve("get")
        assert uc is not None
        assert isinstance(uc, StubUsecase)


class TestFacadeOp:
    """Tests for facade_op descriptor."""

    def test_get_returns_usecase_on_instance(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        """Accessing facade_op on instance returns resolved usecase."""
        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        reg.finalize("test")
        facade = MinimalFacade(ctx=stub_ctx, reg=reg)
        uc = facade.get
        assert uc is not None
        assert isinstance(uc, StubUsecase)
