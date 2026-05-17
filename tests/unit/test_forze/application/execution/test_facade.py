"""Unit tests for forze.application.execution.facade."""

import pytest

from forze.application.execution import (
    ExecutionContext,
    FacadeOperationDescriptor,
    OperationNamespace,
    Usecase,
    UsecaseRegistry,
    UsecasesFacade,
)
from forze.base.errors import CoreError

# ----------------------- #


class StubUsecase(Usecase[str, str]):
    """Minimal usecase for facade tests."""

    async def main(self, args: str) -> str:
        return f"ok:{args}"


_OPS = OperationNamespace(prefix="test")


class MinimalFacade(UsecasesFacade):
    """Facade with a single operation."""

    require_namespace = True
    namespace: OperationNamespace
    get = FacadeOperationDescriptor("get", uc=StubUsecase)


class TestUsecasesFacade:
    """Tests for UsecasesFacade."""

    def test_resolve_returns_usecase(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        """resolve returns usecase from registry."""
        reg = UsecaseRegistry(namespace=_OPS).register(
            "get",
            lambda ctx: StubUsecase(ctx=ctx),
        )
        reg.finalize("test")
        facade = MinimalFacade(ctx=stub_ctx, registry=reg, namespace=_OPS)
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
        reg = UsecaseRegistry(namespace=_OPS).register(
            "get",
            lambda ctx: StubUsecase(ctx=ctx),
        )
        reg.finalize("test")
        facade = MinimalFacade(ctx=stub_ctx, registry=reg, namespace=_OPS)
        uc = facade.get
        assert uc is not None
        assert isinstance(uc, StubUsecase)

    def test_class_access_returns_descriptor(self) -> None:
        assert isinstance(MinimalFacade.get, FacadeOperationDescriptor)
        assert MinimalFacade.get.suffix == "get"

    def test_missing_namespace_for_descriptor_raises(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        class MissingNamespaceFacade(UsecasesFacade):
            get = FacadeOperationDescriptor("get", uc=StubUsecase)

        reg = UsecaseRegistry().register("get", lambda ctx: StubUsecase(ctx=ctx))
        reg.finalize("test")
        facade = MissingNamespaceFacade(ctx=stub_ctx, registry=reg)

        with pytest.raises(CoreError, match="requires an operation namespace"):
            facade.get

    def test_namespace_mismatch_raises(
        self,
        stub_ctx: ExecutionContext,
    ) -> None:
        reg = UsecaseRegistry(namespace="test").register(
            "get",
            lambda ctx: StubUsecase(ctx=ctx),
        )

        with pytest.raises(CoreError, match="namespace must match registry.namespace"):
            MinimalFacade(
                ctx=stub_ctx,
                registry=reg,
                namespace=OperationNamespace(prefix="other"),
            )
