"""Unit tests for forze.application.composition.search."""

from pydantic import BaseModel

from forze.application.composition.search import (
    SearchKernelOp,
    SearchUsecasesFacade,
    build_search_registry,
)
from forze.application.contracts.search import SearchSpec
from forze.application.execution import UsecaseRegistry, operation_namespace_for

# ----------------------- #


class _MinimalSearchModel(BaseModel):
    """Minimal model for search tests."""

    title: str = ""


def _minimal_search_spec() -> SearchSpec[_MinimalSearchModel]:
    """Build a minimal SearchSpec for testing."""
    return SearchSpec(
        name="test",
        model_type=_MinimalSearchModel,
        fields=["title"],
    )


class TestBuildSearchRegistry:
    """Tests for build_search_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        assert reg.exists(operation_namespace_for(spec).op(SearchKernelOp.TYPED))
        assert reg.exists(operation_namespace_for(spec).op(SearchKernelOp.RAW))
        assert reg.exists(operation_namespace_for(spec).op(SearchKernelOp.TYPED_CURSOR))
        assert reg.exists(operation_namespace_for(spec).op(SearchKernelOp.RAW_CURSOR))

    def test_resolve_raw_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        reg.finalize("search")
        uc = reg.resolve(operation_namespace_for(spec).op(SearchKernelOp.RAW), composition_ctx)
        assert uc is not None


class TestSearchFacadeWithRegistry:
    """Tests for SearchUsecasesFacade with build_search_registry."""

    def test_facade_resolves_raw_search_usecase(
        self,
        composition_ctx,
    ) -> None:
        """Facade built from registry resolves raw_search usecase."""

        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        reg.finalize("search")
        facade = SearchUsecasesFacade(
            ctx=composition_ctx,
            registry=reg,
            namespace=operation_namespace_for(spec),
        )
        uc = facade.raw_search
        assert uc is not None
