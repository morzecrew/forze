"""Unit tests for forze.application.composition.search."""

from pydantic import BaseModel

from forze.application.composition.search import (
    SearchDTOs,
    SearchOperation,
    SearchUsecasesFacade,
    build_search_registry,
)
from forze.application.contracts.search import (
    SearchFieldSpec,
    SearchIndexSpec,
    SearchSpec,
)
from forze.application.execution import UsecaseRegistry

# ----------------------- #


class _MinimalSearchModel(BaseModel):
    """Minimal model for search tests."""

    title: str = ""


def _minimal_search_spec() -> SearchSpec[_MinimalSearchModel]:
    """Build a minimal SearchSpec for testing."""
    return SearchSpec(
        namespace="test",
        model=_MinimalSearchModel,
        indexes={
            "default": SearchIndexSpec(
                fields=[SearchFieldSpec(path="title")],
            ),
        },
    )


def _minimal_search_dtos() -> SearchDTOs:
    """Build minimal SearchDTOs for testing."""
    return SearchDTOs(read=_MinimalSearchModel)


class TestBuildSearchRegistry:
    """Tests for build_search_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec, dtos)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec, dtos)
        assert reg.exists(SearchOperation.TYPED_SEARCH)
        assert reg.exists(SearchOperation.RAW_SEARCH)

    def test_resolve_raw_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec, dtos)
        reg.finalize("search")
        uc = reg.resolve(SearchOperation.RAW_SEARCH, composition_ctx)
        assert uc is not None


class TestSearchFacadeWithRegistry:
    """Tests for SearchUsecasesFacade with build_search_registry."""

    def test_facade_resolves_raw_search_usecase(
        self,
        composition_ctx,
    ) -> None:
        """Facade built from registry resolves raw_search usecase."""

        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec, dtos)
        reg.finalize("search")
        facade = SearchUsecasesFacade(ctx=composition_ctx, reg=reg)
        uc = facade.raw_search
        assert uc is not None
