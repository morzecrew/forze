"""Unit tests for forze.application.composition.search."""

from pydantic import BaseModel

from forze.application.composition.search import (
    SearchOperation,
    SearchUsecasesFacade,
    SearchUsecasesFacadeProvider,
    build_search_plan,
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


class TestBuildSearchRegistry:
    """Tests for build_search_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        assert reg.exists(SearchOperation.TYPED_SEARCH)
        assert reg.exists(SearchOperation.RAW_SEARCH)

    def test_resolve_raw_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        uc = reg.resolve(SearchOperation.RAW_SEARCH, composition_ctx)
        assert uc is not None


class TestBuildSearchPlan:
    """Tests for build_search_plan."""

    def test_returns_plan(self) -> None:
        plan = build_search_plan()
        assert plan is not None


class TestSearchUsecasesFacadeProvider:
    """Tests for SearchUsecasesFacadeProvider."""

    def test_call_returns_facade(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        plan = build_search_plan()
        provider = SearchUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            spec=spec,
            read_dto=_MinimalSearchModel,
        )
        facade = provider(composition_ctx)
        assert facade is not None
        assert facade.ctx is composition_ctx

    def test_facade_raw_resolves(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        plan = build_search_plan()
        provider = SearchUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            spec=spec,
            read_dto=_MinimalSearchModel,
        )
        facade = provider(composition_ctx)
        uc = facade.raw()
        assert uc is not None
