"""Unit tests for forze.application.composition.search."""

from pydantic import BaseModel

from forze.application.composition.base import BaseUsecasesFacadeProvider
from forze.application.composition.search import (
    SearchOperation,
    SearchUsecasesFacade,
    SearchUsecasesModule,
    build_search_registry,
)
from forze.application.contracts.search import (
    SearchFieldSpec,
    SearchIndexSpec,
    SearchSpec,
)
from forze.application.execution import UsecasePlan, UsecaseRegistry

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


def _minimal_search_dto_spec() -> dict:
    """Build a minimal SearchDTOSpec for testing."""
    return {"read": _MinimalSearchModel}


class TestBuildSearchRegistry:
    """Tests for build_search_registry."""

    def test_returns_registry(self) -> None:
        spec = _minimal_search_spec()
        dto_spec = _minimal_search_dto_spec()
        reg = build_search_registry(spec, dto_spec)
        assert isinstance(reg, UsecaseRegistry)

    def test_has_core_operations(self) -> None:
        spec = _minimal_search_spec()
        dto_spec = _minimal_search_dto_spec()
        reg = build_search_registry(spec, dto_spec)
        assert reg.exists(SearchOperation.TYPED_SEARCH)
        assert reg.exists(SearchOperation.RAW_SEARCH)

    def test_resolve_raw_returns_usecase(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        dto_spec = _minimal_search_dto_spec()
        reg = build_search_registry(spec, dto_spec)
        uc = reg.resolve(SearchOperation.RAW_SEARCH, composition_ctx)
        assert uc is not None


class TestSearchUsecasesModule:
    """Tests for SearchUsecasesModule."""

    def test_provider_call_returns_facade(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        dto_spec = _minimal_search_dto_spec()
        reg = build_search_registry(spec, dto_spec)
        plan = UsecasePlan()
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=SearchUsecasesFacade,
        )
        module = SearchUsecasesModule(spec=spec, dtos=dto_spec, provider=provider)
        facade = module.provider(composition_ctx)
        assert facade is not None
        assert facade.ctx is composition_ctx

    def test_facade_raw_search_resolves(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_search_spec()
        dto_spec = _minimal_search_dto_spec()
        reg = build_search_registry(spec, dto_spec)
        plan = UsecasePlan()
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=SearchUsecasesFacade,
        )
        module = SearchUsecasesModule(spec=spec, dtos=dto_spec, provider=provider)
        facade = module.provider(composition_ctx)
        uc = facade.raw_search()
        assert uc is not None
