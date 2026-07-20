"""Unit tests for forze_kits.aggregates.search."""

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.application.execution.operations.registry import OperationRegistry
from forze_kits.aggregates.search import (
    SearchFacade,
    SearchKernelOp,
    build_search_registry,
)

from .registry_helpers import registry_has_handler

# ----------------------- #


class _Hit(BaseModel):
    id: str = "1"


def _spec() -> SearchSpec[_Hit]:
    return SearchSpec(name="leg1", model_type=_Hit, fields=["id"])


class TestBuildSearchRegistry:
    def test_returns_registry(self) -> None:
        reg = build_search_registry(_spec())
        assert isinstance(reg, OperationRegistry)

    def test_has_core_operations(self) -> None:
        spec = _spec()
        reg = build_search_registry(spec)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(SearchKernelOp.TYPED))
        assert registry_has_handler(reg, ns.key(SearchKernelOp.RAW))
        assert registry_has_handler(reg, ns.key(SearchKernelOp.TYPED_CURSOR))
        assert registry_has_handler(reg, ns.key(SearchKernelOp.RAW_CURSOR))

    def test_resolve_raw_returns_handler(
        self,
        composition_ctx,
    ) -> None:
        spec = _spec()
        reg = build_search_registry(spec).freeze()
        op = spec.default_namespace.key(SearchKernelOp.RAW)
        resolved = reg.resolve(op, composition_ctx)
        assert resolved is not None


class TestSearchFacadeWithRegistry:
    def test_facade_resolves_raw_search(
        self,
        composition_ctx,
    ) -> None:
        spec = _spec()
        reg = build_search_registry(spec).freeze()
        facade = SearchFacade(
            ctx=composition_ctx,
            registry=reg,
            namespace=spec.default_namespace,
        )
        assert facade.projected_search is not None
