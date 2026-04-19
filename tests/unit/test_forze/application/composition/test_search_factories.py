"""Tests for search usecase registry factories."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from pydantic import BaseModel

from forze.application.composition.search import SearchOperation
from forze.application.composition.search.factories import (
    build_hub_search_registry,
    build_search_raw_mapper,
    build_search_registry,
    build_search_typed_mapper,
)
from forze.application.contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.dto import RawSearchRequestDTO, SearchRequestDTO
from forze.application.execution import Deps, ExecutionContext
from forze.application.usecases.search.query import RawSearch, TypedSearch


class _Hit(BaseModel):
    id: str = "1"


def _search_spec() -> SearchSpec[_Hit]:
    return SearchSpec(name="leg1", model_type=_Hit, fields=["id"])


def _hub_spec() -> HubSearchSpec[_Hit]:
    a = SearchSpec(name="a", model_type=_Hit, fields=["id"])
    b = SearchSpec(name="b", model_type=_Hit, fields=["id"])
    return HubSearchSpec(name="hub1", model_type=_Hit, members=(a, b))


def _search_port_mock() -> MagicMock:
    p = MagicMock()
    p.search = AsyncMock(return_value=([], 0))
    return p


def _ctx_for_search(spec: SearchSpec[_Hit]) -> ExecutionContext:
    def _fac(_ctx: ExecutionContext, _sp: SearchSpec[_Hit]):
        _ = _ctx, _sp
        return _search_port_mock()

    return ExecutionContext(
        deps=Deps.routed(
            {SearchQueryDepKey: {spec.name: _fac}},
        ),
    )


def _ctx_for_hub(spec: HubSearchSpec[_Hit]) -> ExecutionContext:
    def _fac(_ctx: ExecutionContext, _sp: HubSearchSpec[_Hit]):
        _ = _ctx, _sp
        return _search_port_mock()

    return ExecutionContext(
        deps=Deps.routed(
            {HubSearchQueryDepKey: {spec.name: _fac}},
        ),
    )


class TestSearchMapperFactories:
    def test_typed_mapper_round_trip_types(self) -> None:
        m = build_search_typed_mapper()
        assert m.in_ is SearchRequestDTO
        assert m.out is SearchRequestDTO

    def test_raw_mapper_round_trip_types(self) -> None:
        m = build_search_raw_mapper()
        assert m.in_ is RawSearchRequestDTO
        assert m.out is RawSearchRequestDTO


class TestSearchRegistryFactories:
    def test_build_search_registry_registers_operations(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        assert reg.exists(SearchOperation.TYPED_SEARCH)
        assert reg.exists(SearchOperation.RAW_SEARCH)

    def test_factory_builds_typed_search(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        ctx = _ctx_for_search(spec)
        uc = reg.defaults[str(SearchOperation.TYPED_SEARCH)](ctx)
        assert isinstance(uc, TypedSearch)

    def test_factory_builds_raw_search(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        ctx = _ctx_for_search(spec)
        uc = reg.defaults[str(SearchOperation.RAW_SEARCH)](ctx)
        assert isinstance(uc, RawSearch)

    def test_hub_registry_factories_use_hub_search_query(self) -> None:
        spec = _hub_spec()
        reg = build_hub_search_registry(spec)
        ctx = _ctx_for_hub(spec)
        typed = reg.defaults[str(SearchOperation.TYPED_SEARCH)](ctx)
        raw = reg.defaults[str(SearchOperation.RAW_SEARCH)](ctx)
        assert isinstance(typed, TypedSearch)
        assert isinstance(raw, RawSearch)
