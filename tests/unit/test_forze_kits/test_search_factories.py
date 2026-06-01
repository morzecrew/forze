"""Tests for search operation registry factories."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from pydantic import BaseModel

from forze_kits.aggregates.search import SearchKernelOp
from forze_kits.aggregates.search.factories import (
    build_hub_search_registry,
    build_search_registry,
)
from forze.application.contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from tests.support.execution_context import context_from_deps
from forze.application.handlers.search import (
    CursorSearch,
    ProjectedCursorSearch,
    ProjectedSearch,
    Search,
)

from .registry_helpers import handler_at, registry_has_handler

# ----------------------- #


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

    return context_from_deps(
        Deps.routed(
            {SearchQueryDepKey: {spec.name: _fac}},
        ),
    )


def _ctx_for_hub(spec: HubSearchSpec[_Hit]) -> ExecutionContext:
    def _fac(_ctx: ExecutionContext, _sp: HubSearchSpec[_Hit]):
        _ = _ctx, _sp
        return _search_port_mock()

    return context_from_deps(
        Deps.routed(
            {HubSearchQueryDepKey: {spec.name: _fac}},
        ),
    )


class TestSearchRegistryFactories:
    def test_build_search_registry_registers_operations(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        ns = spec.default_namespace
        assert registry_has_handler(reg, ns.key(SearchKernelOp.TYPED))
        assert registry_has_handler(reg, ns.key(SearchKernelOp.RAW))
        assert registry_has_handler(reg, ns.key(SearchKernelOp.TYPED_CURSOR))
        assert registry_has_handler(reg, ns.key(SearchKernelOp.RAW_CURSOR))

    def test_factory_builds_typed_search(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        ctx = _ctx_for_search(spec)
        uc = handler_at(reg, spec.default_namespace.key(SearchKernelOp.TYPED))(ctx)
        assert isinstance(uc, Search)

    def test_factory_builds_raw_search(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        ctx = _ctx_for_search(spec)
        uc = handler_at(reg, spec.default_namespace.key(SearchKernelOp.RAW))(ctx)
        assert isinstance(uc, ProjectedSearch)

    def test_factory_builds_typed_cursor_search(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        ctx = _ctx_for_search(spec)
        uc = handler_at(reg, spec.default_namespace.key(SearchKernelOp.TYPED_CURSOR))(
            ctx
        )
        assert isinstance(uc, CursorSearch)

    def test_factory_builds_raw_cursor_search(self) -> None:
        spec = _search_spec()
        reg = build_search_registry(spec)
        ctx = _ctx_for_search(spec)
        uc = handler_at(reg, spec.default_namespace.key(SearchKernelOp.RAW_CURSOR))(
            ctx
        )
        assert isinstance(uc, ProjectedCursorSearch)

    def test_hub_registry_factories_use_hub_search_query(self) -> None:
        spec = _hub_spec()
        reg = build_hub_search_registry(spec)
        ctx = _ctx_for_hub(spec)
        ns = spec.default_namespace
        typed = handler_at(reg, ns.key(SearchKernelOp.TYPED))(ctx)
        raw = handler_at(reg, ns.key(SearchKernelOp.RAW))(ctx)
        tcur = handler_at(reg, ns.key(SearchKernelOp.TYPED_CURSOR))(ctx)
        rcur = handler_at(reg, ns.key(SearchKernelOp.RAW_CURSOR))(ctx)
        assert isinstance(typed, Search)
        assert isinstance(raw, ProjectedSearch)
        assert isinstance(tcur, CursorSearch)
        assert isinstance(rcur, ProjectedCursorSearch)
