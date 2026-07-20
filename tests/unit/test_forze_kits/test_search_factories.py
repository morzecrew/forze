"""Tests for search operation registry factories."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from pydantic import BaseModel

from forze.application.contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_kits.aggregates.search import SearchKernelOp
from forze_kits.aggregates.search.factories import (
    build_hub_search_registry,
    build_search_registry,
)
from forze_kits.aggregates.search.handlers import (
    CursorSearch,
    ProjectedCursorSearch,
    ProjectedSearch,
    Search,
)
from tests.support.execution_context import context_from_deps

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


class TestSearchCatalog:
    def test_all_search_ops_are_read_only(self) -> None:
        spec = _search_spec()
        cat = build_search_registry(spec).freeze().catalog()

        assert cat  # not empty
        assert all(entry.is_read_only for entry in cat.values())

    def test_typed_descriptor_carries_request_and_response_schema(self) -> None:
        spec = _search_spec()
        ns = spec.default_namespace
        cat = build_search_registry(spec).freeze().catalog()

        typed = cat[ns.key(SearchKernelOp.TYPED)].descriptor
        assert typed is not None
        assert typed.input_schema() is not None
        # SearchPaginated[_Hit] envelope resolves.
        assert "hits" in (typed.output_schema() or {}).get("properties", {})


class TestSearchSensitivePropagation:
    def test_descriptors_not_sensitive_by_default(self) -> None:
        spec = _search_spec()
        cat = build_search_registry(spec).freeze().catalog()

        assert cat
        assert all(
            entry.descriptor is not None and entry.descriptor.sensitive is False
            for entry in cat.values()
        )

    def test_sensitive_spec_marks_every_descriptor(self) -> None:
        spec = SearchSpec(
            name="leg1", model_type=_Hit, fields=["id"], sensitive=True
        )
        cat = build_search_registry(spec).freeze().catalog()

        assert cat
        assert all(
            entry.descriptor is not None and entry.descriptor.sensitive is True
            for entry in cat.values()
        )

    def test_hub_is_sensitive_when_any_member_is(self) -> None:
        a = SearchSpec(name="a", model_type=_Hit, fields=["id"], sensitive=True)
        b = SearchSpec(name="b", model_type=_Hit, fields=["id"])
        hub = HubSearchSpec(name="hub1", model_type=_Hit, members=(a, b))

        cat = build_hub_search_registry(hub).freeze().catalog()

        assert cat
        assert all(
            entry.descriptor is not None and entry.descriptor.sensitive is True
            for entry in cat.values()
        )

    def test_federated_is_sensitive_when_any_member_is(self) -> None:
        from forze.application.contracts.search import FederatedSearchSpec
        from forze_kits.aggregates.search.factories import (
            build_federated_search_registry,
        )

        a = SearchSpec(name="a", model_type=_Hit, fields=["id"], sensitive=True)
        b = SearchSpec(name="b", model_type=_Hit, fields=["id"])
        fed = FederatedSearchSpec(name="fed1", members=(a, b))

        cat = build_federated_search_registry(fed).freeze().catalog()

        assert cat
        assert all(
            entry.descriptor is not None and entry.descriptor.sensitive is True
            for entry in cat.values()
        )
