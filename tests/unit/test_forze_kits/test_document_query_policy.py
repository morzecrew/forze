"""End-to-end: query-field policy enforced at the governed list op, not the port."""

from __future__ import annotations

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying import QueryFieldPolicy
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException
from forze.domain.models import ReadDocument
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)
from forze_kits.aggregates.document.dto import (
    AggregatedListRequestDTO,
    ListRequestDTO,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

pytestmark = pytest.mark.unit


class NoteRead(ReadDocument):
    title: str
    body: str


def _registry(policy: QueryFieldPolicy | None):
    spec = DocumentSpec(name="notes", read=NoteRead, query_policy=policy)
    return spec, build_document_registry(spec, DocumentDTOs(read=NoteRead)).freeze()


def _ctx():
    return context_from_modules(MockDepsModule(state=MockState()))


_RESTRICTED = QueryFieldPolicy(filterable={"title"}, sortable={"title"})


async def _run_list(reg, spec, **dto_kwargs):
    return await run_operation(
        reg,
        spec.default_namespace.key(DocumentKernelOp.LIST),
        ListRequestDTO(**dto_kwargs),
        _ctx(),
    )


class TestGovernedOperationEnforcement:
    async def test_allowed_filter_and_sort_pass(self) -> None:
        spec, reg = _registry(_RESTRICTED)
        res = await _run_list(
            reg, spec, filters={"$values": {"title": "x"}}, sorts={"title": "asc"}
        )
        assert res.count == 0  # empty store, but the query ran

    async def test_forbidden_filter_field_rejected(self) -> None:
        spec, reg = _registry(_RESTRICTED)
        # `body` exists on the read model but is not in the filterable allow-set.
        with pytest.raises(CoreException) as ei:
            await _run_list(reg, spec, filters={"$values": {"body": "x"}})
        assert ei.value.code == "field_not_filterable"

    async def test_forbidden_sort_field_rejected(self) -> None:
        spec, reg = _registry(_RESTRICTED)
        with pytest.raises(CoreException) as ei:
            await _run_list(reg, spec, sorts={"created_at": "desc"})
        assert ei.value.code == "field_not_sortable"

    async def test_no_policy_allows_any_field(self) -> None:
        spec, reg = _registry(None)
        res = await _run_list(reg, spec, filters={"$values": {"body": "x"}})
        assert res.count == 0


class TestAggregateEnforcement:
    async def test_forbidden_group_field_rejected(self) -> None:
        spec = DocumentSpec(
            name="notes",
            read=NoteRead,
            query_policy=QueryFieldPolicy(aggregatable={"title"}),
        )
        reg = build_document_registry(spec, DocumentDTOs(read=NoteRead)).freeze()

        with pytest.raises(CoreException) as ei:
            await run_operation(
                reg,
                spec.default_namespace.key(DocumentKernelOp.AGG_LIST),
                AggregatedListRequestDTO(
                    aggregates={
                        "$groups": {"body": "body"},  # `body` not aggregatable
                        "$computed": {"n": {"$count": None}},
                    },
                ),
                _ctx(),
            )
        assert ei.value.code == "field_not_aggregatable"


class TestPortBypass:
    async def test_query_port_is_not_restricted(self) -> None:
        # Internal code reaching the port directly bypasses the boundary guard —
        # filtering by a non-allowed field works (the guard lives in the operation).
        spec, _ = _registry(_RESTRICTED)
        ctx = _ctx()
        page = await ctx.doc.query(spec).find_page(filters={"$values": {"body": "x"}})
        assert page.count == 0
