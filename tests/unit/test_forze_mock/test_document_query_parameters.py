"""Query-parameter channel on the mock document adapter: contract guards + source parity."""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_mock import MockDepsModule, MockQueryParamsRegistry, MockState
from forze_mock.adapters import MockDocumentAdapter
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Sale(BaseModel):
    region: str = "eu"
    total: int = 0


class _Window(BaseModel):
    window: str = "2026-01-01"


def _source(params: BaseModel, state: MockState) -> list[_Sale]:
    # Model the rows a parametrized view would yield for the bound window.
    return [_Sale(region="eu", total=10), _Sale(region="us", total=20)]


def _spec() -> DocumentSpec[_Sale, object, object, object]:
    return DocumentSpec(name="sales", read=_Sale, query_params=_Window)


# ....................... #
# contract guards


def test_with_parameters_undeclared_rejected() -> None:
    spec = DocumentSpec(name="plain", read=_Sale)  # no query_params contract
    ctx = context_from_deps(MockDepsModule()())
    with pytest.raises(CoreException, match="query_parameters_undeclared"):
        ctx.document.query(spec).with_parameters(_Window())


def test_with_parameters_type_mismatch_rejected() -> None:
    ctx = context_from_deps(MockDepsModule()())

    class _Other(BaseModel):
        x: int = 1

    with pytest.raises(CoreException, match="query_parameters_type_mismatch"):
        ctx.document.query(_spec()).with_parameters(_Other())


@pytest.mark.asyncio
async def test_declared_but_unbound_fails_closed() -> None:
    # Reading a query_params spec without binding fails closed (the relation needs the settings).
    ctx = context_from_deps(MockDepsModule()())
    with pytest.raises(CoreException, match="query_parameters_unbound"):
        await ctx.document.query(_spec()).find_many()


@pytest.mark.asyncio
async def test_bound_without_source_unprogrammed() -> None:
    ctx = context_from_deps(MockDepsModule()())  # no source registered
    with pytest.raises(CoreException, match="mock.query_parameters.unprogrammed"):
        await ctx.document.query(_spec()).with_parameters(_Window()).find_many()


# ....................... #
# source parity (DSL composes over the source rows)


@pytest.mark.asyncio
async def test_bound_read_draws_from_source_and_filters() -> None:
    registry = MockQueryParamsRegistry().on("sales", _source)
    ctx = context_from_deps(MockDepsModule(query_param_sources=registry)())

    page = await (
        ctx.document.query(_spec())
        .with_parameters(_Window(window="2026-01-01"))
        .find_many(filters={"$values": {"region": "eu"}})
    )

    assert [(r.region, r.total) for r in page.hits] == [("eu", 10)]


@pytest.mark.asyncio
async def test_bound_read_sorts_over_source() -> None:
    registry = MockQueryParamsRegistry().on("sales", _source)
    ctx = context_from_deps(MockDepsModule(query_param_sources=registry)())

    page = await (
        ctx.document.query(_spec())
        .with_parameters(_Window())
        .find_many(sorts={"total": "desc"})
    )

    assert [r.total for r in page.hits] == [20, 10]


@pytest.mark.asyncio
async def test_bound_count_over_source() -> None:
    registry = MockQueryParamsRegistry().on("sales", _source)
    ctx = context_from_deps(MockDepsModule(query_param_sources=registry)())

    assert await ctx.document.query(_spec()).with_parameters(_Window()).count() == 2


@pytest.mark.asyncio
async def test_bound_source_rows_are_tenant_scoped() -> None:
    # A source that yields rows for several tenants must still be filtered to the bound tenant —
    # mirroring the tenant WHERE clause Postgres applies — so a multi-tenant source can't leak.
    tid = uuid4()
    other = uuid4()

    def _multi_tenant_source(params: BaseModel, state: MockState) -> list[dict]:
        return [
            {"region": "eu", "total": 10, "tenant_id": str(tid)},
            {"region": "us", "total": 20, "tenant_id": str(other)},
        ]

    adapter: MockDocumentAdapter = MockDocumentAdapter(
        spec=_spec(),
        state=MockState(),
        namespace="sales",
        read_model=_Sale,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
        query_params_source=_multi_tenant_source,
    ).with_parameters(_Window())

    page = await adapter.find_many()

    assert [(r.region, r.total) for r in page.hits] == [("eu", 10)]  # other tenant excluded
