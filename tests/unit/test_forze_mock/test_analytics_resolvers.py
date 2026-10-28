"""Analytics queries answered from the mock's current documents (``MockAnalyticsRegistry``).

A seeded query hit never moves, so a handler that reads one to decide what to do sees the same
number after every write. These pin the resolver path: it computes from the stored rows and the
call's params, reads them through the caller's transaction the way a document read does, shapes
its rows exactly like seeded ones, and leaves an unregistered query on its seeded rows.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Iterable
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockAnalyticsRegistry, MockAnalyticsView, MockDepsModule, MockState
from forze_mock.execution import MockRouteConfig
from tests.support.execution_context import context_from_modules

# ----------------------- #


class Order(Document):
    customer: str
    amount: int


class OrderCreate(CreateDocumentCmd):
    customer: str
    amount: int


class OrderUpdate(BaseDTO):
    amount: int | None = None


class OrderRead(ReadDocument):
    customer: str
    amount: int


ORDERS = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCreate, update_cmd=OrderUpdate),
)


class CustomerTotal(BaseModel):
    customer: str
    total: int


class MinTotal(BaseModel):
    min_total: int = 0


class NoParams(BaseModel):
    pass


TOTALS = AnalyticsSpec(
    name="order_totals",
    read=CustomerTotal,
    queries={
        "by_customer": AnalyticsQueryDefinition(params=MinTotal),
        "seeded": AnalyticsQueryDefinition(params=NoParams),
    },
)


def _totals(view: MockAnalyticsView, params: BaseModel) -> Iterable[dict[str, Any]]:
    assert isinstance(params, MinTotal)
    sums: dict[str, int] = defaultdict(int)

    for row in view.rows(ORDERS):
        sums[row["customer"]] += row["amount"]

    return [
        {"customer": customer, "total": total}
        for customer, total in sorted(sums.items())
        if total >= params.min_total
    ]


def _module(**kwargs: Any) -> MockDepsModule:
    return MockDepsModule(
        analytics=MockAnalyticsRegistry().on(TOTALS, "by_customer", _totals),
        **kwargs,
    )


async def _order(ctx: ExecutionContext, customer: str, amount: int) -> OrderRead:
    return await ctx.document.command(ORDERS).create(OrderCreate(customer=customer, amount=amount))


async def _totals_of(ctx: ExecutionContext, min_total: int = 0) -> dict[str, int]:
    page = await ctx.analytics.query(TOTALS).run("by_customer", MinTotal(min_total=min_total))
    return {hit.customer: hit.total for hit in page.hits}


# ....................... #


class TestAnswersFromCurrentDocuments:
    async def test_the_answer_moves_with_the_writes(self) -> None:
        ctx = context_from_modules(_module())
        assert await _totals_of(ctx) == {}

        await _order(ctx, "ada", 10)
        await _order(ctx, "ada", 5)
        await _order(ctx, "bob", 7)
        assert await _totals_of(ctx) == {"ada": 15, "bob": 7}

        bob = await _order(ctx, "bob", 1)
        await ctx.document.command(ORDERS).update(bob.id, bob.rev, OrderUpdate(amount=100))
        assert await _totals_of(ctx) == {"ada": 15, "bob": 107}

    async def test_the_params_reach_the_resolver(self) -> None:
        ctx = context_from_modules(_module())
        await _order(ctx, "ada", 10)
        await _order(ctx, "bob", 3)

        assert await _totals_of(ctx, min_total=5) == {"ada": 10}

    async def test_rows_take_the_seeded_path_s_shaping(self) -> None:
        ctx = context_from_modules(_module())
        for customer in ("ada", "bob", "cy"):
            await _order(ctx, customer, 1)
        port = ctx.analytics.query(TOTALS)

        page = await port.run_page("by_customer", MinTotal())
        assert (page.count, [hit.customer for hit in page.hits]) == (3, ["ada", "bob", "cy"])

        capped = await port.run("by_customer", MinTotal(), options={"max_rows": 1})
        assert [hit.customer for hit in capped.hits] == ["ada"]

        projected = await port.project_run(["customer"], "by_customer", MinTotal())
        assert projected.hits == [{"customer": "ada"}, {"customer": "bob"}, {"customer": "cy"}]

        cursor = await port.run_cursor("by_customer", MinTotal(), {"limit": 2})
        assert ([hit.customer for hit in cursor.hits], cursor.has_more) == (["ada", "bob"], True)

        dry = await port.run("by_customer", MinTotal(), options={"dry_run": True})
        assert dry.hits == []

    async def test_a_resolver_cannot_change_the_store_through_its_rows(self) -> None:
        def vandal(view: MockAnalyticsView, params: BaseModel) -> list[dict[str, Any]]:
            for row in view.rows("orders"):
                row["amount"] = -1
            return []

        ctx = context_from_modules(
            MockDepsModule(analytics=MockAnalyticsRegistry().on(TOTALS, "by_customer", vandal))
        )
        created = await _order(ctx, "ada", 10)
        await ctx.analytics.query(TOTALS).run("by_customer", MinTotal())

        assert (await ctx.document.query(ORDERS).get(created.id)).amount == 10

    async def test_a_resolver_returning_a_non_mapping_is_refused(self) -> None:
        ctx = context_from_modules(
            MockDepsModule(
                analytics=MockAnalyticsRegistry().on(
                    TOTALS, "by_customer", lambda view, params: [("ada", 1)]
                )
            )
        )

        with pytest.raises(CoreException, match="must return row mappings"):
            await ctx.analytics.query(TOTALS).run("by_customer", MinTotal())


# ....................... #


class TestFallback:
    async def test_a_query_without_a_resolver_keeps_its_seeded_rows(self) -> None:
        state = MockState()
        state.analytics_query_hits["order_totals"] = {
            "seeded": [{"customer": "seeded", "total": 1}],
        }
        ctx = context_from_modules(_module(state=state))
        await _order(ctx, "ada", 10)
        port = ctx.analytics.query(TOTALS)

        assert [hit.customer for hit in (await port.run("seeded", NoParams())).hits] == ["seeded"]
        assert await _totals_of(ctx) == {"ada": 10}

    async def test_no_registry_answers_every_query_from_seeded_rows(self) -> None:
        state = MockState()
        state.analytics_query_hits["order_totals"] = {
            "by_customer": [{"customer": "seeded", "total": 1}],
        }
        ctx = context_from_modules(MockDepsModule(state=state))
        await _order(ctx, "ada", 10)

        assert await _totals_of(ctx) == {"seeded": 1}


# ....................... #


class TestRegistration:
    def test_a_second_resolver_for_one_query_is_refused(self) -> None:
        registry = MockAnalyticsRegistry().on(TOTALS, "by_customer", _totals)

        with pytest.raises(CoreException) as caught:
            registry.on(TOTALS, "by_customer", lambda view, params: [])

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "mock.analytics.duplicate_resolver"
        assert registry.resolver_for("order_totals", "by_customer") is _totals

    def test_an_unknown_query_key_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            MockAnalyticsRegistry().on(TOTALS, "by_custmer", _totals)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "mock.analytics.unknown_query"
        assert "by_customer" in str(caught.value)

    def test_a_spec_that_is_not_an_analytics_spec_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            MockAnalyticsRegistry().on(ORDERS, "by_customer", _totals)  # type: ignore[arg-type]

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "mock.analytics.not_an_analytics_spec"

    def test_on_is_chainable_and_keyed_by_spec_and_query(self) -> None:
        registry = MockAnalyticsRegistry().on(TOTALS, "by_customer", _totals)

        assert registry.resolver_for("order_totals", "by_customer") is _totals
        assert registry.resolver_for("order_totals", "seeded") is None


# ....................... #


class TestTransactionView:
    async def test_a_transaction_sees_its_own_uncommitted_writes(self) -> None:
        ctx = context_from_modules(_module())
        await _order(ctx, "ada", 10)

        async with ctx.tx_ctx.scope("mock"):
            await _order(ctx, "ada", 5)
            assert await _totals_of(ctx) == {"ada": 15}

        assert await _totals_of(ctx) == {"ada": 15}

    async def test_another_transaction_s_uncommitted_writes_stay_invisible(self) -> None:
        module = _module()
        writer, reader = context_from_modules(module), context_from_modules(module)
        await _order(writer, "ada", 10)
        written, read = asyncio.Event(), asyncio.Event()
        seen: dict[str, dict[str, int]] = {}

        async def write() -> None:
            async with writer.tx_ctx.scope("mock"):
                await _order(writer, "ada", 5)
                written.set()
                await read.wait()

        async def observe() -> None:
            await written.wait()
            seen["outside"] = await _totals_of(reader)

            async with reader.tx_ctx.scope("mock"):
                seen["inside"] = await _totals_of(reader)

            read.set()

        await asyncio.gather(write(), observe())

        assert seen == {"outside": {"ada": 10}, "inside": {"ada": 10}}
        assert await _totals_of(reader) == {"ada": 15}

    async def test_a_snapshot_transaction_keeps_its_as_of_begin_answer(self) -> None:
        module = _module()
        reader, writer = context_from_modules(module), context_from_modules(module)
        await _order(writer, "ada", 10)
        began, committed = asyncio.Event(), asyncio.Event()
        seen: list[dict[str, int]] = []

        async def read() -> None:
            async with reader.tx_ctx.scope("mock", isolation=IsolationLevel.SNAPSHOT):
                seen.append(await _totals_of(reader))
                began.set()
                await committed.wait()
                seen.append(await _totals_of(reader))

        async def write() -> None:
            await began.wait()
            await _order(writer, "ada", 5)
            committed.set()

        await asyncio.gather(read(), write())

        assert seen == [{"ada": 10}, {"ada": 10}]
        assert await _totals_of(reader) == {"ada": 15}

    async def test_a_rolled_back_write_never_reaches_the_answer(self) -> None:
        ctx = context_from_modules(_module())

        with pytest.raises(RuntimeError):
            async with ctx.tx_ctx.scope("mock"):
                await _order(ctx, "ada", 5)
                raise RuntimeError("abort")

        assert await _totals_of(ctx) == {}


# ....................... #


class TestTenancy:
    async def test_a_tenant_aware_document_route_is_read_in_the_bound_partition(self) -> None:
        ctx = context_from_modules(_module(routes={"orders": MockRouteConfig(tenant_aware=True)}))
        first, second = TenantIdentity(tenant_id=uuid4()), TenantIdentity(tenant_id=uuid4())

        with ctx.inv_ctx.bind_identity(tenant=first):
            await _order(ctx, "ada", 10)

        with ctx.inv_ctx.bind_identity(tenant=second):
            await _order(ctx, "bob", 3)
            assert await _totals_of(ctx) == {"bob": 3}

        with pytest.raises(CoreException) as caught:
            await _totals_of(ctx)

        assert caught.value.code == "tenant_required"

    async def test_a_tenant_aware_analytics_route_sees_only_its_tenant_s_rows(self) -> None:
        # One unpartitioned namespace holding two tenants' rows (and one with no owner): the
        # analytics route's tenancy must not lean on the document side being partitioned.
        first, second = uuid4(), uuid4()
        state = MockState()
        state.documents["orders"] = {
            uuid4(): {"customer": "ada", "amount": 10, "tenant_id": str(first)},
            uuid4(): {"customer": "bob", "amount": 3, "tenant_id": str(second)},
            uuid4(): {"customer": "nobody", "amount": 1},
        }
        ctx = context_from_modules(
            _module(state=state, routes={"order_totals": MockRouteConfig(tenant_aware=True)})
        )

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=first)):
            assert await _totals_of(ctx) == {"ada": 10}

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=second)):
            assert await _totals_of(ctx) == {"bob": 3}

        with pytest.raises(CoreException) as caught:
            await _totals_of(ctx)

        assert caught.value.code == "tenant_required"
