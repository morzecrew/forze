"""A derived source that aggregates child rows through the reading transaction's view.

A ``(row)`` source sees only its own row, and one closing over ``MockState`` sees committed
rows only, so a handler that writes a child and then reads the parent's aggregate in one
transaction would read a different value on the mock than on the real view. These pin the
``(row, view)`` shape: the view is the one an analytics resolver gets, read through the caller's
transaction and narrowed to its tenant, and a source's shape is settled where it is registered.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockAnalyticsView, MockDepsModule, MockDerivedRegistry, MockState
from forze_mock.execution import MockRouteConfig
from tests.support.execution_context import context_from_modules

# ----------------------- #


class Batch(Document):
    planned: int


class BatchCreate(CreateDocumentCmd):
    planned: int


class BatchRead(ReadDocument):
    planned: int
    released_quantity: int


BATCHES = DocumentSpec(
    name="batches",
    read=BatchRead,
    write=DocumentWriteTypes(domain=Batch, create_cmd=BatchCreate),
    derived_read_fields={"released_quantity": None},
)


class Release(Document):
    batch_id: UUID
    quantity: int


class ReleaseCreate(CreateDocumentCmd):
    batch_id: UUID
    quantity: int


class ReleaseRead(ReadDocument):
    batch_id: UUID
    quantity: int


RELEASES = DocumentSpec(
    name="releases",
    read=ReleaseRead,
    write=DocumentWriteTypes(domain=Release, create_cmd=ReleaseCreate),
)


def _released(row: JsonDict, view: MockAnalyticsView) -> dict[str, Any]:
    children = view.rows(RELEASES)
    total = sum(r["quantity"] for r in children if str(r["batch_id"]) == str(row["id"]))

    return {"released_quantity": total}


def _module(**kwargs: Any) -> MockDepsModule:
    return MockDepsModule(
        derived_values=MockDerivedRegistry().on(BATCHES.name, _released), **kwargs
    )


async def _batch(ctx: ExecutionContext) -> BatchRead:
    return await ctx.document.command(BATCHES).create(BatchCreate(planned=10))


async def _release(ctx: ExecutionContext, batch_id: UUID, quantity: int) -> None:
    await ctx.document.command(RELEASES).create(ReleaseCreate(batch_id=batch_id, quantity=quantity))


async def _released_of(ctx: ExecutionContext, batch_id: UUID) -> int:
    return (await ctx.document.query(BATCHES).get(batch_id)).released_quantity


# ....................... #


class TestAggregatesChildRows:
    async def test_the_value_moves_with_the_children(self) -> None:
        ctx = context_from_modules(_module())
        batch, other = await _batch(ctx), await _batch(ctx)
        assert batch.released_quantity == 0

        await _release(ctx, batch.id, 3)
        await _release(ctx, batch.id, 4)
        await _release(ctx, other.id, 100)

        assert await _released_of(ctx, batch.id) == 7

    async def test_every_read_path_hands_the_source_the_view(self) -> None:
        ctx = context_from_modules(_module())
        batch = await _batch(ctx)
        await _release(ctx, batch.id, 5)
        port = ctx.document.query(BATCHES)

        [many] = await port.get_many([batch.id])
        found = await port.find_many()
        projected = await port.project({"$values": {"id": batch.id}}, ["released_quantity"])

        assert many.released_quantity == 5
        assert [hit.released_quantity for hit in found.hits] == [5]
        assert projected == {"released_quantity": 5}


class TestTransactionView:
    @pytest.mark.parametrize("isolation", [None, IsolationLevel.SNAPSHOT])
    async def test_a_transaction_counts_its_own_uncommitted_child(
        self, isolation: IsolationLevel | None
    ) -> None:
        ctx = context_from_modules(_module())
        batch = await _batch(ctx)

        async with ctx.tx_ctx.scope("mock", isolation=isolation):
            await _release(ctx, batch.id, 5)
            assert await _released_of(ctx, batch.id) == 5

        assert await _released_of(ctx, batch.id) == 5

    async def test_a_rolled_back_child_leaves_the_aggregate(self) -> None:
        ctx = context_from_modules(_module())
        batch = await _batch(ctx)

        with pytest.raises(RuntimeError):
            async with ctx.tx_ctx.scope("mock"):
                await _release(ctx, batch.id, 5)
                assert await _released_of(ctx, batch.id) == 5
                raise RuntimeError("abort")

        assert await _released_of(ctx, batch.id) == 0

    async def test_another_transaction_s_uncommitted_child_stays_invisible(self) -> None:
        module = _module()
        writer, reader = context_from_modules(module), context_from_modules(module)
        batch = await _batch(writer)
        written, read = asyncio.Event(), asyncio.Event()
        seen: dict[str, int] = {}

        async def write() -> None:
            async with writer.tx_ctx.scope("mock"):
                await _release(writer, batch.id, 5)
                written.set()
                await read.wait()

        async def observe() -> None:
            await written.wait()
            seen["outside"] = await _released_of(reader, batch.id)

            async with reader.tx_ctx.scope("mock"):
                seen["inside"] = await _released_of(reader, batch.id)

            read.set()

        await asyncio.gather(write(), observe())

        assert seen == {"outside": 0, "inside": 0}
        assert await _released_of(reader, batch.id) == 5


class TestTenancy:
    async def test_another_tenant_s_children_are_left_out(self) -> None:
        # The children share one unpartitioned namespace; the tenant-aware parent route narrows
        # the view to the bound tenant, so another tenant's row and an unowned one stay out.
        first, second = uuid4(), uuid4()
        state = MockState()
        ctx = context_from_modules(
            _module(state=state, routes={"batches": MockRouteConfig(tenant_aware=True)})
        )

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=first)):
            batch = await _batch(ctx)
            await _release(ctx, batch.id, 3)

        # The child route is not tenant-aware, so the row written above carries no owner yet.
        releases = state.documents["releases"]
        for row in releases.values():
            row["tenant_id"] = str(first)

        releases[uuid4()] = {"batch_id": str(batch.id), "quantity": 50, "tenant_id": str(second)}
        releases[uuid4()] = {"batch_id": str(batch.id), "quantity": 700}

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=first)):
            assert await _released_of(ctx, batch.id) == 3

    async def test_a_tenant_aware_child_route_is_read_in_the_bound_partition(self) -> None:
        routes = {name: MockRouteConfig(tenant_aware=True) for name in ("batches", "releases")}
        ctx = context_from_modules(_module(routes=routes))
        first, second = TenantIdentity(tenant_id=uuid4()), TenantIdentity(tenant_id=uuid4())

        with ctx.inv_ctx.bind_identity(tenant=first):
            batch = await _batch(ctx)
            await _release(ctx, batch.id, 3)

        with ctx.inv_ctx.bind_identity(tenant=second):
            await _release(ctx, batch.id, 50)

        with ctx.inv_ctx.bind_identity(tenant=first):
            assert await _released_of(ctx, batch.id) == 3


# ....................... #


def _row_only(row: JsonDict) -> dict[str, Any]:
    return {"released_quantity": -1}


class _Sources:
    def row_only(self, row: JsonDict) -> dict[str, Any]:
        return {"released_quantity": -2}

    def with_view(self, row: JsonDict, view: MockAnalyticsView) -> dict[str, Any]:
        return _released(row, view)


def _scaled(scale: int, row: JsonDict, view: MockAnalyticsView) -> dict[str, Any]:
    return {"released_quantity": scale * _released(row, view)["released_quantity"]}


async def _async_row_only(row: JsonDict) -> dict[str, Any]:
    return {"released_quantity": -1}


class _AsyncCall:
    async def __call__(self, row: JsonDict) -> dict[str, Any]:
        return {"released_quantity": -1}


class TestRegistration:
    @pytest.mark.parametrize(
        ("source", "expected"),
        [
            (_row_only, -1),
            (lambda row: {"released_quantity": -3}, -3),
            (_Sources().row_only, -2),
            (functools.partial(_row_only), -1),
            (_Sources().with_view, 4),
            (functools.partial(_scaled, 10), 40),
            (lambda row, view, *, scale=2: _scaled(scale, row, view), 8),
            # Binding one argument keeps a source row-only, as before views, even when it
            # could also bind two.
            (lambda row, scale=5: {"released_quantity": -scale}, -5),
            (lambda row, view=None: {"released_quantity": -4 if view is None else 99}, -4),
            (lambda *args: {"released_quantity": -len(args)}, -1),
            (MagicMock(return_value={"released_quantity": -6}), -6),
        ],
    )
    async def test_each_accepted_shape_is_called_as_it_declares(
        self, source: Any, expected: int
    ) -> None:
        ctx = context_from_modules(
            MockDepsModule(derived_values=MockDerivedRegistry().on("batches", source))
        )
        batch = await _batch(ctx)
        await _release(ctx, batch.id, 4)

        assert await _released_of(ctx, batch.id) == expected

    @pytest.mark.parametrize(
        "source",
        [
            lambda: {"released_quantity": 0},
            lambda row, view, extra: {},
            functools.partial(_scaled),
            "released_quantity",
            # A read never awaits a source, so an async one would fail on its first read.
            _async_row_only,
            functools.partial(_async_row_only),
            _AsyncCall(),
        ],
    )
    def test_an_unsupported_shape_is_refused_where_it_is_registered(self, source: Any) -> None:
        with pytest.raises(CoreException) as caught:
            MockDerivedRegistry().on("batches", source)

        assert caught.value.code == "mock.derived.source_shape"

    def test_a_source_with_no_inspectable_signature_stays_row_only(self) -> None:
        with pytest.raises(ValueError):
            inspect.signature(dict)  # the precondition: a C builtin with no signature

        source = MockDerivedRegistry().on("batches", dict).source_for("batches")
        view = MockAnalyticsView(state=MockState())

        # ``dict(row, view)`` would raise, so a passing call proves the view was withheld.
        assert source is not None
        assert source({"released_quantity": 9}, view) == {"released_quantity": 9}
