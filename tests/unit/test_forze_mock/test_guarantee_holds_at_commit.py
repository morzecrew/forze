"""A guarantee that holds at commit lets a transaction pass through a violation it resolves.

The case that asked for it: a positioned child collection, reconciled by inserting at the top.
The new row is written at position 0 while the row it displaces still holds 0, and only once the
displaced rows have moved down is the list unique again. Postgres checks a
``DEFERRABLE INITIALLY DEFERRED`` constraint at commit and accepts that transaction; the in-memory
store has to accept it too, or ordinary application code fails in the one place it is tested.

Each leg says which moment decides: inside a transaction, the commit; outside one, the write —
there is no later moment, as a statement in autocommit is to a deferred constraint.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import NonOverlapping, UniqueTogether
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Item(Document):
    order_id: str
    position: int
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _ItemRead(ReadDocument):
    order_id: str
    position: int
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _ItemCreate(CreateDocumentCmd):
    order_id: str
    position: int
    valid_from: date = date(2026, 1, 1)
    valid_to: date | None = None


class _ItemUpdate(BaseDTO):
    position: int | None = None
    valid_to: date | None = None


def _spec(*guarantees: UniqueTogether | NonOverlapping) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec[_ItemRead, _Item, _ItemCreate, _ItemUpdate](
        name="items",
        read=_ItemRead,
        write=DocumentWriteTypes(domain=_Item, create_cmd=_ItemCreate, update_cmd=_ItemUpdate),
        guarantees=guarantees,
    )


AT_COMMIT = UniqueTogether(fields=("order_id", "position"), holds="commit")
ALWAYS = UniqueTogether(fields=("order_id", "position"))


async def _positions(ctx: ExecutionContext, spec: DocumentSpec[Any, Any, Any, Any]) -> list[int]:
    page = await ctx.doc.query(spec).find_many({"$values": {"order_id": "o1"}})
    return sorted(row.position for row in page.hits)


async def _list_of_two(
    spec: DocumentSpec[Any, Any, Any, Any],
) -> tuple[ExecutionContext, Any, Any]:
    ctx = context_from_modules(MockDepsModule())
    command = ctx.doc.command(spec)
    first = await command.create(_ItemCreate(order_id="o1", position=0))
    second = await command.create(_ItemCreate(order_id="o1", position=1))

    return ctx, first, second


async def _insert_at_the_top(ctx: ExecutionContext, spec: Any, first: Any, second: Any) -> None:
    command = ctx.doc.command(spec)
    await command.create(_ItemCreate(order_id="o1", position=0))  # both rows at 0, for now
    await command.update(first.id, first.rev, _ItemUpdate(position=1))  # both at 1, for now
    await command.update(second.id, second.rev, _ItemUpdate(position=2))


def _is_conflict(error: CoreException) -> bool:
    return error.kind is ExceptionKind.CONFLICT


# ....................... #


class TestUniquenessHoldingAtCommit:
    async def test_a_transaction_may_pass_through_a_duplicate_it_resolves(self) -> None:
        spec = _spec(AT_COMMIT)
        ctx, first, second = await _list_of_two(spec)

        async with ctx.tx_ctx.scope("mock"):
            await _insert_at_the_top(ctx, spec, first, second)

        assert await _positions(ctx, spec) == [0, 1, 2]

    async def test_a_duplicate_still_there_at_commit_is_refused_there(self) -> None:
        spec = _spec(AT_COMMIT)
        ctx, first, _ = await _list_of_two(spec)
        command = ctx.doc.command(spec)

        with pytest.raises(CoreException) as caught:
            async with ctx.tx_ctx.scope("mock"):
                await command.create(_ItemCreate(order_id="o1", position=0))
                await command.update(first.id, first.rev, _ItemUpdate(position=1))
                # the second row was never moved off 1: the list ends with two rows there

        assert _is_conflict(caught.value)
        assert await _positions(ctx, spec) == [0, 1]  # nothing the transaction wrote landed

    async def test_the_first_transaction_into_an_empty_store_is_judged_too(self) -> None:
        # Nothing committed before it, so the transaction's own rows are all there is — and a
        # recheck that skipped a namespace with no committed rows would let both land.
        spec = _spec(AT_COMMIT)
        ctx = context_from_modules(MockDepsModule())
        command = ctx.doc.command(spec)

        with pytest.raises(CoreException) as caught:
            async with ctx.tx_ctx.scope("mock"):
                await command.create(_ItemCreate(order_id="o1", position=0))
                await command.create(_ItemCreate(order_id="o1", position=0))

        assert _is_conflict(caught.value)
        assert await _positions(ctx, spec) == []

    async def test_a_set_based_update_may_pass_through_one_too(self) -> None:
        # The bulk path validates the whole staged batch rather than row by row; inside a
        # transaction it leaves a commit guarantee to the commit as well.
        spec = _spec(AT_COMMIT)
        ctx, first, second = await _list_of_two(spec)
        command = ctx.doc.command(spec)

        async with ctx.tx_ctx.scope("mock"):
            await command.update_matching({"$values": {"id": first.id}}, _ItemUpdate(position=1))
            await command.update(second.id, second.rev, _ItemUpdate(position=2))

        assert await _positions(ctx, spec) == [1, 2]

    async def test_outside_a_transaction_the_write_is_the_commit(self) -> None:
        spec = _spec(AT_COMMIT)
        ctx, _, _ = await _list_of_two(spec)

        with pytest.raises(CoreException) as caught:
            await ctx.doc.command(spec).create(_ItemCreate(order_id="o1", position=0))

        assert _is_conflict(caught.value)


class TestUniquenessHoldingAlways:
    async def test_the_duplicate_is_refused_at_the_write_that_makes_it(self) -> None:
        spec = _spec(ALWAYS)
        ctx, first, second = await _list_of_two(spec)

        with pytest.raises(CoreException) as caught:
            async with ctx.tx_ctx.scope("mock"):
                await _insert_at_the_top(ctx, spec, first, second)

        assert _is_conflict(caught.value)
        assert await _positions(ctx, spec) == [0, 1]


class TestNonOverlapHoldingAtCommit:
    """The other member: a period shortened after its successor was written."""

    @staticmethod
    def _spec(holds: str) -> DocumentSpec[Any, Any, Any, Any]:
        return _spec(
            NonOverlapping(
                key=("order_id",),
                period=("valid_from", "valid_to"),
                holds=holds,  # type: ignore[arg-type]
            )
        )

    async def _open_ended(self, spec: Any) -> tuple[ExecutionContext, Any]:
        ctx = context_from_modules(MockDepsModule())
        row = await ctx.doc.command(spec).create(
            _ItemCreate(order_id="o1", position=0, valid_from=date(2026, 1, 1))
        )

        return ctx, row

    async def test_a_successor_written_before_its_predecessor_ends_commits(self) -> None:
        spec = self._spec("commit")
        ctx, current = await self._open_ended(spec)
        command = ctx.doc.command(spec)

        async with ctx.tx_ctx.scope("mock"):
            await command.create(
                _ItemCreate(order_id="o1", position=1, valid_from=date(2026, 6, 1))
            )
            await command.update(current.id, current.rev, _ItemUpdate(valid_to=date(2026, 6, 1)))

        assert await _positions(ctx, spec) == [0, 1]

    @pytest.mark.parametrize("holds", ["commit", "always"])
    async def test_an_overlap_left_at_commit_is_refused(self, holds: str) -> None:
        spec = self._spec(holds)
        ctx, _ = await self._open_ended(spec)

        with pytest.raises(CoreException) as caught:
            async with ctx.tx_ctx.scope("mock"):
                await ctx.doc.command(spec).create(
                    _ItemCreate(order_id="o1", position=1, valid_from=date(2026, 6, 1))
                )

        assert _is_conflict(caught.value)
        assert await _positions(ctx, spec) == [0]
