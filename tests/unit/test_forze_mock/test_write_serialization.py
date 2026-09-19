"""Writes for one owner do not interleave, on every path that writes.

The declaration's whole claim is that it holds for the writer somebody adds next year, so the
interesting test is not that *a* write serializes — it is that each of them does. The store is a
plain dict behind a dozen public methods, and a lock taken in one of them is a lock eleven paths
ignore, which is the origin defect this replaces: one writer of four took it.

The second half matters as much as the first: two *different* owners must proceed at once. A
global lock would pass every leg above and give an aggregate a throughput ceiling it never
declared.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedUpdate,
)
from forze.application.contracts.guarantees import SerializedBy
from forze.base.primitives import advisory_lock_key
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.tx import MockJournalTxManagerAdapter
from tests.support.execution_context import context_from_modules

pytestmark = [pytest.mark.asyncio]

# ----------------------- #

BY_OWNER = SerializedBy(key=("owner",))


class _Booking(Document):
    owner: str
    label: str = ""
    is_deleted: bool = False


class _BookingRead(ReadDocument):
    owner: str
    label: str = ""
    is_deleted: bool = False


class _BookingCreate(CreateDocumentCmd):
    owner: str
    label: str = ""


class _BookingUpdate(BaseDTO):
    owner: str | None = None
    label: str | None = None


def _spec(*guarantees: SerializedBy) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec[_BookingRead, _Booking, _BookingCreate, _BookingUpdate](
        name="bookings",
        read=_BookingRead,
        write=DocumentWriteTypes(
            domain=_Booking, create_cmd=_BookingCreate, update_cmd=_BookingUpdate
        ),
        guarantees=guarantees,
    )


def _ctx(state: MockState) -> Any:
    return context_from_modules(MockDepsModule(state=state))


def _tx(state: MockState) -> Any:
    return MockJournalTxManagerAdapter(state=state)


async def _observed_overlap(
    state: MockState,
    first: Callable[[Any], Awaitable[None]],
    second: Callable[[Any], Awaitable[None]],
) -> bool:
    """Whether *second* got inside its transaction while *first* was still in its own.

    Observed rather than timed: each side records when it enters and leaves, and the question
    is whether the two spans nest. A sleep-based leg would pass on a slow machine and fail on a
    fast one, which is the shape of test that gets deleted rather than trusted.
    """

    order: list[str] = []
    ctx = _ctx(state)
    tx = _tx(state)

    async def run(name: str, body: Callable[[Any], Awaitable[None]], hold: float) -> None:
        async with tx.transaction():
            await body(ctx)
            # After the write, not before it: entering the transaction is not the event, and a
            # marker laid down before the lock is taken would read as overlap every time.
            order.append(f"{name}:wrote")
            await asyncio.sleep(hold)
            order.append(f"{name}:out")

    await asyncio.gather(run("a", first, 0.05), run("b", second, 0.0))

    return order.index("b:wrote") < order.index("a:out")


# ....................... #


async def _create(owner: str) -> Callable[[Any], Awaitable[None]]:
    async def body(ctx: Any) -> None:
        await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner=owner))

    return body


class TestOneOwnerNeverInterleaves:
    async def test_two_creates_for_one_owner_are_serialized(self) -> None:
        state = MockState()

        assert not await _observed_overlap(state, await _create("o1"), await _create("o1")), (
            "the second write entered while the first still held its transaction"
        )

    async def test_two_creates_for_different_owners_are_not(self) -> None:
        # The leg that fails against a global lock, and the reason the mock keys its locks
        # exactly rather than striping them onto a shared pool.
        state = MockState()

        assert await _observed_overlap(state, await _create("o1"), await _create("o2"))

    async def test_an_undeclared_spec_serializes_nothing(self) -> None:
        # The contrast: without the declaration the two proceed together, so the legs above
        # are measuring the declaration rather than the mock's own bookkeeping.
        state = MockState()

        async def body(ctx: Any) -> None:
            await ctx.doc.command(_spec()).create(_BookingCreate(owner="o1"))

        assert await _observed_overlap(state, body, body)


# ....................... #


WRITE_PATHS: dict[str, Callable[[Any, UUID], Awaitable[None]]] = {
    "create": lambda cmd, pk: cmd.create(_BookingCreate(owner="o1"), id=pk),
    "ensure": lambda cmd, pk: cmd.ensure(pk, _BookingCreate(owner="o1")),
    "upsert": lambda cmd, pk: cmd.upsert(pk, _BookingCreate(owner="o1"), _BookingUpdate(label="x")),
}

SEEDED_PATHS: dict[str, Callable[[Any, Any], Awaitable[None]]] = {
    "update": lambda cmd, row: cmd.update(row.id, row.rev, _BookingUpdate(label="x")),
    "update_many": lambda cmd, row: cmd.update_many(
        [KeyedUpdate(id=row.id, rev=row.rev, dto=_BookingUpdate(label="x"))]
    ),
    "update_matching": lambda cmd, row: cmd.update_matching(
        {"$values": {"owner": "o1"}}, _BookingUpdate(label="x")
    ),
    "touch": lambda cmd, row: cmd.touch(row.id),
    "kill": lambda cmd, row: cmd.kill(row.id),
    "delete": lambda cmd, row: cmd.delete(row.id, row.rev),
}


class TestEveryWritePathAddressedByKeyTakesIt:
    """The same question for the writes that name a row rather than carry one.

    Each writer gets its **own** row under the one owner, so both writes are legal and the only
    thing being measured is whether they overlapped. Pointing both at one row would answer with
    a revision mismatch instead — which is serialization working, and proves it by accident
    rather than by assertion.
    """

    @pytest.mark.parametrize("name", sorted(SEEDED_PATHS))
    async def test_it_serializes(self, name: str) -> None:
        state = MockState()
        ctx = _ctx(state)
        plain = ctx.doc.command(_spec())
        rows = [
            await plain.create(_BookingCreate(owner="o1", label="a")),
            await plain.create(_BookingCreate(owner="o1", label="b")),
        ]
        path = SEEDED_PATHS[name]

        def body(row: Any) -> Callable[[Any], Awaitable[None]]:
            async def run(inner: Any) -> None:
                await path(inner.doc.command(_spec(BY_OWNER)), row)

            return run

        assert not await _observed_overlap(state, body(rows[0]), body(rows[1])), (
            f"{name} did not serialize"
        )


# ....................... #


class TestTheKeyIsTheOwnerAndItsNeighbours:
    def test_the_same_owner_in_two_specs_does_not_contend(self) -> None:
        assert advisory_lock_key("a", None, "o1") != advisory_lock_key("b", None, "o1")

    def test_the_same_owner_in_two_tenants_does_not_contend(self) -> None:
        assert advisory_lock_key("a", "t1", "o1") != advisory_lock_key("a", "t2", "o1")

    def test_the_same_owner_contends_with_itself(self) -> None:
        assert advisory_lock_key("a", "t1", "o1") == advisory_lock_key("a", "t1", "o1")


# ....................... #


class TestTheLockIsReleasedByTheTransaction:
    async def test_a_failed_transaction_frees_the_owner(self) -> None:
        # Released by commit *or* rollback: a lock held past a failure would wedge the owner
        # for the life of the process, which is worse than the race it prevents.
        state = MockState()
        ctx = _ctx(state)

        tx = _tx(state)

        with pytest.raises(RuntimeError):
            async with tx.transaction():
                await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner="o1"))
                raise RuntimeError("rolled back")

        async with tx.transaction():
            row = await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner="o1"))

        assert row.owner == "o1"
        assert all(not lock.locked() for lock in state.write_serialization.values())
