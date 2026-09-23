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
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedUpdate,
)
from forze.application.contracts.guarantees import SerializedBy
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import advisory_lock_key, utcnow
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters._mvcc import StatementLocks
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


class TestEveryFreshWritePathTakesIt:
    """The paths that carry their own row, rather than naming one already stored."""

    @pytest.mark.parametrize("name", sorted(WRITE_PATHS))
    async def test_it_serializes(self, name: str) -> None:
        state = MockState()
        path = WRITE_PATHS[name]

        def body(pk: UUID) -> Callable[[Any], Awaitable[None]]:
            async def run(inner: Any) -> None:
                await path(inner.doc.command(_spec(BY_OWNER)), pk)

            return run

        # Distinct ids, same owner: both writes are legal and the only thing measured is
        # whether they overlapped.
        assert not await _observed_overlap(state, body(uuid4()), body(uuid4())), (
            f"{name} did not serialize"
        )


# ....................... #


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
        assert state.write_serialization.held() == ()


# ....................... #


class TestASetBasedUpdateLocksEveryOwnerItSelects:
    async def test_two_owners_in_one_filter_are_both_held(self) -> None:
        # One statement, every lock it needs: a set-based update that selected two owners and
        # held only one would leave the other open for the length of its own transaction.
        state = MockState()
        ctx = _ctx(state)
        plain = ctx.doc.command(_spec())
        await plain.create(_BookingCreate(owner="o1", label="sweep"))
        second = await plain.create(_BookingCreate(owner="o2", label="sweep"))

        async def sweep(inner: Any) -> None:
            await inner.doc.command(_spec(BY_OWNER)).update_matching(
                {"$values": {"label": "sweep"}}, _BookingUpdate(label="swept")
            )

        async def touch_second(inner: Any) -> None:
            await inner.doc.command(_spec(BY_OWNER)).touch(second.id)

        assert not await _observed_overlap(state, sweep, touch_second)


# ....................... #


BY_OWNER_AND_SLOT = SerializedBy(key=("owner", "label"))


class TestAPartialPatchStillLocksWhereItLands:
    """A patch names part of a composite key; the rest comes from the row it lands on.

    Derived from the patch alone the missing half reads as null, so the destination key belongs
    to an owner no row has — a lock taken on nothing while the owner the row actually moves to
    goes unheld, and another writer for that owner proceeds beside it.
    """

    async def test_the_destination_of_a_partial_patch_is_serialized(self) -> None:
        state = MockState()
        ctx = _ctx(state)
        plain = ctx.doc.command(_spec())
        moving = await plain.create(_BookingCreate(owner="o1", label="before"))

        async def move(inner: Any) -> None:
            # Names `label` only: `owner` has to come from the stored row.
            await inner.doc.command(_spec(BY_OWNER_AND_SLOT)).update(
                moving.id, moving.rev, _BookingUpdate(label="after")
            )

        async def write_destination(inner: Any) -> None:
            await inner.doc.command(_spec(BY_OWNER_AND_SLOT)).create(
                _BookingCreate(owner="o1", label="after")
            )

        assert not await _observed_overlap(state, move, write_destination)

    async def test_an_unrelated_destination_still_proceeds(self) -> None:
        # The contrast: the merge must not serialize against owners the patch never reaches.
        state = MockState()
        ctx = _ctx(state)
        plain = ctx.doc.command(_spec())
        moving = await plain.create(_BookingCreate(owner="o1", label="before"))

        async def move(inner: Any) -> None:
            await inner.doc.command(_spec(BY_OWNER_AND_SLOT)).update(
                moving.id, moving.rev, _BookingUpdate(label="after")
            )

        async def elsewhere(inner: Any) -> None:
            await inner.doc.command(_spec(BY_OWNER_AND_SLOT)).create(
                _BookingCreate(owner="o2", label="unrelated")
            )

        assert await _observed_overlap(state, move, elsewhere)


# ....................... #


class TestTheKeyRefusesWhatItCannotEncodeStably:
    """A value with no canonical rendering is refused, not guessed at.

    Two writers deriving different bytes for one logical owner would not contend at all, which
    is the failure the declaration exists to remove — so a type whose rendering can vary
    between processes or releases is turned away at the key.
    """

    def test_a_mapping_is_refused(self) -> None:
        # Equal dictionaries with different insertion orders render differently.
        with pytest.raises(CoreException) as caught:
            advisory_lock_key("a", None, {"x": 1})

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert "no rendering guaranteed to be identical" in caught.value.summary

    def test_a_sequence_is_refused(self) -> None:
        with pytest.raises(CoreException):
            advisory_lock_key("a", None, [1, 2])

    def test_an_arbitrary_object_is_refused(self) -> None:
        class _Owner:
            def __str__(self) -> str:
                return "o1"

        with pytest.raises(CoreException):
            advisory_lock_key("a", None, _Owner())

    def test_every_supported_type_encodes(self) -> None:
        # The contrast: the refusal is about what a store cannot compare, not about everything.
        for value in ("o1", b"o1", 1, True, uuid4(), Decimal("1.5"), date.today(), utcnow()):
            assert isinstance(advisory_lock_key("a", None, value), int)


# ....................... #


class TestTheKeyDoesNotCollapseTypes:
    def test_bytes_and_text_with_the_same_bytes_differ(self) -> None:
        # The case the type tag is actually load-bearing for: both encode to the same bytes,
        # so only the type travelling with the value keeps them apart.
        assert advisory_lock_key("a", None, b"o1") != advisory_lock_key("a", None, "o1")

    def test_an_integer_and_its_text_are_different_keys(self) -> None:
        # Both render to "1"; an aggregate keyed on an id that is an integer in one caller and
        # its text in another would otherwise serialize them against each other by accident.
        assert advisory_lock_key("a", None, 1) != advisory_lock_key("a", None, "1")

    def test_a_uuid_and_its_text_are_different_keys(self) -> None:
        owner = uuid4()

        assert advisory_lock_key("a", None, owner) != advisory_lock_key("a", None, str(owner))

    def test_the_same_value_is_still_the_same_key(self) -> None:
        owner = uuid4()

        assert advisory_lock_key("a", None, owner) == advisory_lock_key("a", None, owner)


# ....................... #


class TestTwoTransactionsWantingEachOthersOwners:
    """A cycle is refused, not waited on.

    Locks are sorted within a call, so one call cannot invert against itself. Across calls the
    order is the caller's: a transaction can take `o1` then ask for `o2` while another holds
    `o2` and asks for `o1`. Neither can release first, and the locks live until the transaction
    ends — so without detection both sit there until an operation deadline fires, which in a
    test is a hang rather than a failure.

    The store this models detects the cycle and aborts one. So does this.
    """

    async def test_one_side_is_refused(self) -> None:
        state = MockState()
        ctx = _ctx(state)
        took_first = [asyncio.Event(), asyncio.Event()]
        outcomes: list[str] = []

        async def writer(index: int, first: str, second: str) -> None:
            tx = _tx(state)

            try:
                async with tx.transaction():
                    cmd = ctx.doc.command(_spec(BY_OWNER))
                    await cmd.create(_BookingCreate(owner=first))
                    took_first[index].set()
                    # Both hold one owner before either asks for the other's.
                    await took_first[1 - index].wait()
                    await cmd.create(_BookingCreate(owner=second))

                outcomes.append(f"{index}:ok")

            except CoreException:
                outcomes.append(f"{index}:refused")

        await asyncio.wait_for(
            asyncio.gather(writer(0, "o1", "o2"), writer(1, "o2", "o1")),
            timeout=5,
        )

        # Exactly one gave way; the other finished. A hang would have tripped the timeout.
        assert sorted(outcomes) == ["0:ok", "1:refused"] or sorted(outcomes) == [
            "0:refused",
            "1:ok",
        ], outcomes
        assert state.write_serialization.held() == ()

    async def test_the_same_two_owners_in_one_order_both_succeed(self) -> None:
        # The contrast: the refusal is about the cycle, not about two transactions sharing
        # owners. Taken in the same order they queue and both commit.
        state = MockState()
        ctx = _ctx(state)
        outcomes: list[str] = []

        async def writer(index: int) -> None:
            tx = _tx(state)

            async with tx.transaction():
                cmd = ctx.doc.command(_spec(BY_OWNER))
                await cmd.create(_BookingCreate(owner="o1"))
                await cmd.create(_BookingCreate(owner="o2"))

            outcomes.append(f"{index}:ok")

        await asyncio.wait_for(asyncio.gather(writer(0), writer(1)), timeout=5)

        assert sorted(outcomes) == ["0:ok", "1:ok"]


# ....................... #


def _key_of(owner: str) -> int:
    """The lock key the adapter derives for *owner* under `BY_OWNER` on `bookings`."""

    return advisory_lock_key("bookings", None, "owner", owner)


def _owner_pair(*, destination_last: bool) -> tuple[str, str]:
    """A source and destination owner whose keys sort in the order asked for.

    Locks are taken in key order, and each defect these pin shows only when the owner that
    matters is the one taken second.
    """

    source = "src"

    for candidate in (f"dst{index}" for index in range(100)):
        if (_key_of(candidate) > _key_of(source)) is destination_last:
            return source, candidate

    raise AssertionError("no destination in that order")  # pragma: no cover


def _owners_where_destination_sorts_last() -> tuple[str, str]:
    return _owner_pair(destination_last=True)


def _owners_where_destination_sorts_first() -> tuple[str, str]:
    return _owner_pair(destination_last=False)


class TestAWriteOutsideATransactionWaitsForEveryOwner:
    """Outside a transaction a write holds nothing afterwards — but it still has to wait.

    Every owner it touches must be free at the moment it lands, and a write can touch more than
    one: an update touches the owner it leaves and the one it joins. Waiting on only the first
    in key order lets a move land in an owner a transaction is still holding.
    """

    async def test_a_move_into_a_held_owner_waits_for_it(self) -> None:
        state = MockState()
        ctx = _ctx(state)
        plain = ctx.doc.command(_spec())
        source, destination = _owners_where_destination_sorts_last()
        row = await plain.create(_BookingCreate(owner=source))

        held = asyncio.Event()
        order: list[str] = []

        async def holder() -> None:
            async with _tx(state).transaction():
                await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner=destination))
                held.set()
                await asyncio.sleep(0.05)
                order.append("holder:done")

        async def mover() -> None:
            await held.wait()
            # No transaction: the destination is the second key in order.
            await ctx.doc.command(_spec(BY_OWNER)).update(
                row.id, row.rev, _BookingUpdate(owner=destination)
            )
            order.append("mover:moved")

        await asyncio.wait_for(asyncio.gather(holder(), mover()), timeout=5)

        assert order == ["holder:done", "mover:moved"], order

    async def test_the_first_owner_stays_held_while_the_write_waits_for_the_next(self) -> None:
        # Waiting on every owner is half of it; holding them at once is the other. A write that
        # gives each owner back as soon as it has it leaves the first one free while it waits
        # for the second, and a transaction taking the first then has the move land inside it.
        state = MockState()
        ctx = _ctx(state)
        source, destination = _owners_where_destination_sorts_last()
        row = await ctx.doc.command(_spec()).create(_BookingCreate(owner=source))

        held = asyncio.Event()
        order: list[str] = []

        async def holder() -> None:
            async with _tx(state).transaction():
                await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner=destination))
                held.set()
                await asyncio.sleep(0.05)

        async def mover() -> None:
            await held.wait()
            await ctx.doc.command(_spec(BY_OWNER)).update(
                row.id, row.rev, _BookingUpdate(owner=destination)
            )
            order.append("mover:moved")

        async def taker() -> None:
            await held.wait()
            await asyncio.sleep(0.01)
            async with _tx(state).transaction():
                await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner=source))
                order.append("taker:in")
                await asyncio.sleep(0.1)
                order.append("taker:out")

        await asyncio.wait_for(asyncio.gather(holder(), mover(), taker()), timeout=5)

        assert order == ["mover:moved", "taker:in", "taker:out"], order

    @pytest.mark.parametrize("destination_last", [False, True])
    async def test_an_upsert_racing_a_delete_still_upserts(self, destination_last: bool) -> None:
        # The upsert decides its arm inside a section and delegates; if it takes its owners in
        # separate calls, or gives them back before the delegated write, the delegated call
        # waits inside the section it already decided in. A delete landing there turns the
        # upsert into "not found" — a result the operation does not have.
        #
        # Both orders, because each exposes a different half: destination first, a delete lands
        # while the upsert waits and the upsert must see it gone and create; destination last,
        # the delete queues behind the upsert and must still be queued when the upsert writes.
        state = MockState()
        ctx = _ctx(state)
        source, destination = _owner_pair(destination_last=destination_last)
        row = await ctx.doc.command(_spec()).create(_BookingCreate(owner=source))

        held = asyncio.Event()

        async def holder() -> None:
            async with _tx(state).transaction():
                await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner=destination))
                held.set()
                await asyncio.sleep(0.05)

        async def upserter() -> Any:
            await held.wait()
            return await ctx.doc.command(_spec(BY_OWNER)).upsert(
                row.id, _BookingCreate(owner=source), _BookingUpdate(owner=destination)
            )

        async def deleter() -> None:
            await held.wait()
            await asyncio.sleep(0.01)
            await ctx.doc.command(_spec(BY_OWNER)).kill(row.id)

        results = await asyncio.wait_for(
            asyncio.gather(holder(), upserter(), deleter(), return_exceptions=True), timeout=5
        )

        assert not [r for r in results if isinstance(r, BaseException)], results

        if destination_last:
            # The upsert held the row's owner while it waited: it moved the row, then the delete
            # took it.
            assert results[1].owner == destination
            assert row.id not in state.documents["bookings"]

        else:
            # The delete got in while the upsert waited, and the upsert saw the row gone.
            assert results[1].owner == source
            assert row.id in state.documents["bookings"]

    async def test_a_cycle_through_a_write_outside_a_transaction_is_refused(self) -> None:
        # A write outside a transaction still holds one owner while it waits for the next. A
        # transaction that then wants the first owner closes a cycle through it, and only a
        # holder the cycle check can see — what it holds and what it waits for — is refused
        # instead of hanging both.
        state = MockState()
        ctx = _ctx(state)
        first, second = _owner_pair(destination_last=True)
        row = await ctx.doc.command(_spec()).create(_BookingCreate(owner=first))

        held = asyncio.Event()

        async def transaction() -> None:
            async with _tx(state).transaction():
                command = ctx.doc.command(_spec(BY_OWNER))
                await command.create(_BookingCreate(owner=second))
                held.set()
                await asyncio.sleep(0.02)
                await command.create(_BookingCreate(owner=first))

        async def mover() -> Any:
            await held.wait()
            return await ctx.doc.command(_spec(BY_OWNER)).update(
                row.id, row.rev, _BookingUpdate(owner=second)
            )

        refused, moved = await asyncio.wait_for(
            asyncio.gather(transaction(), mover(), return_exceptions=True), timeout=2
        )

        assert isinstance(refused, CoreException), refused
        assert refused.kind is ExceptionKind.CONFLICT
        assert moved.owner == second

    async def test_a_chain_through_a_key_just_given_back_is_not_a_cycle(self) -> None:
        # Between a release and the woken waiter resuming, the waiter still says it waits for a
        # key nobody holds. Nothing closes a cycle through a free key, so the walk stops there
        # instead of reading the holder that is not there. Driven directly: the window is one
        # scheduler step wide and no ordering of tasks lands in it reliably.
        state = MockState()
        port = _ctx(state).doc.command(_spec(BY_OWNER))
        state.write_serialization.hold(1, StatementLocks(waiting_for=2))

        port._refuse_lock_cycle(StatementLocks(write_locks={3: None}), 1)

        with pytest.raises(CoreException) as caught:
            port._refuse_lock_cycle(StatementLocks(write_locks={2: None}), 1)

        assert caught.value.kind is ExceptionKind.CONFLICT

    async def test_a_row_that_changes_owner_while_the_write_waits_is_held_by_its_new_one(
        self,
    ) -> None:
        # The owner is read off the stored row before the wait. A transaction holding both
        # owners can move the row meanwhile; once it commits, the row's owner is one this write
        # never took — and another transaction may be holding it.
        state = MockState()
        ctx = _ctx(state)
        row = await ctx.doc.command(_spec()).create(_BookingCreate(owner="old"))

        moving = asyncio.Event()
        order: list[str] = []

        async def mover() -> None:
            async with _tx(state).transaction():
                command = ctx.doc.command(_spec(BY_OWNER))
                # "new" first, so it is released first and the next holder of "new" wakes
                # before the waiting delete does.
                await command.create(_BookingCreate(owner="new"))
                await command.update(row.id, row.rev, _BookingUpdate(owner="new"))
                moving.set()
                await asyncio.sleep(0.02)

        async def next_holder() -> None:
            await moving.wait()
            async with _tx(state).transaction():
                await ctx.doc.command(_spec(BY_OWNER)).create(_BookingCreate(owner="new"))
                await asyncio.sleep(0.05)
                order.append("holder:done")

        async def deleter() -> None:
            await moving.wait()
            await ctx.doc.command(_spec(BY_OWNER)).kill(row.id)
            order.append("delete:done")

        await asyncio.wait_for(asyncio.gather(mover(), next_holder(), deleter()), timeout=5)

        assert order == ["holder:done", "delete:done"], order
