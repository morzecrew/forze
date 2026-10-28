"""``MockDepsModule(row_locks="block")``: a held row parks its contender, as Postgres does.

Claim mode (the default) aborts whichever of two lockers commits second, so a correct lock order
and a deadlock-prone one look the same under simulation: both abort. Blocking mode tells them
apart — the same order waits and succeeds, opposite orders close a cycle and one side is refused
with ``deadlock_detected``. Both legs run on the simulation loop, because that is the only place
the mock blocks; the fallback leg runs on a plain loop and must behave like claim mode.
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import sys
import weakref
from collections.abc import Awaitable, Callable
from typing import Any, Literal
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.guarantees import SerializedBy
from forze.application.contracts.transaction import IsolationLevel
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules
from forze_dst.runtime import run_simulation
from forze_mock import MockDepsModule, MockState
from forze_mock.state import RowLockTable

# ----------------------- #

Mode = Literal["claim", "block"]


class _Account(Document):
    value: int


class _AccountRead(ReadDocument):
    value: int


class _AccountCreate(CreateDocumentCmd):
    value: int


class _AccountUpdate(BaseDTO):
    value: int | None = None


SPEC = DocumentSpec[_AccountRead, _Account, _AccountCreate, _AccountUpdate](
    name="accounts",
    read=_AccountRead,
    write=DocumentWriteTypes(domain=_Account, create_cmd=_AccountCreate, update_cmd=_AccountUpdate),
)


class _Booking(Document):
    owner: str
    value: int = 0


class _BookingRead(ReadDocument):
    owner: str
    value: int = 0


class _BookingCreate(CreateDocumentCmd):
    owner: str


class _BookingUpdate(BaseDTO):
    value: int | None = None


BOOKINGS = DocumentSpec[_BookingRead, _Booking, _BookingCreate, _BookingUpdate](
    name="bookings",
    read=_BookingRead,
    write=DocumentWriteTypes(domain=_Booking, create_cmd=_BookingCreate, update_cmd=_BookingUpdate),
    guarantees=(SerializedBy(key=("owner",)),),
)
"""Writes serialized per owner: a create holds its owner's lock until the transaction ends."""


async def _seed(module: MockDepsModule, n: int) -> list[UUID]:
    ctx = context_from_modules(module)

    return [(await ctx.doc.command(SPEC).create(_AccountCreate(value=0))).id for _ in range(n)]


async def _bump_in_order(module: MockDepsModule, order: list[UUID]) -> None:
    """Lock each row in *order* and add one to it, in one transaction."""

    ctx = context_from_modules(module)

    async with ctx.tx_ctx.scope("mock"):
        for pk in order:
            row = await ctx.doc.query(SPEC).get(pk, for_update=True)
            await asyncio.sleep(0)
            await ctx.doc.command(SPEC).update(row.id, row.rev, _AccountUpdate(value=row.value + 1))


def _two_lockers(
    *,
    opposite: bool,
    mode: Mode = "block",
    schedule_seed: int | None = None,
) -> dict[str, Any]:
    """Two transactions bump rows A and B, in the same or in opposite orders."""

    out: dict[str, Any] = {}

    async def scenario() -> None:
        module = MockDepsModule(state=MockState(), row_locks=mode)
        a, b = await _seed(module, 2)

        async def run(name: str, order: list[UUID]) -> None:
            try:
                await _bump_in_order(module, order)
                out[name] = "committed"

            except CoreException as error:
                out[name] = error

        await asyncio.gather(run("first", [a, b]), run("second", [b, a] if opposite else [a, b]))

        ctx = context_from_modules(module)
        out["values"] = [(await ctx.doc.query(SPEC).get(pk)).value for pk in (a, b)]
        out["rows"] = {f"accounts/{a}", f"accounts/{b}"}
        out["held"] = dict(module.state.row_locks.holders())

    run_simulation(scenario, seed=0, schedule_seed=schedule_seed)

    return out


# ....................... #


class TestSameOrderWaits:
    def test_the_second_waits_then_builds_on_the_first(self) -> None:
        out = _two_lockers(opposite=False)

        assert out["first"] == out["second"] == "committed"
        # Each row carries both bumps: the second read what the first committed, not a stale row.
        assert out["values"] == [2, 2]
        assert out["held"] == {}, "a finished transaction kept a row lock"

    def test_claim_mode_aborts_the_same_workload(self) -> None:
        # The contrast the option exists for: correct code under contention reports an abort.
        out = _two_lockers(opposite=False, mode="claim")

        assert "committed" in (out["first"], out["second"])
        assert any(isinstance(out[name], CoreException) for name in ("first", "second"))


class TestOppositeOrdersDeadlock:
    def test_one_side_is_refused_naming_both_rows(self) -> None:
        out = _two_lockers(opposite=True)
        refused = [out[name] for name in ("first", "second") if out[name] != "committed"]

        assert len(refused) == 1, f"exactly one victim expected, got {out}"

        error = refused[0]

        assert isinstance(error, CoreException)
        assert error.kind is ExceptionKind.CONCURRENCY
        assert error.code == "deadlock_detected"
        assert set(error.details["rows"]) == out["rows"]
        # The survivor committed both bumps; the victim's were rolled back.
        assert out["values"] == [1, 1]
        assert out["held"] == {}

    @pytest.mark.parametrize("schedule_seed", range(6))
    def test_the_victim_is_a_function_of_the_seed(self, schedule_seed: int) -> None:
        def victim(out: dict[str, Any]) -> tuple[str, ...]:
            return tuple(name for name in ("first", "second") if out[name] != "committed")

        once = _two_lockers(opposite=True, schedule_seed=schedule_seed)
        again = _two_lockers(opposite=True, schedule_seed=schedule_seed)

        assert victim(once) == victim(again)


# ....................... #


async def _hold_then(
    module: MockDepsModule,
    pk: UUID,
    log: list[str],
    *,
    fail: bool = False,
    write: bool = True,
) -> None:
    """Lock *pk*, write it when *write*, keep it a while, then commit (or roll back when *fail*)."""

    ctx = context_from_modules(module)

    try:
        async with ctx.tx_ctx.scope("mock"):
            row = await ctx.doc.query(SPEC).get(pk, for_update=True)
            log.append("holder:locked")

            if write:
                await ctx.doc.command(SPEC).update(
                    row.id, row.rev, _AccountUpdate(value=row.value + 1)
                )

            await asyncio.sleep(1)

            if fail:
                raise RuntimeError("roll back")

        log.append("holder:committed")

    except RuntimeError:
        log.append("holder:rolled back")


def _race(
    mode: Mode,
    contender: Callable[[MockDepsModule, UUID, list[str]], Awaitable[None]],
    *,
    fail: bool = False,
    write: bool = True,
) -> tuple[list[str], int]:
    """A holder locks one row and keeps it; *contender* arrives while it is held."""

    log: list[str] = []
    out: dict[str, int] = {}

    async def scenario() -> None:
        module = MockDepsModule(state=MockState(), row_locks=mode)
        (pk,) = await _seed(module, 1)

        async def late() -> None:
            await asyncio.sleep(0.1)
            await contender(module, pk, log)

        await asyncio.gather(_hold_then(module, pk, log, fail=fail, write=write), late())

        out["value"] = (await context_from_modules(module).doc.query(SPEC).get(pk)).value

    run_simulation(scenario, seed=0)

    return log, out["value"]


async def _locking_read(module: MockDepsModule, pk: UUID, log: list[str]) -> None:
    ctx = context_from_modules(module)

    async with ctx.tx_ctx.scope("mock"):
        row = await ctx.doc.query(SPEC).get(pk, for_update=True)
        log.append(f"contender:read {row.value}")


async def _blind_write(module: MockDepsModule, pk: UUID, log: list[str]) -> None:
    ctx = context_from_modules(module)

    async with ctx.tx_ctx.scope("mock"):
        await ctx.doc.command(SPEC).update_matching(
            {"$values": {"id": {"$eq": pk}}}, _AccountUpdate(value=10), return_new=False
        )
        log.append("contender:wrote")


async def _statement_write(module: MockDepsModule, pk: UUID, log: list[str]) -> None:
    await (
        context_from_modules(module)
        .doc.command(SPEC)
        .update_matching(
            {"$values": {"id": {"$eq": pk}}}, _AccountUpdate(value=10), return_new=False
        )
    )
    log.append("contender:wrote")


async def _claim_statement_write(module: MockDepsModule, pk: UUID, log: list[str]) -> None:
    await _statement_write(MockDepsModule(state=module.state), pk, log)


class TestWhoWaits:
    def test_a_locking_read_waits_and_reads_the_commit(self) -> None:
        log, _ = _race("block", _locking_read)

        assert log == ["holder:locked", "holder:committed", "contender:read 1"]

    def test_a_write_waits_for_the_holder(self) -> None:
        log, value = _race("block", _blind_write)

        assert log == ["holder:locked", "holder:committed", "contender:wrote"]
        assert value == 10

    def test_a_statement_outside_a_transaction_waits_too(self) -> None:
        log, value = _race("block", _statement_write)

        assert log == ["holder:locked", "holder:committed", "contender:wrote"]
        assert value == 10

    def test_a_rollback_releases_the_row(self) -> None:
        log, value = _race("block", _locking_read, fail=True)

        # The contender reads the row as it was: the holder's write never landed.
        assert log == ["holder:locked", "holder:rolled back", "contender:read 0"]
        assert value == 0

    def test_claim_mode_never_waits(self) -> None:
        log, _ = _race("claim", _locking_read)

        assert log == ["holder:locked", "contender:read 0", "holder:committed"]

    def test_a_claim_module_sharing_the_state_never_waits(self) -> None:
        # The holder blocks; the statement comes from a claim-mode module over the same state.
        log, _ = _race("block", _claim_statement_write)

        assert log == ["holder:locked", "contender:wrote", "holder:committed"]


# ....................... #


class _IdValue(BaseModel):
    id: UUID
    value: int


def _by_id(pk: UUID) -> dict[str, Any]:
    return {"$values": {"id": {"$eq": pk}}}


async def _locked_value(ctx: Any, pk: UUID, reader: str) -> int:
    """The row's value through *reader*'s ``for_update`` form."""

    query = ctx.doc.query(SPEC)

    if reader == "get":
        return (await query.get(pk, for_update=True)).value

    if reader == "find":
        row = await query.find(_by_id(pk), for_update=True)

    elif reader == "project":
        return (await query.project(_by_id(pk), ["id", "value"], for_update=True))["value"]

    else:
        row = await query.select(_by_id(pk), _IdValue, for_update=True)

    return row.value


def _reading(
    reader: str,
    isolation: IsolationLevel = IsolationLevel.READ_COMMITTED,
    *,
    pause: float = 0,
) -> Callable[[MockDepsModule, UUID, list[str]], Awaitable[None]]:
    """A contender that opens a transaction at *isolation*, waits *pause*, then locks the row."""

    async def contender(module: MockDepsModule, pk: UUID, log: list[str]) -> None:
        ctx = context_from_modules(module)

        try:
            async with ctx.tx_ctx.scope("mock", isolation=isolation):
                await asyncio.sleep(pause)
                log.append(f"contender:read {await _locked_value(ctx, pk, reader)}")

        except CoreException as error:
            log.append(f"contender:{error.code}")

    return contender


READERS = ["get", "find", "project", "select"]
SNAPSHOT_LEVELS = [IsolationLevel.SNAPSHOT, IsolationLevel.SERIALIZABLE]


class TestLockingReadsReread:
    @pytest.mark.parametrize("reader", READERS)
    def test_read_committed_waits_then_reads_the_commit(self, reader: str) -> None:
        log, _ = _race("block", _reading(reader))

        assert log == ["holder:locked", "holder:committed", "contender:read 1"]

    @pytest.mark.parametrize("isolation", SNAPSHOT_LEVELS)
    @pytest.mark.parametrize("reader", READERS)
    def test_a_snapshot_lock_on_a_row_changed_while_waiting_fails(
        self, reader: str, isolation: IsolationLevel
    ) -> None:
        # Postgres: "could not serialize access due to concurrent update", not the stale row.
        log, value = _race("block", _reading(reader, isolation))

        assert log == ["holder:locked", "holder:committed", "contender:serialization_failure"]
        assert value == 1

    @pytest.mark.parametrize("isolation", SNAPSHOT_LEVELS)
    def test_a_snapshot_lock_on_a_row_changed_before_it_fails_too(
        self, isolation: IsolationLevel
    ) -> None:
        # No wait at all: the holder committed between this snapshot and the lock.
        log, _ = _race("block", _reading("get", isolation, pause=2))

        assert log == ["holder:locked", "holder:committed", "contender:serialization_failure"]

    @pytest.mark.parametrize(
        ("fail", "write", "outcome"),
        [(True, True, "rolled back"), (False, False, "committed")],
    )
    def test_a_snapshot_lock_on_an_unchanged_row_proceeds(
        self, fail: bool, write: bool, outcome: str
    ) -> None:
        # A holder that rolled back, or only locked, changed nothing: Postgres goes ahead.
        log, _ = _race("block", _reading("get", IsolationLevel.SNAPSHOT), fail=fail, write=write)

        assert log == ["holder:locked", f"holder:{outcome}", "contender:read 0"]


# ....................... #


class TestRetriesDoNotNest:
    def test_many_successive_waits_keep_the_stack_flat(self) -> None:
        # Each holder changes the row the finder is parked on, so every wait ends in a re-read
        # that finds the next row, held by the next holder. The finder runs under a recursion
        # limit far below the number of waits: a retry that nests per wait cannot finish.
        n = 300
        out: dict[str, Any] = {}

        async def scenario() -> None:
            module = MockDepsModule(state=MockState(), row_locks="block")
            await _seed(module, n)
            unbumped = {"$values": {"value": {"$eq": 0}}}
            page = (
                await context_from_modules(module)
                .doc.query(SPEC)
                .find_many(filters=unbumped, pagination={"limit": n})
            )

            async def holder(at: int, pk: UUID) -> None:
                ctx = context_from_modules(module)

                async with ctx.tx_ctx.scope("mock"):
                    row = await ctx.doc.query(SPEC).get(pk, for_update=True)
                    await asyncio.sleep(at)
                    await ctx.doc.command(SPEC).update(pk, row.rev, _AccountUpdate(value=1))

            async def finder() -> None:
                await asyncio.sleep(0.5)
                ctx = context_from_modules(module)
                limit = sys.getrecursionlimit()
                sys.setrecursionlimit(len(inspect.stack(0)) + 100)

                try:
                    async with ctx.tx_ctx.scope("mock"):
                        out["found"] = await ctx.doc.query(SPEC).find(unbumped, for_update=True)

                finally:
                    sys.setrecursionlimit(limit)

            await asyncio.gather(
                finder(), *(holder(at, hit.id) for at, hit in enumerate(page.hits, start=1))
            )

        run_simulation(scenario, seed=0)

        assert out["found"] is None


# ....................... #


class TestStatementKeepsItsRows:
    def test_a_statement_waiting_for_an_owner_keeps_its_row(self) -> None:
        # The statement takes the row, then waits for the owner a transaction holds. A locker
        # arriving meanwhile must wait for the statement, not lock the row under it and lose
        # the statement's write.
        log: list[str] = []
        out: dict[str, int] = {}

        async def scenario() -> None:
            module = MockDepsModule(state=MockState(), row_locks="block")
            ctx = context_from_modules(module)
            row = await ctx.doc.command(BOOKINGS).create(_BookingCreate(owner="o1"))

            async def owner_holder() -> None:
                octx = context_from_modules(module)

                async with octx.tx_ctx.scope("mock"):
                    await octx.doc.command(BOOKINGS).create(_BookingCreate(owner="o1"))
                    await asyncio.sleep(1)

                log.append("owner:committed")

            async def statement() -> None:
                await asyncio.sleep(0.1)
                await (
                    context_from_modules(module)
                    .doc.command(BOOKINGS)
                    .update(row.id, row.rev, _BookingUpdate(value=10))
                )
                log.append("statement:wrote")

            async def locker() -> None:
                await asyncio.sleep(0.2)
                lctx = context_from_modules(module)

                try:
                    async with lctx.tx_ctx.scope("mock"):
                        got = await lctx.doc.query(BOOKINGS).get(row.id, for_update=True)
                        log.append(f"locker:read {got.value}")
                        await lctx.doc.command(BOOKINGS).update(
                            got.id, got.rev, _BookingUpdate(value=got.value + 1)
                        )

                    log.append("locker:committed")

                except CoreException:
                    log.append("locker:refused")

            await asyncio.gather(owner_holder(), statement(), locker())
            out["value"] = (await ctx.doc.query(BOOKINGS).get(row.id)).value
            out["held"] = len(module.state.row_locks.holders())

        run_simulation(scenario, seed=0)

        assert log == ["owner:committed", "statement:wrote", "locker:read 10", "locker:committed"]
        assert out == {"value": 11, "held": 0}

    def test_an_owner_holder_parked_on_a_row_is_waited_for(self) -> None:
        # second wants the owner first holds; first is parked on a row a third holds. The walk
        # follows first's row wait, finds no way back to second, so second waits: no deadlock.
        log: list[str] = []

        async def scenario() -> None:
            module = MockDepsModule(state=MockState(), row_locks="block")
            (pk,) = await _seed(module, 1)

            async def booking(name: str, at: float, *, lock: bool = False) -> None:
                await asyncio.sleep(at)
                ctx = context_from_modules(module)

                async with ctx.tx_ctx.scope("mock"):
                    await ctx.doc.command(BOOKINGS).create(_BookingCreate(owner="o1"))

                    if lock:
                        await ctx.doc.query(SPEC).get(pk, for_update=True)

                log.append(f"{name}:committed")

            await asyncio.gather(
                _hold_then(module, pk, log),
                booking("first", 0.1, lock=True),
                booking("second", 0.2),
            )

        run_simulation(scenario, seed=0)

        assert log == ["holder:locked", "holder:committed", "first:committed", "second:committed"]


# ....................... #


class TestRowLockTable:
    def test_a_closed_loop_is_not_kept_alive(self) -> None:
        # A run that ended with a holder still in its map must not pin that loop for good.
        table = RowLockTable()

        async def hold() -> None:
            table.holders()[("accounts", 1)] = object()

        async def look() -> dict[tuple[str, Any], Any]:
            return table.holders()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(hold())
        loop.close()
        ref = weakref.ref(loop)
        del loop

        assert asyncio.run(look()) == {}

        gc.collect()

        assert ref() is None


# ....................... #


def _mixed_cycle(closing: Literal["owner", "row"]) -> dict[str, Any]:
    """A cycle of one row lock and one ``SerializedBy`` owner lock, closed by *closing*'s wait.

    ``first`` takes one lock and parks on the other, which ``second`` holds; ``second`` then
    asks for what ``first`` holds, so the wait that closes the cycle is of the *closing* kind.
    """

    out: dict[str, Any] = {}

    async def scenario() -> None:
        module = MockDepsModule(state=MockState(), row_locks="block")
        (row,) = await _seed(module, 1)
        out["row"] = f"accounts/{row}"

        async def lock_row(ctx: Any) -> None:
            await ctx.doc.query(SPEC).get(row, for_update=True)

        async def take_owner(ctx: Any) -> None:
            await ctx.doc.command(BOOKINGS).create(_BookingCreate(owner="o1"))

        held, wanted = (take_owner, lock_row) if closing == "owner" else (lock_row, take_owner)
        first_holds, second_holds = asyncio.Event(), asyncio.Event()

        async def first() -> None:
            ctx = context_from_modules(module)
            await second_holds.wait()

            try:
                async with ctx.tx_ctx.scope("mock"):
                    await held(ctx)
                    first_holds.set()
                    await wanted(ctx)

                out["first"] = "committed"

            except CoreException as error:
                out["first"] = error

        async def second() -> None:
            ctx = context_from_modules(module)

            try:
                async with ctx.tx_ctx.scope("mock"):
                    await wanted(ctx)
                    second_holds.set()
                    await first_holds.wait()
                    await asyncio.sleep(0.1)  # first is parked on what this holds by now
                    await held(ctx)

                out["second"] = "committed"

            except CoreException as error:
                out["second"] = error

        await asyncio.gather(first(), second())

    run_simulation(scenario, seed=0)

    return out


class TestMixedCycles:
    @pytest.mark.parametrize("closing", ["owner", "row"])
    def test_the_closing_side_is_refused_naming_the_row(
        self, closing: Literal["owner", "row"]
    ) -> None:
        out = _mixed_cycle(closing)

        assert out["first"] == "committed"

        error = out["second"]

        assert isinstance(error, CoreException), f"no victim: {out}"
        assert error.code == "deadlock_detected"
        assert error.details["rows"] == [out["row"]]


# ....................... #


class TestNothingHangs:
    def test_a_cancelled_waiter_leaves_the_graph(self) -> None:
        # The waiter holds B and parks on A. Cancelled, it must release B and stop waiting: the
        # holder then takes B without waiting and without being refused as a deadlock.
        log: list[str] = []

        async def scenario() -> None:
            module = MockDepsModule(state=MockState(), row_locks="block")
            a, b = await _seed(module, 2)
            took_a, parked = asyncio.Event(), asyncio.Event()

            async def waiter() -> None:
                ctx = context_from_modules(module)
                await took_a.wait()

                async with ctx.tx_ctx.scope("mock"):
                    await ctx.doc.query(SPEC).get(b, for_update=True)
                    parked.set()
                    await ctx.doc.query(SPEC).get(a, for_update=True)
                    log.append("waiter took a")

            # Spawned outside the holder's transaction, so it opens one of its own.
            task = asyncio.create_task(waiter())

            async def holder() -> None:
                ctx = context_from_modules(module)

                async with ctx.tx_ctx.scope("mock"):
                    await ctx.doc.query(SPEC).get(a, for_update=True)
                    took_a.set()
                    await parked.wait()
                    await asyncio.sleep(0.1)
                    task.cancel()

                    with pytest.raises(asyncio.CancelledError):
                        await task

                    await ctx.doc.query(SPEC).get(b, for_update=True)
                    log.append("holder took b")

            await holder()
            assert module.state.row_locks.holders() == {}

        run_simulation(scenario, seed=0)

        assert log == ["holder took b"]

    async def test_off_the_simulation_loop_block_falls_back_to_claim(self) -> None:
        # A plain loop cannot report a wait nothing ends, so nothing waits there.
        module = MockDepsModule(state=MockState(), row_locks="block")
        (pk,) = await _seed(module, 1)
        log: list[str] = []
        release = asyncio.Event()

        async def holder() -> None:
            ctx = context_from_modules(module)

            async with ctx.tx_ctx.scope("mock"):
                await ctx.doc.query(SPEC).get(pk, for_update=True)
                log.append("holder:locked")
                await release.wait()

            log.append("holder:committed")

        async def contender() -> None:
            await asyncio.sleep(0)
            await _locking_read(module, pk, log)
            release.set()

        await asyncio.wait_for(asyncio.gather(holder(), contender()), timeout=5)

        assert log == ["holder:locked", "contender:read 0", "holder:committed"]
        assert module.state.row_locks.holders() == {}


class TestConfiguration:
    @pytest.mark.parametrize(
        "kwargs",
        [{"transactions": "strict"}, {"transactions": "none"}, {"strict_tx": True}],
    )
    def test_block_needs_journal_transactions(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as caught:
            MockDepsModule(row_locks="block", **kwargs)

        assert caught.value.code == "mock_row_locks_need_journal"

    def test_an_unknown_mode_is_refused(self) -> None:
        # A typo must not run claim mode silently.
        with pytest.raises(CoreException) as caught:
            MockDepsModule(row_locks="blocking")  # type: ignore[arg-type]

        assert caught.value.code == "mock_row_locks_unknown"
