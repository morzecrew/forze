"""``MockDepsModule(row_locks="block")``: a held row parks its contender, as Postgres does.

Claim mode (the default) aborts whichever of two lockers commits second, so a correct lock order
and a deadlock-prone one look the same under simulation: both abort. Blocking mode tells them
apart — the same order waits and succeeds, opposite orders close a cycle and one side is refused
with ``deadlock_detected``. Both legs run on the simulation loop, because that is the only place
the mock blocks; the fallback leg runs on a plain loop and must behave like claim mode.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Literal
from uuid import UUID

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze.testing import context_from_modules
from forze_dst.runtime import run_simulation
from forze_mock import MockDepsModule, MockState

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
) -> None:
    """Lock *pk*, write it, keep it a while, then commit (or roll back when *fail*)."""

    ctx = context_from_modules(module)

    try:
        async with ctx.tx_ctx.scope("mock"):
            row = await ctx.doc.query(SPEC).get(pk, for_update=True)
            log.append("holder:locked")
            await ctx.doc.command(SPEC).update(row.id, row.rev, _AccountUpdate(value=row.value + 1))
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

        await asyncio.gather(_hold_then(module, pk, log, fail=fail), late())

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
