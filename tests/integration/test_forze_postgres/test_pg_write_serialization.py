"""Writes for one owner do not interleave on Postgres, on every path that writes.

The in-memory store's battery, replayed against two real connections: one transaction holds an
owner and sleeps, and the question each leg asks is whether the other connection's write for that
owner landed while it was held. The lock is a transaction-scoped advisory lock, so the answer is
the database's, not the adapter's bookkeeping — and one leg reads it back out of ``pg_locks``
under the key the in-memory store derives, so the two cannot drift apart unnoticed.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from testcontainers.postgres import PostgresContainer

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedCreate,
    KeyedUpdate,
    UpsertItem,
)
from forze.application.contracts.guarantees import SerializedBy
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import advisory_lock_key
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.kernel.client import PostgresConfig
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

# ----------------------- #

BY_OWNER = SerializedBy(key=("owner",))

HOLD = 0.6
"""How long the holder keeps its transaction open after writing."""

LAG = 0.15
"""How long a read-then-write handler waits between its read and its write."""


class _Booking(Document):
    owner: str
    label: str = ""


class _BookingRead(ReadDocument):
    owner: str
    label: str = ""


class _BookingCreate(CreateDocumentCmd):
    owner: str
    label: str = ""


class _BookingUpdate(BaseDTO):
    owner: str | None = None
    label: str | None = None


def _spec(name: str, *guarantees: SerializedBy) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec[_BookingRead, _Booking, _BookingCreate, _BookingUpdate](
        name=name,
        read=_BookingRead,
        write=DocumentWriteTypes(
            domain=_Booking, create_cmd=_BookingCreate, update_cmd=_BookingUpdate
        ),
        guarantees=guarantees,
    )


async def _table(pg_client: PostgresClient) -> str:
    name = f"bookings_{uuid4().hex[:10]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {name} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            owner text NOT NULL,
            label text NOT NULL DEFAULT ''
        );
        """
    )

    return name


def _ctx(client: PostgresClient, table: str) -> ExecutionContext:
    doc = PostgresDocumentConfig(
        read=("public", table), write=("public", table), bookkeeping_strategy="application"
    )

    return context_from_deps(
        PostgresDepsModule(client=client, rw_documents={table: doc}, tx={"postgres"})()
    )


@pytest.fixture
async def second_client(postgres_container: PostgresContainer) -> AsyncIterator[PostgresClient]:
    """A second pool, so the other writer is a different connection — a different session."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=3))

    yield client

    await client.close()


Write = Callable[[ExecutionContext], Awaitable[Any]]


async def _landed_while_held(
    pg_client: PostgresClient,
    second_client: PostgresClient,
    table: str,
    hold: Write,
    other: Write,
    *,
    other_in_transaction: bool = True,
) -> bool:
    """Whether *other*'s write landed while *hold*'s transaction still held its owners.

    Observed rather than timed: the other writer starts once the holder has written, so holds
    its locks, however long that took; each side records when it wrote and when the holder let
    go, and the question is only whether the second write came before the release.
    """

    order: list[str] = []
    holder_ctx = _ctx(pg_client, table)
    other_ctx = _ctx(second_client, table)
    wrote = asyncio.Event()

    async def holder() -> None:
        async with pg_client.transaction():
            await hold(holder_ctx)
            order.append("holder:wrote")
            wrote.set()
            await asyncio.sleep(HOLD)
            order.append("holder:done")

    async def writer() -> None:
        await wrote.wait()

        if other_in_transaction:
            async with second_client.transaction():
                await other(other_ctx)

        else:
            await other(other_ctx)

        order.append("other:wrote")

    await asyncio.wait_for(asyncio.gather(holder(), writer()), timeout=15)

    return order.index("other:wrote") < order.index("holder:done")


def _create(table: str, owner: str, *guarantees: SerializedBy) -> Write:
    async def run(ctx: ExecutionContext) -> None:
        await ctx.doc.command(_spec(table, *guarantees)).create(_BookingCreate(owner=owner))

    return run


# ....................... #


class TestOneOwnerAtATime:
    async def test_a_second_write_for_the_owner_waits(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)

        assert not await _landed_while_held(
            pg_client,
            second_client,
            table,
            _create(table, "o1", BY_OWNER),
            _create(table, "o1", BY_OWNER),
        )

    async def test_a_write_for_another_owner_does_not(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # The leg a table lock would fail: different owners proceed at once.
        table = await _table(pg_client)

        assert await _landed_while_held(
            pg_client,
            second_client,
            table,
            _create(table, "o1", BY_OWNER),
            _create(table, "o2", BY_OWNER),
        )

    async def test_an_undeclared_spec_serializes_nothing(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # The contrast: without the declaration the same two writes run together, so the first
        # leg measures the declaration rather than something else holding a lock.
        table = await _table(pg_client)

        assert await _landed_while_held(
            pg_client, second_client, table, _create(table, "o1"), _create(table, "o1")
        )

    async def test_a_write_outside_a_transaction_waits_too(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # A lone statement's lock would be released at the end of the statement that took it;
        # the gateway runs the write in a transaction of its own so the lock covers it.
        table = await _table(pg_client)

        assert not await _landed_while_held(
            pg_client,
            second_client,
            table,
            _create(table, "o1", BY_OWNER),
            _create(table, "o1", BY_OWNER),
            other_in_transaction=False,
        )


# ....................... #


FRESH_PATHS: dict[str, Callable[[Any, UUID], Awaitable[Any]]] = {
    "create": lambda cmd, pk: cmd.create(_BookingCreate(owner="o1"), id=pk),
    "create_many": lambda cmd, pk: cmd.create_many([_BookingCreate(owner="o1")]),
    "ensure": lambda cmd, pk: cmd.ensure(pk, _BookingCreate(owner="o1")),
    "ensure_many": lambda cmd, pk: cmd.ensure_many(
        [KeyedCreate(id=pk, payload=_BookingCreate(owner="o1"))]
    ),
    "upsert": lambda cmd, pk: cmd.upsert(
        pk, _BookingCreate(owner="o1"), _BookingUpdate(label="x")
    ),
    "upsert_many": lambda cmd, pk: cmd.upsert_many(
        [UpsertItem(id=pk, create=_BookingCreate(owner="o1"), update=_BookingUpdate(label="x"))]
    ),
}

SEEDED_PATHS: dict[str, Callable[[Any, Any], Awaitable[Any]]] = {
    "update": lambda cmd, row: cmd.update(row.id, row.rev, _BookingUpdate(label="x")),
    "update_many": lambda cmd, row: cmd.update_many(
        [KeyedUpdate(id=row.id, rev=row.rev, dto=_BookingUpdate(label="x"))]
    ),
    "update_matching": lambda cmd, row: cmd.update_matching(
        {"$values": {"id": row.id}}, _BookingUpdate(label="x")
    ),
    "touch": lambda cmd, row: cmd.touch(row.id),
    "touch_many": lambda cmd, row: cmd.touch_many([row.id]),
    "kill": lambda cmd, row: cmd.kill(row.id),
    "kill_many": lambda cmd, row: cmd.kill_many([row.id]),
}


class TestEveryWritePathWaits:
    @pytest.mark.parametrize("in_tx", [True, False], ids=["in-tx", "alone"])
    @pytest.mark.parametrize("name", sorted(FRESH_PATHS))
    async def test_a_path_carrying_its_row(
        self, name: str, in_tx: bool, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)
        path = FRESH_PATHS[name]

        async def other(ctx: ExecutionContext) -> None:
            await path(ctx.doc.command(_spec(table, BY_OWNER)), uuid4())

        assert not await _landed_while_held(
            pg_client,
            second_client,
            table,
            _create(table, "o1", BY_OWNER),
            other,
            other_in_transaction=in_tx,
        ), f"{name} did not wait"

    # Both in a caller's transaction and alone: alone, a path that forgot to open its own
    # would take a lock the end of its first statement releases.
    @pytest.mark.parametrize("in_tx", [True, False], ids=["in-tx", "alone"])
    @pytest.mark.parametrize("name", sorted(SEEDED_PATHS))
    async def test_a_path_naming_a_stored_row(
        self, name: str, in_tx: bool, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # The owner is on the stored row, not in the call: the path has to read it to know
        # what to wait for.
        table = await _table(pg_client)
        row = await _ctx(pg_client, table).doc.command(_spec(table)).create(
            _BookingCreate(owner="o1")
        )
        path = SEEDED_PATHS[name]

        async def other(ctx: ExecutionContext) -> None:
            await path(ctx.doc.command(_spec(table, BY_OWNER)), row)

        assert not await _landed_while_held(
            pg_client,
            second_client,
            table,
            _create(table, "o1", BY_OWNER),
            other,
            other_in_transaction=in_tx,
        ), f"{name} did not wait"


# ....................... #


class TestAMoveHoldsBothOwners:
    @pytest.mark.parametrize("owner", ["from", "to"])
    async def test_a_write_to_either_side_waits(
        self, owner: str, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)
        row = await _ctx(pg_client, table).doc.command(_spec(table)).create(
            _BookingCreate(owner="from")
        )

        async def move(ctx: ExecutionContext) -> None:
            await ctx.doc.command(_spec(table, BY_OWNER)).update(
                row.id, row.rev, _BookingUpdate(owner="to")
            )

        assert not await _landed_while_held(
            pg_client, second_client, table, move, _create(table, owner, BY_OWNER)
        )


class TestALoneWriteHoldsItsLockUntilItLands:
    @pytest.mark.parametrize("name", ["create", "kill"])
    async def test_the_lock_is_still_held_after_it_is_taken(
        self, name: str, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # Outside a caller's transaction an advisory lock taken in a statement of its own is
        # released as that statement ends, and the write after it is unprotected. Waiting
        # still looks right from outside, so this pauses the write just after the lock and
        # asks the database whether it is still held.
        table = await _table(pg_client)
        row = await _ctx(pg_client, table).doc.command(_spec(table)).create(
            _BookingCreate(owner="o1")
        )
        key = advisory_lock_key(table, None, "owner", "o1") & 0xFFFF_FFFF_FFFF_FFFF
        taken = asyncio.Event()
        release = asyncio.Event()
        original = PostgresClient.execute

        async def pausing(self: PostgresClient, query: Any, *args: Any, **kwargs: Any) -> Any:
            result = await original(self, query, *args, **kwargs)

            if self is second_client and "pg_advisory_xact_lock" in str(query):
                taken.set()
                await release.wait()

            return result

        cmd = _ctx(second_client, table).doc.command(_spec(table, BY_OWNER))
        write = (
            cmd.create(_BookingCreate(owner="o1")) if name == "create" else cmd.kill(row.id)
        )

        with patch.object(PostgresClient, "execute", pausing):
            task = asyncio.create_task(write)
            await asyncio.wait_for(taken.wait(), timeout=10)
            rows = await pg_client.fetch_all(
                "SELECT classid::bigint AS hi, objid::bigint AS lo FROM pg_locks "
                "WHERE locktype = 'advisory' AND objsubid = 1 AND granted",
                [],
                row_factory="dict",
            )
            release.set()
            await asyncio.wait_for(task, timeout=10)

        assert (key >> 32, key & 0xFFFF_FFFF) in {(r["hi"], r["lo"]) for r in rows}, rows


class TestAnOwnerThatChangesDuringTheWait:
    async def test_the_write_waits_for_the_owner_the_row_has_now(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # The delete reads the row's owner before it waits. A transaction holding both owners
        # moves the row meanwhile; once it commits, the row's owner is one the delete never
        # took — and another transaction is already holding it.
        table = await _table(pg_client)
        row = await _ctx(pg_client, table).doc.command(_spec(table)).create(
            _BookingCreate(owner="old")
        )
        moving = asyncio.Event()
        order: list[str] = []

        async def mover() -> None:
            ctx = _ctx(pg_client, table)

            async with pg_client.transaction():
                await ctx.doc.command(_spec(table, BY_OWNER)).update(
                    row.id, row.rev, _BookingUpdate(owner="new")
                )
                moving.set()
                await asyncio.sleep(HOLD)

        async def next_holder() -> None:
            await moving.wait()
            await asyncio.sleep(0.1)
            ctx = _ctx(second_client, table)

            async with second_client.transaction():
                await ctx.doc.command(_spec(table, BY_OWNER)).create(_BookingCreate(owner="new"))
                order.append("holder:in")
                await asyncio.sleep(HOLD)
                order.append("holder:done")

        async def deleter() -> None:
            await moving.wait()
            await asyncio.sleep(0.2)
            await _ctx(second_client, table).doc.command(_spec(table, BY_OWNER)).kill(row.id)
            order.append("delete:done")

        await asyncio.wait_for(asyncio.gather(mover(), next_holder(), deleter()), timeout=15)

        assert order == ["holder:in", "holder:done", "delete:done"], order


# ....................... #


class TestACycleIsRefused:
    async def test_one_side_is_refused_as_concurrency(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # Two transactions each holding the owner the other wants. Postgres detects the
        # deadlock and aborts one; the kind is the one the in-memory store gives the same cycle.
        table = await _table(pg_client)
        took_first = [asyncio.Event(), asyncio.Event()]
        kinds: list[ExceptionKind] = []
        finished: list[int] = []

        async def writer(index: int, client: PostgresClient, first: str, second: str) -> None:
            ctx = _ctx(client, table)
            cmd = ctx.doc.command(_spec(table, BY_OWNER))

            try:
                async with client.transaction():
                    await cmd.create(_BookingCreate(owner=first))
                    took_first[index].set()
                    await took_first[1 - index].wait()
                    await cmd.create(_BookingCreate(owner=second))

                finished.append(index)

            except CoreException as caught:
                kinds.append(caught.kind)

        await asyncio.wait_for(
            asyncio.gather(
                writer(0, pg_client, "o1", "o2"), writer(1, second_client, "o2", "o1")
            ),
            timeout=15,
        )

        assert kinds == [ExceptionKind.CONCURRENCY], kinds
        assert len(finished) == 1, finished


# ....................... #


class TestTheLockIsTheOneTheMockTakes:
    async def test_pg_locks_shows_the_owner_key(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # Read back from the database while held: an advisory lock on exactly the key the
        # in-memory store derives for this spec, tenant and owner.
        table = await _table(pg_client)
        key = advisory_lock_key(table, None, "owner", "o1") & 0xFFFF_FFFF_FFFF_FFFF
        held = asyncio.Event()
        seen: list[tuple[int, int]] = []

        async def holder() -> None:
            async with pg_client.transaction():
                await _create(table, "o1", BY_OWNER)(_ctx(pg_client, table))
                held.set()
                await asyncio.sleep(HOLD)

        async def reader() -> None:
            await held.wait()
            rows = await second_client.fetch_all(
                "SELECT classid::bigint AS hi, objid::bigint AS lo FROM pg_locks "
                "WHERE locktype = 'advisory' AND objsubid = 1 AND granted",
                [],
                row_factory="dict",
            )
            seen.extend((row["hi"], row["lo"]) for row in rows)

        await asyncio.wait_for(asyncio.gather(holder(), reader()), timeout=15)

        assert (key >> 32, key & 0xFFFF_FFFF) in seen, seen


class _Priced(Document):
    price: Decimal


class _PricedRead(ReadDocument):
    price: Decimal


class _PricedCreate(CreateDocumentCmd):
    price: Decimal


class _PricedUpdate(BaseDTO):
    price: Decimal | None = None


# ....................... #

BY_SLOT = SerializedBy(key=("slot",))

_SLOT_UTC = datetime(2026, 3, 29, 9, 0, tzinfo=UTC)


class _Slot(Document):
    slot: datetime
    label: str = ""


class _SlotRead(ReadDocument):
    slot: datetime
    label: str = ""


class _SlotCreate(CreateDocumentCmd):
    slot: datetime
    label: str = ""


class _SlotUpdate(BaseDTO):
    slot: datetime | None = None
    label: str | None = None


def _slot_spec(name: str, *guarantees: SerializedBy) -> DocumentSpec[Any, Any, Any, Any]:
    return DocumentSpec[_SlotRead, _Slot, _SlotCreate, _SlotUpdate](
        name=name,
        read=_SlotRead,
        write=DocumentWriteTypes(domain=_Slot, create_cmd=_SlotCreate, update_cmd=_SlotUpdate),
        guarantees=guarantees,
    )


async def _client_in(container: PostgresContainer, zone: str) -> PostgresClient:
    """A pool whose sessions run in time zone *zone*, set rather than left to the server."""

    url = container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(
        dsn=f"{url}?options=-c%20TimeZone%3D{zone.replace('/', '%2F')}",
        config=PostgresConfig(min_size=1, max_size=3),
    )

    return client


@pytest.fixture
async def utc_writer(postgres_container: PostgresContainer) -> AsyncIterator[PostgresClient]:
    """The writer, in a UTC session: the one zone where Postgres stores a naive value as UTC."""

    client = await _client_in(postgres_container, "Etc/UTC")

    yield client

    await client.close()


@pytest.fixture(params=["Etc/UTC", "Asia/Tokyo"])
async def zoned_client(
    request: pytest.FixtureRequest, postgres_container: PostgresContainer
) -> AsyncIterator[PostgresClient]:
    """A second pool whose sessions read a ``timestamptz`` back in the given time zone."""

    client = await _client_in(postgres_container, str(request.param))

    yield client

    await client.close()


class TestAnInstantIsOneOwnerInAnyOffset:
    @pytest.mark.parametrize(
        "written",
        [_SLOT_UTC.astimezone(timezone(timedelta(hours=3))), _SLOT_UTC.replace(tzinfo=None)],
        ids=["+03:00", "naive"],
    )
    async def test_an_update_read_back_in_the_session_zone_waits_for_the_create(
        self, written: datetime, utc_writer: PostgresClient, zoned_client: PostgresClient
    ) -> None:
        # The create keys the caller's offset; the update keys the stored row, which the other
        # session reads back in its own zone. One instant, so one owner.
        table = f"slots_{uuid4().hex[:10]}"
        await utc_writer.execute(
            f"""
            CREATE TABLE {table} (
                id uuid PRIMARY KEY,
                rev integer NOT NULL,
                created_at timestamptz NOT NULL,
                last_update_at timestamptz NOT NULL,
                slot timestamptz NOT NULL,
                label text NOT NULL DEFAULT ''
            );
            """
        )
        row = await _ctx(utc_writer, table).doc.command(_slot_spec(table)).create(
            _SlotCreate(slot=written)
        )
        [stored] = await zoned_client.fetch_all(
            f"SELECT slot FROM {table}", [], row_factory="dict"
        )
        # The premise: the update sees the instant in an offset the create did not write.
        assert stored["slot"] == _SLOT_UTC
        assert stored["slot"].utcoffset() != written.utcoffset()

        async def create(ctx: ExecutionContext) -> None:
            await ctx.doc.command(_slot_spec(table, BY_SLOT)).create(_SlotCreate(slot=written))

        async def update(ctx: ExecutionContext) -> None:
            await ctx.doc.command(_slot_spec(table, BY_SLOT)).update(
                row.id, row.rev, _SlotUpdate(label="x")
            )

        assert not await _landed_while_held(utc_writer, zoned_client, table, create, update)



class TestAnInstantKeysAsItsColumnStoresIt:
    """A ``timestamptz(3)`` key rounds what a write sends: two instants it stores alike are one
    owner, whether a write sends one finer than the column keeps or reads it back as stored."""

    @pytest.mark.parametrize("other", ["create", "update", "move"])
    async def test_instants_stored_alike_take_one_lock(
        self, other: str, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        table = f"slots3_{uuid4().hex[:10]}"
        await pg_client.execute(
            f"""
            CREATE TABLE {table} (
                id uuid PRIMARY KEY,
                rev integer NOT NULL,
                created_at timestamptz NOT NULL,
                last_update_at timestamptz NOT NULL,
                slot timestamptz(3) NOT NULL,
                label text NOT NULL DEFAULT ''
            );
            """
        )
        # Both round to 09:00:00.123, which the stored row already holds.
        sent, alike = (_SLOT_UTC.replace(microsecond=us) for us in (123456, 123400))
        cmd = _ctx(pg_client, table).doc.command(_slot_spec(table))
        stored = await cmd.create(_SlotCreate(slot=alike))
        elsewhere = await cmd.create(_SlotCreate(slot=_SLOT_UTC + timedelta(hours=1)))

        async def create(ctx: ExecutionContext) -> None:
            await ctx.doc.command(_slot_spec(table, BY_SLOT)).create(_SlotCreate(slot=sent))

        async def write(ctx: ExecutionContext) -> None:
            command = ctx.doc.command(_slot_spec(table, BY_SLOT))

            if other == "create":
                await command.create(_SlotCreate(slot=alike))

            elif other == "update":
                await command.update(stored.id, stored.rev, _SlotUpdate(label="x"))

            else:
                await command.update(elsewhere.id, elsewhere.rev, _SlotUpdate(slot=alike))

        assert not await _landed_while_held(pg_client, second_client, table, create, write)


# ....................... #

def _hold(table: str, owner: str) -> Write:
    async def run(ctx: ExecutionContext) -> None:
        await ctx.doc.command(_spec(table, BY_OWNER)).hold({"owner": owner})

    return run


class TestAHoldTakesTheWriteLockWithoutWriting:
    async def test_a_write_for_the_held_owner_waits(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)

        assert not await _landed_while_held(
            pg_client, second_client, table, _hold(table, "o1"), _create(table, "o1", BY_OWNER)
        )

    async def test_a_write_for_another_owner_does_not(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)

        assert await _landed_while_held(
            pg_client, second_client, table, _hold(table, "o1"), _create(table, "o2", BY_OWNER)
        )

    async def test_a_hold_keys_the_owner_its_column_stores(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        # ``numeric(10,2)`` rounds both to 1.01, the owner a write for either takes.
        table = f"priced_{uuid4().hex[:10]}"
        await pg_client.execute(
            f"""
            CREATE TABLE {table} (
                id uuid PRIMARY KEY,
                rev integer NOT NULL,
                created_at timestamptz NOT NULL,
                last_update_at timestamptz NOT NULL,
                price numeric(10,2) NOT NULL
            );
            """
        )
        spec = DocumentSpec[_PricedRead, _Priced, _PricedCreate, _PricedUpdate](
            name=table,
            read=_PricedRead,
            write=DocumentWriteTypes(
                domain=_Priced, create_cmd=_PricedCreate, update_cmd=_PricedUpdate
            ),
            guarantees=(SerializedBy(key=("price",)),),
        )

        async def hold(ctx: ExecutionContext) -> None:
            await ctx.doc.command(spec).hold({"price": Decimal("1.005")})

        async def create(ctx: ExecutionContext) -> None:
            await ctx.doc.command(spec).create(_PricedCreate(price=Decimal("1.01")))

        assert not await _landed_while_held(pg_client, second_client, table, hold, create)

    @pytest.mark.parametrize("held", [True, False])
    async def test_two_read_then_write_handlers_decide_one_at_a_time(
        self, pg_client: PostgresClient, second_client: PostgresClient, held: bool
    ) -> None:
        # Each reads the owner's rows, decides, then writes. Without the hold both read
        # before either writes, and both decide to insert.
        table = await _table(pg_client)
        spec = _spec(table, BY_OWNER)

        async def handler(client: PostgresClient) -> None:
            ctx = _ctx(client, table)

            async with client.transaction():
                if held:
                    await ctx.doc.command(spec).hold({"owner": "o1"})

                taken = await ctx.doc.query(spec).count({"$values": {"owner": "o1"}})
                await asyncio.sleep(LAG)

                if not taken:
                    await ctx.doc.command(spec).create(_BookingCreate(owner="o1"))

        await asyncio.wait_for(
            asyncio.gather(handler(pg_client), handler(second_client)), timeout=15
        )

        rows = await _ctx(pg_client, table).doc.query(spec).count({"$values": {"owner": "o1"}})
        assert rows == (1 if held else 2)

    async def test_a_write_after_the_hold_in_its_transaction_re_enters_it(
        self, pg_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)
        cmd = _ctx(pg_client, table).doc.command(_spec(table, BY_OWNER))

        async with pg_client.transaction():
            await cmd.hold({"owner": "o1"})
            row = await asyncio.wait_for(cmd.create(_BookingCreate(owner="o1")), timeout=5)

        assert row.owner == "o1"

    async def test_outside_a_transaction_it_is_refused(self, pg_client: PostgresClient) -> None:
        # Released with its own statement, a hold would end before the read it is for.
        table = await _table(pg_client)
        cmd = _ctx(pg_client, table).doc.command(_spec(table, BY_OWNER))

        with pytest.raises(CoreException) as refused:
            await cmd.hold({"owner": "o1"})

        assert refused.value.code == "document_hold_outside_transaction"

    async def test_pg_locks_shows_the_key_a_write_takes(
        self, pg_client: PostgresClient, second_client: PostgresClient
    ) -> None:
        table = await _table(pg_client)
        key = advisory_lock_key(table, None, "owner", "o1") & 0xFFFF_FFFF_FFFF_FFFF
        held = asyncio.Event()
        seen: list[tuple[int, int]] = []

        async def holder() -> None:
            async with pg_client.transaction():
                await _hold(table, "o1")(_ctx(pg_client, table))
                held.set()
                await asyncio.sleep(HOLD)

        async def reader() -> None:
            await held.wait()
            rows = await second_client.fetch_all(
                "SELECT classid::bigint AS hi, objid::bigint AS lo FROM pg_locks "
                "WHERE locktype = 'advisory' AND objsubid = 1 AND granted",
                [],
                row_factory="dict",
            )
            seen.extend((row["hi"], row["lo"]) for row in rows)

        await asyncio.wait_for(asyncio.gather(holder(), reader()), timeout=15)

        assert (key >> 32, key & 0xFFFF_FFFF) in seen, seen
