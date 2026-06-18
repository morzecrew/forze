"""Strict mock transactions: rollback, savepoints, serialization, read-only roots.

Every behavioral test in here would fail under the default no-op transaction
manager — that is the point of ``MockDepsModule(strict_tx=True)``: writes inside
a transaction that rolls back must not persist, so "forgot to run it in the same
transaction" bugs become visible under mock.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.queue import QueueCommandDepKey, QueueSpec
from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageSpec,
    UploadedObject,
)
from forze.application.execution import (
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class Thing(Document):
    name: str = "x"


class ThingCreate(CreateDocumentCmd):
    name: str = "x"


class ThingUpdate(BaseDTO):
    name: str | None = None


class ThingRead(ReadDocument):
    name: str


SPEC = DocumentSpec(
    name="things",
    read=ThingRead,
    write=DocumentWriteTypes(
        domain=Thing, create_cmd=ThingCreate, update_cmd=ThingUpdate
    ),
)


class _EventPayload(BaseModel):
    note: str


OUTBOX_SPEC = OutboxSpec(name="things-events", codec=PydanticModelCodec(_EventPayload))


class _Msg(BaseModel):
    value: str


class _Boom(RuntimeError):
    pass


# ....................... #


def _strict_ctx(state: MockState) -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=state, strict_tx=True))


def _docs(state: MockState) -> dict:
    return state.documents.get("things", {})


# ----------------------- #


class TestRootRollback:
    async def test_rollback_reverts_create(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await ctx.document.command(SPEC).create(ThingCreate(name="a"))
                assert len(_docs(state)) == 1  # visible inside the tx
                raise _Boom()

        assert _docs(state) == {}

    async def test_commit_persists(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)

        async with ctx.tx_ctx.scope("mock"):
            created = await ctx.document.command(SPEC).create(ThingCreate(name="a"))

        assert created.id in _docs(state)

    async def test_rollback_reverts_update_and_kill(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)
        port = ctx.document.command(SPEC)

        keep = await port.create(ThingCreate(name="keep"))
        gone = await port.create(ThingCreate(name="gone"))

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await port.update(keep.id, keep.rev, ThingUpdate(name="changed"))
                await port.kill(gone.id)
                raise _Boom()

        store = _docs(state)
        assert store[keep.id]["name"] == "keep"
        assert gone.id in store

    async def test_default_journal_mode_rolls_back_writes(self) -> None:
        # The default is now faithful atomicity: a failed transaction leaves no writes.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await ctx.document.command(SPEC).create(ThingCreate(name="a"))
                raise _Boom()

        assert len(_docs(state)) == 0  # rolled back

    async def test_none_mode_keeps_writes_on_rollback(self) -> None:
        # Opt-out: the legacy no-op manager still leaves partial writes on rollback.
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state, transactions="none"))

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await ctx.document.command(SPEC).create(ThingCreate(name="a"))
                raise _Boom()

        assert len(_docs(state)) == 1  # no-op manager: write persists


class TestOutboxAtomicity:
    async def test_rolled_back_tx_persists_no_outbox_rows(self) -> None:
        # THE bug class strict mode exists for: stage + flush ran in the same
        # transaction as the business write, the transaction rolled back — no
        # rows may survive.
        module = MockDepsModule(strict_tx=True)
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(_Boom):
                async with ctx.tx_ctx.scope("mock"):
                    outbox = ctx.outbox.command(OUTBOX_SPEC)
                    await outbox.stage("thing.created", _EventPayload(note="n"))
                    assert await outbox.flush() == 1
                    raise _Boom()

        assert module.state.outbox_rows.get("things-events", []) == []

    async def test_committed_tx_persists_outbox_rows(self) -> None:
        module = MockDepsModule(strict_tx=True)
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

        async with runtime.scope():
            ctx = runtime.get_context()

            async with ctx.tx_ctx.scope("mock"):
                outbox = ctx.outbox.command(OUTBOX_SPEC)
                await outbox.stage("thing.created", _EventPayload(note="n"))
                assert await outbox.flush() == 1

        assert len(module.state.outbox_rows["things-events"]) == 1


class TestInboxAtomicity:
    async def test_rolled_back_mark_allows_redelivery(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)
        inbox = ctx.deps.provide(InboxDepKey)(ctx, InboxSpec(name="consumer"))

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                assert await inbox.mark_if_unseen("orders", "msg-1") is True
                raise _Boom()

        assert state.inbox == set()
        # Redelivery re-processes: the mark is unseen again.
        assert await inbox.mark_if_unseen("orders", "msg-1") is True


class TestNestedSavepoints:
    async def test_inner_rollback_keeps_outer_writes(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)
        port = ctx.document.command(SPEC)

        async with ctx.tx_ctx.scope("mock"):
            outer = await port.create(ThingCreate(name="outer"))

            with pytest.raises(_Boom):
                async with ctx.tx_ctx.scope("mock"):  # savepoint
                    await port.create(ThingCreate(name="inner"))
                    raise _Boom()

            # Inner writes reverted, outer write intact, transaction continues.
            names = {d["name"] for d in _docs(state).values()}
            assert names == {"outer"}

            await port.create(ThingCreate(name="after"))

        names = {d["name"] for d in _docs(state).values()}
        assert names == {"outer", "after"}
        assert outer.id in _docs(state)


class TestNonParticipatingFidelity:
    async def test_queue_and_storage_survive_rollback(self) -> None:
        # Production-faithful: brokers and object storage are not transactional —
        # rolling them back with the DB would make the mock LESS faithful.
        state = MockState()
        ctx = _strict_ctx(state)

        queue = ctx.deps.provide(QueueCommandDepKey)(
            ctx, QueueSpec(name="jobs", codec=PydanticModelCodec(_Msg))
        )
        storage = ctx.deps.provide(StorageCommandDepKey)(ctx, StorageSpec(name="files"))

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await queue.enqueue("tasks", _Msg(value="x"))
                await storage.upload(UploadedObject(filename="f.txt", data=b"bytes"))
                raise _Boom()

        assert len(state.queues["jobs"]["tasks"]) == 1
        assert len(state.storage["files"]) == 1


class TestReadOnlyRoot:
    async def test_document_write_raises_read_only_tx(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)

        with pytest.raises(CoreException) as ei:
            async with ctx.tx_ctx.scope("mock", read_only=True):
                await ctx.document.command(SPEC).create(ThingCreate(name="a"))

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == "read_only_tx"
        assert _docs(state) == {}

    async def test_outbox_and_inbox_writes_raise_read_only_tx(self) -> None:
        module = MockDepsModule(strict_tx=True)
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(CoreException) as ei:
                async with ctx.tx_ctx.scope("mock", read_only=True):
                    outbox = ctx.outbox.command(OUTBOX_SPEC)
                    await outbox.stage("thing.created", _EventPayload(note="n"))
                    await outbox.flush()
            assert ei.value.code == "read_only_tx"

            inbox = ctx.deps.provide(InboxDepKey)(ctx, InboxSpec(name="consumer"))
            with pytest.raises(CoreException) as ei:
                async with ctx.tx_ctx.scope("mock", read_only=True):
                    await inbox.mark_if_unseen("orders", "msg-1")
            assert ei.value.code == "read_only_tx"

        assert module.state.outbox_rows.get("things-events", []) == []
        assert module.state.inbox == set()

    async def test_reads_are_fine_and_flag_resets(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)
        created = await ctx.document.command(SPEC).create(ThingCreate(name="a"))

        async with ctx.tx_ctx.scope("mock", read_only=True):
            found = await ctx.document.query(SPEC).get(created.id)
            assert found.id == created.id

        # The per-task flag is reset: writes work again outside the scope.
        await ctx.document.command(SPEC).create(ThingCreate(name="b"))
        assert len(_docs(state)) == 2


class TestConcurrency:
    async def test_root_transactions_serialize(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)
        port = ctx.document.command(SPEC)

        active = 0
        max_active = 0

        async def _tx(name: str) -> None:
            nonlocal active, max_active
            async with ctx.tx_ctx.scope("mock"):
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.01)
                await port.create(ThingCreate(name=name))
                await asyncio.sleep(0.01)
                active -= 1

        await asyncio.gather(_tx("t1"), _tx("t2"))

        assert max_active == 1  # roots serialized on the shared state
        names = {d["name"] for d in _docs(state).values()}
        assert names == {"t1", "t2"}

    async def test_rollback_in_one_task_does_not_corrupt_the_other(self) -> None:
        state = MockState()
        ctx = _strict_ctx(state)
        port = ctx.document.command(SPEC)

        async def _ok() -> None:
            async with ctx.tx_ctx.scope("mock"):
                await port.create(ThingCreate(name="ok"))

        async def _fail() -> None:
            with pytest.raises(_Boom):
                async with ctx.tx_ctx.scope("mock"):
                    await port.create(ThingCreate(name="fail"))
                    await asyncio.sleep(0.01)
                    raise _Boom()

        await asyncio.gather(_fail(), _ok())

        names = {d["name"] for d in _docs(state).values()}
        assert names == {"ok"}


@attrs.define(slots=True)
class _WriteDocStageOutboxAndRaise(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.command(SPEC).create(ThingCreate(name="doomed"))
        outbox = self.ctx.outbox.command(OUTBOX_SPEC)
        await outbox.stage("thing.created", _EventPayload(note="n"))
        await outbox.flush()
        raise _Boom()


class TestEndToEndOperation:
    async def test_failing_operation_rolls_back_doc_and_outbox(self) -> None:
        module = MockDepsModule(strict_tx=True)
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())

        reg = (
            OperationRegistry(
                handlers={"op": lambda c: _WriteDocStageOutboxAndRaise(ctx=c)}
            )
            .bind("op")
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(_Boom):
                await run_operation(reg, "op", None, ctx)

        assert module.state.documents.get("things", {}) == {}
        assert module.state.outbox_rows.get("things-events", []) == []


class TestUpsertRace:
    def test_concurrent_upserts_one_creates_one_updates(self) -> None:
        # The read-decide-write sequence is one critical section now: two
        # threads upserting the same id must not both observe "absent" (which
        # used to surface as a duplicate-create conflict).
        state = MockState()
        ctx = context_from_modules(MockDepsModule(state=state))
        port = ctx.document.command(SPEC)
        pk = uuid4()
        barrier = threading.Barrier(2)

        def _upsert(name: str) -> None:
            barrier.wait()
            asyncio.run(port.upsert(pk, ThingCreate(name=name), ThingUpdate(name=name)))

        for _ in range(20):  # repeat to give the race a real chance
            state.documents.pop("things", None)
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(_upsert, n) for n in ("one", "two")]
                for f in futures:
                    f.result()  # no conflict / no corruption

            store = state.documents["things"]
            assert set(store) == {pk}
            assert store[pk]["name"] in {"one", "two"}
