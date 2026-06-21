"""Journal/MVCC atomicity across ALL participating stores (hardening).

The default journal manager (and the MVCC overlay for snapshot/serializable) must leave a
rolled-back transaction with NO partial writes — not just for documents, but for the outbox
``list`` and inbox ``set`` too (the outbox pattern's whole point: the outbox row commits
atomically with the business write). Identity, mutated in place, is reverted by a coarse deep
snapshot. Unlike the strict manager, this stays concurrency-preserving: a rollback removes
only *this* transaction's entries, never clobbering a concurrent transaction's committed ones.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.outbox import OutboxSpec
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.base.serialization import PydanticModelCodec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst.runtime import run_simulation
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
    write=DocumentWriteTypes(domain=Thing, create_cmd=ThingCreate, update_cmd=ThingUpdate),
)


class _EventPayload(BaseModel):
    note: str


OUTBOX_SPEC = OutboxSpec(name="things-events", codec=PydanticModelCodec(_EventPayload))


class _Boom(RuntimeError):
    pass


def _journal_ctx(state: MockState) -> ExecutionContext:
    # The default manager is the journal manager.
    return context_from_modules(MockDepsModule(state=state))


async def _write_all(ctx: ExecutionContext) -> None:
    await ctx.document.command(SPEC).create(ThingCreate(name="a"))
    outbox = ctx.outbox.command(OUTBOX_SPEC)
    await outbox.stage("thing.created", _EventPayload(note="n"))
    await outbox.flush()
    inbox = ctx.deps.provide(InboxDepKey)(ctx, InboxSpec(name="consumer"))
    await inbox.mark_if_unseen("orders", "msg-1")


def _counts(state: MockState) -> tuple[int, int, int]:
    docs = len(state.documents.get("things", {}))
    rows = len(state.outbox_rows.get("things-events", []))
    return docs, rows, len(state.inbox)


# ....................... #


async def test_read_committed_rollback_reverts_document_outbox_and_inbox() -> None:
    state = MockState()
    ctx = _journal_ctx(state)

    with pytest.raises(_Boom):
        async with ctx.tx_ctx.scope("mock"):
            await _write_all(ctx)
            assert _counts(state) == (1, 1, 1)  # visible inside the tx
            raise _Boom()

    # All three participating stores rolled back — not just documents.
    assert _counts(state) == (0, 0, 0)


async def test_serializable_rollback_reverts_outbox_and_inbox() -> None:
    state = MockState()
    ctx = _journal_ctx(state)

    with pytest.raises(_Boom):
        async with ctx.tx_ctx.scope("mock", isolation=IsolationLevel.SERIALIZABLE):
            await _write_all(ctx)
            raise _Boom()

    # Documents discarded via the MVCC overlay; outbox/inbox undone via the journal.
    assert _counts(state) == (0, 0, 0)


async def test_commit_persists_all_participating_stores() -> None:
    state = MockState()
    ctx = _journal_ctx(state)

    async with ctx.tx_ctx.scope("mock"):
        await _write_all(ctx)

    assert _counts(state) == (1, 1, 1)


def test_concurrent_rollback_keeps_the_committed_transactions_outbox_row() -> None:
    # Two transactions interleave: both stage an outbox row before either finishes. One
    # commits, the other rolls back. Per-entry undo removes ONLY the loser's row — a
    # whole-store restore would have wiped the committed one too.
    state = MockState()
    ctx = _journal_ctx(state)

    async def worker(note: str, *, fail: bool) -> None:
        try:
            async with ctx.tx_ctx.scope("mock"):
                outbox = ctx.outbox.command(OUTBOX_SPEC)
                await outbox.stage("thing.created", _EventPayload(note=note))
                await outbox.flush()
                if fail:
                    raise _Boom()
        except _Boom:
            pass

    async def scenario() -> None:
        await asyncio.gather(
            worker("kept", fail=False), worker("rolled-back", fail=True)
        )

    run_simulation(scenario, seed=0, schedule_seed=0)

    rows = state.outbox_rows.get("things-events", [])
    notes = {row.payload["note"] for row in rows}
    assert notes == {"kept"}


def test_identity_snapshot_restore_reverts_in_place_mutation() -> None:
    state = MockState()
    state.identity["authn"] = {"sessions": {"s1": {"revoked_at": None}}}

    snapshot = state.snapshot_identity()
    # Mutate in place (the pattern per-key journaling can't capture) + add a record.
    state.identity["authn"]["sessions"]["s1"]["revoked_at"] = "2026-01-01"
    state.identity["authn"]["sessions"]["s2"] = {"revoked_at": None}

    state.restore_identity(snapshot)

    assert state.identity["authn"] == {"sessions": {"s1": {"revoked_at": None}}}
