"""A device's cursor uses a deterministic id, so concurrent first-acks converge on one row.

Codacy MEDIUM: the old `advance` did find-then-`create` with a fresh ``uuid7``, so two
concurrent first-acks both saw no row and inserted **two** cursor records. The fix derives
the id from ``(principal, client_key)`` and uses ``ensure``, so the inserts converge on a
single row. Asserting the *id* (not just a row count) is the real regression guard — a
random ``uuid7`` would not equal :func:`_cursor_id`.
"""

from __future__ import annotations

from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import realtime_cursor_spec
from forze_kits.integrations.realtime.mailbox import DocumentMailboxCursors, _cursor_id
from forze_mock import MockDepsModule

_SPEC = realtime_cursor_spec()


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


async def test_cursor_row_uses_the_deterministic_id() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        query = ctx.document.query(_SPEC)
        cursors = DocumentMailboxCursors(command=ctx.document.command(_SPEC), query=query)

        await cursors.advance(
            principal="u1", client_key="dev", up_to=HlcTimestamp(physical_ms=3, logical=0)
        )

        row = await query.find(filters={"$values": {"principal": "u1", "client_key": "dev"}})

        # the id is derived from (principal, client_key), NOT a random uuid7 — so a concurrent
        # first-ack hits the same id and ensure is idempotent (one row, never a duplicate)
        assert row is not None
        assert row.id == _cursor_id("u1", "dev")


async def test_second_ack_advances_the_same_row_not_a_duplicate() -> None:
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        query = ctx.document.query(_SPEC)
        cursors = DocumentMailboxCursors(command=ctx.document.command(_SPEC), query=query)

        await cursors.advance(principal="u1", client_key="dev", up_to=HlcTimestamp(physical_ms=3, logical=0))
        await cursors.advance(principal="u1", client_key="dev", up_to=HlcTimestamp(physical_ms=9, logical=0))

        rows = await query.find_many(filters={"$values": {"principal": "u1", "client_key": "dev"}})
        assert len(rows.hits) == 1
        assert await cursors.get(principal="u1", client_key="dev") == HlcTimestamp(physical_ms=9, logical=0)
