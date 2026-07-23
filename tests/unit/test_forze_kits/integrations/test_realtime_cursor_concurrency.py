"""A device's cursor uses a deterministic id, so concurrent first-acks converge on one row.

Codacy MEDIUM: the old `advance` did find-then-`create` with a fresh ``uuid7``, so two
concurrent first-acks both saw no row and inserted **two** cursor records. The fix derives
the id from ``(principal, client_key)`` so the inserts converge on a single row, and the
loser of that insert reconciles up to the max via a monotonic update. Asserting the *id*
(not just a row count) is the real regression guard — a random ``uuid7`` would not equal
:func:`_cursor_id`.
"""

from __future__ import annotations

import asyncio

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

        # the id is derived from (tenant, principal, client_key), NOT a random uuid7 — so a
        # concurrent first-ack hits the same id and ensure is idempotent (one row, never a
        # duplicate); untenanted here, so the tenant part is empty
        assert row is not None
        assert row.id == _cursor_id(None, "u1", "dev")


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


async def test_concurrent_first_acks_keep_the_higher_position() -> None:
    # two first-acks for the same device race with different positions: the deterministic
    # id keeps it to one row, and the loser of the insert reconciles up to the max — the
    # lower value must never be the one that persists
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        query = ctx.document.query(_SPEC)
        cursors = DocumentMailboxCursors(command=ctx.document.command(_SPEC), query=query)

        await asyncio.gather(
            cursors.advance(principal="u1", client_key="dev", up_to=HlcTimestamp(physical_ms=3, logical=0)),
            cursors.advance(principal="u1", client_key="dev", up_to=HlcTimestamp(physical_ms=9, logical=0)),
        )

        rows = await query.find_many(filters={"$values": {"principal": "u1", "client_key": "dev"}})
        assert len(rows.hits) == 1  # converged on the deterministic id, never duplicated
        assert await cursors.get(principal="u1", client_key="dev") == HlcTimestamp(physical_ms=9, logical=0)


def test_cursor_id_includes_the_tenant() -> None:
    # On the tagged-tenancy table shape the kit recommends, every tenant shares one
    # physical primary-key space while the lookup is tenant-scoped. A tenant-blind id
    # collides for a principal present in two orgs (the org-switcher flow): the other
    # tenant's row is invisible to _find yet holds the PK, so the first ack loops on
    # find-miss/create-conflict forever. The tenant must be part of the derivation.
    from uuid import UUID

    from forze.application.contracts.tenancy import TenantIdentity

    org_a = TenantIdentity(tenant_id=UUID(int=1))
    org_b = TenantIdentity(tenant_id=UUID(int=2))

    ids = {
        _cursor_id(org_a, "u1", "dev"),
        _cursor_id(org_b, "u1", "dev"),
        _cursor_id(None, "u1", "dev"),
    }

    assert len(ids) == 3  # same device, three scopes, three distinct ids
    # ...and the derivation stays deterministic per scope.
    assert _cursor_id(org_a, "u1", "dev") == _cursor_id(org_a, "u1", "dev")


async def test_unreachable_cursor_row_fails_bounded_instead_of_spinning() -> None:
    # Backstop for any residual id collision (e.g. a pre-upgrade tenant-blind row):
    # find-miss + create-conflict must surface a real error after a bounded number of
    # attempts, never pin the scope in a tight retry loop on the user-facing ack path.
    import pytest

    from forze.base.exceptions import CoreException, exc
    from forze_kits.integrations.realtime.mailbox import _MAX_ADVANCE_ATTEMPTS

    class _AlwaysConflictCommand:
        def __init__(self) -> None:
            self.create_calls = 0

        async def create(self, cmd: object, *, id: object, return_new: bool) -> None:
            self.create_calls += 1
            raise exc.conflict("a foreign row holds this id")

    class _NeverFindsQuery:
        async def find(self, filters: object) -> None:
            return None

    command = _AlwaysConflictCommand()
    cursors = DocumentMailboxCursors(
        command=command,  # type: ignore[arg-type]
        query=_NeverFindsQuery(),  # type: ignore[arg-type]
    )

    with pytest.raises(CoreException) as ei:
        await cursors.advance(
            principal="u1", client_key="dev", up_to=HlcTimestamp(physical_ms=3, logical=0)
        )

    assert ei.value.code == "realtime_cursor_advance_stalled"
    assert command.create_calls == _MAX_ADVANCE_ATTEMPTS  # bounded, then surfaced
