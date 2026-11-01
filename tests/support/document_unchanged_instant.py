"""Resending an unchanged instant is not a change, whatever form either side holds it in.

A store hands a ``datetime`` back in its own form: ``timestamptz`` and BSON read back aware
whether a naive or an aware value went in, and BSON keeps milliseconds only. A reload that
resends the same instant naive, aware at another offset, or finer than the store keeps it,
must still find the row unchanged — no ``rev`` bump, no ``last_update_at`` move, an empty
diff — or every reload rewrites every row it touches. A naive value is read as UTC.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.document import KeyedUpdate, UpsertItem
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _InstantFields(BaseModel):
    name: str
    at: datetime


class InstantCreate(CreateDocumentCmd, _InstantFields):
    pass


class InstantDoc(Document, _InstantFields):
    pass


class InstantRead(ReadDocument, _InstantFields):
    pass


class InstantUpdate(BaseDTO):
    name: str | None = None
    at: datetime | None = None


AT_UTC = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)
"""Finer than a millisecond, which BSON drops."""

FORMS: dict[str, datetime] = {
    "naive": AT_UTC.replace(tzinfo=None),
    "utc": AT_UTC,
    "+03:00": AT_UTC.astimezone(timezone(timedelta(hours=3))),
}
"""One instant in each form a caller may send it in."""


def _instant(at: datetime) -> datetime:
    """*at* as an aware instant: a naive one, as a ``timestamp`` column reads back, is UTC."""

    return at if at.tzinfo is not None else at.replace(tzinfo=UTC)


async def assert_an_unchanged_instant_is_not_a_change(
    command: Any, query: Any, *, set_based: bool = True
) -> list[Any]:
    """Store each form of :data:`AT_UTC` and resend each form twice through ``upsert``,
    ``upsert_many`` (set-based too, when *set_based*) and ``update``: none of them changes the
    row. Then move the instant on one row: that is still a change.

    :returns: The documents it created.
    """

    created: list[Any] = []

    for stored_form, stored_at in FORMS.items():
        for sent_form, sent_at in FORMS.items():
            case = f"stored {stored_form}, sent {sent_form}"
            create = InstantCreate(name="n", at=stored_at)
            doc = await command.create(create)
            created.append(doc)
            update = InstantUpdate(name="n", at=sent_at)
            item = UpsertItem(id=doc.id, create=create, update=update)

            for _ in range(2):
                await command.upsert(doc.id, create, update, return_new=False)
                await command.upsert_many([item], return_new=False)

                if set_based:
                    await command.upsert_many([item], return_new=False, set_based=True)

            _, diff = await command.update(doc.id, doc.rev, update, return_diff=True)
            stored = await query.get(doc.id, skip_cache=True)

            assert diff == {}, case
            assert stored.rev == doc.rev, case
            assert stored.last_update_at == doc.last_update_at, case

    doc = created[0]
    moved = datetime(2026, 1, 1, 13, tzinfo=UTC)
    await command.upsert(
        doc.id, InstantCreate(name="n", at=moved), InstantUpdate(at=moved), return_new=False
    )
    stored = await query.get(doc.id, skip_cache=True)
    assert (stored.rev, _instant(stored.at)) == (doc.rev + 1, moved)

    return created


async def assert_a_stale_resend_keeps_a_concurrent_change(command: Any, query: Any) -> None:
    """A writer at a stale revision resends the instant it read, in each form, after another
    writer moved it: the move is kept, and only what the stale writer changed (``name``) lands.

    Needs revision history, which merges a stale write instead of refusing it.
    """

    moved = datetime(2026, 1, 1, 15, tzinfo=UTC)

    for form, sent_at in FORMS.items():
        doc = await command.create(InstantCreate(name="a", at=AT_UTC))
        await command.update(doc.id, doc.rev, InstantUpdate(at=moved))
        await command.update(doc.id, doc.rev, InstantUpdate(name="b", at=sent_at))
        stored = await query.get(doc.id, skip_cache=True)

        assert (stored.name, _instant(stored.at), stored.rev) == ("b", moved, doc.rev + 2), form


async def assert_a_batch_writes_only_the_rows_it_changes(
    command: Any, query: Any, history_rows: Callable[[UUID], Awaitable[int]]
) -> None:
    """``update_many`` over a fresh change, a fresh no-op, a stale resend and a stale change:
    each row takes what it changes, the no-op and the resend nothing, and history holds one
    snapshot per revision written. Needs revision history.
    """

    moved = datetime(2026, 1, 1, 15, tzinfo=UTC)
    fresh, same, resend, change = [
        await command.create(InstantCreate(name=name, at=AT_UTC)) for name in "abcd"
    ]

    for doc in (resend, change):
        await command.update(doc.id, doc.rev, InstantUpdate(at=moved))

    await command.update_many(
        [
            KeyedUpdate(id=fresh.id, rev=fresh.rev, dto=InstantUpdate(name="a2")),
            KeyedUpdate(id=same.id, rev=same.rev, dto=InstantUpdate(name="b")),
            KeyedUpdate(id=resend.id, rev=resend.rev, dto=InstantUpdate(at=AT_UTC)),
            KeyedUpdate(id=change.id, rev=change.rev, dto=InstantUpdate(name="d2", at=AT_UTC)),
        ]
    )

    expected = {fresh.id: ("a2", 2), same.id: ("b", 1), resend.id: ("c", 2), change.id: ("d2", 3)}

    for pk, (name, rev) in expected.items():
        stored = await query.get(pk, skip_cache=True)

        assert (stored.name, stored.rev, await history_rows(pk)) == (name, rev, rev), name

        if pk in (resend.id, change.id):
            assert _instant(stored.at) == moved, name
