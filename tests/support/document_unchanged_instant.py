"""Resending an unchanged instant is not a change, whatever form either side holds it in.

A store hands a ``datetime`` back in its own form: ``timestamptz`` and BSON read back aware
whether a naive or an aware value went in, and BSON keeps milliseconds only. A reload that
resends the same instant naive, aware at another offset, or finer than the store keeps it,
must still find the row unchanged — no ``rev`` bump, no ``last_update_at`` move, an empty
diff — or every reload rewrites every row it touches. A naive value is read as UTC.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel

from forze.application.contracts.document import UpsertItem
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
    assert (stored.rev, stored.at) == (doc.rev + 1, moved)

    return created
