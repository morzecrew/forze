"""A bulk update whose patches are of different DTO types writes each patch whole.

A batch is encoded at once; each patch must still be encoded as its own type, so the fields a
subclass adds to the first patch's type are written, not dropped.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from forze.application.contracts.document import KeyedUpdate
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _MixedFields(BaseModel):
    name: str
    qty: int
    note: str | None = None


class MixedCreate(CreateDocumentCmd, _MixedFields):
    pass


class MixedDoc(Document, _MixedFields):
    pass


class MixedRead(ReadDocument, _MixedFields):
    pass


class MixedUpdate(BaseDTO):
    qty: int | None = None


class MixedRename(MixedUpdate):
    """A subclass: its extra fields are what a first-type encoding would drop."""

    name: str | None = None
    note: str | None = None


async def assert_mixed_update_dtos(command: Any, query: Any) -> None:
    """Update two documents in one ``update_many``, the narrower patch type first."""

    a = await command.create(MixedCreate(name="a", qty=1))
    b = await command.create(MixedCreate(name="b", qty=2))

    await command.update_many(
        [
            KeyedUpdate(id=a.id, rev=a.rev, dto=MixedUpdate(qty=10)),
            KeyedUpdate(id=b.id, rev=b.rev, dto=MixedRename(qty=20, name="b2", note="n")),
        ],
        return_new=False,
    )

    stored = {pk: await query.get(pk, skip_cache=True) for pk in (a.id, b.id)}
    assert (stored[a.id].name, stored[a.id].qty, stored[a.id].note) == ("a", 10, None)
    assert (stored[b.id].name, stored[b.id].qty, stored[b.id].note) == ("b2", 20, "n")
