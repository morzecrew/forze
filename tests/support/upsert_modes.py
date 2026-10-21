"""A mirrored upsert and a ``skip_none`` one write the same rows on every backend and path.

``UpsertItem.mirror`` updates with the create payload, so a stored row takes every field the
create sets, the update command's or not; ``skip_none`` leaves out each field an update sets
to ``None``, so a stored value is kept rather than cleared, and the create arm inserts its
payload as it is. Run with ``set_based`` on and off: a backend with no set-based statement
takes its usual path, which must write the same.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.document import UpsertItem
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _ModeFields(BaseModel):
    name: str
    qty: int
    note: str | None = None
    label: str = "dflt"


class ModeCreate(CreateDocumentCmd, _ModeFields):
    pass


class ModeDoc(Document, _ModeFields):
    pass


class ModeRead(ReadDocument, _ModeFields):
    pass


class ModeUpdate(BaseDTO):
    """Narrower than the create: a mirror writes the fields this leaves out too."""

    qty: int | None = None


def _state(row: Any) -> tuple[str, int, str | None, str, int]:
    return row.name, row.qty, row.note, row.label, row.rev


async def assert_upsert_modes(
    command: Any, query: Any, ids: Sequence[UUID], *, set_based: bool
) -> None:
    """Upsert at the three *ids* in turn, mirrored and with ``skip_none``, checking what each
    write leaves stored."""

    a, b, c = ids

    async def upsert(*items: UpsertItem[Any, Any]) -> None:
        await command.upsert_many(list(items), return_new=False, set_based=set_based)

    async def state(pk: UUID) -> tuple[str, int, str | None, str, int]:
        return _state(await query.get(pk, skip_cache=True))

    await upsert(
        UpsertItem.mirror(a, ModeCreate(name="a", qty=1, note="kept", label="custom")),
        UpsertItem.mirror(b, ModeCreate(name="b", qty=2, note="n")),
    )

    # A mirror rewrites what the create sets, ``name`` too, which the update command lacks; a
    # row it leaves as it was keeps its revision.
    await upsert(
        UpsertItem.mirror(a, ModeCreate(name="a2", qty=1, note="kept", label="custom")),
        UpsertItem.mirror(b, ModeCreate(name="b", qty=2, note="n")),
    )
    assert await state(a) == ("a2", 1, "kept", "custom", 2)
    assert await state(b) == ("b", 2, "n", "dflt", 1)

    # A null is left out with ``skip_none`` (the stored note is kept) and written without it.
    await upsert(UpsertItem.mirror(a, ModeCreate(name="a2", qty=5, note=None), skip_none=True))
    assert await state(a) == ("a2", 5, "kept", "custom", 3)

    await upsert(UpsertItem.mirror(a, ModeCreate(name="a2", qty=5, note=None)))
    assert await state(a) == ("a2", 5, None, "custom", 4)

    # On an ordinary item: a null the domain would refuse (``qty`` is required) is left out, so
    # nothing changes. The create arm inserts its payload, nulls and all.
    await upsert(
        UpsertItem(
            id=b, create=ModeCreate(name="x", qty=0), update=ModeUpdate(qty=None), skip_none=True
        ),
        UpsertItem.mirror(c, ModeCreate(name="c", qty=3, note=None, label="l"), skip_none=True),
    )
    assert await state(b) == ("b", 2, "n", "dflt", 1)
    assert await state(c) == ("c", 3, None, "l", 1)

    # A mirror updating a stored row after an ordinary item in the same call, first in input
    # and in id order: each update is written as its own type, so the mirror's every field.
    lo, hi = sorted((a, b))
    name, _, note, label, rev = await state(lo)
    hi_rev = (await state(hi))[4]
    await upsert(
        UpsertItem(id=lo, create=ModeCreate(name="x", qty=0), update=ModeUpdate(qty=10)),
        UpsertItem.mirror(hi, ModeCreate(name="renamed", qty=20, note="new", label="L")),
    )
    assert await state(lo) == (name, 10, note, label, rev + 1)
    assert await state(hi) == ("renamed", 20, "new", "L", hi_rev + 1)
