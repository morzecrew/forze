"""An update of a field-encrypted document works for every field type.

The patch is merged into the decrypted document and the fields it writes are sealed, so a
sealed ``int``, mapping or ``str`` updates like a plain one: the merged value is what the store
holds (sealed) and what a read returns (opened). A sealed field the patch names is written
even when its value is unchanged, sealed afresh, as a key rotation needs.
"""

from __future__ import annotations

import base64
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from forze.application.contracts.crypto import FieldEncryption, KeyRef, StaticKeyDirectory
from forze.application.contracts.document import KeyedUpdate
from forze.base.crypto import is_envelope
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #

SECRET_FIELDS = frozenset({"pin", "profile", "note"})
ENCRYPTION = FieldEncryption(encrypted=SECRET_FIELDS | {"hint"})
KEY = StaticKeyDirectory(KeyRef(key_id="enc-update"))


class _Fields(BaseModel):
    name: str
    pin: int
    profile: dict[str, Any] = Field(default_factory=dict)
    note: str = ""
    hint: str | None = None


class EncCreate(CreateDocumentCmd, _Fields):
    pass


class EncDoc(Document, _Fields):
    pass


class EncRead(ReadDocument, _Fields):
    pass


class EncUpdate(BaseDTO):
    name: str | None = None
    pin: int | None = None
    profile: dict[str, Any] | None = None
    note: str | None = None
    hint: str | None = None


SEED = EncCreate(name="n", pin=1234, profile={"a": 1, "b": 2}, note="hush")


RawReader = Callable[[UUID], Awaitable[dict[str, Any]]]
"""Reads a document's stored fields as they are at rest, sealed values unopened."""


async def assert_encrypted_updates(
    command: Any, query: Any, raw: RawReader | None = None
) -> None:
    """Create :data:`SEED` twice and update each sealed field type, one document with
    ``update`` and the other with ``update_many``, reading back after each; with *raw*, check
    every sealed field is still sealed at rest."""

    one, many = await command.create(SEED), await command.create(SEED)
    revs = {one.id: one.rev, many.id: many.rev}

    for update, expected in (
        (EncUpdate(pin=4321), {"pin": 4321}),
        (EncUpdate(profile={"a": 9}), {"profile": {"a": 9, "b": 2}}),
        (EncUpdate(note="quiet"), {"note": "quiet"}),
    ):
        single, diff = await command.update(one.id, revs[one.id], update, return_diff=True)
        (bulk,) = await command.update_many([KeyedUpdate(id=many.id, rev=revs[many.id], dto=update)])

        # The diff reports what the caller set, open and in its own type.
        for field, value in update.model_dump(exclude_unset=True).items():
            assert diff[field] == value, (update, field, diff[field])

        for written in (single, bulk):
            assert written.rev == revs[written.id] + 1, update
            revs[written.id] = written.rev
            stored = await query.get(written.id, skip_cache=True)

            for field, value in expected.items():
                assert getattr(stored, field) == value, (update, field, getattr(stored, field))
                assert getattr(written, field) == value, (update, field)

            if raw is not None:
                at_rest = await raw(written.id)

                for field in SECRET_FIELDS:
                    sealed = at_rest[field]
                    assert isinstance(sealed, str), (update, field, sealed)
                    assert is_envelope(base64.b64decode(sealed)), (update, field)

    # Naming a sealed field writes it again, sealed afresh, though its value is the same: a
    # key rotation re-encrypts by just such an update.
    same = EncUpdate(note="quiet")
    before = {pk: (await raw(pk))["note"] for pk in revs} if raw is not None else {}
    single = await command.update(one.id, revs[one.id], same)
    (bulk,) = await command.update_many([KeyedUpdate(id=many.id, rev=revs[many.id], dto=same)])

    for written in (single, bulk):
        assert written.rev == revs[written.id] + 1 and written.note == "quiet"

        if raw is not None:
            assert (await raw(written.id))["note"] != before[written.id]

        revs[written.id] = written.rev

    # ``None`` is not sealed, so naming a sealed field that already holds it changes nothing.
    single = await command.update(one.id, revs[one.id], EncUpdate(hint=None))
    (bulk,) = await command.update_many(
        [KeyedUpdate(id=many.id, rev=revs[many.id], dto=EncUpdate(hint=None))]
    )

    for written in (single, bulk):
        assert written.rev == revs[written.id] and written.hint is None
