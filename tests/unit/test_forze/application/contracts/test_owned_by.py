"""``OwnedBy`` refuses an owner field its read could answer only one way.

An owned read has two paths — the owner in the database predicate, or a row checked after a
read through the cache — and a field only one of them can evaluate would make the answer depend
on whether the row happened to be cached. Every such field is refused before either path runs.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.crypto import FieldEncryption
from forze.application.contracts.document import OwnedBy
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules
from tests.support.owned_reads_conformance import OwnedCreate, owned_spec

# ----------------------- #


@pytest.mark.parametrize(
    ("field", "spec_kwargs", "code"),
    [
        ("ownr_id", {}, "owned_by_unknown_field"),
        ("owner_id", {"encryption": FieldEncryption(encrypted={"owner_id"})}, "owned_by_unfilterable_field"),
        ("title", {"lenient_read_fields": {"title"}}, "owned_by_unfilterable_field"),
    ],
    ids=["not_on_the_model", "sealed_at_random", "not_stored"],
)
def test_a_field_the_predicate_cannot_evaluate_is_refused(
    field: str,
    spec_kwargs: dict[str, object],
    code: str,
) -> None:
    with pytest.raises(CoreException) as caught:
        OwnedBy(field=field, value=uuid4()).check(owned_spec("notes", **spec_kwargs))

    assert (caught.value.kind, caught.value.code) == (ExceptionKind.CONFIGURATION, code)


def test_a_deterministically_sealed_field_can_own() -> None:
    spec = owned_spec("notes", encryption=FieldEncryption(searchable={"owner_id"}))

    OwnedBy(field="owner_id", value=uuid4()).check(spec)


async def test_an_adapter_refuses_it_before_reading() -> None:
    spec = owned_spec("notes", encryption=FieldEncryption(encrypted={"owner_id"}))
    ctx = context_from_modules(MockDepsModule())
    owner = uuid4()
    row = await ctx.doc.command(spec).create(OwnedCreate(owner_id=owner))
    owned_by = OwnedBy(field="owner_id", value=owner)

    for read in (
        ctx.doc.query(spec).get(row.id, owned_by=owned_by),
        ctx.doc.query(spec).get_many([row.id], owned_by=owned_by),
    ):
        with pytest.raises(CoreException) as caught:
            await read

        assert caught.value.code == "owned_by_unfilterable_field"


async def test_a_batch_read_that_fails_otherwise_is_not_turned_into_a_not_found() -> None:
    """Only a missing row is folded into the one summary; an outage must still read as one."""

    outage = CoreException.infrastructure("database unavailable")

    async def read() -> list[BaseModel]:
        raise outage

    with pytest.raises(CoreException) as caught:
        await OwnedBy(field="owner_id", value=uuid4()).read_batch(read())

    assert caught.value is outage
