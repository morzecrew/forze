"""An update merges into a stored mapping or nested model, on every backend.

``Document.update`` merges a patch into the stored value (a JSON merge patch: a key it names
is set, a key set to ``None`` is removed, the rest are kept) and revalidates. What a store
holds afterwards must be the merged value, not the patch: reading the document back gives the
siblings the patch did not name.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from forze.application.contracts.document import KeyedUpdate
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class MergeAddress(BaseModel):
    city: str
    street: str


class MergeAddressPatch(BaseModel):
    city: str | None = None
    street: str | None = None


class _MergeFields(BaseModel):
    name: str
    meta: dict[str, Any] = Field(default_factory=dict)
    address: MergeAddress


class MergeCreate(CreateDocumentCmd, _MergeFields):
    pass


class MergeDoc(Document, _MergeFields):
    pass


class MergeRead(ReadDocument, _MergeFields):
    pass


class MergeUpdate(BaseDTO):
    name: str | None = None
    meta: dict[str, Any] | None = None
    address: MergeAddressPatch | None = None


SEED = MergeCreate(
    name="stored",
    meta={"k": 1, "j": 5, "deep": {"a": 1, "b": 2}},
    address=MergeAddress(city="A", street="S"),
)

UPDATES: tuple[tuple[MergeUpdate, dict[str, Any], dict[str, str], dict[str, Any]], ...] = (
    # One key changed, the rest kept.
    (
        MergeUpdate(meta={"k": 2}),
        {"k": 2, "j": 5, "deep": {"a": 1, "b": 2}},
        {"city": "A", "street": "S"},
        {"meta": {"k": 2}},
    ),
    # A nested key changed, its siblings kept.
    (
        MergeUpdate(meta={"deep": {"a": 9}}),
        {"k": 2, "j": 5, "deep": {"a": 9, "b": 2}},
        {"city": "A", "street": "S"},
        {"meta": {"deep": {"a": 9}}},
    ),
    # A key removed by naming it with null.
    (
        MergeUpdate(meta={"j": None}),
        {"k": 2, "deep": {"a": 9, "b": 2}},
        {"city": "A", "street": "S"},
        {"meta": {"j": None}},
    ),
    # One field of a nested model changed, the other kept.
    (
        MergeUpdate(address=MergeAddressPatch(city="B")),
        {"k": 2, "deep": {"a": 9, "b": 2}},
        {"city": "B", "street": "S"},
        {"address": {"city": "B"}},
    ),
)
"""Each update, the stored ``meta`` and ``address`` after it, and the merge patch it reports."""


def _unwrapped(value: Any) -> Any:
    # A store may report a JSON value in its driver's wrapper (psycopg's ``Jsonb``).
    return getattr(value, "obj", value)


async def assert_updates_merge(command: Any, query: Any) -> None:
    """Create :data:`SEED` twice and apply each of :data:`UPDATES` in turn, to one document
    with ``update`` and to the other with ``update_many``: each stores the merged value and
    still reports the merge patch it applied."""

    one, many = await command.create(SEED), await command.create(SEED)
    revs = {one.id: one.rev, many.id: many.rev}

    for update, meta, address, patch in UPDATES:
        single = await command.update(one.id, revs[one.id], update, return_diff=True)
        (bulk,) = await command.update_many(
            [KeyedUpdate(id=many.id, rev=revs[many.id], dto=update)], return_diff=True
        )

        for written, diff in (single, bulk):
            revs[written.id] = written.rev
            stored = await query.get(written.id, skip_cache=True)

            assert stored.meta == meta, (update, stored.meta)
            assert stored.address.model_dump() == address, (update, stored.address)
            assert written.meta == meta and written.address.model_dump() == address, update
            assert {k: _unwrapped(diff[k]) for k in patch} == patch, (update, diff)


async def assert_update_matching_refuses_a_merge(command: Any, query: Any) -> None:
    """``update_matching`` writes its patch as it is, so it refuses one that would replace a
    mapping or nested model with a fragment, and still writes a plain field."""

    created = await command.create(SEED)
    by_id = {"$values": {"id": {"$eq": created.id}}}

    for update in (
        MergeUpdate(meta={"k": 7}),
        MergeUpdate(address=MergeAddressPatch(city="Z")),
    ):
        try:
            await command.update_matching(by_id, update)

        except CoreException as refused:
            assert refused.code == "update_matching_merge_unsupported", update

        else:
            raise AssertionError(f"update_matching merged {update}")

    stored = await query.get(created.id, skip_cache=True)
    assert stored.meta == SEED.meta and stored.address == SEED.address

    await command.update_matching(by_id, MergeUpdate(name="renamed"))
    assert (await query.get(created.id, skip_cache=True)).name == "renamed"
