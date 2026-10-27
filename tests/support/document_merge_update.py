"""An update merges into a stored mapping or nested model, on every backend.

``Document.update`` merges a patch into the stored value (a key it names is set, the rest are
kept) and revalidates. In a mapping or ``Any`` value a key set to ``None`` is stored as null,
never removed; a list is replaced. What a store holds afterwards must be the merged value, not
the patch: reading the document back gives the siblings the patch did not name, and every null.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field
from pydantic_core import to_json

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
    items: list[dict[str, Any]] = Field(default_factory=list)
    extra: Any = None


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
    items: list[dict[str, Any]] | None = None
    extra: Any = None


SEED = MergeCreate(
    name="stored",
    meta={"k": 1, "j": 5, "deep": {"a": 1, "b": None}},
    address=MergeAddress(city="A", street="S"),
    items=[{"a": 0, "b": None}],
    extra={"x": 0, "y": [1, None]},
)

UPDATES: tuple[tuple[MergeUpdate, dict[str, Any], dict[str, str], dict[str, Any]], ...] = (
    # One key changed; the keys the update leaves out are kept, a nested null among them.
    (
        MergeUpdate(meta={"k": 2}),
        {"k": 2, "j": 5, "deep": {"a": 1, "b": None}},
        {"city": "A", "street": "S"},
        {"meta": {"k": 2}},
    ),
    # A nested key changed, its siblings kept.
    (
        MergeUpdate(meta={"deep": {"a": 9}}),
        {"k": 2, "j": 5, "deep": {"a": 9, "b": None}},
        {"city": "A", "street": "S"},
        {"meta": {"deep": {"a": 9}}},
    ),
    # A nested key changed beside a null the update sends again: the null is kept.
    (
        MergeUpdate(meta={"deep": {"a": 8, "b": None}}),
        {"k": 2, "j": 5, "deep": {"a": 8, "b": None}},
        {"city": "A", "street": "S"},
        {"meta": {"deep": {"a": 8}}},
    ),
    # A key set to null is stored as null, not removed.
    (
        MergeUpdate(meta={"j": None}),
        {"k": 2, "j": None, "deep": {"a": 8, "b": None}},
        {"city": "A", "street": "S"},
        {"meta": {"j": None}},
    ),
    # A nested key set to null.
    (
        MergeUpdate(meta={"deep": {"a": None}}),
        {"k": 2, "j": None, "deep": {"a": None, "b": None}},
        {"city": "A", "street": "S"},
        {"meta": {"deep": {"a": None}}},
    ),
    # One field of a nested model changed, the other kept.
    (
        MergeUpdate(address=MergeAddressPatch(city="B")),
        {"k": 2, "j": None, "deep": {"a": None, "b": None}},
        {"city": "B", "street": "S"},
        {"address": {"city": "B"}},
    ),
)
"""Each update, the stored ``meta`` and ``address`` after it, and the merge patch it reports."""


async def assert_updates_merge(command: Any, query: Any) -> None:
    """Create :data:`SEED` twice and apply each of :data:`UPDATES` in turn, to one document
    with ``update`` and to the other with ``update_many``: each stores the merged value and
    still reports the merge patch it applied, as plain values a response can serialize."""

    one, many = await command.create(SEED), await command.create(SEED)
    revs = {one.id: one.rev, many.id: many.rev}
    created = await query.get(one.id, skip_cache=True)
    assert (created.meta, created.items, created.extra) == (SEED.meta, SEED.items, SEED.extra)

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
            assert {k: diff[k] for k in patch} == patch, (update, diff)
            to_json(diff)  # what the generated update route does with it

    # A field typed ``Any`` that holds a mapping merges the same way; a list is replaced, the
    # nulls in it kept.
    update = MergeUpdate(items=[{"a": None}, {"b": 1}], extra={"y": [None, {"z": None}]})
    written, diff = await command.update(one.id, revs[one.id], update, return_diff=True)
    assert diff["items"] == [{"a": None}, {"b": 1}], diff
    assert diff["extra"] == {"y": [None, {"z": None}]}, diff
    to_json(diff)
    stored = await query.get(one.id, skip_cache=True)

    for read in (written, stored):
        assert read.items == [{"a": None}, {"b": 1}], read.items
        assert read.extra == {"x": 0, "y": [None, {"z": None}]}, read.extra


async def assert_update_matching_refuses_a_merge(command: Any, query: Any) -> None:
    """``update_matching`` writes its patch as it is, so it refuses one setting a field to a
    mapping or model (whatever the field's type), and still writes a scalar or a list."""

    created = await command.create(SEED)
    by_id = {"$values": {"id": {"$eq": created.id}}}

    for update in (
        MergeUpdate(meta={"k": 7}),
        MergeUpdate(address=MergeAddressPatch(city="Z")),
        MergeUpdate(extra={"x": 7}),
    ):
        try:
            await command.update_matching(by_id, update)

        except CoreException as refused:
            assert refused.code == "update_matching_merge_unsupported", update

        else:
            raise AssertionError(f"update_matching merged {update}")

    stored = await query.get(created.id, skip_cache=True)
    assert stored.meta == SEED.meta and stored.address == SEED.address
    assert stored.extra == SEED.extra

    # A list replaces the stored one on every path, mappings in it included.
    await command.update_matching(by_id, MergeUpdate(name="renamed", items=[{"b": 1}]))
    stored = await query.get(created.id, skip_cache=True)
    assert (stored.name, stored.items) == ("renamed", [{"b": 1}])
