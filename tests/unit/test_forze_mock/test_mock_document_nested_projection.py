"""Nested/dotted field projection on the mock document adapter (the conformance oracle).

Projecting a dotted path (``addr.city``) reshapes each result into the nested
``{"addr": {"city": ...}}`` shape — the same shape the real backends emit — with sibling
leaves merging under one parent, a requested root subsuming its leaves, and an absent leaf
omitted. The mock is the cross-backend parity oracle, so these assertions are the contract.
"""

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze_mock.adapters import MockDocumentAdapter, MockState

# ----------------------- #


class _Addr(BaseModel):
    city: str
    zip: str | None = None


class _PersonDoc(DocWithSoftDeletion):
    name: str
    addr: _Addr


class _PersonCreate(CreateDocumentCmd):
    name: str
    addr: _Addr


class _PersonUpdate(BaseDTO):
    name: str | None = None


class _PersonRead(ReadDocument):
    name: str
    addr: _Addr
    is_deleted: bool = False


def _adapter() -> (
    MockDocumentAdapter[_PersonRead, _PersonDoc, _PersonCreate, _PersonUpdate]
):
    spec = DocumentSpec(
        name="people",
        read=_PersonRead,
        write=DocumentWriteTypes(
            domain=_PersonDoc,
            create_cmd=_PersonCreate,
            update_cmd=_PersonUpdate,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="people",
        read_model=_PersonRead,
        domain_model=_PersonDoc,
    )


# ....................... #


@pytest.mark.asyncio
async def test_project_nested_leaf_reshapes_into_nested_dict() -> None:
    doc = _adapter()
    await doc.create(_PersonCreate(name="ann", addr=_Addr(city="paris", zip="75001")))

    out = await doc.project({"$values": {"name": "ann"}}, ["addr.city"])

    assert out == {"addr": {"city": "paris"}}


@pytest.mark.asyncio
async def test_project_sibling_leaves_merge_under_one_parent() -> None:
    doc = _adapter()
    await doc.create(_PersonCreate(name="ann", addr=_Addr(city="paris", zip="75001")))

    out = await doc.project(
        {"$values": {"name": "ann"}}, ["name", "addr.city", "addr.zip"]
    )

    assert out == {"name": "ann", "addr": {"city": "paris", "zip": "75001"}}


@pytest.mark.asyncio
async def test_project_root_subsumes_leaf() -> None:
    doc = _adapter()
    await doc.create(_PersonCreate(name="ann", addr=_Addr(city="paris", zip="75001")))

    out = await doc.project({"$values": {"name": "ann"}}, ["addr", "addr.city"])

    assert out == {"addr": {"city": "paris", "zip": "75001"}}


@pytest.mark.asyncio
async def test_project_absent_nested_leaf_is_omitted() -> None:
    doc = _adapter()
    await doc.create(_PersonCreate(name="ann", addr=_Addr(city="paris")))

    out = await doc.project({"$values": {"name": "ann"}}, ["addr.city", "addr.zip"])

    # ``zip`` is None on the document → present, kept; a truly-absent path is omitted.
    assert out == {"addr": {"city": "paris", "zip": None}}


@pytest.mark.asyncio
async def test_project_many_nested_over_multiple_rows() -> None:
    doc = _adapter()
    for city in ["paris", "berlin"]:
        await doc.create(_PersonCreate(name=city[0], addr=_Addr(city=city)))

    page = await doc.project_many(["addr.city"], sorts={"addr.city": "asc"})

    assert [h for h in page.hits] == [{"addr": {"city": "berlin"}}, {"addr": {"city": "paris"}}]


@pytest.mark.asyncio
async def test_project_cursor_with_dotted_field_and_id_concatenates() -> None:
    doc = _adapter()
    for city in ["paris", "berlin", "delhi"]:
        await doc.create(_PersonCreate(name=city[0], addr=_Addr(city=city)))

    collected: list[Any] = []
    cursor: dict[str, Any] = {"limit": 2}
    while True:
        # A dotted return field satisfies a nested sort key by its root column; ``id`` is the
        # tie-breaker and must be projected for keyset cursors.
        page = await doc.project_cursor(
            ["id", "addr.city"], sorts={"addr.city": "asc"}, cursor=cursor
        )
        collected.extend(h["addr"]["city"] for h in page.hits)
        if not page.has_more or page.next_cursor is None:
            break
        cursor = {"limit": 2, "after": page.next_cursor}

    assert collected == ["berlin", "delhi", "paris"]


# ....................... #
# Array element projection: a dotted path through a list of sub-models maps over each
# element, preserving structure and length.


class _Item(BaseModel):
    sku: str
    qty: int = 0


class _OrderDoc(DocWithSoftDeletion):
    ref: str
    items: list[_Item]


class _OrderCreate(CreateDocumentCmd):
    ref: str
    items: list[_Item]


class _OrderUpdate(BaseDTO):
    ref: str | None = None


class _OrderRead(ReadDocument):
    ref: str
    items: list[_Item]
    is_deleted: bool = False


def _order_adapter() -> (
    MockDocumentAdapter[_OrderRead, _OrderDoc, _OrderCreate, _OrderUpdate]
):
    spec = DocumentSpec(
        name="orders",
        read=_OrderRead,
        write=DocumentWriteTypes(
            domain=_OrderDoc,
            create_cmd=_OrderCreate,
            update_cmd=_OrderUpdate,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="orders",
        read_model=_OrderRead,
        domain_model=_OrderDoc,
    )


@pytest.mark.asyncio
async def test_project_array_leaf_maps_over_elements() -> None:
    doc = _order_adapter()
    await doc.create(
        _OrderCreate(ref="o1", items=[_Item(sku="A", qty=2), _Item(sku="B", qty=1)])
    )

    out = await doc.project({"$values": {"ref": "o1"}}, ["items.sku"])

    assert out == {"items": [{"sku": "A"}, {"sku": "B"}]}


@pytest.mark.asyncio
async def test_project_array_multi_leaf_merges_per_element() -> None:
    doc = _order_adapter()
    await doc.create(
        _OrderCreate(ref="o1", items=[_Item(sku="A", qty=2), _Item(sku="B", qty=1)])
    )

    out = await doc.project({"$values": {"ref": "o1"}}, ["ref", "items.sku", "items.qty"])

    assert out == {
        "ref": "o1",
        "items": [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}],
    }


@pytest.mark.asyncio
async def test_project_cursor_sibling_leaf_of_sort_key_rejected() -> None:
    doc = _adapter()
    await doc.create(_PersonCreate(name="a", addr=_Addr(city="paris", zip="75001")))

    # Projecting a sibling leaf (``addr.zip``) cannot serve a cursor sort on ``addr.city`` —
    # the token would read the wrong value. The projection guard must reject it.
    with pytest.raises(Exception, match="projection must include"):
        await doc.project_cursor(
            ["id", "addr.zip"], sorts={"addr.city": "asc"}, cursor={"limit": 2}
        )


@pytest.mark.asyncio
async def test_cursor_sort_through_list_is_rejected() -> None:
    doc = _order_adapter()
    await doc.create(_OrderCreate(ref="o1", items=[_Item(sku="A"), _Item(sku="B")]))

    # A sort key whose path crosses a list resolves to no scalar; sort validation rejects it
    # before a cursor token is ever built from it.
    with pytest.raises(Exception, match="not on read model"):
        await doc.project_cursor(
            ["id", "items.sku"], sorts={"items.sku": "asc"}, cursor={"limit": 2}
        )
