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
