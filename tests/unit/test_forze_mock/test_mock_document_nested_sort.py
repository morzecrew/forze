"""Nested/dotted sort on the mock document adapter (the conformance oracle).

Sorting on a field inside a nested Pydantic sub-model resolves the same way nested
filters do; the in-memory oracle walks the dotted path for both offset and keyset-cursor
reads, so the two agree and pages concatenate to the full sorted query.
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


def _adapter() -> MockDocumentAdapter[_PersonRead, _PersonDoc, _PersonCreate, _PersonUpdate]:
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


async def _seed(doc: Any, cities: list[str]) -> None:
    for i, city in enumerate(cities):
        await doc.create(_PersonCreate(name=f"p{i}", addr=_Addr(city=city)))


# ....................... #


@pytest.mark.asyncio
async def test_offset_sort_on_nested_field() -> None:
    doc = _adapter()
    await _seed(doc, ["paris", "amsterdam", "cairo", "berlin"])

    page = await doc.find_many(sorts={"addr.city": "asc"})

    assert [h.addr.city for h in page.hits] == [
        "amsterdam",
        "berlin",
        "cairo",
        "paris",
    ]


@pytest.mark.asyncio
async def test_cursor_pages_on_nested_field_concatenate_to_full_sort() -> None:
    doc = _adapter()
    await _seed(doc, ["paris", "amsterdam", "cairo", "berlin", "delhi"])

    collected: list[str] = []
    cursor: dict[str, Any] = {"limit": 2}

    while True:
        page = await doc.find_cursor(sorts={"addr.city": "asc"}, cursor=cursor)
        collected.extend(h.addr.city for h in page.hits)
        if not page.has_more or page.next_cursor is None:
            break
        cursor = {"limit": 2, "after": page.next_cursor}

    full = await doc.find_many(sorts={"addr.city": "asc"})
    assert collected == [h.addr.city for h in full.hits]
    assert collected == ["amsterdam", "berlin", "cairo", "delhi", "paris"]


@pytest.mark.asyncio
async def test_cursor_on_nested_field_stable_under_insert_before_position() -> None:
    doc = _adapter()
    await _seed(doc, ["berlin", "delhi", "paris"])

    page1 = await doc.find_cursor(sorts={"addr.city": "asc"}, cursor={"limit": 2})
    assert [h.addr.city for h in page1.hits] == ["berlin", "delhi"]
    assert page1.next_cursor is not None

    # A new row sorting before the cursor must not re-deliver or skip rows.
    await doc.create(_PersonCreate(name="x", addr=_Addr(city="amsterdam")))

    page2 = await doc.find_cursor(
        sorts={"addr.city": "asc"},
        cursor={"limit": 2, "after": page1.next_cursor},
    )
    assert [h.addr.city for h in page2.hits] == ["paris"]
