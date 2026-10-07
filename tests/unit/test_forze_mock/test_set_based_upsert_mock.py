"""The mock refuses a set-based ``upsert_many`` as a real store does and otherwise writes the
rows the usual path writes."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes, UpsertItem
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockState

pytestmark = pytest.mark.unit


class _Create(CreateDocumentCmd):
    name: str


class _Doc(Document):
    name: str


class _Read(ReadDocument):
    name: str


class _Update(BaseDTO):
    name: str | None = None


def _mock(*, history_enabled: bool = False) -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="sb",
        read=_Read,
        write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create, update_cmd=_Update),
        history_enabled=history_enabled,
    )
    return MockDocumentAdapter(
        spec=spec, state=MockState(), namespace="sb", read_model=_Read, domain_model=_Doc
    )


@pytest.mark.asyncio
async def test_an_allowed_set_based_upsert_writes_what_the_usual_path_writes() -> None:
    doc = _mock()
    a, b = uuid4(), uuid4()
    await doc.create(_Create(name="a"), id=a)

    await doc.upsert_many(
        [
            UpsertItem(id=a, create=_Create(name="x"), update=_Update(name="renamed")),
            UpsertItem(id=b, create=_Create(name="b"), update=_Update(name="y")),
        ],
        return_new=False,
        set_based=True,
    )

    assert {(r.id, r.name) for r in (await doc.find_many()).hits} == {(a, "renamed"), (b, "b")}


@pytest.mark.asyncio
@pytest.mark.parametrize(("history", "return_new"), [(True, False), (False, True)])
async def test_a_refused_set_based_upsert_writes_nothing(history: bool, return_new: bool) -> None:
    doc = _mock(history_enabled=history)

    with pytest.raises(CoreException) as refused:
        await doc.upsert_many(  # type: ignore[call-overload]
            [UpsertItem(id=uuid4(), create=_Create(name="a"), update=_Update())],
            return_new=return_new,
            set_based=True,
        )

    assert refused.value.code == "set_based_upsert_unsupported"
    assert (await doc.find_many()).hits == []
