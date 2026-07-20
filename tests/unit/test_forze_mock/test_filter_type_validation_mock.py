"""The mock validates operator/field types like the real gateways (dev ↔ prod symmetry).

A type-incompatible filter (``$like`` on a number) raises the same
``query_operator_type_mismatch`` precondition on the mock as on Postgres/Mongo, instead
of silently matching nothing — so a bug surfaces in tests, not only in production.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import OPERATOR_TYPE_MISMATCH_CODE
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockState

pytestmark = pytest.mark.unit


class _Fields(BaseModel):
    name: str
    age: int
    tags: list[str] = []


class _Create(CreateDocumentCmd, _Fields):
    pass


class _Doc(Document, _Fields):
    pass


class _Read(ReadDocument, _Fields):
    pass


def _mock() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="t",
        read=_Read,
        write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="t",
        read_model=_Read,
        domain_model=_Doc,
    )


_BAD = {"$values": {"age": {"$like": "x"}}}  # $like on an int


@pytest.mark.asyncio
async def test_find_many_rejects_type_mismatch() -> None:
    doc = _mock()
    await doc.create(_Create(name="a", age=5))

    with pytest.raises(CoreException) as ei:
        await doc.find_many(filters=_BAD, pagination={"limit": 10})

    assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE


@pytest.mark.asyncio
async def test_count_rejects_type_mismatch() -> None:
    doc = _mock()
    with pytest.raises(CoreException) as ei:
        await doc.count(filters=_BAD)

    assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE


@pytest.mark.asyncio
async def test_find_cursor_rejects_type_mismatch() -> None:
    doc = _mock()
    with pytest.raises(CoreException) as ei:
        await doc.find_cursor(filters=_BAD, cursor={"limit": 10})

    assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE


@pytest.mark.asyncio
async def test_valid_filters_pass() -> None:
    doc = _mock()
    await doc.create(_Create(name="alice", age=5, tags=["x"]))

    # text op on str, ordering on int, and membership on an array (overlap) all valid
    by_name = await doc.find_many(
        filters={"$values": {"name": {"$like": "a%"}}}, pagination={"limit": 10}
    )
    assert len(by_name.hits) == 1
    assert (await doc.count(filters={"$values": {"age": {"$gte": 1}}})) == 1
    by_tag = await doc.find_many(
        filters={"$values": {"tags": {"$in": ["x"]}}}, pagination={"limit": 10}
    )
    assert len(by_tag.hits) == 1
