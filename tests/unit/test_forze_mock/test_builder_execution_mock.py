"""The fluent builder is interchangeable with the dict form against a real adapter."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import Q
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
        name="people",
        read=_Read,
        write=DocumentWriteTypes(domain=_Doc, create_cmd=_Create),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="people",
        read_model=_Read,
        domain_model=_Doc,
    )


async def _names(doc: Any, filt: dict[str, Any]) -> set[str]:
    page = await doc.find_page(filt, pagination={"limit": 100})
    return {r.name for r in page.hits}


@pytest.mark.asyncio
async def test_builder_matches_dict_against_mock() -> None:
    doc = _mock()
    await doc.create(_Create(name="ann", age=30, tags=["x", "y"]))
    await doc.create(_Create(name="bob", age=17, tags=["y"]))
    await doc.create(_Create(name="cy", age=40, tags=["z"]))

    cases = [
        (
            Q.field("age").gte(18) & Q.field("name").like("a%"),
            {
                "$and": [
                    {"$values": {"age": {"$gte": 18}}},
                    {"$values": {"name": {"$like": "a%"}}},
                ]
            },
        ),
        (
            Q.field("age").lt(18) | Q.field("age").gt(35),
            {"$or": [{"$values": {"age": {"$lt": 18}}}, {"$values": {"age": {"$gt": 35}}}]},
        ),
        (
            Q.field("tags").any("y"),
            {"$values": {"tags": {"$any": {"$eq": "y"}}}},
        ),
        (
            ~Q.field("name").eq("bob"),
            {"$not": {"$values": {"name": {"$eq": "bob"}}}},
        ),
    ]

    for built, hand in cases:
        # The builder's dict equals the hand-written dict, and both select the same rows.
        assert built.build() == hand
        assert await _names(doc, built.build()) == await _names(doc, hand)
