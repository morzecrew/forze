"""Filters on a plain ``Enum`` field, one set of cases for the mock and a real store.

A store holds a plain (non-``str``) ``Enum`` as its value — ``"open"`` in a Postgres ``text``
column — and a filter names that value, since the parser takes no member operand. The mock keeps
the member in its rows. :data:`ROWS` is each row as a store holds it; :data:`CASES` are the rows
each filter must match, asserted by :func:`assert_enum_cases` on every backend.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from tests.support.query_dsl_corpus import QueryCase

# ----------------------- #


class Status(Enum):
    OPEN = "open"
    CLOSED = "closed"


class StatusItem(BaseModel):
    sku: str
    status: Status


class _StatusFields(BaseModel):
    status: Status
    statuses: list[Status] = []
    items: list[StatusItem] = []


class StatusCreate(CreateDocumentCmd, _StatusFields):
    pass


class StatusDoc(Document, _StatusFields):
    pass


class StatusRead(ReadDocument, _StatusFields):
    pass


# ....................... #

ROWS: dict[str, dict[str, Any]] = {
    "a": {
        "status": "open",
        "statuses": ["open", "closed"],
        "items": [{"sku": "x", "status": "closed"}],
    },
    "b": {"status": "closed", "statuses": ["closed"], "items": [{"sku": "y", "status": "open"}]},
    "c": {"status": "open", "statuses": [], "items": []},
    "d": {"status": "closed", "statuses": ["open"], "items": [{"sku": "z", "status": "open"}]},
}

CASES: tuple[QueryCase, ...] = (
    QueryCase(name="eq", filters={"$values": {"status": "open"}}, expected=frozenset("ac")),
    QueryCase(
        name="neq", filters={"$values": {"status": {"$neq": "open"}}}, expected=frozenset("bd")
    ),
    QueryCase(
        name="in", filters={"$values": {"status": {"$in": ["closed"]}}}, expected=frozenset("bd")
    ),
    QueryCase(
        name="nin", filters={"$values": {"status": {"$nin": ["closed"]}}}, expected=frozenset("ac")
    ),
    # Ordered as its value: "open" > "closed".
    QueryCase(
        name="gt", filters={"$values": {"status": {"$gt": "closed"}}}, expected=frozenset("ac")
    ),
    QueryCase(
        name="lte", filters={"$values": {"status": {"$lte": "closed"}}}, expected=frozenset("bd")
    ),
    QueryCase(
        name="array_any",
        filters={"$values": {"statuses": {"$any": "closed"}}},
        expected=frozenset("ab"),
    ),
    QueryCase(
        name="array_overlaps",
        filters={"$values": {"statuses": {"$overlaps": ["open"]}}},
        expected=frozenset("ad"),
    ),
    QueryCase(
        name="nested_any",
        filters={"$values": {"items": {"$any": {"$values": {"status": "closed"}}}}},
        expected=frozenset("a"),
    ),
)


async def assert_enum_cases(query: Any, ids: Mapping[str, UUID], *, backend: str) -> None:
    """Assert every case on *query* (a document query port seeded with :data:`ROWS`)."""

    keys = {pk: key for key, pk in ids.items()}

    for case in CASES:
        page = await query.find_many(filters=case.filters, pagination={"limit": 100})
        got = frozenset(keys[hit.id] for hit in page.hits)

        assert got == case.expected, f"{backend}/{case.name}: {sorted(got)}"
        assert await query.count(case.filters) == len(case.expected), f"{backend}/{case.name}"
