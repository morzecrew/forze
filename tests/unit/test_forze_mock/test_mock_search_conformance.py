"""The in-memory oracle against the shared search battery.

The oracle searches the mock document store, so its corpus arrives through the document
adapter — the same shape as Postgres and Mongo searching their system of record.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockSearchAdapter, MockState
from forze_mock.adapters.search.command import (
    MockSearchCommandAdapter,
    MockSearchManagementAdapter,
)
from tests.support.search_conformance import (
    CORPUS,
    SEARCH_BATTERY,
    SEARCH_WRITE_BATTERY,
    Check,
    SearchHarness,
    SearchWriteHarness,
    WriteCheck,
    searchable_fields,
)

pytestmark = pytest.mark.asyncio


class _Row(BaseModel):
    id: UUID
    title: str
    content: str
    category: str = ""


class _Domain(Document):
    title: str
    content: str
    category: str = ""


class _Read(ReadDocument):
    title: str
    content: str
    category: str = ""


class _Create(CreateDocumentCmd):
    title: str
    content: str
    category: str = ""


class _Update(BaseDTO):
    title: str | None = None


@pytest_asyncio.fixture
async def harness() -> SearchHarness:
    state = MockState()
    documents = MockDocumentAdapter(
        spec=DocumentSpec(
            name="rows",
            read=_Read,
            write=DocumentWriteTypes(
                domain=_Domain,
                create_cmd=_Create,
                update_cmd=_Update,
            ),
        ),
        state=state,
        namespace="rows",
        read_model=_Read,
        domain_model=_Domain,
    )

    for title, content, category in CORPUS:
        await documents.create(_Create(title=title, content=content, category=category))

    return SearchHarness(
        query=MockSearchAdapter(
            state=state,
            spec=SearchSpec(
                name="rows",
                model_type=_Row,
                fields=searchable_fields(),
                facetable_fields=frozenset({"category"}),
            ),
        ),
        backend="mock",
        # The oracle reads a blank query as "filter-only over everything".
        blank_query_matches_all=True,
    )


@pytest.mark.parametrize("check", SEARCH_BATTERY, ids=lambda check: check.__name__)
async def test_search_battery(check: Check, harness: SearchHarness) -> None:
    await check(harness)


# ....................... #


@pytest.fixture
def write_harness() -> SearchWriteHarness:
    state = MockState()
    spec = SearchSpec(name="rows", model_type=_Row, fields=searchable_fields())

    return SearchWriteHarness(
        command=MockSearchCommandAdapter(state=state, spec=spec),
        management=MockSearchManagementAdapter(state=state, spec=spec),
        query=MockSearchAdapter(state=state, spec=spec),
        backend="mock",
        new_row=lambda title: _Row(id=uuid4(), title=title, content="python"),
    )


@pytest.mark.parametrize("check", SEARCH_WRITE_BATTERY, ids=lambda check: check.__name__)
async def test_search_write_battery(check: WriteCheck, write_harness: SearchWriteHarness) -> None:
    await check(write_harness)
