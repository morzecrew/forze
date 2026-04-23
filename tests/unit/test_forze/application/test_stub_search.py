"""Unit tests for MockSearchAdapter (forze_mock)."""

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

from forze_mock import MockState
from forze_mock.adapters import MockDocumentAdapter, MockSearchAdapter

# ----------------------- #


class _DocWithTitle(Document):
    """Document with title for search tests."""

    title: str = ""


class _CreateWithTitle(CreateDocumentCmd):
    """Create command with title."""

    title: str = ""


class _ReadWithTitle(ReadDocument):
    """Read model with title."""

    title: str = ""


class _SearchHit(BaseModel):
    """Search hit model."""

    id: UUID
    title: str
    a: int = 0


def _doc_adapter(state: MockState) -> MockDocumentAdapter:
    spec = DocumentSpec(
        name="search_stub",
        read=_ReadWithTitle,
        write=DocumentWriteTypes(
            domain=_DocWithTitle,
            create_cmd=_CreateWithTitle,
            update_cmd=BaseDTO,
        ),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="search_stub",
        read_model=_ReadWithTitle,
        domain_model=_DocWithTitle,
    )


def _search_adapter(state: MockState) -> MockSearchAdapter[_SearchHit]:
    spec = SearchSpec(
        name="search_stub",
        model_type=_SearchHit,
        fields=["title", "a"],
    )
    return MockSearchAdapter(state=state, spec=spec)


class TestMockSearchAdapter:
    """Tests for MockSearchAdapter."""

    @pytest.mark.asyncio
    async def test_search_empty_returns_empty(self) -> None:
        state = MockState()
        search = _search_adapter(state)
        page = await search.search("q", return_count=True)
        assert page.hits == []
        assert page.count == 0

    @pytest.mark.asyncio
    async def test_search_returns_documents_matching_query(self) -> None:
        state = MockState()
        doc = _doc_adapter(state)
        search = _search_adapter(state)

        await doc.create(_CreateWithTitle(title="foo"))
        await doc.create(_CreateWithTitle(title="foo"))
        page = await search.search("foo", return_count=True)
        assert page.count == 2
        assert page.hits[0].title == "foo"
        assert page.hits[1].title == "foo"

    @pytest.mark.asyncio
    async def test_search_list_query_matches_any_term(self) -> None:
        state = MockState()
        doc = _doc_adapter(state)
        search = _search_adapter(state)

        await doc.create(_CreateWithTitle(title="alpha"))
        await doc.create(_CreateWithTitle(title="beta"))
        page = await search.search(
            ["alpha", "gamma"],
            return_count=True,
        )
        assert page.count == 1
        assert page.hits[0].title == "alpha"

    @pytest.mark.asyncio
    async def test_search_respects_limit(self) -> None:
        state = MockState()
        doc = _doc_adapter(state)
        search = _search_adapter(state)

        for i in range(5):
            await doc.create(_CreateWithTitle(title="q"))
        page = await search.search(
            "q", pagination={"limit": 2}, return_count=True
        )
        assert page.count == 5
        assert len(page.hits) == 2

    @pytest.mark.asyncio
    async def test_search_respects_offset(self) -> None:
        state = MockState()
        doc = _doc_adapter(state)
        search = _search_adapter(state)

        for _ in range(5):
            await doc.create(_CreateWithTitle(title="q"))
        page = await search.search(
            "q",
            pagination={"offset": 2, "limit": 2},
            return_count=True,
        )
        assert page.count == 5
        assert len(page.hits) == 2

    @pytest.mark.asyncio
    async def test_search_with_return_fields_projects(self) -> None:
        state = MockState()
        doc = _doc_adapter(state)
        search = _search_adapter(state)

        await doc.create(_CreateWithTitle(title="foo"))
        page = await search.search(
            "foo", return_fields=["title"], return_count=True
        )
        assert page.count == 1
        assert page.hits[0] == {"title": "foo"}

    @pytest.mark.asyncio
    async def test_search_with_typed_hits(self) -> None:
        state = MockState()
        doc = _doc_adapter(state)
        search = _search_adapter(state)

        await doc.create(_CreateWithTitle(title="first"))
        page = await search.search("first", return_count=True)
        assert page.count == 1
        assert page.hits[0].title == "first"
