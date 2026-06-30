"""Mock search facets & highlights — the reference-oracle behavior."""

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.search import FacetBucket, SearchSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze_mock.adapters import MockDocumentAdapter, MockSearchAdapter, MockState

# ----------------------- #


class _ProductDoc(DocWithSoftDeletion):
    title: str
    category: str
    tags: list[str] = []


class _ProductCreate(CreateDocumentCmd):
    title: str
    category: str
    tags: list[str] = []


class _ProductUpdate(BaseDTO):
    title: str | None = None


class _ProductRead(ReadDocument):
    title: str
    category: str
    tags: list[str] = []
    is_deleted: bool = False


class _ProductSearch(BaseModel):
    id: UUID
    title: str
    category: str
    tags: list[str] = []


# ....................... #


def _search_adapter(state: MockState) -> MockSearchAdapter[_ProductSearch]:
    spec = SearchSpec(
        name="products",
        model_type=_ProductSearch,
        fields=["title", "category", "tags"],
        facetable_fields=frozenset({"category", "tags"}),
    )
    return MockSearchAdapter(state=state, spec=spec)


async def _seed(state: MockState) -> None:
    doc: MockDocumentAdapter[_ProductRead, _ProductDoc, _ProductCreate, _ProductUpdate]
    doc = MockDocumentAdapter(
        spec=DocumentSpec(
            name="products",
            read=_ProductRead,
            write=DocumentWriteTypes(
                domain=_ProductDoc,
                create_cmd=_ProductCreate,
                update_cmd=_ProductUpdate,
            ),
        ),
        state=state,
        namespace="products",
        read_model=_ProductRead,
        domain_model=_ProductDoc,
    )
    await doc.create(_ProductCreate(title="Rust Book", category="books", tags=["rust"]))
    await doc.create(
        _ProductCreate(title="Python Book", category="books", tags=["python"])
    )
    await doc.create(
        _ProductCreate(title="Gaming Mouse", category="hardware", tags=["pc"])
    )


# ....................... #


@pytest.mark.asyncio
async def test_facets_over_full_matching_set() -> None:
    state = MockState()
    await _seed(state)
    search = _search_adapter(state)

    # Empty query = filter-only over everything; facet by category.
    page = await search.search_page("", options={"facets": ["category"]})

    assert page.count == 3
    assert page.facets is not None
    assert page.facets["category"] == (
        FacetBucket(value="books", count=2),
        FacetBucket(value="hardware", count=1),
    )


@pytest.mark.asyncio
async def test_facets_independent_of_page_window() -> None:
    state = MockState()
    await _seed(state)
    search = _search_adapter(state)

    # One hit per page, but facets span the full matching set.
    page = await search.search_page(
        "", pagination={"limit": 1}, options={"facets": ["category"]}
    )

    assert len(page.hits) == 1
    assert page.facets is not None
    assert sum(b.count for b in page.facets["category"]) == 3


@pytest.mark.asyncio
async def test_facet_size_caps_buckets() -> None:
    state = MockState()
    await _seed(state)
    search = _search_adapter(state)

    page = await search.search_page(
        "", options={"facets": ["category"], "facet_size": 1}
    )

    assert page.facets is not None
    assert page.facets["category"] == (FacetBucket(value="books", count=2),)


@pytest.mark.asyncio
async def test_facet_on_non_facetable_field_refused() -> None:
    state = MockState()
    await _seed(state)
    search = _search_adapter(state)

    with pytest.raises(CoreException) as ei:
        await search.search_page("", options={"facets": ["title"]})

    assert ei.value.kind is ExceptionKind.PRECONDITION


@pytest.mark.asyncio
async def test_highlights_wrap_matched_terms_index_aligned() -> None:
    state = MockState()
    await _seed(state)
    search = _search_adapter(state)

    page = await search.search_page("book", options={"highlight": True})

    assert page.highlights is not None
    assert len(page.highlights) == len(page.hits)
    titles_to_hl = {hit.title: hl for hit, hl in zip(page.hits, page.highlights)}
    # Matched fragment preserves original case; markers wrap the substring match.
    assert titles_to_hl["Rust Book"]["title"] == ("Rust <em>Book</em>",)
    # The mock matches by substring (consistent with its scoring), so category
    # "books" also highlights the "book" substring.
    assert titles_to_hl["Rust Book"]["category"] == ("<em>book</em>s",)
    # The non-matching "Gaming Mouse" hit is absent from this query's results.
    assert "Gaming Mouse" not in titles_to_hl


@pytest.mark.asyncio
async def test_highlights_custom_tags_and_field_subset() -> None:
    state = MockState()
    await _seed(state)
    search = _search_adapter(state)

    page = await search.search_page(
        "book",
        options={"highlight": {"fields": ["title"], "pre_tag": "[", "post_tag": "]"}},
    )

    assert page.highlights is not None
    fragments = [hl["title"][0] for hl in page.highlights if "title" in hl]
    assert any(frag.endswith("[Book]") for frag in fragments)


@pytest.mark.asyncio
async def test_no_facets_or_highlights_when_not_requested() -> None:
    state = MockState()
    await _seed(state)
    search = _search_adapter(state)

    page = await search.search_page("book")

    assert page.facets is None
    assert page.highlights is None
