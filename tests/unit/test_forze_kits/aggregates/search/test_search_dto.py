"""Response DTOs surface page-level facets and highlights (offset + cursor)."""

from pydantic import BaseModel

from forze.application.contracts.search import (
    FacetBucket,
    SearchCursorPage,
    SearchPage,
)
from forze_kits.aggregates.search import (
    ProjectedSearchCursorPaginated,
    ProjectedSearchPaginated,
    SearchCursorPaginated,
    SearchPaginated,
)

# ----------------------- #


class _Hit(BaseModel):
    id: int
    title: str


_FACETS = {"category": (FacetBucket(value="books", count=2),)}
_HIGHLIGHTS = [{"title": ("Rust <em>Book</em>",)}]


# ....................... #


def test_offset_dto_carries_facets_and_highlights() -> None:
    page = SearchPage(
        hits=[_Hit(id=1, title="Rust Book")],
        page=1,
        size=10,
        count=1,
        facets=_FACETS,
        highlights=_HIGHLIGHTS,
    )

    dto = SearchPaginated.from_search_page(page)

    assert dto.facets is not None
    assert dto.facets["category"][0].value == "books"
    assert dto.facets["category"][0].count == 2
    assert dto.highlights == [{"title": ("Rust <em>Book</em>",)}]


def test_cursor_dto_carries_facets_and_highlights() -> None:
    page = SearchCursorPage(
        hits=[_Hit(id=1, title="Rust Book")],
        next_cursor="c",
        prev_cursor=None,
        has_more=False,
        facets=_FACETS,
        highlights=_HIGHLIGHTS,
    )

    dto = SearchCursorPaginated.from_search_page(page)

    assert dto.facets is not None and dto.facets["category"][0].count == 2
    assert dto.highlights == [{"title": ("Rust <em>Book</em>",)}]


def test_projected_dtos_default_to_none_when_absent() -> None:
    page: SearchPage[dict] = SearchPage(hits=[], page=1, size=10, count=0)
    cursor: SearchCursorPage[dict] = SearchCursorPage(
        hits=[], next_cursor=None, prev_cursor=None, has_more=False
    )

    offset = ProjectedSearchPaginated.from_search_page(page)
    cur = ProjectedSearchCursorPaginated.from_search_page(cursor)

    assert offset.facets is None and offset.highlights is None
    assert cur.facets is None and cur.highlights is None


def test_facets_serialize_to_plain_json() -> None:
    page = SearchPage(
        hits=[_Hit(id=1, title="x")],
        page=1,
        size=10,
        count=1,
        facets=_FACETS,
        highlights=_HIGHLIGHTS,
    )

    dumped = SearchPaginated.from_search_page(page).model_dump()

    assert dumped["facets"] == {"category": [{"value": "books", "count": 2}]}
    assert dumped["highlights"] == [{"title": ("Rust <em>Book</em>",)}]
