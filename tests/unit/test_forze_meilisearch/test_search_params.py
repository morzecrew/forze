"""Unit tests for Meilisearch search parameter helpers."""

from unittest.mock import MagicMock

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze_meilisearch.adapters.search._offset_run import page_sort
from forze_meilisearch.adapters.search._search_params import (
    attributes_to_search_on,
    build_search_query_string,
)


class _M(BaseModel):
    title: str
    body: str = ""


def test_build_search_query_any() -> None:
    assert build_search_query_string(("a", "b"), combine="any") == "a b"


def test_build_search_query_all() -> None:
    assert build_search_query_string(("a", "b"), combine="all") == '"a" "b"'


def test_build_search_query_strips_embedded_quotes() -> None:
    # An embedded ``"`` must not break phrase boundaries / split the query.
    assert build_search_query_string(('a"b', "c"), combine="all") == '"ab" "c"'
    assert build_search_query_string(('a"b', "c"), combine="any") == "ab c"


def test_attributes_to_search_on_fields_option() -> None:
    spec = SearchSpec(name="s", model_type=_M, fields=["title", "body"])
    attrs = attributes_to_search_on(spec, {"fields": ["title"]}, {})
    assert attrs == ["title"]


# ....................... #


class _Row(BaseModel):
    id: str
    title: str
    rank: int = 0


def _gw(*, sortable: list[str] | None) -> MagicMock:
    gw = MagicMock()
    gw.config = MagicMock(sortable_attributes=sortable)
    gw.field_map = {}
    return gw


_SPEC = SearchSpec(name="s", model_type=_Row, fields=["title"], default_sort={"rank": "desc"})


def test_an_unsorted_page_takes_the_default_sort_then_the_id() -> None:
    assert page_sort(_gw(sortable=None), _SPEC, None) == ["rank:desc", "id:desc"]


def test_a_caller_sort_replaces_the_default() -> None:
    assert page_sort(_gw(sortable=None), _SPEC, {"title": "asc"}) == ["title:asc", "id:asc"]


def test_a_sort_value_in_its_long_form_is_read() -> None:
    sort = page_sort(_gw(sortable=None), _SPEC, {"title": {"dir": "desc", "nulls": "last"}})

    assert sort == ["title:desc", "id:desc"]


def test_an_index_pinned_without_the_id_is_not_sorted_by_it() -> None:
    # The engine would refuse a sort on an attribute the index does not declare sortable.
    assert page_sort(_gw(sortable=["title", "rank"]), _SPEC, None) == ["rank:desc"]
