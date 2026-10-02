"""Unit tests for Meilisearch search parameter helpers."""

from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze_meilisearch.adapters.search._offset_run import page_sort
from forze_meilisearch.adapters.search._search_params import (
    attributes_to_search_on,
    build_search_query_string,
)
from forze_meilisearch.execution.deps.configs import MeilisearchSearchConfig


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


def _gw(*, sortable: list[str] | None = None, primary_key: str = "id") -> MagicMock:
    gw = MagicMock()
    gw.config = MeilisearchSearchConfig(
        index_uid="rows", sortable_attributes=sortable, primary_key=primary_key
    )
    gw.primary_key = primary_key
    return gw


_SPEC = SearchSpec(name="s", model_type=_Row, fields=["title"], default_sort={"rank": "desc"})


def test_a_blank_page_takes_the_default_sort_then_the_id() -> None:
    assert page_sort(_gw(), _SPEC, None, ranked=False) == ["rank:desc", "id:desc"]


def test_a_caller_sort_replaces_the_default() -> None:
    assert page_sort(_gw(), _SPEC, {"title": "asc"}, ranked=False) == ["title:asc", "id:asc"]


def test_a_page_with_search_text_sends_only_the_requests_sorts() -> None:
    # Meilisearch applies sort before exactness; anything added would outrank relevance ties.
    assert page_sort(_gw(), _SPEC, None, ranked=True) is None
    assert page_sort(_gw(), _SPEC, {"title": "asc"}, ranked=True) == ["title:asc"]


def test_the_id_sorts_by_the_primary_key() -> None:
    sort = page_sort(_gw(primary_key="doc_id"), _SPEC, {"id": "desc"}, ranked=False)

    assert sort == ["doc_id:desc"]
    assert page_sort(_gw(primary_key="doc_id"), _SPEC, None, ranked=False) == [
        "rank:desc",
        "doc_id:desc",
    ]


def test_a_sort_value_in_its_long_form_is_read() -> None:
    sort = page_sort(_gw(), _SPEC, {"title": {"dir": "desc"}}, ranked=False)

    assert sort == ["title:desc", "id:desc"]


def test_an_explicit_null_placement_is_refused() -> None:
    with pytest.raises(CoreException, match="nulls"):
        page_sort(_gw(), _SPEC, {"title": {"dir": "asc", "nulls": "last"}}, ranked=False)


def test_an_index_pinned_without_the_id_is_not_sorted_by_it() -> None:
    # The engine would refuse a sort on an attribute the index does not declare sortable.
    assert page_sort(_gw(sortable=["title", "rank"]), _SPEC, None, ranked=False) == ["rank:desc"]
