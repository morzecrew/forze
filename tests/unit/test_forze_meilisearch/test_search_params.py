"""Unit tests for Meilisearch search parameter helpers."""

from pydantic import BaseModel

from forze.application.contracts.search import SearchSpec
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


def test_attributes_to_search_on_fields_option() -> None:
    spec = SearchSpec(name="s", model_type=_M, fields=["title", "body"])
    attrs = attributes_to_search_on(spec, {"fields": ["title"]}, {})
    assert attrs == ["title"]
