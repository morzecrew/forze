"""Unit tests for ranked cursor key specs."""

import pytest

from forze.base.exceptions import CoreException

from forze.application.contracts.search import (
    cursor_return_fields_for_select,
    ranked_search_cursor_key_spec,
)
from forze.domain.constants import ID_FIELD


def test_ranked_search_cursor_key_spec_adds_id_tie_breaker() -> None:
    spec = ranked_search_cursor_key_spec(
        rank_field="_r",
        sorts=None,
        read_fields=frozenset({ID_FIELD}),
    )
    assert spec[0] == ("_r", "desc")
    assert spec[-1] == (ID_FIELD, "asc")


def test_ranked_search_cursor_key_spec_skips_id_when_not_on_model() -> None:
    spec = ranked_search_cursor_key_spec(
        rank_field="_r",
        sorts={"title": "desc"},
        read_fields=frozenset({"title"}),
    )
    assert spec == [("_r", "desc"), ("title", "desc")]


def test_ranked_search_cursor_key_spec_inherits_uniform_sort_direction_for_id() -> None:
    spec = ranked_search_cursor_key_spec(
        rank_field="_r",
        sorts={"title": "desc"},
        read_fields=frozenset({ID_FIELD, "title"}),
    )
    keys = [k for k, _ in spec]
    assert keys[0] == "_r"
    assert keys[-1] == ID_FIELD
    assert spec[-1][1] == "desc"


def test_ranked_search_cursor_key_spec_rejects_bad_direction() -> None:
    with pytest.raises(CoreException, match="Invalid sort direction"):
        ranked_search_cursor_key_spec(
            rank_field="_r",
            sorts={"x": "sideways"},  # type: ignore[arg-type]
            read_fields=frozenset({"x"}),
        )


def test_cursor_return_fields_for_select_merges_and_drops_rank() -> None:
    out = cursor_return_fields_for_select(
        sort_keys=("_rank", "title", "id"),
        rank_field="_rank",
        return_fields=("title",),
    )
    assert out == ("title", "id")


def test_cursor_return_fields_for_select_browse_order() -> None:
    out = cursor_return_fields_for_select(
        sort_keys=("name", "id"),
        rank_field=None,
        return_fields=("name",),
    )
    assert out == ("name", "id")


def test_cursor_return_fields_for_select_reduces_nested_sort_key_to_root() -> None:
    # A nested sort key projects its ROOT column (the whole JSON column); the cursor
    # token reads the nested value out of it, so the dotted path is never selected.
    out = cursor_return_fields_for_select(
        sort_keys=("_rank", "addr.city", "id"),
        rank_field="_rank",
        return_fields=("name",),
    )
    assert out == ("addr", "id", "name")


def test_cursor_return_fields_for_select_nested_root_dedupes_with_return_field() -> None:
    out = cursor_return_fields_for_select(
        sort_keys=("addr.city", "id"),
        rank_field=None,
        return_fields=("addr",),
    )
    # ``addr`` contributed by both the sort root and the caller — kept once.
    assert out == ("addr", "id")
