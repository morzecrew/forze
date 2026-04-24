"""Unit tests for ranked cursor key specs."""

import pytest

from forze.base.errors import CoreError
from forze.domain.constants import ID_FIELD
from forze_postgres.adapters.search._cursor_keyset import (
    cursor_return_fields_for_select,
    ranked_search_cursor_key_spec,
)


def test_ranked_search_cursor_key_spec_adds_id_tie_breaker() -> None:
    spec = ranked_search_cursor_key_spec(rank_field="_r", sorts=None)
    assert spec[0] == ("_r", "desc")
    assert spec[-1] == (ID_FIELD, "asc")


def test_ranked_search_cursor_key_spec_inherits_uniform_sort_direction_for_id() -> None:
    spec = ranked_search_cursor_key_spec(
        rank_field="_r",
        sorts={"title": "desc"},
    )
    keys = [k for k, _ in spec]
    assert keys[0] == "_r"
    assert keys[-1] == ID_FIELD
    assert spec[-1][1] == "desc"


def test_ranked_search_cursor_key_spec_rejects_bad_direction() -> None:
    with pytest.raises(CoreError, match="Invalid sort direction"):
        ranked_search_cursor_key_spec(rank_field="_r", sorts={"x": "sideways"})  # type: ignore[arg-type]


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
