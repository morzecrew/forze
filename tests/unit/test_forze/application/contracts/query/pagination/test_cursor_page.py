"""Tests for :mod:`forze.application.contracts.querying.pagination.cursor_page`."""

from typing import Any, cast

import pytest

from forze.base.exceptions import CoreException

from forze.application.contracts.querying.pagination.cursor_page import (
    assemble_keyset_cursor_page,
    assert_cursor_projection_includes_sort_keys,
    resolved_cursor_limit,
)
from forze.application.contracts.querying.pagination.cursor_token import (
    decode_keyset_v1,
)
from forze.base.primitives import JsonDict
from forze.domain.constants import ID_FIELD


def _as_json_dict(row: Any) -> JsonDict:
    return cast(JsonDict, row)


def test_assert_cursor_projection_ok_when_no_projection() -> None:
    assert_cursor_projection_includes_sort_keys(
        return_fields=None,
        sort_keys=[ID_FIELD],
    )


def test_assert_cursor_projection_ok_when_all_keys_included() -> None:
    assert_cursor_projection_includes_sort_keys(
        return_fields=[ID_FIELD, "name"],
        sort_keys=[ID_FIELD, "name"],
    )


def test_assert_cursor_projection_raises_when_sort_key_missing() -> None:
    with pytest.raises(CoreException, match="projection must include"):
        assert_cursor_projection_includes_sort_keys(
            return_fields=[ID_FIELD],
            sort_keys=[ID_FIELD, "name"],
        )


def test_assert_cursor_projection_nested_key_satisfied_by_root() -> None:
    # Projecting the root JSON column satisfies a nested sort key (the token reads
    # the nested value out of it).
    assert_cursor_projection_includes_sort_keys(
        return_fields=["addr", ID_FIELD],
        sort_keys=["addr.city", ID_FIELD],
    )


def test_assert_cursor_projection_nested_key_satisfied_by_exact_leaf() -> None:
    # Projecting the leaf itself also serves the sort key — the projected row nests it
    # (``{"addr": {"city": ...}}``) and the token reads ``addr.city`` back out.
    assert_cursor_projection_includes_sort_keys(
        return_fields=["addr.city", ID_FIELD],
        sort_keys=["addr.city", ID_FIELD],
    )


def test_assert_cursor_projection_sibling_leaf_does_not_satisfy() -> None:
    # A sibling leaf shares the root but not the value: projecting ``addr.zip`` cannot serve
    # a sort on ``addr.city`` (the token would read None and seek from the wrong key).
    with pytest.raises(CoreException, match="projection must include"):
        assert_cursor_projection_includes_sort_keys(
            return_fields=["addr.zip", ID_FIELD],
            sort_keys=["addr.city", ID_FIELD],
        )


def test_assert_cursor_projection_nested_key_missing_root_raises() -> None:
    with pytest.raises(CoreException, match="projection must include"):
        assert_cursor_projection_includes_sort_keys(
            return_fields=["name", ID_FIELD],
            sort_keys=["addr.city", ID_FIELD],
        )


def test_resolved_cursor_limit_default() -> None:
    assert resolved_cursor_limit(None) == 10
    assert resolved_cursor_limit({}) == 10


def test_resolved_cursor_limit_explicit() -> None:
    assert resolved_cursor_limit({"limit": 25}) == 25


def test_assemble_empty_fetch() -> None:
    hits, has_more, nxt, prev = assemble_keyset_cursor_page(
        [],
        cursor=None,
        sort_keys=[ID_FIELD],
        directions=["asc"],
        dump_row=_as_json_dict,
    )
    assert hits == []
    assert has_more is False
    assert nxt is None
    assert prev is None


def test_assemble_has_more_and_next_cursor() -> None:
    rows = [
        {ID_FIELD: "a"},
        {ID_FIELD: "b"},
        {ID_FIELD: "c"},
    ]
    hits, has_more, nxt, prev = assemble_keyset_cursor_page(
        rows,
        cursor={"limit": 2},
        sort_keys=[ID_FIELD],
        directions=["asc"],
        dump_row=_as_json_dict,
    )
    assert hits == rows[:2]
    assert has_more is True
    assert nxt is not None
    k, _d, _n, v = decode_keyset_v1(nxt)
    assert k == [ID_FIELD]
    assert v == ["b"]
    assert prev is None


def test_assemble_prev_when_after_cursor_present() -> None:
    rows = [{ID_FIELD: "x"}, {ID_FIELD: "y"}]
    _hits, _hm, _nxt, prev = assemble_keyset_cursor_page(
        rows,
        cursor={"limit": 10, "after": "opaque"},
        sort_keys=[ID_FIELD],
        directions=["desc"],
        dump_row=_as_json_dict,
    )
    assert prev is not None
    k, d, _n, v = decode_keyset_v1(prev)
    assert k == [ID_FIELD]
    assert d == ["desc"]
    assert v == ["x"]
