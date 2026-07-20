"""Tests for :mod:`forze_postgres.kernel.sql.seek`."""

import pytest
from psycopg import sql

from forze.base.exceptions import CoreException
from forze_postgres.kernel.sql.seek import build_order_by_sql, build_seek_condition


def test_build_seek_after_asc() -> None:
    a = sql.Identifier("h", "a")
    c, p = build_seek_condition([a], ["asc"], [10], "after")
    assert p == [10]
    s = str(c)
    assert ">" in s


def test_build_seek_after_desc_uses_lt() -> None:
    a = sql.Identifier("h", "a")
    c, p = build_seek_condition([a], ["desc"], [10], "after")
    assert p == [10]
    assert "<" in str(c)


def test_build_seek_before_asc_uses_lt() -> None:
    a = sql.Identifier("h", "a")
    c, p = build_seek_condition([a], ["asc"], [10], "before")
    assert p == [10]
    assert "<" in str(c)


def test_build_seek_before_desc_uses_gt() -> None:
    a = sql.Identifier("h", "a")
    c, p = build_seek_condition([a], ["desc"], [10], "before")
    assert p == [10]
    assert ">" in str(c)


def test_build_seek_multi_column_or_chain() -> None:
    a = sql.Identifier("h", "a")
    b = sql.Identifier("h", "b")
    c, p = build_seek_condition([a, b], ["asc", "asc"], [1, 2], "after")
    assert p == [1, 1, 2]
    text = str(c)
    assert "OR" in text
    assert text.count("=") >= 1


def test_build_seek_invalid_shape_raises() -> None:
    a = sql.Identifier("h", "a")
    with pytest.raises(CoreException, match="Invalid keyset shape"):
        build_seek_condition([a], ["asc"], [], "after")
    with pytest.raises(CoreException, match="Invalid keyset shape"):
        build_seek_condition([], [], [], "after")


def test_build_order_by_sql_flip() -> None:
    a = sql.Identifier("h", "a")
    b = sql.Identifier("h", "b")
    ob = build_order_by_sql([a, b], ["asc", "asc"], flip=True)
    t = str(ob)
    assert t.count("DESC") == 2


def test_build_order_by_sql_mixed_directions_no_flip() -> None:
    a = sql.Identifier("h", "a")
    b = sql.Identifier("h", "b")
    ob = build_order_by_sql([a, b], ["asc", "desc"], flip=False)
    t = str(ob)
    assert "ASC" in t
    assert "DESC" in t


# Null-aware seek (canonical: null is the smallest value — asc nulls first, desc last).


class TestNullAwareSeek:
    a = sql.Identifier("h", "a")

    @pytest.mark.parametrize(
        ("direction", "nav", "needs_not_null", "is_false"),
        [
            ("asc", "after", True, False),
            ("desc", "after", False, True),
            ("asc", "before", False, True),
            ("desc", "before", True, False),
        ],
    )
    def test_null_boundary(
        self, direction: str, nav: str, needs_not_null: bool, is_false: bool
    ) -> None:
        # A null boundary value: only non-null columns can be strictly past it on the
        # ">" side; on the "<" side nothing is smaller than a null.
        c, p = build_seek_condition([self.a], [direction], [None], nav)  # type: ignore[arg-type]
        s = str(c)

        assert p == []  # no bound param for a null boundary
        if needs_not_null:
            assert "IS NOT NULL" in s
        if is_false:
            assert "FALSE" in s

    @pytest.mark.parametrize(
        ("direction", "nav", "op", "includes_null_col"),
        [
            ("asc", "after", ">", False),
            ("desc", "after", "<", True),
            ("asc", "before", "<", True),
            ("desc", "before", ">", False),
        ],
    )
    def test_non_null_boundary_includes_null_column_on_lt_side(
        self, direction: str, nav: str, op: str, includes_null_col: bool
    ) -> None:
        c, p = build_seek_condition([self.a], [direction], [10], nav)  # type: ignore[arg-type]
        s = str(c)

        assert p == [10]
        assert op in s
        # The "<" navs also admit a null column (the smallest value); ">" navs don't.
        assert ("IS NULL" in s) is includes_null_col

    def test_null_prefix_value_uses_is_null_equality(self) -> None:
        # A null value on a prefix key uses IS NULL (null-safe), binding no param for it.
        b = sql.Identifier("h", "b")
        c, p = build_seek_condition([self.a, b], ["asc", "asc"], [None, 5], "after")  # type: ignore[arg-type]
        s = str(c)

        assert "IS NULL" in s
        assert p == [5]  # only the non-null key 'b' binds a param

    def test_order_by_emits_canonical_nulls(self) -> None:
        a = sql.Identifier("h", "a")
        b = sql.Identifier("h", "b")
        ob = str(build_order_by_sql([a, b], ["asc", "desc"]))

        # The Composed repr renders each fragment separately, so check the pieces.
        assert "'ASC'" in ob and "'NULLS FIRST'" in ob
        assert "'DESC'" in ob and "'NULLS LAST'" in ob
