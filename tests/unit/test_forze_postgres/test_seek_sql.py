"""Tests for :mod:`forze_postgres.pagination.seek_sql`."""

import pytest

from forze.base.errors import CoreError
from forze_postgres.pagination.seek_sql import build_order_by_sql, build_seek_condition
from psycopg import sql


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
    with pytest.raises(CoreError, match="Invalid keyset shape"):
        build_seek_condition([a], ["asc"], [], "after")
    with pytest.raises(CoreError, match="Invalid keyset shape"):
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
