"""Tests for :mod:`forze_postgres.pagination.seek_sql`."""

from forze_postgres.pagination.seek_sql import build_order_by_sql, build_seek_condition
from psycopg import sql


def test_build_seek_after_asc() -> None:
    a = sql.Identifier("h", "a")
    c, p = build_seek_condition([a], ["asc"], [10], "after")
    assert p == [10]
    s = str(c)
    assert ">" in s


def test_build_order_by_sql_flip() -> None:
    a = sql.Identifier("h", "a")
    b = sql.Identifier("h", "b")
    ob = build_order_by_sql([a, b], ["asc", "asc"], flip=True)
    t = str(ob)
    assert t.count("DESC") == 2
