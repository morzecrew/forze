"""Unit tests for :mod:`forze_mongo.adapters.search._materialize`."""

from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze_mongo.adapters.search._materialize import materialize_search_page


class Hit(BaseModel):
    id: int
    name: str


class HitView(BaseModel):
    id: int


def test_materialize_return_fields_from_page_rows() -> None:
    rows: list[JsonDict] = [{"id": 1, "name": "a", "extra": 9}]
    out = materialize_search_page(
        page_rows=rows,
        pool=None,
        u=0,
        page_limit=10,
        return_type=None,
        return_fields=("id", "name"),
        model_type=Hit,
    )
    assert out == [{"id": 1, "name": "a"}]


def test_materialize_reuses_pool_slice_when_return_type_none() -> None:
    pool = [Hit(id=1, name="x"), Hit(id=2, name="y")]
    out = materialize_search_page(
        page_rows=[{"id": 2, "name": "y"}],
        pool=pool,
        u=1,
        page_limit=1,
        return_type=None,
        return_fields=None,
        model_type=Hit,
    )
    assert out[0] is pool[1]


def test_materialize_validates_when_no_pool() -> None:
    out = materialize_search_page(
        page_rows=[{"id": 9, "name": "n"}],
        pool=None,
        u=0,
        page_limit=10,
        return_type=None,
        return_fields=None,
        model_type=Hit,
    )
    assert isinstance(out[0], Hit)


def test_materialize_return_type_same_as_model_uses_pool() -> None:
    pool = [Hit(id=1, name="a"), Hit(id=2, name="b")]
    out = materialize_search_page(
        page_rows=[{"id": 2, "name": "b"}],
        pool=pool,
        u=1,
        page_limit=1,
        return_type=Hit,
        return_fields=None,
        model_type=Hit,
    )
    assert out == [pool[1]]


def test_materialize_different_return_type_validates_rows() -> None:
    out = materialize_search_page(
        page_rows=[{"id": 1}],
        pool=[Hit(id=1, name="a")],
        u=0,
        page_limit=1,
        return_type=HitView,
        return_fields=None,
        model_type=Hit,
    )
    assert isinstance(out[0], HitView)
