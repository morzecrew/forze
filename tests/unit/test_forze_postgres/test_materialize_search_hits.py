"""Unit tests for :mod:`forze_postgres.adapters.search._materialize_hits`."""

import pytest
from pydantic import BaseModel, field_validator

from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze.base.serialization import PydanticModelCodec
from forze_postgres.adapters.search._materialize_hits import (
    decode_search_hits,
    materialize_search_page,
    search_trust_source,
)


class Hit(BaseModel):
    id: int
    name: str


class HitView(BaseModel):
    id: int


_CODEC = PydanticModelCodec(Hit)


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
        codec=_CODEC,
    )
    assert out == [{"id": 1, "name": "a"}]


def test_materialize_reuses_pool_slice_when_return_type_none() -> None:
    pool = [Hit(id=1, name="x"), Hit(id=2, name="y"), Hit(id=3, name="z")]
    rows_slice: list[JsonDict] = [{"id": 2, "name": "y"}]
    out = materialize_search_page(
        page_rows=rows_slice,
        pool=pool,
        u=1,
        page_limit=1,
        return_type=None,
        return_fields=None,
        model_type=Hit,
        codec=_CODEC,
    )
    assert len(out) == 1
    assert out[0] is pool[1]


def test_materialize_validates_when_no_pool() -> None:
    rows: list[JsonDict] = [{"id": 9, "name": "n"}]
    out = materialize_search_page(
        page_rows=rows,
        pool=None,
        u=0,
        page_limit=10,
        return_type=None,
        return_fields=None,
        model_type=Hit,
        codec=_CODEC,
    )
    assert isinstance(out[0], Hit)
    assert out[0].id == 9


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
        codec=_CODEC,
    )
    assert out == [pool[1]]


def test_materialize_different_return_type_validates_rows() -> None:
    pool = [Hit(id=1, name="a")]
    page_rows: list[JsonDict] = [{"id": 1}]
    out = materialize_search_page(
        page_rows=page_rows,
        pool=pool,
        u=0,
        page_limit=1,
        return_type=HitView,
        return_fields=None,
        model_type=Hit,
        codec=_CODEC,
    )
    assert len(out) == 1
    assert isinstance(out[0], HitView)
    assert out[0].id == 1


class _StrictHit(BaseModel):
    id: int
    name: str

    @field_validator("name")
    @classmethod
    def name_must_be_long(cls, value: str) -> str:
        if len(value) < 100:
            msg = "name too short for strict validation"
            raise ValueError(msg)

        return value


def test_search_trust_source() -> None:
    assert search_trust_source("strict") is False
    assert search_trust_source("trusted") is True


def test_decode_search_hits_return_type_uses_projection_codec() -> None:
    rows: list[JsonDict] = [{"id": 1}]
    out = decode_search_hits(
        rows=rows,
        model_type=Hit,
        codec=_CODEC,
        return_type=HitView,
        trust_source=False,
    )
    assert isinstance(out[0], HitView)


def test_materialize_trusted_runs_validators() -> None:
    """Trusted decode trusts columns only: values are still validated/coerced."""

    from pydantic import ValidationError

    codec = PydanticModelCodec(_StrictHit)
    valid_rows: list[JsonDict] = [{"id": 1, "name": "x" * 100}]
    out = materialize_search_page(
        page_rows=valid_rows,
        pool=None,
        u=0,
        page_limit=10,
        return_type=None,
        return_fields=None,
        model_type=_StrictHit,
        codec=codec,
        trust_source=True,
    )
    assert isinstance(out[0], _StrictHit)

    with pytest.raises(ValidationError):
        materialize_search_page(
            page_rows=[{"id": 1, "name": "x"}],
            pool=None,
            u=0,
            page_limit=10,
            return_type=None,
            return_fields=None,
            model_type=_StrictHit,
            codec=codec,
            trust_source=True,
        )


def test_materialize_trusted_rejects_extra_columns() -> None:
    codec = PydanticModelCodec(Hit)
    rows: list[JsonDict] = [{"id": 1, "name": "a", "extra": 9}]

    with pytest.raises(CoreException, match="unknown field"):
        materialize_search_page(
            page_rows=rows,
            pool=None,
            u=0,
            page_limit=10,
            return_type=None,
            return_fields=None,
            model_type=Hit,
            codec=codec,
            trust_source=True,
        )
