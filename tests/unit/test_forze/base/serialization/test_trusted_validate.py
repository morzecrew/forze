"""Trusted Pydantic row decode."""

from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec
from forze.base.serialization.pydantic import (
    pydantic_validate,
    pydantic_validate_many,
    pydantic_validate_many_batched,
    pydantic_validate_many_trusted,
    pydantic_validate_trusted,
)


class _Model(BaseModel):
    id: str
    name: str = Field(min_length=1)


def test_trusted_matches_strict_for_well_formed_rows() -> None:
    row = {"id": str(uuid4()), "name": "ok"}
    strict = pydantic_validate(_Model, row)
    trusted = pydantic_validate_trusted(_Model, row)

    assert strict == trusted


def test_trusted_rejects_unknown_columns() -> None:
    row = {"id": "1", "name": "ok", "extra_col": 99}

    with pytest.raises(CoreException) as raised:
        pydantic_validate_trusted(_Model, row)

    assert raised.value.kind is ExceptionKind.PRECONDITION


def test_codec_trusted_mode() -> None:
    row = {"id": "1", "name": "ok"}
    codec = PydanticModelCodec(_Model)

    assert codec.decode_mapping(row, trust_source=True).name == "ok"


def test_trusted_many_matches_strict() -> None:
    rows = [
        {"id": str(uuid4()), "name": "a"},
        {"id": str(uuid4()), "name": "b"},
    ]
    strict = pydantic_validate_many(_Model, rows, trust_source=False)
    trusted = pydantic_validate_many_trusted(_Model, rows)

    assert strict == trusted
    assert pydantic_validate_many(_Model, rows, trust_source=True) == trusted


def test_trusted_many_rejects_unknown_columns() -> None:
    rows = [
        {"id": "1", "name": "ok"},
        {"id": "2", "name": "bad", "extra_col": 1},
    ]

    with pytest.raises(CoreException) as raised:
        pydantic_validate_many_trusted(_Model, rows)

    assert raised.value.kind is ExceptionKind.PRECONDITION


def test_trusted_many_batched_matches_many() -> None:
    rows = [{"id": str(i), "name": f"n{i}"} for i in range(5)]
    flat = pydantic_validate_many_trusted(_Model, rows)
    batched = list(pydantic_validate_many_batched(_Model, rows, batch_size=2, trust_source=True))

    assert [m for chunk in batched for m in chunk] == flat
