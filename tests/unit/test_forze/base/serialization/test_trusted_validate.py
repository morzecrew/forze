"""Trusted Pydantic row decode."""

from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import (
    PydanticRecordMappingCodec,
    pydantic_validate,
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
    codec = PydanticRecordMappingCodec(_Model)

    assert codec.decode_mapping(row, trust_source=True).name == "ok"
