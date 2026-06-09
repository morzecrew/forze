"""Parity between PydanticModelCodec and direct pydantic helpers."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import BaseModel

from forze.base.serialization import PydanticModelCodec
from forze.base.serialization.pydantic import pydantic_validate, pydantic_validate_many


class _RowModel(BaseModel):
    id: UUID
    name: str
    value: int
    amount: Decimal
    created_at: datetime


def test_codec_decode_matches_pydantic_validate() -> None:
    row = {
        "id": uuid4(),
        "name": "item",
        "value": 7,
        "amount": Decimal("1.25"),
        "created_at": datetime(2024, 6, 1, 12, 0, 0),
    }
    codec = PydanticModelCodec(_RowModel)

    assert codec.decode_mapping(row) == pydantic_validate(_RowModel, row)


def test_codec_decode_many_matches_pydantic_validate_many() -> None:
    rows = [
        {
            "id": uuid4(),
            "name": f"n-{i}",
            "value": i,
            "amount": Decimal("0.5"),
            "created_at": datetime(2024, 6, 1, 12, 0, i),
        }
        for i in range(5)
    ]
    codec = PydanticModelCodec(_RowModel)

    assert codec.decode_mapping_many(rows) == pydantic_validate_many(_RowModel, rows)
