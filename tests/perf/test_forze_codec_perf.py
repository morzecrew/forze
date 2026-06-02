"""Micro-benchmarks for ModelCodec decode paths."""

import pytest
from pydantic import BaseModel, field_validator

from forze.base.primitives import JsonDict
from forze.base.serialization import PydanticModelCodec

_ROWS = 1_000


class _CodecRow(BaseModel):
    id: int
    name: str
    value: int

    @field_validator("name")
    @classmethod
    def name_not_empty(cls, value: str) -> str:
        if not value:
            msg = "name must be non-empty"
            raise ValueError(msg)

        return value


def _sample_rows(n: int) -> list[JsonDict]:
    return [{"id": i, "name": f"row-{i}", "value": i * 2} for i in range(n)]


@pytest.mark.perf
def test_codec_strict_decode_benchmark(benchmark) -> None:
    """Benchmark strict ``decode_mapping_many`` (validators run)."""

    codec = PydanticModelCodec(_CodecRow)
    rows = _sample_rows(_ROWS)

    benchmark(lambda: codec.decode_mapping_many(rows, trust_source=False))


@pytest.mark.perf
def test_codec_trusted_decode_benchmark(benchmark) -> None:
    """Benchmark trusted ``decode_mapping_many`` (``model_construct``)."""

    codec = PydanticModelCodec(_CodecRow)
    rows = _sample_rows(_ROWS)

    benchmark(lambda: codec.decode_mapping_many(rows, trust_source=True))
