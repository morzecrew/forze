from forze.base.exceptions import CoreException, exc
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID


import msgspec
import pytest
from pydantic import BaseModel

from forze.base.serialization import MsgspecRecordMappingCodec
from forze.base.serialization.msgspec import (
    msgspec_decode_json_bytes,
    msgspec_dump,
    msgspec_dump_many,
    msgspec_dump_many_batched,
    msgspec_encode_json_bytes,
    msgspec_field_names,
    msgspec_transform,
    msgspec_transform_many,
    msgspec_validate,
    msgspec_validate_many,
    msgspec_validate_many_batched,
)

class SampleStruct(msgspec.Struct):
    a: int
    b: int | None = None

class RichStruct(msgspec.Struct):
    id: UUID
    created_at: datetime
    amount: Decimal

class RenamedStruct(msgspec.Struct):
    snake_name: int = msgspec.field(name="wire_name")

class SourceModel(BaseModel):
    a: int
    b: int | None = None

class TargetStruct(msgspec.Struct):
    a: int
    b: int | None = None

def _chunk_lengths[T](chunks: Sequence[Sequence[T]]) -> list[int]:
    return [len(chunk) for chunk in chunks]

def test_msgspec_record_codec_binds_model_type() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)

    assert isinstance(codec, MsgspecRecordMappingCodec)
    assert codec.model_type is SampleStruct

def test_decode_mapping_matches_msgspec_validate() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    data = {"a": "2", "b": 3}

    assert codec.decode_mapping(data) == msgspec_validate(SampleStruct, data)

def test_decode_mapping_forbid_extra_matches_msgspec_validate() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    data = {"a": 1, "extra": 2}

    with pytest.raises(msgspec.ValidationError) as expected:
        msgspec_validate(SampleStruct, data, forbid_extra=True)

    with pytest.raises(msgspec.ValidationError) as actual:
        codec.decode_mapping(data, forbid_extra=True)

    assert type(actual.value) is type(expected.value)
    assert str(actual.value) == str(expected.value)

def test_decode_mapping_many_matches_msgspec_validate_many() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    data = [{"a": 1}, {"a": "2", "b": 3}]

    assert codec.decode_mapping_many(data) == msgspec_validate_many(SampleStruct, data)

def test_decode_mapping_many_batched_matches_msgspec_validate_many_batched() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    data = [{"a": i} for i in range(5)]

    actual_chunks = list(codec.decode_mapping_many_batched(data, batch_size=2))
    expected_chunks = list(
        msgspec_validate_many_batched(SampleStruct, data, batch_size=2)
    )

    assert _chunk_lengths(actual_chunks) == [2, 2, 1]
    assert _chunk_lengths(actual_chunks) == _chunk_lengths(expected_chunks)
    assert actual_chunks == expected_chunks

def test_encode_mapping_matches_msgspec_dump() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    model = SampleStruct(a=1, b=2)

    assert codec.encode_mapping(model) == msgspec_dump(model)

def test_encode_mapping_forwards_json_mode() -> None:
    codec = MsgspecRecordMappingCodec(RichStruct)
    model = RichStruct(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        created_at=datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
        amount=Decimal("1.23"),
    )

    assert codec.encode_mapping(model, mode="json") == msgspec_dump(model, mode="json")

def test_encode_mapping_many_matches_msgspec_dump_many() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    models = [SampleStruct(a=1), SampleStruct(a=2, b=3)]

    assert codec.encode_mapping_many(models) == msgspec_dump_many(models)

def test_encode_mapping_many_batched_matches_msgspec_dump_many_batched() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    models = [SampleStruct(a=i) for i in range(5)]

    actual_chunks = list(codec.encode_mapping_many_batched(models, batch_size=2))
    expected_chunks = list(msgspec_dump_many_batched(models, batch_size=2))

    assert _chunk_lengths(actual_chunks) == [2, 2, 1]
    assert _chunk_lengths(actual_chunks) == _chunk_lengths(expected_chunks)
    assert actual_chunks == expected_chunks

def test_encode_mapping_rejects_unset_exclusion() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)

    with pytest.raises(
        CoreException,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        codec.encode_mapping(SampleStruct(a=1), exclude={"unset": True})

def test_transform_matches_msgspec_transform() -> None:
    codec = MsgspecRecordMappingCodec(TargetStruct)
    source = SourceModel(a=1, b=2)

    assert codec.transform(source) == msgspec_transform(TargetStruct, source)

def test_transform_many_matches_msgspec_transform_many() -> None:
    codec = MsgspecRecordMappingCodec(TargetStruct)
    sources = [SourceModel(a=1), SourceModel(a=2, b=3)]

    assert codec.transform_many(sources) == msgspec_transform_many(
        TargetStruct,
        sources,
    )

def test_stored_field_names_matches_msgspec_field_names() -> None:
    codec = MsgspecRecordMappingCodec(RenamedStruct)

    assert codec.stored_field_names() == msgspec_field_names(RenamedStruct)

def test_stored_field_names_ignores_include_computed_toggle() -> None:
    codec = MsgspecRecordMappingCodec(RenamedStruct)

    assert codec.stored_field_names(
        include_computed=False,
    ) == msgspec_field_names(RenamedStruct, include_computed=False)

def test_encode_json_bytes_matches_msgspec_encode_json_bytes() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    model = SampleStruct(a=1, b=2)

    assert codec.encode_json_bytes(model) == msgspec_encode_json_bytes(model)

def test_encode_json_bytes_rejects_unset_exclusion() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)

    with pytest.raises(
        CoreException,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        codec.encode_json_bytes(SampleStruct(a=1), exclude={"unset": True})

def test_encode_json_bytes_json_mode_types() -> None:
    codec = MsgspecRecordMappingCodec(RichStruct)
    model = RichStruct(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        created_at=datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
        amount=Decimal("1.23"),
    )

    raw = codec.encode_json_bytes(model).decode()

    assert "12345678-1234-5678-1234-567812345678" in raw
    assert "2025-01-02" in raw
    assert "1.23" in raw

def test_decode_json_bytes_matches_msgspec_decode_json_bytes() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    payload = b'{"a": 1, "b": 2}'

    assert codec.decode_json_bytes(payload) == msgspec_decode_json_bytes(
        SampleStruct,
        payload,
    )

def test_decode_json_bytes_accepts_str() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)

    assert codec.decode_json_bytes('{"a": 1}') == SampleStruct(a=1)

def test_decode_json_bytes_forbid_extra() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    payload = b'{"a": 1, "extra": 2}'

    with pytest.raises(msgspec.ValidationError):
        codec.decode_json_bytes(payload, forbid_extra=True)

def test_json_bytes_round_trip() -> None:
    codec = MsgspecRecordMappingCodec(SampleStruct)
    model = SampleStruct(a=1, b=2)

    assert codec.decode_json_bytes(codec.encode_json_bytes(model)) == model
