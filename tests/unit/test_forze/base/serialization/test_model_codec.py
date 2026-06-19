from forze.base.exceptions import CoreException
from collections.abc import Sequence
from datetime import UTC, datetime
from uuid import UUID, uuid4


import pytest
from pydantic import BaseModel, ValidationError, computed_field

from forze.base.serialization import PydanticModelCodec
from forze.base.serialization.pydantic import (
    pydantic_decode_json_bytes,
    pydantic_dump,
    pydantic_dump_many,
    pydantic_dump_many_batched,
    pydantic_encode_json_bytes,
    pydantic_field_names,
    pydantic_transform,
    pydantic_transform_many,
    pydantic_validate,
    pydantic_validate_many,
    pydantic_validate_many_batched,
)

class SampleModel(BaseModel):
    a: int
    b: int | None = None

class JsonModeModel(BaseModel):
    id: UUID
    created_at: datetime

class FieldsModel(BaseModel):
    a: int

    @computed_field
    @property
    def doubled(self) -> int:
        return self.a * 2

class SourceModel(BaseModel):
    a: int
    b: int | None = None

class TargetModel(BaseModel):
    a: int
    b: int | None = None

def _chunk_lengths[T](chunks: Sequence[Sequence[T]]) -> list[int]:
    return [len(chunk) for chunk in chunks]

def test_pydantic_record_codec_factory_binds_model_type() -> None:
    codec = PydanticModelCodec(SampleModel)

    assert isinstance(codec, PydanticModelCodec)
    assert codec.model_type is SampleModel

def test_decode_mapping_matches_pydantic_validate() -> None:
    codec = PydanticModelCodec(SampleModel)
    data = {"a": 1, "b": 2}

    assert codec.decode_mapping(data) == pydantic_validate(SampleModel, data)

def test_decode_mapping_forbid_extra_matches_pydantic_validate() -> None:
    codec = PydanticModelCodec(SampleModel)
    data = {"a": 1, "extra": 2}

    with pytest.raises(ValidationError) as expected:
        pydantic_validate(SampleModel, data, forbid_extra=True)

    with pytest.raises(ValidationError) as actual:
        codec.decode_mapping(data, forbid_extra=True)

    assert type(actual.value) is type(expected.value)
    assert actual.value.errors() == expected.value.errors()

def test_decode_mapping_many_matches_pydantic_validate_many() -> None:
    codec = PydanticModelCodec(SampleModel)
    data = [{"a": 1}, {"a": 2, "b": 3}]

    assert codec.decode_mapping_many(data) == pydantic_validate_many(SampleModel, data)

def test_decode_mapping_many_batched_matches_pydantic_validate_many_batched() -> None:
    codec = PydanticModelCodec(SampleModel)
    data = [{"a": i} for i in range(5)]

    actual_chunks = list(codec.decode_mapping_many_batched(data, batch_size=2))
    expected_chunks = list(
        pydantic_validate_many_batched(SampleModel, data, batch_size=2)
    )

    assert _chunk_lengths(actual_chunks) == [2, 2, 1]
    assert _chunk_lengths(actual_chunks) == _chunk_lengths(expected_chunks)
    assert actual_chunks == expected_chunks

def test_encode_mapping_matches_pydantic_dump() -> None:
    codec = PydanticModelCodec(SampleModel)
    model = SampleModel(a=1, b=2)

    assert codec.encode_mapping(model) == pydantic_dump(model)

def test_encode_mapping_forwards_json_mode() -> None:
    codec = PydanticModelCodec(JsonModeModel)
    model = JsonModeModel(
        id=uuid4(),
        created_at=datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
    )

    actual = codec.encode_mapping(model, mode="json")
    expected = pydantic_dump(model, mode="json")

    assert actual == expected
    assert actual["id"] == str(model.id)
    assert actual["created_at"] == "2025-01-02T03:04:05Z"

def test_encode_mapping_exclude_unset_matches_pydantic_dump() -> None:
    codec = PydanticModelCodec(SampleModel)
    model = SampleModel(a=1)

    assert codec.encode_mapping(
        model,
        exclude={"unset": True},
    ) == pydantic_dump(model, exclude={"unset": True})

def test_encode_mapping_many_matches_pydantic_dump_many() -> None:
    codec = PydanticModelCodec(SampleModel)
    models = [SampleModel(a=1), SampleModel(a=2, b=3)]

    assert codec.encode_mapping_many(models) == pydantic_dump_many(models)

def test_encode_mapping_many_batched_matches_pydantic_dump_many_batched() -> None:
    codec = PydanticModelCodec(SampleModel)
    models = [SampleModel(a=i) for i in range(5)]

    actual_chunks = list(codec.encode_mapping_many_batched(models, batch_size=2))
    expected_chunks = list(pydantic_dump_many_batched(models, batch_size=2))

    assert _chunk_lengths(actual_chunks) == [2, 2, 1]
    assert _chunk_lengths(actual_chunks) == _chunk_lengths(expected_chunks)
    assert actual_chunks == expected_chunks

def test_transform_matches_pydantic_transform() -> None:
    codec = PydanticModelCodec(TargetModel)
    source = SourceModel(a=1)

    assert codec.transform(source) == pydantic_transform(TargetModel, source)

def test_transform_many_matches_pydantic_transform_many() -> None:
    codec = PydanticModelCodec(TargetModel)
    sources = [SourceModel(a=1), SourceModel(a=2, b=3)]

    assert codec.transform_many(sources) == pydantic_transform_many(
        TargetModel,
        sources,
    )

def test_stored_field_names_matches_pydantic_field_names() -> None:
    codec = PydanticModelCodec(FieldsModel)

    assert codec.stored_field_names() == pydantic_field_names(FieldsModel)

def test_stored_field_names_without_computed_matches_pydantic_field_names() -> None:
    codec = PydanticModelCodec(FieldsModel)

    assert codec.stored_field_names(
        include_computed=False,
    ) == pydantic_field_names(FieldsModel, include_computed=False)

def test_encode_json_bytes_matches_pydantic_encode_json_bytes() -> None:
    codec = PydanticModelCodec(SampleModel)
    model = SampleModel(a=1, b=2)

    assert codec.encode_json_bytes(model) == pydantic_encode_json_bytes(model)

def test_encode_json_bytes_exclude_unset_matches_pydantic_encode_json_bytes() -> None:
    codec = PydanticModelCodec(SampleModel)
    model = SampleModel(a=1)

    assert codec.encode_json_bytes(
        model,
        exclude={"unset": True},
    ) == pydantic_encode_json_bytes(model, exclude={"unset": True})

def test_encode_json_bytes_json_mode_types() -> None:
    codec = PydanticModelCodec(JsonModeModel)
    model = JsonModeModel(
        id=uuid4(),
        created_at=datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
    )

    raw = codec.encode_json_bytes(model).decode()

    assert '"id"' in raw
    assert "2025-01-02" in raw

def test_decode_json_bytes_matches_pydantic_decode_json_bytes() -> None:
    codec = PydanticModelCodec(SampleModel)
    payload = b'{"a": 1, "b": 2}'

    assert codec.decode_json_bytes(payload) == pydantic_decode_json_bytes(
        SampleModel,
        payload,
    )

def test_decode_json_bytes_accepts_str() -> None:
    codec = PydanticModelCodec(SampleModel)

    assert codec.decode_json_bytes('{"a": 1}') == SampleModel(a=1)

def test_decode_json_bytes_forbid_extra_matches_pydantic_decode_json_bytes() -> None:
    codec = PydanticModelCodec(SampleModel)
    payload = b'{"a": 1, "extra": 2}'

    with pytest.raises(ValidationError) as expected:
        pydantic_decode_json_bytes(SampleModel, payload, forbid_extra=True)

    with pytest.raises(ValidationError) as actual:
        codec.decode_json_bytes(payload, forbid_extra=True)

    assert type(actual.value) is type(expected.value)
    assert actual.value.errors() == expected.value.errors()

def test_json_bytes_round_trip() -> None:
    codec = PydanticModelCodec(SampleModel)
    model = SampleModel(a=1, b=2)

    assert codec.decode_json_bytes(codec.encode_json_bytes(model)) == model


# ----------------------- #
# Materialized computed fields (persisted + queryable)


def test_persisted_field_names_excludes_computed_by_default() -> None:
    codec = PydanticModelCodec(FieldsModel)

    assert codec.materialized == frozenset()
    assert codec.persisted_field_names() == frozenset({"a"})


def test_encode_persistence_mapping_excludes_computed_by_default() -> None:
    codec = PydanticModelCodec(FieldsModel)

    assert codec.encode_persistence_mapping(FieldsModel(a=3)) == {"a": 3}


def test_materialized_field_is_persisted_and_queryable() -> None:
    codec = PydanticModelCodec(FieldsModel, materialized=frozenset({"doubled"}))

    assert codec.persisted_field_names() == frozenset({"a", "doubled"})
    assert codec.encode_persistence_mapping(FieldsModel(a=3)) == {"a": 3, "doubled": 6}


def test_materialized_field_is_persisted_for_many() -> None:
    codec = PydanticModelCodec(FieldsModel, materialized=frozenset({"doubled"}))

    assert codec.encode_persistence_mapping_many(
        [FieldsModel(a=1), FieldsModel(a=4)],
    ) == [{"a": 1, "doubled": 2}, {"a": 4, "doubled": 8}]


def test_materialized_honours_caller_override_keeping_all_computed() -> None:
    # Caller opting back into all computed fields is unaffected by materialized.
    codec = PydanticModelCodec(FieldsModel, materialized=frozenset({"doubled"}))

    assert codec.encode_persistence_mapping(
        FieldsModel(a=3),
        exclude={"computed_fields": False},
    ) == {"a": 3, "doubled": 6}


def test_materialized_unknown_field_rejected_at_construction() -> None:
    with pytest.raises(CoreException, match="not computed fields"):
        PydanticModelCodec(FieldsModel, materialized=frozenset({"a"}))

    with pytest.raises(CoreException, match="not computed fields"):
        PydanticModelCodec(FieldsModel, materialized=frozenset({"ghost"}))
