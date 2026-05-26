from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import msgspec
import pytest
from pydantic import BaseModel

from forze.base.serialization.msgspec import (
    msgspec_dump,
    msgspec_dump_many,
    msgspec_dump_many_batched,
    msgspec_field_names,
    msgspec_transform,
    msgspec_validate,
    msgspec_validate_many,
    msgspec_validate_many_batched,
)


class SampleStruct(msgspec.Struct):
    a: int
    b: int | None = None


class ChildStruct(msgspec.Struct):
    x: int


class ParentStruct(msgspec.Struct):
    child: ChildStruct


class RenamedStruct(msgspec.Struct):
    snake_name: int = msgspec.field(name="wire_name")


class DefaultsStruct(msgspec.Struct):
    a: int
    tags: list[int] = msgspec.field(default_factory=list)
    b: int | None = None


class RichStruct(msgspec.Struct):
    id: UUID
    created_at: datetime
    amount: Decimal


class PydanticSourceModel(BaseModel):
    a: int
    b: int | None = None


class SourceStruct(msgspec.Struct):
    a: int
    b: int | None = None


class TargetStruct(msgspec.Struct):
    a: int
    b: int | None = None


def _chunk_lengths[T](chunks: Sequence[Sequence[T]]) -> list[int]:
    return [len(chunk) for chunk in chunks]


def test_msgspec_validate_simple() -> None:
    model = msgspec_validate(SampleStruct, {"a": 1, "b": 2})

    assert model == SampleStruct(a=1, b=2)


def test_msgspec_validate_coerces_like_pydantic() -> None:
    model = msgspec_validate(SampleStruct, {"a": "2"})

    assert model == SampleStruct(a=2)


def test_msgspec_validate_many() -> None:
    models = msgspec_validate_many(SampleStruct, [{"a": 1}, {"a": "2", "b": 3}])

    assert models == [SampleStruct(a=1), SampleStruct(a=2, b=3)]


def test_msgspec_validate_many_batched() -> None:
    rows = [{"a": i} for i in range(5)]
    chunks = list(msgspec_validate_many_batched(SampleStruct, rows, batch_size=2))

    assert _chunk_lengths(chunks) == [2, 2, 1]
    assert [model.a for chunk in chunks for model in chunk] == [0, 1, 2, 3, 4]


def test_msgspec_validate_ignores_unknown_fields_by_default() -> None:
    model = msgspec_validate(SampleStruct, {"a": 1, "extra": 2})

    assert model == SampleStruct(a=1)


def test_msgspec_validate_forbid_extra_rejects_top_level_unknown_fields() -> None:
    with pytest.raises(
        msgspec.ValidationError, match="Object contains unknown field `extra`"
    ):
        msgspec_validate(SampleStruct, {"a": 1, "extra": 2}, forbid_extra=True)


def test_msgspec_validate_forbid_extra_rejects_nested_unknown_fields() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra` - at `\$\.child`",
    ):
        msgspec_validate(
            ParentStruct,
            {"child": {"x": 1, "extra": 2}},
            forbid_extra=True,
        )


def test_msgspec_dump_python_mode_preserves_rich_python_types() -> None:
    model = RichStruct(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        created_at=datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
        amount=Decimal("1.23"),
    )

    dumped = msgspec_dump(model, mode="python")

    assert dumped["id"] == model.id
    assert dumped["created_at"] == model.created_at
    assert dumped["amount"] == model.amount


def test_msgspec_dump_json_mode_returns_json_compatible_values() -> None:
    model = RichStruct(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        created_at=datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
        amount=Decimal("1.23"),
    )

    dumped = msgspec_dump(model, mode="json")

    assert dumped == {
        "id": "12345678-1234-5678-1234-567812345678",
        "created_at": "2025-01-02T03:04:05Z",
        "amount": "1.23",
    }


def test_msgspec_dump_excludes_none_values() -> None:
    dumped = msgspec_dump(SampleStruct(a=1), exclude={"none": True})

    assert dumped == {"a": 1}


def test_msgspec_dump_excludes_defaults_including_default_factory_values() -> None:
    dumped = msgspec_dump(DefaultsStruct(a=1), exclude={"defaults": True})

    assert dumped == {"a": 1}


def test_msgspec_dump_accepts_computed_fields_option_as_noop() -> None:
    model = SampleStruct(a=1)

    assert msgspec_dump(
        model,
        exclude={"computed_fields": True},
    ) == msgspec_dump(model)


def test_msgspec_dump_rejects_unset_exclusion() -> None:
    with pytest.raises(
        exc.internal,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        msgspec_dump(SampleStruct(a=1), exclude={"unset": True})


def test_msgspec_dump_uses_encoded_field_names() -> None:
    dumped = msgspec_dump(RenamedStruct(snake_name=1))

    assert dumped == {"wire_name": 1}


def test_msgspec_field_names_returns_encoded_names() -> None:
    fields = msgspec_field_names(RenamedStruct)

    assert fields == frozenset({"wire_name"})


def test_msgspec_field_names_ignores_include_computed_toggle() -> None:
    assert msgspec_field_names(RenamedStruct) == msgspec_field_names(
        RenamedStruct,
        include_computed=False,
    )


def test_msgspec_dump_many_matches_itemwise_dump() -> None:
    models = [SampleStruct(a=1), SampleStruct(a=2, b=3)]

    assert msgspec_dump_many(models) == [msgspec_dump(model) for model in models]


def test_msgspec_dump_many_batched() -> None:
    models = [SampleStruct(a=i) for i in range(5)]
    chunks = list(msgspec_dump_many_batched(models, batch_size=2))

    assert _chunk_lengths(chunks) == [2, 2, 1]
    assert [item["a"] for chunk in chunks for item in chunk] == [0, 1, 2, 3, 4]


def test_msgspec_transform_from_msgspec_source() -> None:
    transformed = msgspec_transform(TargetStruct, SourceStruct(a=1, b=2))

    assert transformed == TargetStruct(a=1, b=2)


def test_msgspec_transform_from_pydantic_source() -> None:
    transformed = msgspec_transform(TargetStruct, PydanticSourceModel(a=1, b=2))

    assert transformed == TargetStruct(a=1, b=2)


def test_msgspec_transform_rejects_unset_exclusion_for_msgspec_source() -> None:
    with pytest.raises(
        exc.internal,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        msgspec_transform(
            TargetStruct,
            SourceStruct(a=1),
            exclude={"unset": True},
        )


def test_msgspec_transform_rejects_unset_exclusion_for_pydantic_source() -> None:
    with pytest.raises(
        exc.internal,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        msgspec_transform(
            TargetStruct,
            PydanticSourceModel(a=1),
            exclude={"unset": True},
        )
