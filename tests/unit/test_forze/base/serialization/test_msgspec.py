from forze.base.exceptions import CoreException
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID


import msgspec
import pytest
from pydantic import BaseModel

from forze.base.serialization.msgspec import (
    _type_may_contain_struct,
    _validate_no_unknown_fields,
    msgspec_convert,
    msgspec_convert_many,
    msgspec_convert_many_batched,
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
        CoreException,
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
        CoreException,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        msgspec_transform(
            TargetStruct,
            SourceStruct(a=1),
            exclude={"unset": True},
        )

def test_msgspec_transform_rejects_unset_exclusion_for_pydantic_source() -> None:
    with pytest.raises(
        CoreException,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        msgspec_transform(
            TargetStruct,
            PydanticSourceModel(a=1),
            exclude={"unset": True},
        )


# ----------------------- #
# forbid_extra: union / collection / mapping field types
# ----------------------- #


class OptionalChildStruct(msgspec.Struct):
    child: ChildStruct | None = None


class StructOrScalarStruct(msgspec.Struct):
    # union with a single struct-ish member alongside a scalar.
    child: ChildStruct | int


class ListChildStruct(msgspec.Struct):
    children: list[ChildStruct] = msgspec.field(default_factory=list)


class TupleHomogeneousStruct(msgspec.Struct):
    children: tuple[ChildStruct, ...]


class TupleFixedStruct(msgspec.Struct):
    pair: tuple[ChildStruct, int]


class DictChildStruct(msgspec.Struct):
    children: dict[str, ChildStruct] = msgspec.field(default_factory=dict)


class AnnotatedChildStruct(msgspec.Struct):
    child: Annotated[ChildStruct, "meta"]


class SetIntStruct(msgspec.Struct):
    values: set[int] = msgspec.field(default_factory=set)


class OptionalSampleStruct(msgspec.Struct):
    inner: SampleStruct | None = None


def test_forbid_extra_optional_struct_field_accepts_valid() -> None:
    model = msgspec_validate(
        OptionalChildStruct,
        {"child": {"x": 1}},
        forbid_extra=True,
    )

    assert model == OptionalChildStruct(child=ChildStruct(x=1))


def test_forbid_extra_optional_struct_field_accepts_none() -> None:
    model = msgspec_validate(
        OptionalChildStruct,
        {"child": None},
        forbid_extra=True,
    )

    assert model == OptionalChildStruct(child=None)


def test_forbid_extra_optional_struct_field_rejects_unknown_nested() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra` - at `\$\.child`",
    ):
        msgspec_validate(
            OptionalChildStruct,
            {"child": {"x": 1, "extra": 2}},
            forbid_extra=True,
        )


def test_union_walker_multi_structish_returns_on_first_match() -> None:
    # msgspec.convert forbids multi-struct-like unions, so exercise the union
    # walker (_validate_no_unknown_fields) directly: first member validates and
    # the walker returns without raising.
    _validate_no_unknown_fields(
        {"x": 1},
        ChildStruct | list[ChildStruct],
    )


def test_union_walker_multi_structish_all_members_raise() -> None:
    # Both struct-like members reject the unknown field; with every union arg
    # being struct-ish the walker re-raises the first collected error.
    with pytest.raises(
        msgspec.ValidationError,
        match="Object contains unknown field `extra`",
    ):
        _validate_no_unknown_fields(
            {"x": 1, "extra": 2},
            ChildStruct | ParentStruct,
        )


def test_forbid_extra_single_structish_union_walks_struct_member() -> None:
    model = msgspec_validate(
        StructOrScalarStruct,
        {"child": {"x": 1}},
        forbid_extra=True,
    )

    assert model == StructOrScalarStruct(child=ChildStruct(x=1))


def test_forbid_extra_single_structish_union_accepts_scalar() -> None:
    model = msgspec_validate(
        StructOrScalarStruct,
        {"child": 5},
        forbid_extra=True,
    )

    assert model == StructOrScalarStruct(child=5)


def test_forbid_extra_annotated_struct_field_rejects_unknown() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra` - at `\$\.child`",
    ):
        msgspec_validate(
            AnnotatedChildStruct,
            {"child": {"x": 1, "extra": 2}},
            forbid_extra=True,
        )


def test_forbid_extra_list_of_structs_rejects_unknown_in_item() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra` - at `\$\.children\[1\]`",
    ):
        msgspec_validate(
            ListChildStruct,
            {"children": [{"x": 1}, {"x": 2, "extra": 3}]},
            forbid_extra=True,
        )


def test_forbid_extra_list_of_structs_accepts_valid_items() -> None:
    model = msgspec_validate(
        ListChildStruct,
        {"children": [{"x": 1}, {"x": 2}]},
        forbid_extra=True,
    )

    assert model == ListChildStruct(children=[ChildStruct(x=1), ChildStruct(x=2)])


def test_forbid_extra_set_of_scalars_is_noop() -> None:
    model = msgspec_validate(
        SetIntStruct,
        {"values": [1, 2, 3]},
        forbid_extra=True,
    )

    assert model == SetIntStruct(values={1, 2, 3})


def test_forbid_extra_homogeneous_tuple_rejects_unknown_in_item() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra` - at `\$\.children\[0\]`",
    ):
        msgspec_validate(
            TupleHomogeneousStruct,
            {"children": [{"x": 1, "extra": 2}]},
            forbid_extra=True,
        )


def test_forbid_extra_homogeneous_tuple_accepts_valid() -> None:
    model = msgspec_validate(
        TupleHomogeneousStruct,
        {"children": [{"x": 1}, {"x": 2}]},
        forbid_extra=True,
    )

    assert model == TupleHomogeneousStruct(
        children=(ChildStruct(x=1), ChildStruct(x=2)),
    )


def test_forbid_extra_fixed_tuple_rejects_unknown_in_struct_member() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra` - at `\$\.pair\[0\]`",
    ):
        msgspec_validate(
            TupleFixedStruct,
            {"pair": [{"x": 1, "extra": 2}, 5]},
            forbid_extra=True,
        )


def test_forbid_extra_fixed_tuple_accepts_valid() -> None:
    model = msgspec_validate(
        TupleFixedStruct,
        {"pair": [{"x": 1}, 5]},
        forbid_extra=True,
    )

    assert model == TupleFixedStruct(pair=(ChildStruct(x=1), 5))


def test_forbid_extra_dict_of_structs_rejects_unknown_in_value() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match=r"Object contains unknown field `extra`",
    ):
        msgspec_validate(
            DictChildStruct,
            {"children": {"a": {"x": 1, "extra": 2}}},
            forbid_extra=True,
        )


def test_forbid_extra_dict_of_structs_accepts_valid() -> None:
    model = msgspec_validate(
        DictChildStruct,
        {"children": {"a": {"x": 1}}},
        forbid_extra=True,
    )

    assert model == DictChildStruct(children={"a": ChildStruct(x=1)})


def test_forbid_extra_struct_field_given_non_mapping_is_noop() -> None:
    # child field is struct-typed but value is not a Mapping -> walker returns early,
    # leaving the actual type error to msgspec.convert.
    with pytest.raises(msgspec.ValidationError):
        msgspec_validate(
            ParentStruct,
            {"child": 5},
            forbid_extra=True,
        )


def test_forbid_extra_list_value_not_sequence_is_noop() -> None:
    # children typed as list[ChildStruct] but value is not a sequence -> early return,
    # then convert raises.
    with pytest.raises(msgspec.ValidationError):
        msgspec_validate(
            ListChildStruct,
            {"children": 5},
            forbid_extra=True,
        )


# ----------------------- #
# trusted convert helpers
# ----------------------- #


def test_msgspec_convert_returns_struct() -> None:
    assert msgspec_convert(SampleStruct, {"a": "2", "b": 3}) == SampleStruct(a=2, b=3)


def test_msgspec_convert_ignores_unknown_fields() -> None:
    assert msgspec_convert(SampleStruct, {"a": 1, "extra": 9}) == SampleStruct(a=1)


def test_msgspec_convert_many_returns_structs() -> None:
    assert msgspec_convert_many(SampleStruct, [{"a": 1}, {"a": "2", "b": 3}]) == [
        SampleStruct(a=1),
        SampleStruct(a=2, b=3),
    ]


def test_msgspec_convert_many_empty_returns_empty_list() -> None:
    assert msgspec_convert_many(SampleStruct, []) == []


def test_msgspec_convert_many_batched_chunks() -> None:
    rows = [{"a": i} for i in range(5)]
    chunks = list(msgspec_convert_many_batched(SampleStruct, rows, batch_size=2))

    assert _chunk_lengths(chunks) == [2, 2, 1]
    assert [model.a for chunk in chunks for model in chunk] == [0, 1, 2, 3, 4]


def test_msgspec_convert_many_batched_empty_yields_nothing() -> None:
    assert list(msgspec_convert_many_batched(SampleStruct, [])) == []


def test_msgspec_convert_many_batched_rejects_bad_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        list(msgspec_convert_many_batched(SampleStruct, [{"a": 1}], batch_size=0))


# ----------------------- #
# batched empty / validation early returns
# ----------------------- #


def test_msgspec_validate_many_batched_empty_yields_nothing() -> None:
    assert list(msgspec_validate_many_batched(SampleStruct, [])) == []


def test_msgspec_validate_many_batched_rejects_bad_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        list(msgspec_validate_many_batched(SampleStruct, [{"a": 1}], batch_size=0))


def test_msgspec_validate_many_batched_forbid_extra_rejects_unknown() -> None:
    with pytest.raises(msgspec.ValidationError):
        list(
            msgspec_validate_many_batched(
                SampleStruct,
                [{"a": 1, "extra": 2}],
                forbid_extra=True,
            )
        )


def test_msgspec_dump_many_empty_returns_empty_list() -> None:
    assert msgspec_dump_many([]) == []


def test_msgspec_dump_many_batched_empty_yields_nothing() -> None:
    assert list(msgspec_dump_many_batched([])) == []


def test_msgspec_dump_many_batched_rejects_bad_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        list(msgspec_dump_many_batched([SampleStruct(a=1)], batch_size=0))


# ----------------------- #
# dump value coverage (nested containers)
# ----------------------- #


class NestedContainerStruct(msgspec.Struct):
    mapping: dict[str, int]
    items: list[int]
    pair: tuple[int, int]
    bag: set[int]
    child: ChildStruct


def test_dump_value_handles_nested_containers_python_mode() -> None:
    model = NestedContainerStruct(
        mapping={"a": 1},
        items=[1, 2],
        pair=(1, 2),
        bag={1},
        child=ChildStruct(x=9),
    )

    dumped = msgspec_dump(model, mode="python")

    assert dumped["mapping"] == {"a": 1}
    assert dumped["items"] == [1, 2]
    assert dumped["pair"] == (1, 2)
    assert dumped["bag"] == [1]
    assert dumped["child"] == {"x": 9}


def test_dump_value_tuple_becomes_list_in_json_mode() -> None:
    model = NestedContainerStruct(
        mapping={"a": 1},
        items=[1, 2],
        pair=(1, 2),
        bag={1},
        child=ChildStruct(x=9),
    )

    dumped = msgspec_dump(model, mode="json")

    assert dumped["pair"] == [1, 2]


# ----------------------- #
# json bytes encode / decode
# ----------------------- #


def test_encode_json_bytes_default_round_trips() -> None:
    raw = msgspec_encode_json_bytes(SampleStruct(a=1, b=2))

    assert msgspec_decode_json_bytes(SampleStruct, raw) == SampleStruct(a=1, b=2)


def test_encode_json_bytes_with_exclude_none_uses_dump_path() -> None:
    raw = msgspec_encode_json_bytes(SampleStruct(a=1), exclude={"none": True})

    assert msgspec.json.decode(raw) == {"a": 1}


def test_encode_json_bytes_rejects_unset_exclusion() -> None:
    with pytest.raises(
        CoreException,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        msgspec_encode_json_bytes(SampleStruct(a=1), exclude={"unset": True})


def test_decode_json_bytes_accepts_str_input() -> None:
    assert msgspec_decode_json_bytes(SampleStruct, '{"a": 1}') == SampleStruct(a=1)


def test_decode_json_bytes_forbid_extra_rejects_unknown() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match="Object contains unknown field `extra`",
    ):
        msgspec_decode_json_bytes(
            SampleStruct,
            b'{"a": 1, "extra": 2}',
            forbid_extra=True,
        )


def test_decode_json_bytes_forbid_extra_accepts_valid() -> None:
    model = msgspec_decode_json_bytes(
        SampleStruct,
        b'{"a": 1, "b": 2}',
        forbid_extra=True,
    )

    assert model == SampleStruct(a=1, b=2)


def test_decode_json_bytes_forbid_extra_rejects_non_object_payload() -> None:
    with pytest.raises(
        msgspec.ValidationError,
        match="Expected object at .*, got list",
    ):
        msgspec_decode_json_bytes(
            SampleStruct,
            b"[1, 2, 3]",
            forbid_extra=True,
        )


# ----------------------- #
# transform_many
# ----------------------- #


def test_msgspec_transform_many_mixed_sources() -> None:
    transformed = msgspec_transform_many(
        TargetStruct,
        [SourceStruct(a=1, b=2), PydanticSourceModel(a=3)],
    )

    assert transformed == [TargetStruct(a=1, b=2), TargetStruct(a=3)]


def test_msgspec_transform_many_rejects_unset_exclusion() -> None:
    with pytest.raises(
        CoreException,
        match="msgspec codec does not support exclude=\\{'unset': True\\}",
    ):
        msgspec_transform_many(
            TargetStruct,
            [SourceStruct(a=1)],
            exclude={"unset": True},
        )


# ----------------------- #
# _type_may_contain_struct branch coverage
# ----------------------- #


@pytest.mark.parametrize(
    ("tp", "expected"),
    [
        (ChildStruct, True),
        (int, False),
        (ChildStruct | None, True),
        (int | str, False),
        (list[ChildStruct], True),
        (list[int], False),
        (tuple[ChildStruct, ...], True),
        (tuple[int, ...], False),
        (tuple[ChildStruct, int], True),
        (set[ChildStruct], True),
        (frozenset[ChildStruct], True),
        (dict[str, ChildStruct], True),
        (dict[str, int], False),
        (dict[str], False),
        (Annotated[ChildStruct, "meta"], True),
    ],
)
def test_type_may_contain_struct(tp: object, expected: bool) -> None:
    assert _type_may_contain_struct(tp) is expected


# ----------------------- #
# union walker: structish subset re-raise gating (line 163 false branch)
# ----------------------- #


def test_union_walker_structish_subset_does_not_reraise() -> None:
    # Union where only some members are struct-ish (struct + scalar). When the
    # struct member rejects but `len(structish) != len(args)`, the walker does
    # NOT re-raise (collected error is swallowed).
    _validate_no_unknown_fields(
        {"x": 1, "extra": 2},
        ChildStruct | int,
    )


def test_union_walker_no_structish_members_returns() -> None:
    # No member can contain a struct -> walker returns immediately.
    _validate_no_unknown_fields({"anything": 1}, int | str)


# ----------------------- #
# collection walker early-return branches
# ----------------------- #


def test_tuple_walker_non_sequence_value_returns() -> None:
    # tuple-typed expectation but a non-sequence value -> early return, no raise.
    _validate_no_unknown_fields(5, tuple[ChildStruct, int])


def test_homogeneous_tuple_walker_scalar_inner_returns() -> None:
    # homogeneous tuple of scalars -> inner not struct-ish -> early return.
    _validate_no_unknown_fields([1, 2, 3], tuple[int, ...])


def test_dict_walker_non_mapping_value_returns() -> None:
    # dict-typed expectation but value is not a Mapping -> early return.
    _validate_no_unknown_fields([1, 2], dict[str, ChildStruct])


def test_dict_walker_wrong_arity_returns() -> None:
    # dict type with non-2 args -> early return.
    _validate_no_unknown_fields({"a": {"x": 1}}, dict[str])


# ----------------------- #
# dump struct: required field is never compared against defaults
# ----------------------- #


def test_dump_excludes_defaults_keeps_required_field() -> None:
    # `a` is required so the defaults branch is skipped for it; `tags`/`b` carry
    # their defaults and are dropped.
    dumped = msgspec_dump(DefaultsStruct(a=7), exclude={"defaults": True})

    assert dumped == {"a": 7}


def test_dump_excludes_defaults_keeps_non_default_optional() -> None:
    dumped = msgspec_dump(
        DefaultsStruct(a=1, tags=[9], b=3),
        exclude={"defaults": True},
    )

    assert dumped == {"a": 1, "tags": [9], "b": 3}
