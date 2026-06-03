from typing import Any

import pytest
from pydantic import BaseModel

from forze.base.serialization.pydantic import (
    pydantic_dump,
    pydantic_dump_many_batched,
    pydantic_field_names,
    pydantic_model_hash,
    pydantic_transform_many,
    pydantic_validate,
    pydantic_validate_many,
    pydantic_validate_many_batched,
)


class SampleModel(BaseModel):
    a: int
    b: int | None = None


def test_pydantic_validate_forbids_extra_fields() -> None:
    data: dict[str, Any] = {"a": 1}
    m = pydantic_validate(SampleModel, data, forbid_extra=True)
    assert m.a == 1


def test_pydantic_dump_respects_exclude_options() -> None:
    m = SampleModel(a=1, b=None)
    dumped = pydantic_dump(m, exclude={"none": True})
    assert "b" not in dumped


def test_pydantic_field_names_includes_fields() -> None:
    fields = pydantic_field_names(SampleModel)
    assert "a" in fields
    assert "b" in fields


def test_pydantic_field_names_returns_frozenset() -> None:
    fields = pydantic_field_names(SampleModel)
    assert isinstance(fields, frozenset)


def test_pydantic_field_names_caching_returns_same_object() -> None:
    fields1 = pydantic_field_names(SampleModel)
    fields2 = pydantic_field_names(SampleModel)
    assert fields1 is fields2


def test_pydantic_validate_many_preserves_list_identity_when_input_is_list() -> None:
    rows: list[dict[str, Any]] = [{"a": 1}, {"a": 2}]
    row_id = id(rows)
    pydantic_validate_many(SampleModel, rows)
    assert id(rows) == row_id


def test_pydantic_validate_many_batched_roundtrip() -> None:
    rows = [{"a": i} for i in range(5)]
    chunks = list(
        pydantic_validate_many_batched(SampleModel, rows, batch_size=2),
    )
    assert len(chunks) == 3
    assert [m.a for part in chunks for m in part] == [0, 1, 2, 3, 4]


def test_pydantic_validate_many_batched_forbid_extra() -> None:
    rows = [{"a": 1}]
    (chunk,) = tuple(
        pydantic_validate_many_batched(SampleModel, rows, forbid_extra=True)
    )
    assert chunk[0].a == 1


def test_pydantic_dump_many_batched() -> None:
    models = [SampleModel(a=i) for i in range(5)]
    chunks = list(pydantic_dump_many_batched(models, batch_size=2))
    assert len(chunks) == 3
    assert [d["a"] for part in chunks for d in part] == [0, 1, 2, 3, 4]


def test_pydantic_transform_many_two_step() -> None:
    class Src(BaseModel):
        a: int

    class Dst(BaseModel):
        a: int

    out = pydantic_transform_many(Dst, [Src(a=1), Src(a=2)])
    assert len(out) == 2
    assert all(isinstance(x, Dst) for x in out)


def test_pydantic_validate_many_batched_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        next(iter(pydantic_validate_many_batched(SampleModel, [], batch_size=0)))


def test_pydantic_model_hash_is_stable_for_same_data() -> None:
    m1 = SampleModel(a=1, b=2)
    m2 = SampleModel(a=1, b=2)
    h1 = pydantic_model_hash(m1)
    h2 = pydantic_model_hash(m2)
    assert h1 == h2
