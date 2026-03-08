from typing import Any

from pydantic import BaseModel

from forze.base.serialization.pydantic import (
    pydantic_dump,
    pydantic_field_names,
    pydantic_model_hash,
    pydantic_validate,
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


def test_pydantic_model_hash_is_stable_for_same_data() -> None:
    m1 = SampleModel(a=1, b=2)
    m2 = SampleModel(a=1, b=2)
    h1 = pydantic_model_hash(m1)
    h2 = pydantic_model_hash(m2)
    assert h1 == h2
