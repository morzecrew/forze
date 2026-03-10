"""Tests for forze.domain.validation.updates."""

import warnings

import pytest
from pydantic import BaseModel

from forze.base.primitives import JsonDict
from forze.domain.validation import collect_update_validators, update_validator
from forze.domain.validation.updates import (
    UPDATE_VALIDATOR_METADATA_FIELD,
    UpdateValidatorMetadata,
)


# ----------------------- #


class Model(BaseModel):
    value: int


class TestUpdateValidatorDecorator:
    def test_normalizes_one_param(self) -> None:
        calls: list[int] = []

        @update_validator
        def v(before: Model) -> None:
            calls.append(before.value)

        v(Model(value=1), Model(value=2), {"value": 2})
        assert calls == [1]

    def test_normalizes_two_params(self) -> None:
        calls: list[tuple[int, int]] = []

        @update_validator
        def v(before: Model, after: Model) -> None:
            calls.append((before.value, after.value))

        v(Model(value=1), Model(value=2), {"value": 2})
        assert calls == [(1, 2)]

    def test_normalizes_three_params(self) -> None:
        calls: list[tuple[int, int, JsonDict]] = []

        @update_validator
        def v(before: Model, after: Model, diff: JsonDict) -> None:
            calls.append((before.value, after.value, diff))

        diff: JsonDict = {"value": 2}
        v(Model(value=1), Model(value=2), diff)
        assert calls == [(1, 2, diff)]

    def test_zero_params_raises(self) -> None:
        with pytest.raises(TypeError, match="at least one parameter"):

            @update_validator
            def v() -> None:  # type: ignore[arg-type]
                pass

    def test_four_params_raises(self) -> None:
        with pytest.raises(TypeError, match="at most three parameters"):

            @update_validator
            def v(a: Model, b: Model, c: JsonDict, d: int) -> None:  # type: ignore[arg-type]
                pass

    def test_with_fields(self) -> None:
        @update_validator(fields=["name", "value"])
        def v(before: Model) -> None:
            pass

        meta = getattr(v, UPDATE_VALIDATOR_METADATA_FIELD)
        assert isinstance(meta, UpdateValidatorMetadata)
        assert meta.fields == frozenset({"name", "value"})

    def test_without_fields(self) -> None:
        @update_validator
        def v(before: Model) -> None:
            pass

        meta = getattr(v, UPDATE_VALIDATOR_METADATA_FIELD)
        assert meta.fields is None

    def test_preserves_function_name(self) -> None:
        @update_validator
        def my_validator(before: Model) -> None:
            pass

        assert my_validator.__name__ == "my_validator"


class TestCollectUpdateValidators:
    def test_collects_from_single_class(self) -> None:
        class M(BaseModel):
            @update_validator
            def v1(self) -> None:
                pass

        validators = collect_update_validators(M)
        assert len(validators) == 1
        assert validators[0][0] == "v1"

    def test_respects_inheritance_order(self) -> None:
        class Base(BaseModel):
            @update_validator
            def base_v(self) -> None:
                pass

        class Child(Base):
            @update_validator
            def child_v(self) -> None:
                pass

        validators = collect_update_validators(Child)
        names = [n for n, _ in validators]
        assert names == ["base_v", "child_v"]

    def test_override_conflict_warn(self) -> None:
        class Base(BaseModel):
            @update_validator
            def shared(self) -> None:
                pass

        class Child(Base):
            @update_validator
            def shared(self) -> None:
                pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            validators = collect_update_validators(Child, on_conflict="warn")
            assert len(w) == 1
            assert "overrides" in str(w[0].message)
        assert len(validators) == 1
        assert validators[0][0] == "shared"

    def test_override_conflict_error(self) -> None:
        class Base(BaseModel):
            @update_validator
            def shared(self) -> None:
                pass

        class Child(Base):
            @update_validator
            def shared(self) -> None:
                pass

        with pytest.raises(TypeError, match="overrides"):
            collect_update_validators(Child, on_conflict="error")

    def test_override_conflict_overwrite_silently(self) -> None:
        class Base(BaseModel):
            @update_validator
            def shared(self) -> None:
                pass

        class Child(Base):
            @update_validator
            def shared(self) -> None:
                pass

        validators = collect_update_validators(Child, on_conflict="overwrite")
        assert len(validators) == 1

    def test_no_validators(self) -> None:
        class Plain(BaseModel):
            x: int

        assert collect_update_validators(Plain) == []

    def test_non_validator_attrs_ignored(self) -> None:
        class M(BaseModel):
            @update_validator
            def real(self) -> None:
                pass

            def not_validator(self) -> None:
                pass

        validators = collect_update_validators(M)
        names = [n for n, _ in validators]
        assert "not_validator" not in names
        assert "real" in names

    def test_field_metadata_preserved(self) -> None:
        class M(BaseModel):
            @update_validator(fields=["name"])
            def check_name(self) -> None:
                pass

        validators = collect_update_validators(M)
        assert len(validators) == 1
        _, meta = validators[0]
        assert meta.fields == frozenset({"name"})
