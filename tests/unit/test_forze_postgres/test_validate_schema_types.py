"""Unit tests for Postgres schema type compatibility helpers."""

from __future__ import annotations

from datetime import datetime, time
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException
from forze_postgres.kernel.catalog.introspect.types import PostgresType
from forze_postgres.kernel.catalog.validation.validate_schema_types import (
    expected_pg_bases_for_annotation,
    expected_pg_bases_for_field,
    validate_field_nullability,
    validate_field_type_compatibility,
)

# ----------------------- #


class _Model(BaseModel):
    id: UUID
    name: str
    score: int | None = None
    payload: dict[str, Any] | None = None
    tags: list[str] | None = None


def _pg(base: str, *, is_array: bool = False, not_null: bool = True) -> PostgresType:
    return PostgresType(base=base, is_array=is_array, not_null=not_null)


class TestExpectedPgBases:
    def test_scalar_uuid(self) -> None:
        assert expected_pg_bases_for_annotation(UUID) == frozenset({"uuid"})

    def test_required_int_field(self) -> None:
        class Row(BaseModel):
            count: int

        field = Row.model_fields["count"]
        assert expected_pg_bases_for_field(field) == frozenset({"int2", "int4", "int8"})

    def test_json_for_dict_and_model_subclass(self) -> None:
        assert expected_pg_bases_for_annotation(dict[str, str]) == frozenset({"json", "jsonb"})
        assert expected_pg_bases_for_annotation(_Model) == frozenset({"json", "jsonb"})

    def test_list_element_bases(self) -> None:
        class Row(BaseModel):
            tags: list[str]

        field = Row.model_fields["tags"]
        assert expected_pg_bases_for_field(field) == frozenset({"text", "varchar", "char", "citext", "name"})

    def test_any_returns_none(self) -> None:
        assert expected_pg_bases_for_annotation(Any) is None


class TestValidateFieldTypeCompatibility:
    def test_compatible_scalar(self) -> None:
        validate_field_type_compatibility(
            model=_Model,
            column_types={"id": _pg("uuid"), "name": _pg("text")},
            omit_fields=frozenset(),
            label="read",
        )

    def test_incompatible_scalar_raises(self) -> None:
        with pytest.raises(CoreException, match="incompatible"):
            validate_field_type_compatibility(
                model=_Model,
                column_types={"name": _pg("int4")},
                omit_fields=frozenset(),
                label="read",
            )

    @pytest.mark.parametrize(
        ("annotation", "column_base"),
        [
            # Regression: parameterized type modifiers are compatible with the
            # same base type and must not be rejected by an exact-match check.
            (float, "numeric(10,2)"),
            (float, "numeric(10, 2)"),
            (datetime, "timestamp(3) with time zone"),
            (datetime, "timestamp(6) without time zone"),
            (time, "time(6) without time zone"),
            (time, "time(6) with time zone"),
            (str, "character varying(255)"),
        ],
    )
    def test_parameterized_type_modifier_is_compatible(
        self, annotation: type, column_base: str
    ) -> None:
        class Row(BaseModel):
            value: annotation  # type: ignore[valid-type]

        validate_field_type_compatibility(
            model=Row,
            column_types={"value": _pg(column_base)},
            omit_fields=frozenset(),
            label="read",
        )

    def test_parameterized_modifier_still_rejects_incompatible_base(self) -> None:
        # Stripping the modifier must not turn a genuinely wrong type compatible.
        class Row(BaseModel):
            value: int

        with pytest.raises(CoreException, match="incompatible"):
            validate_field_type_compatibility(
                model=Row,
                column_types={"value": _pg("numeric(10,2)")},
                omit_fields=frozenset(),
                label="read",
            )

    def test_incompatible_array_element_raises(self) -> None:
        class Tags(BaseModel):
            tags: list[str]

        with pytest.raises(CoreException, match="array element"):
            validate_field_type_compatibility(
                model=Tags,
                column_types={"tags": _pg("int4", is_array=True)},
                omit_fields=frozenset(),
                label="read",
            )

    def test_omits_fields(self) -> None:
        validate_field_type_compatibility(
            model=_Model,
            column_types={"name": _pg("int4")},
            omit_fields=frozenset({"name"}),
            label="read",
        )


class TestValidateFieldNullability:
    def test_required_on_nullable_column_raises(self) -> None:
        class Row(BaseModel):
            name: str

        with pytest.raises(CoreException, match="nullable column"):
            validate_field_nullability(
                model=Row,
                column_types={"name": _pg("text", not_null=False)},
                omit_fields=frozenset(),
                label="write",
            )

    def test_optional_field_allows_nullable_column(self) -> None:
        class Row(BaseModel):
            name: str | None = None

        validate_field_nullability(
            model=Row,
            column_types={"name": _pg("text", not_null=False)},
            omit_fields=frozenset(),
            label="write",
        )


class TestBoolAndOptionalRegression:
    """Two coupled bugs a downstream app hit at startup.

    ``issubclass(bool, int)`` is True, so with ``int`` probed first every bool field
    demanded an int2/int4/int8 column and a CORRECT boolean column failed startup
    validation. And ``T | None`` (PEP 604) unions have origin ``types.UnionType``,
    not ``typing.Union`` — the unwrap missed them, so every such field silently
    skipped type validation, which is also why the bool bug hid on optional fields.
    """

    def test_bool_field_accepts_a_boolean_column(self) -> None:
        class Row(BaseModel):
            is_active: bool

        validate_field_type_compatibility(
            model=Row,
            column_types={"is_active": _pg("bool")},
            omit_fields=frozenset(),
            label="t",
        )

    def test_bool_field_rejects_an_int_column(self) -> None:
        class Row(BaseModel):
            is_active: bool

        with pytest.raises(CoreException):
            validate_field_type_compatibility(
                model=Row,
                column_types={"is_active": _pg("int4")},
                omit_fields=frozenset(),
                label="t",
            )

    def test_int_field_still_accepts_int_columns(self) -> None:
        class Row(BaseModel):
            count: int

        validate_field_type_compatibility(
            model=Row,
            column_types={"count": _pg("int8")},
            omit_fields=frozenset(),
            label="t",
        )

    def test_pep604_optional_field_is_validated_not_skipped(self) -> None:
        class Row(BaseModel):
            is_active: bool | None = None
            name: str | None = None

        # the inner type is enforced...
        with pytest.raises(CoreException):
            validate_field_type_compatibility(
                model=Row,
                column_types={"is_active": _pg("int4", not_null=False)},
                omit_fields=frozenset(),
                label="t",
            )

        # ...and a correct column passes
        validate_field_type_compatibility(
            model=Row,
            column_types={
                "is_active": _pg("bool", not_null=False),
                "name": _pg("text", not_null=False),
            },
            omit_fields=frozenset(),
            label="t",
        )

    def test_typing_optional_spelling_matches_pep604(self) -> None:
        from typing import Optional

        class Row(BaseModel):
            flag: Optional[bool] = None  # noqa: UP045 — the OTHER union spelling is the point

        with pytest.raises(CoreException):
            validate_field_type_compatibility(
                model=Row,
                column_types={"flag": _pg("int4", not_null=False)},
                omit_fields=frozenset(),
                label="t",
            )

    def test_datetime_field_still_beats_its_date_superclass(self) -> None:
        # the same subclass overlap as bool < int: datetime < date
        class Row(BaseModel):
            at: datetime

        validate_field_type_compatibility(
            model=Row,
            column_types={"at": _pg("timestamptz")},
            omit_fields=frozenset(),
            label="t",
        )

        with pytest.raises(CoreException):
            validate_field_type_compatibility(
                model=Row,
                column_types={"at": _pg("date")},
                omit_fields=frozenset(),
                label="t",
            )


class TestAnnotationFallbacks:
    def test_multi_member_union_skips_typing_but_keeps_optionality(self) -> None:
        # ``str | int | None`` has no single scalar mapping — typing is skipped
        # (never guessed) — but the ``None`` member still counts as optional for
        # the nullability check.
        class Row(BaseModel):
            either: str | int | None = None

        assert expected_pg_bases_for_field(Row.model_fields["either"]) is None

        validate_field_nullability(
            model=Row,
            column_types={"either": _pg("text", not_null=False)},
            omit_fields=frozenset(),
            label="t",
        )

    def test_enum_subclasses_fall_back_to_their_scalar_base(self) -> None:
        from enum import IntEnum, StrEnum

        class Color(StrEnum):
            RED = "red"

        class Rank(IntEnum):
            ONE = 1

        class Row(BaseModel):
            color: Color
            rank: Rank

        color_bases = expected_pg_bases_for_field(Row.model_fields["color"])
        assert color_bases is not None and "text" in color_bases

        # IntEnum walks the fallback too — and must land on int, not bool
        rank_bases = expected_pg_bases_for_field(Row.model_fields["rank"])
        assert rank_bases is not None and "int4" in rank_bases and "bool" not in rank_bases

    def test_unmapped_type_is_skipped(self) -> None:
        class Opaque:
            pass

        assert expected_pg_bases_for_annotation(Opaque) is None

    def test_bare_list_and_unknown_element_are_skipped(self) -> None:
        class Opaque:
            pass

        class Row(BaseModel):
            model_config = {"arbitrary_types_allowed": True}

            bare: list  # type: ignore[type-arg]
            weird: list[Opaque]

        assert expected_pg_bases_for_field(Row.model_fields["bare"]) is None
        assert expected_pg_bases_for_field(Row.model_fields["weird"]) is None
