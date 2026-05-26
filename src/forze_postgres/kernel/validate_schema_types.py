"""Map Pydantic field types to expected Postgres column types for schema validation."""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from forze.base.errors import CoreError
from forze_postgres.kernel.introspect.types import PostgresType

# ----------------------- #

_SCALAR_PG_BASES: dict[type[Any], frozenset[str]] = {
    UUID: frozenset({"uuid"}),
    str: frozenset({"text", "varchar", "char", "citext", "name"}),
    int: frozenset({"int2", "int4", "int8"}),
    float: frozenset({"float4", "float8", "numeric"}),
    bool: frozenset({"bool"}),
    datetime: frozenset({"timestamp", "timestamptz"}),
    date: frozenset({"date"}),
    time: frozenset({"time", "timetz"}),
    bytes: frozenset({"bytea"}),
}

_JSON_PG_BASES = frozenset({"json", "jsonb"})


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Return (inner annotation, is_optional)."""

    origin = get_origin(annotation)

    if origin is Union:
        args = [a for a in get_args(annotation) if a is not type(None)]

        if len(args) == 1:
            return _unwrap_optional(args[0])

        return annotation, True

    return annotation, False


def expected_pg_bases_for_field(field: FieldInfo) -> frozenset[str] | None:
    """Return acceptable Postgres ``base`` names for a Pydantic field, or ``None`` to skip."""

    annotation, _ = _unwrap_optional(field.annotation)
    origin = get_origin(annotation)

    if origin is list:
        args = get_args(annotation)

        if not args:
            return None

        elem = args[0]
        elem_bases = expected_pg_bases_for_annotation(elem)

        if elem_bases is None:
            return None

        return elem_bases

    return expected_pg_bases_for_annotation(annotation)


def expected_pg_bases_for_annotation(annotation: Any) -> frozenset[str] | None:
    """Return acceptable Postgres bases for a Python type annotation."""

    if annotation is Any:
        return None

    origin = get_origin(annotation)

    if origin is dict or origin is list:
        return _JSON_PG_BASES

    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _JSON_PG_BASES

    if isinstance(annotation, type):
        for py_t, bases in _SCALAR_PG_BASES.items():
            if issubclass(annotation, py_t):
                return bases

    return None


def validate_field_type_compatibility(
    *,
    model: type[BaseModel],
    column_types: dict[str, PostgresType],
    omit_fields: frozenset[str],
    label: str,
) -> None:
    """Raise when a mapped field's Postgres type is incompatible with the Pydantic annotation."""

    for name, field in model.model_fields.items():
        if name in omit_fields:
            continue

        pg_t = column_types.get(name)

        if pg_t is None:
            continue

        expected = expected_pg_bases_for_field(field)

        if expected is None:
            continue

        actual = pg_t.base

        if pg_t.is_array:
            elem_ok = actual in expected
            if not elem_ok:
                raise CoreError(
                    f"Postgres schema validation failed for {label!r}: "
                    f"field {name!r} array element type {actual!r} "
                    f"is incompatible with annotation {field.annotation!r}.",
                    code="postgres_schema_validation_failed",
                    details={
                        "label": label,
                        "field": name,
                        "expected_bases": sorted(expected),
                        "actual_base": actual,
                        "is_array": True,
                    },
                )
            continue

        if actual not in expected:
            raise CoreError(
                f"Postgres schema validation failed for {label!r}: "
                f"field {name!r} column type {actual!r} "
                f"is incompatible with annotation {field.annotation!r}.",
                code="postgres_schema_validation_failed",
                details={
                    "label": label,
                    "field": name,
                    "expected_bases": sorted(expected),
                    "actual_base": actual,
                    "is_array": False,
                },
            )


def validate_field_nullability(
    *,
    model: type[BaseModel],
    column_types: dict[str, PostgresType],
    omit_fields: frozenset[str],
    label: str,
) -> None:
    """Raise when a required Pydantic field maps to a nullable Postgres column."""

    for name, field in model.model_fields.items():
        if name in omit_fields:
            continue

        pg_t = column_types.get(name)

        if pg_t is None:
            continue

        _, optional = _unwrap_optional(field.annotation)
        required = field.is_required() and not optional

        if required and not pg_t.not_null:
            raise CoreError(
                f"Postgres schema validation failed for {label!r}: "
                f"required field {name!r} maps to a nullable column.",
                code="postgres_schema_validation_failed",
                details={
                    "label": label,
                    "field": name,
                    "not_null": pg_t.not_null,
                },
            )
