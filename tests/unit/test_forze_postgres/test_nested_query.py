"""Unit tests for nested JSON path helpers (filters / ORDER BY)."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping, Union
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze_postgres.kernel.introspect import PostgresType
from forze_postgres.kernel.query.nested import (
    build_nested_json_scalar_expr,
    python_type_to_postgres_scalar,
    resolve_leaf_python_type,
    sort_key_expr,
    walk_pydantic_path,
)

# ----------------------- #


class _Inner(BaseModel):
    score: int


class _Outer(BaseModel):
    inner: _Inner


class _Row(BaseModel):
    meta: _Outer


class _TagsHolder(BaseModel):
    tags: list[str]


class _RowListLeaf(BaseModel):
    meta: _TagsHolder


class _RowDictLeaf(BaseModel):
    meta: dict[str, str]


class _TriUnion(BaseModel):
    meta: int | str | bytes


class _Color(str, Enum):
    red = "red"


class _RowDictStrInt(BaseModel):
    meta: dict[str, int]


class _RowDictStrInner(BaseModel):
    meta: dict[str, _Inner]


class _RowNestedDictStr(BaseModel):
    meta: dict[str, dict[str, str]]


class _RowMappingUuid(BaseModel):
    meta: Mapping[str, UUID]


class _RowIntKeyDict(BaseModel):
    data: dict[int, str]


def test_walk_pydantic_path_missing_returns_none() -> None:
    assert walk_pydantic_path(_Row, ["meta", "nope"]) is None


def test_resolve_leaf_bad_root() -> None:
    with pytest.raises(CoreError, match="root field"):
        resolve_leaf_python_type(
            model_type=_Row,
            path="unknown.x",
            segments=["unknown", "x"],
            nested_field_hints=None,
        )


def test_resolve_leaf_nested_model_not_scalar() -> None:
    with pytest.raises(CoreError, match="nested Pydantic model"):
        resolve_leaf_python_type(
            model_type=_Row,
            path="meta.inner",
            segments=["meta", "inner"],
            nested_field_hints=None,
        )


def test_resolve_leaf_list_not_supported() -> None:
    with pytest.raises(CoreError, match="array-typed"):
        resolve_leaf_python_type(
            model_type=_RowListLeaf,
            path="meta.tags",
            segments=["meta", "tags"],
            nested_field_hints=None,
        )


def test_resolve_leaf_dict_mapping_requires_hint() -> None:
    with pytest.raises(CoreError, match="cannot infer scalar type from mapping"):
        resolve_leaf_python_type(
            model_type=_RowDictLeaf,
            path="meta",
            segments=["meta"],
            nested_field_hints=None,
        )


def test_resolve_leaf_dict_mapping_with_hint() -> None:
    t = resolve_leaf_python_type(
        model_type=_RowDictLeaf,
        path="meta",
        segments=["meta"],
        nested_field_hints={"meta": int},
    )
    assert t is int


def test_resolve_path_not_walkable() -> None:
    with pytest.raises(CoreError, match="not found"):
        resolve_leaf_python_type(
            model_type=_Row,
            path="meta.inner.score.oops",
            segments=["meta", "inner", "score", "oops"],
            nested_field_hints=None,
        )


def test_resolve_ambiguous_union() -> None:
    with pytest.raises(CoreError, match="ambiguous type"):
        resolve_leaf_python_type(
            model_type=_TriUnion,
            path="meta",
            segments=["meta"],
            nested_field_hints=None,
        )


def test_resolve_any_like_uses_hint() -> None:
    class _AnyLeaf(BaseModel):
        meta: Any

    t = resolve_leaf_python_type(
        model_type=_AnyLeaf,
        path="meta.x",
        segments=["meta", "x"],
        nested_field_hints={"meta.x": float},
    )
    assert t is float


def test_walk_pydantic_path_dict_str_scalar() -> None:
    assert walk_pydantic_path(_RowDictStrInt, ["meta", "k"]) is int


def test_walk_pydantic_path_dict_str_basemodel_then_field() -> None:
    assert walk_pydantic_path(_RowDictStrInner, ["meta", "slot", "score"]) is int


def test_walk_pydantic_path_nested_dict() -> None:
    assert walk_pydantic_path(_RowNestedDictStr, ["meta", "a", "b"]) is str


def test_resolve_leaf_dict_str_int_without_hint() -> None:
    assert (
        resolve_leaf_python_type(
            model_type=_RowDictStrInt,
            path="meta.count",
            segments=["meta", "count"],
            nested_field_hints=None,
        )
        is int
    )


def test_resolve_leaf_dict_str_inner_nested_field() -> None:
    assert (
        resolve_leaf_python_type(
            model_type=_RowDictStrInner,
            path="meta.tenant_a.score",
            segments=["meta", "tenant_a", "score"],
            nested_field_hints=None,
        )
        is int
    )


def test_resolve_leaf_nested_dict_str_str() -> None:
    assert (
        resolve_leaf_python_type(
            model_type=_RowNestedDictStr,
            path="meta.outer.inner_key",
            segments=["meta", "outer", "inner_key"],
            nested_field_hints=None,
        )
        is str
    )


def test_resolve_leaf_mapping_str_uuid() -> None:
    t = resolve_leaf_python_type(
        model_type=_RowMappingUuid,
        path="meta.owner_id",
        segments=["meta", "owner_id"],
        nested_field_hints=None,
    )
    assert t is UUID


def test_build_nested_uuid_from_mapping_str_uuid() -> None:
    col_types = {"meta": PostgresType(base="jsonb", is_array=False, not_null=True)}
    _expr, pg = build_nested_json_scalar_expr(
        path="meta.owner_id",
        segments=["meta", "owner_id"],
        column_types=col_types,
        model_type=_RowMappingUuid,
        nested_field_hints=None,
        table_alias=None,
    )
    assert pg is not None
    assert pg.base == "uuid"


def test_resolve_non_string_mapping_key_raises() -> None:
    with pytest.raises(CoreError, match="string object keys"):
        resolve_leaf_python_type(
            model_type=_RowIntKeyDict,
            path="data.1",
            segments=["data", "1"],
            nested_field_hints=None,
        )


def test_nested_field_hint_overrides_inferred_dict_value_type() -> None:
    """Explicit ``nested_field_hints`` entry wins over the model annotation."""

    t = resolve_leaf_python_type(
        model_type=_RowDictStrInt,
        path="meta.n",
        segments=["meta", "n"],
        nested_field_hints={"meta.n": str},
    )
    assert t is str


def test_python_type_optional_int_unwraps() -> None:
    pg = python_type_to_postgres_scalar(int | None)
    assert pg is not None
    assert pg.base == "int8"


def test_python_type_union_multi_returns_none() -> None:
    assert python_type_to_postgres_scalar(Union[int, str]) is None


def test_python_type_scalars() -> None:
    assert python_type_to_postgres_scalar(UUID).base == "uuid"
    assert python_type_to_postgres_scalar(bool).base == "bool"
    assert python_type_to_postgres_scalar(int).base == "int8"
    assert python_type_to_postgres_scalar(float).base == "float8"
    assert python_type_to_postgres_scalar(datetime).base == "timestamptz"
    assert python_type_to_postgres_scalar(date).base == "date"
    assert python_type_to_postgres_scalar(str).base == "text"
    assert python_type_to_postgres_scalar(_Color).base == "text"
    assert python_type_to_postgres_scalar(bytes).base == "text"
    assert python_type_to_postgres_scalar(Any) is None


def test_build_nested_unknown_column() -> None:
    col_types = {"meta": PostgresType(base="jsonb", is_array=False, not_null=True)}
    with pytest.raises(CoreError, match="Unknown column"):
        build_nested_json_scalar_expr(
            path="other.x",
            segments=["other", "x"],
            column_types=col_types,
            model_type=_Row,
            nested_field_hints=None,
            table_alias=None,
        )


def test_build_nested_non_json_column() -> None:
    col_types = {"meta": PostgresType(base="text", is_array=False, not_null=True)}
    with pytest.raises(CoreError, match="json or jsonb"):
        build_nested_json_scalar_expr(
            path="meta.inner.score",
            segments=["meta", "inner", "score"],
            column_types=col_types,
            model_type=_Row,
            nested_field_hints=None,
            table_alias=None,
        )


def test_build_nested_json_array_root() -> None:
    col_types = {"meta": PostgresType(base="jsonb", is_array=True, not_null=True)}
    with pytest.raises(CoreError, match="must not be a Postgres array"):
        build_nested_json_scalar_expr(
            path="meta.inner.score",
            segments=["meta", "inner", "score"],
            column_types=col_types,
            model_type=_Row,
            nested_field_hints=None,
            table_alias=None,
        )


def test_build_nested_requires_inner_path() -> None:
    col_types = {"meta": PostgresType(base="jsonb", is_array=False, not_null=True)}
    with pytest.raises(CoreError, match="at least one segment"):
        build_nested_json_scalar_expr(
            path="meta",
            segments=["meta"],
            column_types=col_types,
            model_type=_Row,
            nested_field_hints=None,
            table_alias=None,
        )


def test_build_nested_int_leaf_returns_cast_and_int8() -> None:
    col_types = {"meta": PostgresType(base="jsonb", is_array=False, not_null=True)}
    _expr, pg = build_nested_json_scalar_expr(
        path="meta.inner.score",
        segments=["meta", "inner", "score"],
        column_types=col_types,
        model_type=_Row,
        nested_field_hints=None,
        table_alias="t",
    )
    assert pg is not None
    assert pg.base == "int8"


def test_build_nested_text_leaf_no_cast_returns_text_type() -> None:
    class _StrRow(BaseModel):
        meta: dict[str, str]

    col_types = {"meta": PostgresType(base="jsonb", is_array=False, not_null=True)}
    _expr, pg = build_nested_json_scalar_expr(
        path="meta.title",
        segments=["meta", "title"],
        column_types=col_types,
        model_type=_StrRow,
        nested_field_hints=None,
        table_alias=None,
    )
    assert pg is not None
    assert pg.base == "text"


def test_sort_key_expr_top_level_with_alias() -> None:
    col_types: dict[str, PostgresType] = {}
    expr = sort_key_expr(
        field="id",
        column_types=col_types,
        model_type=_Row,
        nested_field_hints=None,
        table_alias="d",
    )
    assert expr is not None


def test_sort_key_expr_nested_delegates() -> None:
    col_types = {"meta": PostgresType(base="jsonb", is_array=False, not_null=True)}
    expr = sort_key_expr(
        field="meta.inner.score",
        column_types=col_types,
        model_type=_Row,
        nested_field_hints=None,
        table_alias="d",
    )
    assert expr is not None
