"""Nested JSON path resolution for Postgres filters and sorts (dot-separated keys)."""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from datetime import date, datetime
from enum import Enum
from typing import Any, Mapping, Union, get_args, get_origin
from uuid import UUID

from psycopg import sql
from pydantic import BaseModel

from forze.base.errors import CoreError

from ..introspect import PostgresColumnTypes, PostgresType

# ----------------------- #

try:
    from types import UnionType
except ImportError:  # pragma: no cover
    UnionType = type(Union[int, str])  # type: ignore[misc,assignment]


# ....................... #


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Union or origin is UnionType:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return annotation


def _is_basemodel_type(obj: Any) -> bool:
    return isinstance(obj, type) and issubclass(obj, BaseModel)


def _mapping_key_value_types(annotation: Any) -> tuple[Any, Any] | None:
    """If *annotation* is ``dict[K, V]`` or ``Mapping[K, V]``, return ``(K, V)``."""

    origin = get_origin(annotation)
    if origin is not dict and origin is not MappingABC:
        return None
    args = get_args(annotation)
    if len(args) < 2:
        return None
    return args[0], args[1]


def _is_str_like_mapping_key(key_ann: Any) -> bool:
    """Whether *key_ann* is suitable for JSON object keys in dot paths (string keys)."""

    if key_ann is Any:
        return True
    k = _unwrap_optional(key_ann)
    if k is str:
        return True
    return isinstance(k, type) and issubclass(k, str)


def _mapping_key_must_be_str_for_json_path(*, filter_path: str, key_ann: Any) -> None:
    if not _is_str_like_mapping_key(key_ann):
        raise CoreError(
            f"Nested filter path {filter_path!r}: mapping key type {key_ann!r} is not "
            "supported; dot-separated JSON paths assume string object keys.",
        )


def _walk_field_chain(
    model: type[BaseModel],
    segments: list[str],
    *,
    filter_path: str | None,
) -> Any | None:
    """Walk ``model.model_fields`` for the first segment, then dispatch into nested types."""

    if not segments:
        return None
    head, *tail = segments
    info = model.model_fields.get(head)
    if info is None:
        return None
    ann = _unwrap_optional(info.annotation)
    if not tail:
        return ann
    return _walk_through_ann(ann, tail, filter_path=filter_path)


def _walk_through_ann(ann: Any, segments: list[str], *, filter_path: str | None) -> Any | None:
    """Follow *segments* inside annotation *ann* (nested model, mapping hop, or leaf)."""

    if not segments:
        return ann
    if _is_basemodel_type(ann):
        return _walk_field_chain(ann, segments, filter_path=filter_path)

    kv = _mapping_key_value_types(ann)
    if kv is not None:
        key_t, val_t = kv
        if filter_path is not None:
            _mapping_key_must_be_str_for_json_path(filter_path=filter_path, key_ann=key_t)
        elif not _is_str_like_mapping_key(key_t):
            return None
        if not segments:
            return None
        _, *rest = segments
        val_ann = _unwrap_optional(val_t)
        if not rest:
            return val_ann
        return _walk_through_ann(val_ann, rest, filter_path=filter_path)

    origin = get_origin(ann)
    if origin is list:
        return None

    return None


def walk_pydantic_path(
    model: type[BaseModel],
    segments: list[str],
    *,
    filter_path: str | None = None,
) -> Any | None:
    """Return the leaf annotation for *segments* or ``None`` if the path is not walkable."""

    return _walk_field_chain(model, segments, filter_path=filter_path)


def _is_any_like(annotation: Any) -> bool:
    if annotation is Any:
        return True
    origin = get_origin(annotation)
    if origin is Union or origin is UnionType:
        return len(get_args(annotation)) > 2
    return False


def resolve_leaf_python_type(
    *,
    model_type: type[BaseModel],
    path: str,
    segments: list[str],
    nested_field_hints: Mapping[str, type[Any]] | None,
) -> Any:
    """Resolve the Python type used for value coercion (read model first, then hints)."""

    hints = nested_field_hints or {}
    root = segments[0]
    if root not in model_type.model_fields:
        raise CoreError(
            f"Filter path {path!r}: root field {root!r} is not defined on "
            f"{model_type.__name__}.",
        )

    hint = hints.get(path)
    if hint is not None:
        return hint

    walked = walk_pydantic_path(model_type, segments, filter_path=path)

    if walked is not None and not _is_any_like(walked):
        if _is_basemodel_type(walked):
            raise CoreError(
                f"Nested filter path {path!r}: leaf field is a nested Pydantic model; "
                "filter on a scalar leaf inside it.",
            )
        origin = get_origin(walked)
        if origin is list:
            raise CoreError(
                f"Nested filter path {path!r}: array-typed leaves in JSON columns are "
                "not supported yet; use a top-level Postgres array column.",
            )
        if origin is dict or origin is MappingABC:
            raise CoreError(
                f"Nested filter path {path!r}: cannot infer scalar type from mapping "
                f"annotation on {model_type.__name__}. Set nested_field_hints[{path!r}].",
            )
        return walked

    if walked is None:
        raise CoreError(
            f"Nested filter path {path!r}: not found under {model_type.__name__}. "
            f"Intermediate fields must be nested Pydantic models or parameterized "
            f"``dict[str, ...]`` / ``Mapping[str, ...]`` with a dynamic key segment. "
            f"Fix the path or set nested_field_hints[{path!r}].",
        )

    raise CoreError(
        f"Nested filter path {path!r}: ambiguous type on {model_type.__name__}. "
        f"Set nested_field_hints[{path!r}] to a concrete Python type.",
    )


def python_type_to_postgres_scalar(py_t: Any) -> PostgresType | None:
    """Map a Python type to a :class:`PostgresType` for JSON text extraction casts."""

    if py_t is Any:
        return None

    origin = get_origin(py_t)
    args = get_args(py_t)
    if origin is Union or origin is UnionType:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return python_type_to_postgres_scalar(non_none[0])
        return None

    if isinstance(py_t, type):
        if issubclass(py_t, UUID):
            return PostgresType(base="uuid", is_array=False, not_null=True)
        if issubclass(py_t, bool):
            return PostgresType(base="bool", is_array=False, not_null=True)
        if issubclass(py_t, int):
            return PostgresType(base="int8", is_array=False, not_null=True)
        if issubclass(py_t, float):
            return PostgresType(base="float8", is_array=False, not_null=True)
        if issubclass(py_t, datetime):
            return PostgresType(base="timestamptz", is_array=False, not_null=True)
        if issubclass(py_t, date):
            return PostgresType(base="date", is_array=False, not_null=True)
        if issubclass(py_t, str):
            return PostgresType(base="text", is_array=False, not_null=True)
        if issubclass(py_t, Enum):
            return PostgresType(base="text", is_array=False, not_null=True)
        if issubclass(py_t, bytes):
            return PostgresType(base="text", is_array=False, not_null=True)

    return None


def _json_path_expr(
    base_column: sql.Composable,
    json_segments: list[str],
) -> sql.Composable:
    """Build ``base -> 'a' ->> 'b'`` from JSON segments (last step uses ``->>``)."""

    expr: sql.Composable = base_column
    for key in json_segments[:-1]:
        expr = sql.SQL("{}->{}").format(expr, sql.Literal(key))
    expr = sql.SQL("{}->>{}").format(expr, sql.Literal(json_segments[-1]))
    return expr


def _cast_sql_for_json_text(pg: PostgresType) -> sql.Composable | None:
    if pg.is_array:
        return None
    match pg.base:
        case "uuid":
            return sql.SQL("uuid")
        case "int2" | "int4" | "int8":
            return sql.SQL(pg.base)
        case "float4" | "float8" | "numeric":
            return sql.SQL(pg.base)
        case "bool":
            return sql.SQL("boolean")
        case "date":
            return sql.SQL("date")
        case "timestamptz" | "timestamp":
            return sql.SQL(pg.base)
        case "text" | "varchar" | "char" | "citext":
            return None
        case _:
            return None


def build_nested_json_scalar_expr(
    *,
    path: str,
    segments: list[str],
    column_types: PostgresColumnTypes,
    model_type: type[BaseModel],
    nested_field_hints: Mapping[str, type[Any]] | None,
    table_alias: str | None,
) -> tuple[sql.Composable, PostgresType | None]:
    """SQL scalar expression for a multi-segment path into a ``json`` / ``jsonb`` column."""

    root = segments[0]
    col_t = column_types.get(root)
    if col_t is None:
        raise CoreError(
            f"Unknown column {root!r} for nested filter path {path!r}.",
        )
    if col_t.base not in {"json", "jsonb"}:
        raise CoreError(
            f"Nested filter path {path!r} requires column {root!r} to be json or jsonb; "
            f"got {col_t.base!r}.",
        )
    if col_t.is_array:
        raise CoreError(
            f"Nested filter path {path!r}: root column {root!r} must not be a Postgres array.",
        )

    inner = segments[1:]
    if not inner:
        raise CoreError(
            f"Nested filter path {path!r} must contain at least one segment after the column.",
        )

    leaf_py = resolve_leaf_python_type(
        model_type=model_type,
        path=path,
        segments=segments,
        nested_field_hints=nested_field_hints,
    )
    leaf_pg = python_type_to_postgres_scalar(leaf_py)

    base_ident = (
        sql.Identifier(table_alias, root)
        if table_alias is not None
        else sql.Identifier(root)
    )
    text_expr = _json_path_expr(base_ident, inner)
    cast_name = _cast_sql_for_json_text(leaf_pg) if leaf_pg is not None else None

    if cast_name is None:
        return text_expr, PostgresType(base="text", is_array=False, not_null=False)

    cast_expr = sql.SQL("CAST({} AS {})").format(text_expr, cast_name)
    return cast_expr, leaf_pg


def sort_key_expr(
    *,
    field: str,
    column_types: PostgresColumnTypes,
    model_type: type[BaseModel],
    nested_field_hints: Mapping[str, type[Any]] | None,
    table_alias: str | None,
) -> sql.Composable:
    """SQL expression for ORDER BY on a top-level or nested JSON field."""

    segments = field.split(".")
    if len(segments) > 1:
        expr, _ = build_nested_json_scalar_expr(
            path=field,
            segments=segments,
            column_types=column_types,
            model_type=model_type,
            nested_field_hints=nested_field_hints,
            table_alias=table_alias,
        )
        return expr
    if table_alias is not None:
        return sql.Identifier(table_alias, field)
    return sql.Identifier(field)
