"""Render parsed query expressions into psycopg SQL composables."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Mapping

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.query import (
    QueryAnd,
    QueryExpr,
    QueryField,
    QueryOp,
    QueryOr,
    QueryValue,
    QueryValueCaster,
)
from forze.base.errors import CoreError

from ..introspect import PostgresColumnTypes, PostgresType
from .nested import build_nested_json_scalar_expr
from .utils import PsycopgPositionalBinder

# ----------------------- #

_NESTED_JSON_UNSUPPORTED: frozenset[str] = frozenset(
    ("$empty", "$superset", "$subset", "$disjoint", "$overlaps"),
)


@attrs.define(slots=True, frozen=True)
class PsycopgValueCoercer:
    """
    Central place for coercion rules used by query renderer.
    Keeps renderer code clean and consistent.
    """

    caster: QueryValueCaster = attrs.field(factory=QueryValueCaster, init=False)

    # ....................... #

    def bool_flag(self, v: Any) -> bool:
        return self.caster.as_bool(v)

    # ....................... #

    def scalar(self, v: Any, *, t: PostgresType | None) -> Any:
        if v is None:
            return None

        if t is None:
            return self.caster.pass_through(v)

        if t.is_array:
            raise CoreError(f"Array type not supported: {t!r}")

        match t.base:
            case "uuid":
                return self.caster.as_uuid(v)

            case "text" | "varchar" | "char" | "citext":
                return str(v)

            case "bool":
                return self.caster.as_bool(v)

            case "int2" | "int4" | "int8":
                return self.caster.as_int(v)

            case "float4" | "float8" | "numeric":
                return self.caster.as_float(v)

            case "date":
                return self.caster.as_date(v)

            case "timestamptz" | "timestamp":
                return self.caster.as_datetime(v, force_tz=t.base == "timestamptz")

            case _:
                return v

    # ....................... #

    def array(
        self,
        v: Any,
        *,
        t: PostgresType | None,
        raise_on_scalar_t: bool = False,
    ) -> list[Any]:
        if v is None:
            return []

        if isinstance(v, QueryValue.Scalar):
            raise CoreError(f"Scalar value not supported: {v!r}")

        if t is None:
            return [self.scalar(x, t=None) for x in v]

        if not t.is_array and raise_on_scalar_t:
            raise CoreError("Expected array column, got scalar")

        elem_t = PostgresType(base=t.base, is_array=False, not_null=True)

        return [self.scalar(x, t=elem_t) for x in v]


# ....................... #


@attrs.define(slots=True)
class PsycopgQueryRenderer:
    """Render :class:`QueryExpr` trees into psycopg :class:`~psycopg.sql.Composable` SQL with positional parameters.

    When *types* is provided, values are coerced to match the Postgres column
    type and array operators are normalized automatically.
    """

    types: PostgresColumnTypes | None = attrs.field(default=None)

    model_type: type[BaseModel] | None = attrs.field(default=None)
    """Read model used to validate dot-separated paths and infer JSON leaf types."""

    nested_field_hints: Mapping[str, type[Any]] | None = attrs.field(default=None)
    """Per-path Python type hints when the model annotation is ambiguous."""

    table_alias: str | None = attrs.field(default=None)
    """Qualify top-level column names (e.g. projection alias in search CTEs)."""

    # Non initable fields
    binder: PsycopgPositionalBinder = attrs.field(
        factory=PsycopgPositionalBinder,
        init=False,
    )
    coercer: PsycopgValueCoercer = attrs.field(
        factory=PsycopgValueCoercer,
        init=False,
    )

    # ....................... #

    def render(self, expr: QueryExpr) -> tuple[sql.Composable, list[Any]]:
        query = self._render_expr(expr)
        params = self.binder.values()

        return query, params

    # ....................... #

    def _render_expr(self, expr: QueryExpr) -> sql.Composable:
        match expr:
            case QueryField(name, op, value):
                segments = name.split(".")
                if len(segments) > 1:
                    if self.types is None:
                        raise CoreError(
                            f"Nested filter path {name!r} requires column type metadata "
                            "(introspected types).",
                        )
                    if self.model_type is None:
                        raise CoreError(
                            f"Nested filter path {name!r} requires gateway model_type "
                            "for read-model validation.",
                        )
                    if op in _NESTED_JSON_UNSUPPORTED:
                        raise CoreError(
                            f"Operator {op!r} is not supported for nested JSON path {name!r}.",
                        )
                    col_expr, t = build_nested_json_scalar_expr(
                        path=name,
                        segments=segments,
                        column_types=self.types,
                        model_type=self.model_type,
                        nested_field_hints=self.nested_field_hints,
                        table_alias=self.table_alias,
                    )
                    return self._render_field(col_expr, op, value, t=t)

                if self.types is not None:
                    t = self.types.get(name)

                    if t is None:
                        raise CoreError(f"Unknown column: {name!r}")

                else:
                    t = None

                col = (
                    sql.Identifier(self.table_alias, name)
                    if self.table_alias is not None
                    else sql.Identifier(name)
                )
                return self._render_field(col, op, value, t=t)

            case QueryAnd(items):
                if not items:
                    return sql.SQL("TRUE")

                and_parts = [self._render_expr(i) for i in items]

                if len(and_parts) == 1:
                    return and_parts[0]

                return sql.SQL("(") + sql.SQL(" AND ").join(and_parts) + sql.SQL(")")

            case QueryOr(items):
                if not items:
                    return sql.SQL("FALSE")

                or_parts = [self._render_expr(i) for i in items]

                if len(or_parts) == 1:
                    return or_parts[0]

                return sql.SQL("(") + sql.SQL(" OR ").join(or_parts) + sql.SQL(")")

            case _:
                raise CoreError(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_field(
        self,
        col: sql.Composable,
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        op, value = self._normalize_op(op, value, t=t)

        match op:
            case "$null" | "$empty":
                return self._render_unary(col, op, value, t=t)

            case "$gt" | "$gte" | "$lt" | "$lte":
                return self._render_ord(col, op, value, t=t)

            case "$eq" | "$neq":
                return self._render_eq(col, op, value, t=t)

            case "$in" | "$nin":
                return self._render_memb(col, op, value, t=t)

            case "$superset" | "$subset" | "$disjoint" | "$overlaps":
                return self._render_set_rel(col, op, value, t=t)

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise CoreError(f"Unknown operator: {op!r}")

    # ....................... #

    def _render_unary(
        self,
        col: sql.Composable,
        op: QueryOp.Unary,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        v = self.coercer.bool_flag(value)

        match op:
            case "$null":
                return self._render_null(col, v, t=t)

            case "$empty":
                return self._render_empty(col, v, t=t)

    # ....................... #

    def _render_null(
        self,
        col: sql.Composable,
        value: bool,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        return (
            sql.SQL("{} IS NULL").format(col)
            if value is True
            else sql.SQL("{} IS NOT NULL").format(col)
        )

    # ....................... #

    def _render_empty(
        self,
        col: sql.Composable,
        value: bool,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        # JSON/JSONB columns store JSON arrays; cardinality() is for native PG arrays only.
        if t is not None and not t.is_array and t.base in ("jsonb", "json"):
            j = sql.SQL("({})::jsonb").format(col)
            if value is True:
                return sql.SQL(
                    "(jsonb_typeof({j}) = 'array' AND jsonb_array_length({j}) = 0)"
                ).format(j=j)

            return sql.SQL(
                "(jsonb_typeof({j}) = 'array' AND jsonb_array_length({j}) > 0)"
            ).format(j=j)

        return (
            sql.SQL("cardinality({}) = 0").format(col)
            if value is True
            else sql.SQL("cardinality({}) > 0").format(col)
        )

    # ....................... #

    def _render_ord(
        self,
        col: sql.Composable,
        op: QueryOp.Ord,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        op_map: dict[QueryOp.Ord, str] = {  # type: ignore[valid-type]
            "$gt": ">",
            "$gte": ">=",
            "$lt": "<",
            "$lte": "<=",
        }
        op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]
        value = self.coercer.scalar(value, t=t)

        return sql.SQL("{} {} {}").format(col, op_sql, self.binder.add(value))

    # ....................... #

    def _render_eq(
        self,
        col: sql.Composable,
        op: QueryOp.Eq,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        op_map: dict[QueryOp.Eq, str] = {  # type: ignore[valid-type]
            "$eq": "=",
            "$neq": "<>",
        }
        op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]
        value = self.coercer.scalar(value, t=t)

        return sql.SQL("{} {} {}").format(col, op_sql, self.binder.add(value))

    # ....................... #

    def _render_memb(
        self,
        col: sql.Composable,
        op: QueryOp.Memb,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        value = self.coercer.array(value, t=t)
        expr = sql.SQL("{} = ANY({})").format(col, self.binder.add(value))

        return expr if op == "$in" else sql.SQL("NOT ({})").format(expr)

    # ....................... #

    def _render_set_rel(
        self,
        col: sql.Composable,
        op: QueryOp.SetRel,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        op_map: dict[QueryOp.SetRel, str] = {  # type: ignore[valid-type]
            "$superset": "@>",
            "$subset": "<@",
            "$overlaps": "&&",
        }
        value = self.coercer.array(value, t=t, raise_on_scalar_t=True)
        ph = self.binder.add(value)

        if op == "$disjoint":
            return sql.SQL("NOT ({} && {})").format(col, ph)

        else:
            op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]

            return sql.SQL("{} {} {}").format(col, op_sql, ph)

    # ....................... #

    @staticmethod
    def _normalize_op(
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> tuple[QueryOp.All, Any]:  # type: ignore[valid-type]
        if t is not None and t.is_array:
            if op == "$eq":
                if isinstance(value, QueryValue.Scalar):
                    return "$superset", [value]

                return "$superset", value

            if op == "$in":
                return "$overlaps", value

            if op == "$nin":
                return "$disjoint", value

        return op, value
