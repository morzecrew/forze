from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Optional, get_args

import attrs
from psycopg import sql

from forze.application.contracts.query import (
    EqOp,
    MembOp,
    Op,
    OrdOp,
    Scalar,
    SetRelOp,
    UnaryOp,
)
from forze.application.dsl.query import And, Expr, Field, Or, ValueCaster

from ..introspect import PostgresColumnTypes, PostgresType
from .utils import PsycopgPositionalBinds

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class PsycopgValueCoercer:
    """
    Central place for coercion rules used by query renderer.
    Keeps renderer code clean and consistent.
    """

    caster: ValueCaster = attrs.field(factory=ValueCaster, init=False)

    # ....................... #

    def bool_flag(self, v: Any) -> bool:
        return self.caster.as_bool(v)

    # ....................... #

    def scalar(self, v: Any, *, t: Optional[PostgresType]) -> Any:
        if v is None:
            return None

        if t is None:
            if isinstance(v, Scalar):
                return v

            # fallback: convert to string
            return str(v)

        if t.is_array:
            raise ValueError(f"Array type not supported: {t!r}")

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

    def array(self, v: Any, *, t: Optional[PostgresType]) -> list[Any]:
        if v is None:
            return []

        if isinstance(v, Scalar):
            raise ValueError(f"Scalar value not supported: {v!r}")

        if t is None:
            return [self.scalar(x, t=None) for x in v]

        if not t.is_array:
            raise ValueError("Expected array column, got scalar")

        elem_t = PostgresType(base=t.base, is_array=False, not_null=True)

        return [self.scalar(x, t=elem_t) for x in v]


# ....................... #


@attrs.define(slots=True)
class PsycopgQueryRenderer:
    types: Optional[PostgresColumnTypes] = None

    # Non initable fields
    binds: PsycopgPositionalBinds = attrs.field(
        factory=PsycopgPositionalBinds,
        init=False,
    )
    coercer: PsycopgValueCoercer = attrs.field(
        factory=PsycopgValueCoercer,
        init=False,
    )

    # ....................... #

    def render(self, expr: Expr) -> tuple[sql.Composable, list[Any]]:
        query = self._render_expr(expr)
        params = self.binds.values()

        return query, params

    # ....................... #

    def _render_expr(self, expr: Expr) -> sql.Composable:
        match expr:
            case Field(name, op, value):
                if self.types is not None:
                    t = self.types.get(name)

                    if t is None:
                        raise ValueError(f"Unknown column: {name!r}")

                else:
                    t = None

                col = sql.Identifier(name)
                return self._render_op(col, op, value, t=t)

            case And(items):
                if not items:
                    return sql.SQL("TRUE")

                ops = [self._render_expr(i) for i in items]

                if len(ops) == 1:
                    return ops[0]

                return sql.SQL("(") + sql.SQL(" AND ").join(ops) + sql.SQL(")")

            case Or(items):
                if not items:
                    return sql.SQL("FALSE")

                ops = [self._render_expr(i) for i in items]

                if len(ops) == 1:
                    return ops[0]

                return sql.SQL("(") + sql.SQL(" OR ").join(ops) + sql.SQL(")")

            case _:
                raise ValueError(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_op(
        self,
        col: sql.Composable,
        op: Op,
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> sql.Composable:
        op, value = self._normalize_op(op, value, t=t)

        if op in get_args(UnaryOp):
            return self._render_unary(
                col, op, value, t=t  # pyright: ignore[reportArgumentType]
            )

        elif op in get_args(OrdOp):
            return self._render_ord(
                col, op, value, t=t  # pyright: ignore[reportArgumentType]
            )

        elif op in get_args(EqOp):
            return self._render_eq(
                col, op, value, t=t  # pyright: ignore[reportArgumentType]
            )

        elif op in get_args(MembOp):
            return self._render_memb(
                col, op, value, t=t  # pyright: ignore[reportArgumentType]
            )

        elif op in get_args(SetRelOp):
            return self._render_set_rel(
                col, op, value, t=t  # pyright: ignore[reportArgumentType]
            )

        else:
            raise ValueError(f"Unknown operator: {op!r}")

    # ....................... #

    def _render_unary(
        self,
        col: sql.Composable,
        op: UnaryOp,
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> sql.Composable:
        value = self.coercer.bool_flag(value)

        if op == "$null":
            return self._render_null(col, value, t=t)

        else:
            return self._render_empty(col, value, t=t)

    # ....................... #

    def _render_null(
        self,
        col: sql.Composable,
        value: Any,
        *,
        t: Optional[PostgresType],
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
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> sql.Composable:
        return (
            sql.SQL("cardinality({}) = 0").format(col)
            if value is True
            else sql.SQL("cardinality({}) > 0").format(col)
        )

    # ....................... #

    def _render_ord(
        self,
        col: sql.Composable,
        op: OrdOp,
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> sql.Composable:
        op_map: dict[OrdOp, str] = {
            "$gt": ">",
            "$gte": ">=",
            "$lt": "<",
            "$lte": "<=",
        }
        op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]
        value = self.coercer.scalar(value, t=t)

        return sql.SQL("{} {} {}").format(col, op_sql, self.binds.add(value))

    # ....................... #

    def _render_eq(
        self,
        col: sql.Composable,
        op: EqOp,
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> sql.Composable:
        op_map: dict[EqOp, str] = {
            "$eq": "=",
            "$neq": "<>",
        }
        op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]
        value = self.coercer.scalar(value, t=t)

        return sql.SQL("{} {} {}").format(col, op_sql, self.binds.add(value))

    # ....................... #

    def _render_memb(
        self,
        col: sql.Composable,
        op: MembOp,
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> sql.Composable:
        value = self.coercer.array(value, t=t)
        expr = sql.SQL("{} = ANY({})").format(col, self.binds.add(value))

        return expr if op == "$in" else sql.SQL("NOT ({})").format(expr)

    # ....................... #

    def _render_set_rel(
        self,
        col: sql.Composable,
        op: SetRelOp,
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> sql.Composable:
        op_map: dict[SetRelOp, str] = {
            "$superset": "@>",
            "$subset": "<@",
            "$overlaps": "&&",
        }
        value = self.coercer.array(value, t=t)
        ph = self.binds.add(value)

        if op == "$disjoint":
            return sql.SQL("NOT ({} && {})").format(col, ph)

        else:
            op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]

            return sql.SQL("{} {} {}").format(col, op_sql, ph)

    # ....................... #

    @staticmethod
    def _normalize_op(
        op: Op,
        value: Any,
        *,
        t: Optional[PostgresType],
    ) -> tuple[Op, Any]:
        if t is not None and t.is_array:
            if op == "$eq":
                if isinstance(value, Scalar):
                    return "$superset", [value]

                return "$superset", value

            if op == "$in":
                return "$overlaps", value

            if op == "$nin":
                return "$disjoint", value

        return op, value


# ....................... #
