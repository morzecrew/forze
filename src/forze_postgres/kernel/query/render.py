"""Render parsed query expressions into psycopg SQL composables."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from datetime import timedelta
from typing import Any, Mapping

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    AggregateComputedField,
    AggregatesExpression,
    AggregatesExpressionParser,
    ELEM_SCALAR_FIELD,
    GroupKey,
    GroupRef,
    GroupTrunc,
    ParsedAggregates,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryNot,
    QueryOp,
    QueryOr,
    QueryValue,
    QueryValueCaster,
)
from forze.base.errors import CoreError

from ..introspect import PostgresColumnTypes, PostgresType
from ..type_cast import cast_sql_for_column_type
from .nested import (
    build_nested_json_scalar_expr,
    resolve_leaf_python_type,
    sort_key_expr,
    walk_pydantic_path,
)
from .utils import PsycopgPositionalBinder

# ----------------------- #

_NESTED_JSON_UNSUPPORTED: frozenset[str] = frozenset(
    ("$empty", "$superset", "$subset", "$disjoint", "$overlaps"),
)

_COMPARE_EQ_SQL: dict[str, str] = {"$eq": "=", "$neq": "<>"}
_COMPARE_ORD_SQL: dict[str, str] = {
    "$gt": ">",
    "$gte": ">=",
    "$lt": "<",
    "$lte": "<=",
}

_COMPARE_COMPAT_BASE_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"int2", "int4", "int8"}),
    frozenset({"float4", "float8", "numeric"}),
    frozenset({"timestamp", "timestamptz"}),
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
    type. Native array columns use the same filter-operator semantics as other
    backends (exact equality for ``$eq`` / ``$neq``, element-wise ``$in`` /
    ``$nin``); use ``$superset``, ``$subset``, ``$overlaps``, or ``$disjoint``
    for set-relation predicates.
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

    def render_aggregates(
        self,
        aggregates: AggregatesExpression,
    ) -> tuple[ParsedAggregates, sql.Composable, sql.Composable | None, list[Any]]:
        """Render aggregate SELECT and GROUP BY clauses."""

        parsed = AggregatesExpressionParser.parse(aggregates)
        select_parts: list[sql.Composable] = []
        group_parts: list[sql.Composable] = []

        for group in parsed.groups:
            group_expr, ident = self._render_group_expr(group)
            select_parts.append(sql.SQL("{} AS {}").format(group_expr, ident))
            group_parts.append(group_expr)

        for computed in parsed.computed_fields:
            expr = self._render_aggregate_function(computed)
            select_parts.append(
                sql.SQL("{} AS {}").format(expr, sql.Identifier(computed.alias)),
            )

        group_clause = sql.SQL(", ").join(group_parts) if group_parts else None
        return (
            parsed,
            sql.SQL(", ").join(select_parts),
            group_clause,
            self.binder.values(),
        )

    # ....................... #

    def _render_group_expr(
        self,
        group: GroupKey,
    ) -> tuple[sql.Composable, sql.Composable]:
        ident = sql.Identifier(group.alias)
        if isinstance(group.expr, GroupRef):
            expr = self._render_source_expr(group.expr.field)
        else:
            expr = self._render_trunc_expr(group.expr)

        return expr, ident

    def _render_trunc_expr(self, trunc: GroupTrunc) -> sql.Composable:
        col = self._render_source_expr(trunc.field)
        unit = trunc.unit
        tz = trunc.timezone

        if tz.mode == "iana":
            return sql.SQL("date_trunc({}, {} AT TIME ZONE {})").format(
                sql.Literal(unit),
                col,
                sql.Literal(tz.iana),
            )

        offset = tz.offset if tz.offset is not None else timedelta(0)
        return sql.SQL("date_trunc({}, {} AT TIME ZONE 'UTC' + {})").format(
            sql.Literal(unit),
            col,
            sql.Literal(offset),
        )

    # ....................... #

    @staticmethod
    def render_aggregate_order_by(
        parsed: ParsedAggregates,
        sorts: Mapping[str, str] | None,
    ) -> sql.Composable | None:
        """Render ORDER BY for aggregate result aliases."""

        if not sorts:
            return None

        aliases = parsed.aliases
        bad = [field for field in sorts if field not in aliases]

        if bad:
            raise CoreError(f"Invalid aggregate sort fields: {bad}")

        parts: list[sql.Composable] = []
        for field, order in sorts.items():
            direction = sql.SQL("ASC") if order == "asc" else sql.SQL("DESC")
            parts.append(sql.SQL("{} {}").format(sql.Identifier(field), direction))

        return sql.SQL(", ").join(parts)

    # ....................... #

    def _render_aggregate_function(
        self,
        computed: AggregateComputedField,
    ) -> sql.Composable:
        function = computed.function
        field = computed.field

        if function == "$count":
            count_expr = sql.SQL("COUNT(*)")
            return self._render_aggregate_filter(count_expr, computed)

        if field is None:
            raise CoreError("Computed field has no field path")

        field_expr = self._render_source_expr(field)
        agg_expr: sql.Composable

        match function:
            case "$sum":
                agg_expr = sql.SQL("SUM({})").format(field_expr)

            case "$avg":
                agg_expr = sql.SQL("AVG({})").format(field_expr)

            case "$min":
                agg_expr = sql.SQL("MIN({})").format(field_expr)

            case "$max":
                agg_expr = sql.SQL("MAX({})").format(field_expr)

            case "$median":
                agg_expr = sql.SQL(
                    "percentile_cont(0.5) WITHIN GROUP (ORDER BY {})",
                ).format(field_expr)

        return self._render_aggregate_filter(agg_expr, computed)

    # ....................... #

    def _render_aggregate_filter(
        self,
        expr: sql.Composable,
        computed: AggregateComputedField,
    ) -> sql.Composable:
        if computed.filter is None:
            return expr

        filter_expr = computed.parsed_filter
        if filter_expr is None:
            filter_expr = QueryFilterExpressionParser.parse(computed.filter)

        filter_sql = self._render_expr(filter_expr)

        return sql.SQL("{} FILTER (WHERE {})").format(expr, filter_sql)

    # ....................... #

    def _render_source_expr(self, field: str) -> sql.Composable:
        if self.types is None:
            raise CoreError("Aggregate rendering requires column type metadata")

        if self.model_type is None:
            raise CoreError("Aggregate rendering requires gateway model_type")

        return sort_key_expr(
            field=field,
            column_types=self.types,
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
            table_alias=self.table_alias,
        )

    # ....................... #

    def _resolve_column_expr(
        self, field: str
    ) -> tuple[sql.Composable, PostgresType | None]:
        """Resolve a field path to a SQL column/expression and optional Postgres type."""

        segments = field.split(".")
        if len(segments) > 1:
            if self.types is None:
                raise CoreError(
                    f"Nested compare path {field!r} requires column type metadata "
                    "(introspected types).",
                )
            if self.model_type is None:
                raise CoreError(
                    f"Nested compare path {field!r} requires gateway model_type "
                    "for read-model validation.",
                )
            return build_nested_json_scalar_expr(
                path=field,
                segments=segments,
                column_types=self.types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=self.table_alias,
            )

        if self.types is not None:
            t = self.types.get(segments[0])
            if t is None:
                raise CoreError(f"Unknown column: {segments[0]!r}")
        else:
            t = None

        col = (
            sql.Identifier(self.table_alias, segments[0])
            if self.table_alias is not None
            else sql.Identifier(segments[0])
        )
        return col, t

    # ....................... #

    @staticmethod
    def _assert_compare_types_compatible(
        left: str,
        right: str,
        left_t: PostgresType | None,
        right_t: PostgresType | None,
    ) -> None:
        if left_t is None or right_t is None:
            return

        if left_t.is_array or right_t.is_array:
            raise CoreError(
                f"Field compare between {left!r} and {right!r} does not support array columns.",
            )

        if left_t.base == right_t.base:
            return

        for group in _COMPARE_COMPAT_BASE_GROUPS:
            if left_t.base in group and right_t.base in group:
                return

        raise CoreError(
            f"Incompatible types for field compare {left!r} ({left_t.base!r}) "
            f"and {right!r} ({right_t.base!r}).",
        )

    # ....................... #

    def _render_compare(self, left: str, op: QueryOp.Compare, right: str) -> sql.Composable:  # type: ignore[valid-type]
        left_expr, left_t = self._resolve_column_expr(left)
        right_expr, right_t = self._resolve_column_expr(right)
        self._assert_compare_types_compatible(left, right, left_t, right_t)

        if op in _COMPARE_EQ_SQL:
            op_sql = sql.SQL(_COMPARE_EQ_SQL[op])  # pyright: ignore[reportArgumentType]

        elif op in _COMPARE_ORD_SQL:
            op_sql = sql.SQL(
                _COMPARE_ORD_SQL[op]  # pyright: ignore[reportArgumentType]
            )

        else:
            raise CoreError(f"Unknown compare operator: {op!r}")

        return sql.SQL("{} {} {}").format(left_expr, op_sql, right_expr)

    # ....................... #

    def _render_expr(self, expr: QueryExpr) -> sql.Composable:
        match expr:
            case QueryCompare(left, op, right):
                return self._render_compare(left, op, right)

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

            case QueryNot(item):
                inner = self._render_expr(item)
                return sql.SQL("NOT ({})").format(inner)

            case QueryElem(path, quantifier, inner):
                return self._render_elem(path, quantifier, inner)

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

        if t is not None and t.is_array:
            if not isinstance(value, (list, tuple)) or isinstance(
                value,
                (str, bytes, bytearray),
            ):
                raise CoreError(
                    f"Array column filter {op!r} requires a list/tuple value; "
                    "use $null for null checks, $superset / $overlaps / $in for "
                    "containment-style matches.",
                )
            bound = self.binder.add(self.coercer.array(value, t=t))
            return sql.SQL("{} {} {}").format(col, op_sql, bound)

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
        coerced = self.coercer.array(value, t=t)

        if t is not None and t.is_array:
            inner = sql.SQL(
                "EXISTS (SELECT 1 FROM unnest({}) AS _fz_u WHERE _fz_u = ANY({}))",
            ).format(col, self.binder.add(coerced))

            return inner if op == "$in" else sql.SQL("NOT ({})").format(inner)

        expr = sql.SQL("{} = ANY({})").format(col, self.binder.add(coerced))

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

    def _elem_vacuous_sql(self, quantifier: str) -> sql.Composable:
        if quantifier in ("$all", "$none"):
            return sql.SQL("TRUE")
        return sql.SQL("FALSE")

    def _elem_has_array_sql(
        self,
        col: sql.Composable,
        t: PostgresType | None,
    ) -> sql.Composable:
        if t is not None and t.is_array:
            return sql.SQL("{} IS NOT NULL").format(col)

        j = sql.SQL("({})::jsonb").format(col)
        return sql.SQL(
            "({col} IS NOT NULL AND jsonb_typeof({j}) = 'array' AND jsonb_array_length({j}) > 0)",
        ).format(col=col, j=j)

    def _render_elem(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> sql.Composable:
        col, t = self._resolve_column_and_type(path)
        vacuous = self._elem_vacuous_sql(quantifier)

        if t is not None and not t.is_array and t.base not in ("jsonb", "json"):
            raise CoreError(
                f"Element quantifier on {path!r} requires an array or jsonb array column",
            )

        elem_pred = self._render_elem_inner(inner, col, t, path)
        has_array = self._elem_has_array_sql(col, t)

        if t is not None and t.is_array:
            exists = sql.SQL(
                "EXISTS (SELECT 1 FROM unnest({col}) AS _fz_elem WHERE {pred})",
            ).format(col=col, pred=elem_pred)
            forall = sql.SQL(
                "NOT EXISTS (SELECT 1 FROM unnest({col}) AS _fz_elem WHERE NOT ({pred}))",
            ).format(col=col, pred=elem_pred)
        else:
            arr = sql.SQL(
                "CASE WHEN jsonb_typeof({col}::jsonb) = 'array' THEN {col}::jsonb ELSE '[]'::jsonb END",
            ).format(col=col)
            exists = sql.SQL(
                "EXISTS (SELECT 1 FROM jsonb_array_elements({arr}) AS _fz_elem WHERE {pred})",
            ).format(arr=arr, pred=elem_pred)
            forall = sql.SQL(
                "NOT EXISTS (SELECT 1 FROM jsonb_array_elements({arr}) AS _fz_elem WHERE NOT ({pred}))",
            ).format(arr=arr, pred=elem_pred)

        if quantifier == "$any":
            match = exists
        elif quantifier == "$all":
            match = forall
        else:
            match = sql.SQL("NOT ({})").format(exists)

        return sql.SQL(
            "(({not_has} AND {vac}) OR ({has} AND {match}))",
        ).format(
            not_has=sql.SQL("NOT ({})").format(has_array),
            vac=vacuous,
            has=has_array,
            match=match,
        )

    def _resolve_column_and_type(
        self,
        path: str,
    ) -> tuple[sql.Composable, PostgresType | None]:
        segments = path.split(".")
        if len(segments) > 1:
            if self.types is None or self.model_type is None:
                raise CoreError(
                    f"Nested element path {path!r} requires column types and model_type",
                )
            col_expr, t = build_nested_json_scalar_expr(
                path=path,
                segments=segments,
                column_types=self.types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=self.table_alias,
            )
            return col_expr, t

        if self.types is not None:
            t = self.types.get(path)
            if t is None:
                raise CoreError(f"Unknown column: {path!r}")
        else:
            t = None

        col = (
            sql.Identifier(self.table_alias, path)
            if self.table_alias is not None
            else sql.Identifier(path)
        )
        return col, t

    def _render_elem_inner(
        self,
        inner: QueryExpr,
        col: sql.Composable,
        t: PostgresType | None,
        path: str,
    ) -> sql.Composable:
        if self._elem_inner_is_scalar(inner):
            return self._render_elem_scalar_inner(inner, t)

        return self._render_elem_object_inner(inner, col, t, path)

    @staticmethod
    def _elem_inner_is_scalar(inner: QueryExpr) -> bool:
        match inner:
            case QueryField(name, _, _):
                return name == ELEM_SCALAR_FIELD
            case QueryAnd(items):
                return all(
                    isinstance(i, QueryField) and i.name == ELEM_SCALAR_FIELD
                    for i in items
                )
            case _:
                return False

    def _render_elem_scalar_inner(
        self,
        inner: QueryExpr,
        t: PostgresType | None,
    ) -> sql.Composable:
        match inner:
            case QueryField(name, _, _) if name == ELEM_SCALAR_FIELD:
                fields = [inner]
            case QueryAnd(items):
                fields = [i for i in items if isinstance(i, QueryField)]
            case _:
                raise CoreError(f"Invalid scalar element inner: {inner!r}")

        elem_t = (
            PostgresType(base=t.base, is_array=False, not_null=True) if t and t.is_array else t
        )
        elem = sql.Identifier("_fz_elem")
        parts = [
            self._render_field(elem, f.op, f.value, t=elem_t)  # type: ignore[arg-type]
            for f in fields
        ]
        if len(parts) == 1:
            return parts[0]
        return sql.SQL("(") + sql.SQL(" AND ").join(parts) + sql.SQL(")")

    def _render_elem_object_inner(
        self,
        inner: QueryExpr,
        col: sql.Composable,
        t: PostgresType | None,
        path: str,
    ) -> sql.Composable:
        match inner:
            case QueryAnd(items):
                fields = [i for i in items if isinstance(i, QueryField)]
            case QueryField() as f:
                fields = [f]
            case _:
                raise CoreError(f"Invalid object element inner: {inner!r}")

        parts: list[sql.Composable] = []
        for f in fields:
            segments = f.name.split(".")
            leaf_t = None
            if self.model_type is not None:
                ann = walk_pydantic_path(self.model_type, [path, *segments])
                if ann is not None:
                    try:
                        leaf_t = resolve_leaf_python_type(
                            model_type=self.model_type,
                            path=f"{path}.{f.name}",
                            segments=[path, *segments],
                            nested_field_hints=self.nested_field_hints,
                        )
                    except CoreError:
                        leaf_t = None
            pg_t = self._python_ann_to_postgres_type(leaf_t) if leaf_t else None
            if len(segments) == 1:
                key = segments[0]
                field_expr = sql.SQL("(_fz_elem ->> {})").format(sql.Literal(key))
            else:
                field_expr = sql.SQL("(_fz_elem #>> {})").format(
                    sql.Literal("{" + ",".join(segments) + "}"),
                )
            if pg_t is not None:
                cast = cast_sql_for_column_type(pg_t)
                if cast is not None:
                    field_expr = sql.SQL("({})::{}").format(field_expr, cast)
            parts.append(self._render_field(field_expr, f.op, f.value, t=pg_t))

        if len(parts) == 1:
            return parts[0]
        return sql.SQL("(") + sql.SQL(" AND ").join(parts) + sql.SQL(")")

    @staticmethod
    def _python_ann_to_postgres_type(ann: Any) -> PostgresType | None:
        if ann is None:
            return None
        from datetime import date, datetime
        from uuid import UUID

        if ann is UUID:
            return PostgresType(base="uuid", is_array=False, not_null=False)
        if ann is bool:
            return PostgresType(base="bool", is_array=False, not_null=False)
        if ann is int:
            return PostgresType(base="int8", is_array=False, not_null=False)
        if ann is float:
            return PostgresType(base="float8", is_array=False, not_null=False)
        if ann is datetime:
            return PostgresType(base="timestamptz", is_array=False, not_null=False)
        if ann is date:
            return PostgresType(base="date", is_array=False, not_null=False)
        if ann is str:
            return PostgresType(base="text", is_array=False, not_null=False)
        return None
