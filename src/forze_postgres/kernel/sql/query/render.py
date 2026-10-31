"""Render parsed query expressions into psycopg SQL composables."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

import math
from collections.abc import Callable, Mapping
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import UnionType
from typing import Any, Union, get_args, get_origin
from uuid import UUID

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    FULL_QUERY_CAPABILITIES,
    UNSUPPORTED_QUERY_FEATURE_CODE,
    AggregateComputedField,
    AggregatesExpression,
    AggregatesExpressionParser,
    GroupField,
    GroupKey,
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
    elem_inner_is_scalar,
    parse_sort_value,
    unwrap_annotated,
    validate_aggregate_capabilities,
    validate_query_capabilities,
)
from forze.base.exceptions import exc
from forze.base.serialization import enum_values

from ...catalog.introspect import PostgresColumnTypes, PostgresType
from ...catalog.introspect.utils import normalize_pg_type, strip_type_modifier
from ..type_cast import cast_sql_for_column_type
from .nested import (
    build_nested_json_scalar_expr,
    python_type_to_postgres_scalar,
    resolve_leaf_python_type,
    sort_key_expr,
    walk_pydantic_path,
)
from .utils import PsycopgPositionalBinder

# ----------------------- #

POSTGRES_QUERY_CAPABILITIES = FULL_QUERY_CAPABILITIES
"""Postgres compiles the full DSL surface at the AST level: every operator, element
quantifiers (including *nested* quantifiers — a quantifier inside another's element
predicate, via an alias-parameterized nested ``EXISTS`` over the object sub-array),
``$not``, and field-to-field comparison. The one residual gap — set operators /
``$empty`` on a *nested JSON* path rather than a native array column — depends on a
column's storage shape the AST cannot see, so it stays a backend-internal check
(raising the same ``query_feature_unsupported`` code)."""

# ....................... #

_PLAIN: frozenset[type] = frozenset(
    {str, bytes, int, float, bool, Decimal, UUID, date, datetime, time, timedelta}
)
"""Exact types :func:`enum_values` returns as they are: neither a member nor a container."""

_NESTED_JSON_UNSUPPORTED: frozenset[str] = frozenset(
    ("$empty", "$superset", "$subset", "$disjoint", "$overlaps"),
)

_TEXT_LIKE_BASES: frozenset[str] = frozenset({"text", "varchar", "char", "citext"})

_TEXT_TYPE = PostgresType(base="text", is_array=False, not_null=False)
_NUMERIC_TYPE = PostgresType(base="numeric", is_array=False, not_null=False)
_TIMESTAMPTZ_TYPE = PostgresType(base="timestamptz", is_array=False, not_null=False)
"""Types an aggregate output renders as in ``$having`` (:meth:`aggregate_outputs`)."""

_NUMERIC_BASES: frozenset[str] = frozenset({"int2", "int4", "int8", "float4", "float8", "numeric"})


def _having_kind(t: PostgresType | None) -> PostgresType:
    """An output's type for ``$having``: every number is ``numeric``, so a fractional bound or
    another numeric output always compares; anything else keeps its source type, so a value
    coerces exactly as a filter on that column coerces it (a timestamp keeps its zone)."""

    if t is None:
        return _TEXT_TYPE

    # A declared precision is the column's, not the output's: ``numeric(10,2)`` is a number,
    # ``timestamp(3) with time zone`` a ``timestamptz``.
    base = normalize_pg_type(strip_type_modifier(t.base))

    if not t.is_array and base in _NUMERIC_BASES:
        return _NUMERIC_TYPE

    return attrs.evolve(t, base=base)


_LTREE_BASE: str = "ltree"

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


def _compares_numerically(value: Any, *, decimal_elem: bool) -> bool:
    """Whether one scalar operand compares by numeric value with a ``jsonb`` element.

    A ``Decimal`` always does; an int or float does on an element annotated to hold
    ``Decimal`` (*decimal_elem*), whose stored values are JSON strings.
    """

    if isinstance(value, Decimal):
        return True

    return decimal_elem and isinstance(value, (int, float)) and not isinstance(value, bool)


# ....................... #


def _scalar_ann_may_hold_decimal(ann: Any, seen: set[int] | None = None) -> bool:
    """Whether the scalar reached by unwrapping lists/unions in *ann* can be a ``Decimal``.

    ``Annotated`` metadata and ``type`` aliases are unwrapped too: pydantic keeps them
    inside a union or a container, as in ``list[Annotated[Decimal, Field(ge=0)]]``. *seen*
    carries the aliases expanded so far, so a recursive one (``type Json = str |
    list[Json]``) ends the walk.
    """

    seen = set() if seen is None else seen
    ann = unwrap_annotated(ann, seen)
    origin = get_origin(ann)

    if origin is list:
        args = get_args(ann)
        return bool(args) and _scalar_ann_may_hold_decimal(args[0], seen)

    if origin is Union or origin is UnionType:
        return any(_scalar_ann_may_hold_decimal(a, seen) for a in get_args(ann))

    return isinstance(ann, type) and issubclass(ann, Decimal)


# ....................... #

_JSONB_COMPARE_OPS: frozenset[str] = frozenset(
    {"$eq", "$neq", "$gt", "$gte", "$lt", "$lte", "$in", "$nin"},
)
"""Operators an element field with no resolved type compares in ``jsonb`` space."""


_NUMERIC_TEXT_PATTERN = (
    r"^[ \t\n\r\f\v]*[-+]?([0-9]+(\.[0-9]*)?|\.[0-9]+)"
    r"([eE](\+?0*[0-9]{1,5}|-0*([0-9]{1,4}|1[0-4][0-9]{3})))?[ \t\n\r\f\v]*$"
)
"""Decimal text ``::numeric`` always accepts, read with :data:`_NUMERIC_TEXT_MAX_LENGTH`.

ASCII classes, not ``\\d`` / ``\\s``: under an ICU or builtin locale those match any Unicode
digit or space, which ``::numeric`` refuses.

``numeric`` holds 131072 digits before the point and 16383 after. With at most 1000
characters of digits, an exponent up to ``e99999`` or down to ``e-14999`` stays inside
both, so nothing that matches can overflow. The ``NaN`` / ``Infinity`` / hex / underscore
forms (PG 16+ accepts the last two) match no evaluator decimal and stay out."""

_NUMERIC_TEXT_MAX_LENGTH = 1000


def _jsonb_operand_type(value: Any) -> tuple[str, str]:
    """The cast that binds *value* for ``to_jsonb``, and the ``jsonb_typeof`` it yields."""

    if isinstance(value, bool):
        return "boolean", "boolean"

    if isinstance(value, int):
        # ``numeric``, not ``bigint``: an int past int64 would overflow at execute time.
        return "numeric", "number"

    if isinstance(value, float):
        return "double precision", "number"

    return "text", "string"


# ....................... #


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
        # A member binds as the value a write stores: ``str()`` names a ``(str, Enum)`` member.
        v = enum_values(v)

        if v is None:
            return None

        if t is None:
            return self.caster.pass_through(v)

        if t.is_array:
            raise exc.internal(f"Array type not supported: {t!r}")

        match t.base:
            case "uuid":
                return self.caster.as_uuid(v)

            case "text" | "varchar" | "char" | "citext":
                return str(v)

            case "bool":
                return self.caster.as_bool(v)

            case "int2" | "int4" | "int8":
                return self.caster.as_int(v)

            case "float4" | "float8":
                return self.caster.as_float(v)

            case "numeric":
                return self.caster.as_decimal(v)

            case "date":
                return self.caster.as_date(v)

            case "timestamptz" | "timestamp":
                return self.caster.as_datetime(v, force_tz=t.base == "timestamptz")

            case _:
                return v

    # ....................... #

    @staticmethod
    def kept(base: str) -> Callable[[Any], bool]:
        """Whether :meth:`scalar` returns a value of a *base* column as it is, judged from its
        exact type: a ``StrEnum`` member, a ``str`` subclass, is never kept. Mirrors
        :meth:`scalar` case by case, so a case added there needs its line here."""

        match base:
            case "uuid":
                return lambda v: type(v) is UUID

            case "text" | "varchar" | "char" | "citext":
                return lambda v: type(v) is str

            case "bool":
                return lambda v: type(v) is bool

            case "int2" | "int4" | "int8":
                return lambda v: type(v) is int

            case "float4" | "float8":
                return lambda v: type(v) is float and math.isfinite(v)

            case "numeric":
                return lambda v: type(v) is Decimal and v.is_finite()

            case "date":
                return lambda v: type(v) is date

            case "timestamptz":
                return lambda v: type(v) is datetime and v.tzinfo is not None

            case "timestamp":
                return lambda v: type(v) is datetime and v.tzinfo is None

            case _:
                return lambda v: type(v) in _PLAIN

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
            raise exc.internal(f"Scalar value not supported: {v!r}")

        if t is None:
            return [self.scalar(x, t=None) for x in v]

        if not t.is_array and raise_on_scalar_t:
            raise exc.precondition(
                "Set operators ($superset / $subset / $disjoint / $overlaps) "
                "require an array column on the 'postgres' backend.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

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

    column_exprs: Mapping[str, sql.Composable] | None = attrs.field(default=None)
    """Expressions that stand in for top-level columns of the same name."""

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
        validate_query_capabilities(expr, POSTGRES_QUERY_CAPABILITIES, backend="postgres")
        query = self._render_expr(expr)
        params = self.binder.values()

        return query, params

    # ....................... #

    def render_aggregates(
        self,
        aggregates: AggregatesExpression,
        *,
        filter_parser: QueryFilterExpressionParser | None = None,
    ) -> tuple[ParsedAggregates, sql.Composable, sql.Composable | None, list[Any]]:
        """Render aggregate SELECT and GROUP BY clauses.

        :param filter_parser: Parses the metric filters and ``$having`` (the spec's limits).
        """

        validate_aggregate_capabilities(aggregates, POSTGRES_QUERY_CAPABILITIES, backend="postgres")

        parsed = AggregatesExpressionParser.parse(
            aggregates,
            filter_parser=filter_parser,
            model_type=self.model_type,
        )
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

    def aggregate_outputs(
        self,
        parsed: ParsedAggregates,
        *,
        table_alias: str,
    ) -> tuple[PostgresColumnTypes, dict[str, sql.Composable]]:
        """Each aggregate output alias of *table_alias* as ``$having`` reads it: its type, and
        the expression standing in for an alias that is not read as reported.

        A field group or a ``$min`` / ``$max`` takes its source column's type, and every other
        measure is ``numeric`` (:func:`_having_kind`), so an operator the output cannot take is
        refused before the statement runs and a threshold coerces as a filter on that type
        would. A time compares as the instant it is, whatever the session's zone: a bucket,
        reported as wall time in its zone (:meth:`_render_trunc_expr`), is read back from that
        zone, and a ``timestamp`` output, UTC wall time as a filter reads its column, from UTC.
        """

        types: PostgresColumnTypes = {}
        columns: dict[str, sql.Composable] = {}

        for group in parsed.groups:
            if isinstance(group.expr, GroupField):
                types[group.alias] = _having_kind(self._resolve_column_expr(group.expr.field)[1])
                continue

            wall = sql.Identifier(table_alias, group.alias)
            tz = group.expr.timezone
            types[group.alias] = _TIMESTAMPTZ_TYPE

            if tz.mode == "iana":
                columns[group.alias] = sql.SQL("({} AT TIME ZONE {})").format(
                    wall,
                    sql.Literal(tz.iana),
                )

            else:
                offset = tz.offset if tz.offset is not None else timedelta(0)
                columns[group.alias] = sql.SQL("(({} - {}) AT TIME ZONE 'UTC')").format(
                    wall,
                    sql.Literal(offset),
                )

        for computed in parsed.computed_fields:
            if computed.function in ("$min", "$max") and computed.field is not None:
                types[computed.alias] = _having_kind(self._resolve_column_expr(computed.field)[1])

            else:
                types[computed.alias] = _NUMERIC_TYPE

        for alias, t in types.items():
            if t.base == "timestamp" and not t.is_array:
                columns[alias] = sql.SQL("({} AT TIME ZONE 'UTC')").format(
                    sql.Identifier(table_alias, alias)
                )
                types[alias] = _TIMESTAMPTZ_TYPE

        return types, columns

    # ....................... #

    def _render_group_expr(
        self,
        group: GroupKey,
    ) -> tuple[sql.Composable, sql.Composable]:
        ident = sql.Identifier(group.alias)

        if isinstance(group.expr, GroupField):
            expr = self._render_source_expr(group.expr.field)

        else:
            expr = self._render_trunc_expr(group.expr)

        return expr, ident

    # ....................... #

    def _render_trunc_expr(self, trunc: GroupTrunc) -> sql.Composable:
        col = self._render_source_expr(trunc.field)
        source_t = (self.types or {}).get(trunc.field)

        # A ``timestamp`` column holds UTC wall time, as a filter on it reads it; bucket the
        # instant that is, not the same wall time in the bucket's zone.
        if source_t is not None and _having_kind(source_t).base == "timestamp":
            col = sql.SQL("({} AT TIME ZONE 'UTC')").format(col)

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
        sorts: Mapping[str, Any] | None,
    ) -> sql.Composable | None:
        """Render ORDER BY for aggregate result aliases.

        Each key carries an explicit null placement, the sort's own or the canonical one (first
        ascending, last descending), as every other read orders a null: Postgres's default
        would put a null group last ascending.
        """

        if not sorts:
            return None

        aliases = parsed.aliases
        bad = [field for field in sorts if field not in aliases]

        if bad:
            raise exc.precondition(f"Invalid aggregate sort fields: {bad}")

        parts: list[sql.Composable] = []

        for field, value in sorts.items():
            order, nulls = parse_sort_value(value, field=field)
            parts.append(
                sql.SQL("{} {} {}").format(
                    sql.Identifier(field),
                    sql.SQL("ASC") if order == "asc" else sql.SQL("DESC"),
                    sql.SQL("NULLS FIRST") if nulls == "first" else sql.SQL("NULLS LAST"),
                )
            )

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
            raise exc.internal("Computed field has no field path")

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

            case "$count_distinct":
                agg_expr = sql.SQL("COUNT(DISTINCT {})").format(field_expr)

            case "$stddev_pop":
                agg_expr = sql.SQL("stddev_pop({})").format(field_expr)

            case "$stddev_samp":
                agg_expr = sql.SQL("stddev_samp({})").format(field_expr)

            case "$var_pop":
                agg_expr = sql.SQL("var_pop({})").format(field_expr)

            case "$var_samp":
                agg_expr = sql.SQL("var_samp({})").format(field_expr)

            case "$percentile":
                agg_expr = sql.SQL(
                    "percentile_cont({}) WITHIN GROUP (ORDER BY {})",
                ).format(sql.Literal(computed.p), field_expr)

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise exc.internal(f"Unsupported aggregate function: {function!r}")

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
            raise exc.internal("Aggregate rendering requires column type metadata")

        if self.model_type is None:
            raise exc.internal("Aggregate rendering requires gateway model_type")

        return sort_key_expr(
            field=field,
            column_types=self.types,
            model_type=self.model_type,
            nested_field_hints=self.nested_field_hints,
            table_alias=self.table_alias,
        )

    # ....................... #

    def _resolve_column_expr(
        self,
        field: str,
    ) -> tuple[sql.Composable, PostgresType | None]:
        """Resolve a field path to a SQL column/expression and optional Postgres type."""

        segments = field.split(".")
        if len(segments) > 1:
            if self.types is None:
                raise exc.internal(
                    f"Nested compare path {field!r} requires column type metadata "
                    "(introspected types).",
                )
            if self.model_type is None:
                raise exc.internal(
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
                raise exc.internal(f"Unknown column: {segments[0]!r}")

        else:
            t = None

        return self._top_column(segments[0]), t

    # ....................... #

    def _top_column(self, name: str) -> sql.Composable:
        """A top-level column, or the expression standing in for it (:attr:`column_exprs`)."""

        if self.column_exprs is not None and name in self.column_exprs:
            return self.column_exprs[name]

        if self.table_alias is not None:
            return sql.Identifier(self.table_alias, name)

        return sql.Identifier(name)

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
            raise exc.precondition(
                f"Field-to-field comparison between {left!r} and {right!r} does not "
                "support array columns on the 'postgres' backend.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        if left_t.base == right_t.base:
            return

        for group in _COMPARE_COMPAT_BASE_GROUPS:
            if left_t.base in group and right_t.base in group:
                return

        raise exc.precondition(
            f"Incompatible types between fields {left!r} and {right!r}: field-to-field "
            "comparison of these fields is not supported on the 'postgres' backend "
            "(compare fields of compatible types).",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
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
            raise exc.internal(f"Unknown compare operator: {op!r}")

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
                        raise exc.internal(
                            f"Nested filter path {name!r} requires column type metadata "
                            "(introspected types).",
                        )

                    if self.model_type is None:
                        raise exc.internal(
                            f"Nested filter path {name!r} requires gateway model_type "
                            "for read-model validation.",
                        )

                    if op in _NESTED_JSON_UNSUPPORTED:
                        # Storage-shape-dependent (a nested JSON path is not a native
                        # array column), so the AST-level capability check cannot see
                        # it — but surface the same clean code, not an internal 500.
                        raise exc.precondition(
                            f"Operator {op!r} is not supported on the nested JSON path "
                            f"{name!r} for the 'postgres' backend (use a top-level array "
                            "column for set/empty operators).",
                            code=UNSUPPORTED_QUERY_FEATURE_CODE,
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
                        raise exc.internal(f"Unknown column: {name!r}")

                else:
                    t = None

                return self._render_field(self._top_column(name), op, value, t=t)

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
                raise exc.internal(f"Unknown expression: {expr!r}")

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

            case "$like" | "$ilike" | "$regex":
                return self._render_text(col, op, value, t=t)

            case "$descendant_of" | "$ancestor_of":
                return self._render_hierarchy(col, op, value, t=t)

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise exc.internal(f"Unknown operator: {op!r}")

    # ....................... #

    def _ensure_text_column(self, t: PostgresType | None, op: str) -> None:
        if t is None:
            return

        if t.is_array:
            raise exc.precondition(
                f"Operator {op!r} is not supported on an array column for the "
                "'postgres' backend; use a text-like column.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        if t.base not in _TEXT_LIKE_BASES:
            raise exc.precondition(
                f"Operator {op!r} requires a text-like column for the 'postgres' backend.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

    # ....................... #

    def _render_text(
        self,
        col: sql.Composable,
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        self._ensure_text_column(t, op)
        pattern = str(self.coercer.scalar(value, t=t))

        match op:
            case "$like":
                op_sql = sql.SQL("LIKE")

            case "$ilike":
                op_sql = sql.SQL("ILIKE")

            case "$regex":
                op_sql = sql.SQL("~")

            case _:
                raise exc.internal(f"Unknown text operator: {op!r}")

        return sql.SQL("{} {} {}").format(col, op_sql, self.binder.add(pattern))

    # ....................... #

    def _render_hierarchy(
        self,
        col: sql.Composable,
        op: str,
        value: Any,
        *,
        t: PostgresType | None,
    ) -> sql.Composable:
        """Render a materialized-path containment predicate (both ends inclusive).

        Native ``ltree`` columns use the index-backed ancestor/descendant operators
        (``@>`` / ``<@``). Plain text columns fall back to label-aware prefix matching via
        ``starts_with`` (appending the ``.`` separator so a label boundary is required —
        ``top.science`` is *not* a descendant of ``top.sci``). The parser has already
        expanded a list operand into an ``OR`` of single-path predicates, so *value* is a
        single path string here.
        """

        path = str(value)

        if t is not None and t.is_array:
            raise exc.precondition(
                f"Hierarchy operator {op!r} is not supported on an array column for the "
                "'postgres' backend; use an ltree or text-like column.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        if t is not None and t.base == _LTREE_BASE:
            bound = sql.SQL("{}::ltree").format(self.binder.add(path))

            # ``a <@ b`` ⇔ a is a descendant of b; ``a @> b`` ⇔ a is an ancestor of b
            # (both inclusive). Column on the left, the given node on the right.
            ltree_op = sql.SQL("<@") if op == "$descendant_of" else sql.SQL("@>")

            return sql.SQL("{} {} {}").format(col, ltree_op, bound)

        if t is not None and t.base not in _TEXT_LIKE_BASES:
            raise exc.precondition(
                f"Hierarchy operator {op!r} requires an ltree or text-like column for "
                "the 'postgres' backend.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        # Compare label sequences with a trailing separator so equality and strict
        # containment fold into one ``starts_with`` (and a label boundary is required:
        # ``top.science.`` is not a prefix of ``top.scientist.``). The bound node value
        # is referenced once — appending ``'.'`` to *both* sides makes ``col == node``
        # the inclusive case.
        col_path = sql.SQL("(({})::text || '.')").format(col)
        node_path = sql.SQL("({} || '.')").format(self.binder.add(path))

        if op == "$descendant_of":
            # col is at or below the node: col's path starts with the node's path.
            return sql.SQL("starts_with({}, {})").format(col_path, node_path)

        # col is at or above the node: the node's path starts with col's path.
        return sql.SQL("starts_with({}, {})").format(node_path, col_path)

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

            return sql.SQL("(jsonb_typeof({j}) = 'array' AND jsonb_array_length({j}) > 0)").format(
                j=j
            )

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
            # ``IS DISTINCT FROM`` (not ``<>``) so a NULL / absent column *matches* $neq,
            # aligning with the in-memory evaluator's documented semantics, with Mongo
            # (``null_matches_missing``), and with this renderer's own field-to-field $neq
            # path — plain ``<>`` would three-valued-exclude NULL rows and diverge.
            "$neq": "IS DISTINCT FROM",
        }
        op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]

        if t is not None and t.is_array:
            if not isinstance(value, (list, tuple)) or isinstance(
                value,
                (str, bytes, bytearray),
            ):
                raise exc.precondition(
                    f"Filter {op!r} on an array column requires a list/tuple value on "
                    "the 'postgres' backend; use $null for null checks, or "
                    "$superset / $overlaps / $in for containment-style matches.",
                    code=UNSUPPORTED_QUERY_FEATURE_CODE,
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

        if op == "$in":
            return expr

        # $nin: also match rows whose column is NULL / absent — the in-memory evaluator
        # (and Mongo) treat a missing/null field as satisfying $nin; a bare ``NOT (…)`` would
        # three-valued-exclude NULL rows. (The array-column branch above already includes
        # NULL: ``unnest(NULL)`` is empty, so ``NOT EXISTS`` is TRUE.)
        return sql.SQL("({} IS NULL OR NOT ({}))").format(col, expr)

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
            # A NULL / absent array is disjoint from everything (empty ∩ x = ∅), matching the
            # in-memory evaluator; a bare ``NOT (col && ph)`` would three-valued-exclude it.
            return sql.SQL("({} IS NULL OR NOT ({} && {}))").format(col, col, ph)

        else:
            op_sql = sql.SQL(op_map[op])  # pyright: ignore[reportArgumentType]

            return sql.SQL("{} {} {}").format(col, op_sql, ph)

    # ....................... #

    def _elem_source(
        self,
        col: sql.Composable,
        t: PostgresType | None,
        depth: int,
    ) -> sql.Composable:
        """``FROM`` items yielding the elements of array *col* as ``_fz_elem[_depth]``.

        *col* is named once: a view column computed by a correlated subquery runs that
        subquery once per name. A JSON value goes through an ``OFFSET 0`` subquery, which
        the planner cannot pull up, so the ``CASE`` reads its one evaluation twice.
        ``jsonb_array_elements`` and ``unnest`` are strict, so a NULL, a non-array JSON
        value and an empty array yield no rows and the quantifiers need no guard.
        """

        elem = sql.Identifier(self._elem_alias(depth))

        if t is not None and t.is_array:
            return sql.SQL("unnest({}) AS {}").format(col, elem)

        src = sql.Identifier("_fz_src" if depth == 0 else f"_fz_src_{depth}")
        value = sql.SQL("{}.{}").format(src, sql.Identifier("v"))

        return sql.SQL(
            "(SELECT ({col})::jsonb AS {v} OFFSET 0) AS {src}, jsonb_array_elements("
            "CASE WHEN jsonb_typeof({value}) = 'array' THEN {value} END) AS {elem}"
        ).format(col=col, v=sql.Identifier("v"), src=src, value=value, elem=elem)

    # ....................... #

    @staticmethod
    def _quantify(quantifier: str, source: sql.Composable, pred: sql.Composable) -> sql.Composable:
        """``$any`` / ``$all`` / ``$none`` of *pred* over the elements *source* yields.

        Over no rows ``$any`` is false and ``$all`` / ``$none`` are true, which is the
        vacuous reading of a missing, null, non-array or empty array. An element whose
        predicate is NULL (a missing key, a NULL element) has not matched: it fails
        ``$all`` and is no match for ``$any`` / ``$none``, as in the in-memory evaluator.
        """

        if quantifier == "$any":
            return sql.SQL("EXISTS (SELECT 1 FROM {} WHERE {})").format(source, pred)

        if quantifier == "$all":
            return sql.SQL("NOT EXISTS (SELECT 1 FROM {} WHERE ({}) IS NOT TRUE)").format(
                source, pred
            )

        return sql.SQL("NOT (EXISTS (SELECT 1 FROM {} WHERE {}))").format(source, pred)

    # ....................... #

    def _object_elements_only(
        self,
        pred: sql.Composable,
        depth: int,
        typeof: str,
    ) -> sql.Composable:
        """Gate an object predicate on a JSON element: a scalar or null never matches.

        Without it ``$neq`` / ``$nin`` and a nested ``$all`` / ``$none`` hold on a scalar
        (its fields read as NULL), where the in-memory evaluator matches no field of
        anything that is not an object. *typeof* is ``jsonb_typeof`` or ``json_typeof``.
        """

        return sql.SQL("({}({}) = 'object' AND {})").format(
            sql.SQL(typeof),
            sql.Identifier(self._elem_alias(depth)),
            pred,
        )

    # ....................... #

    @staticmethod
    def _elem_alias(depth: int) -> str:
        """Per-depth subquery alias so nested quantifiers don't shadow their parents."""

        return "_fz_elem" if depth == 0 else f"_fz_elem_{depth}"

    # ....................... #

    def _render_elem(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> sql.Composable:
        col, t = self._resolve_column_and_type(path)

        if t is not None and not t.is_array and t.base not in ("jsonb", "json"):
            raise exc.precondition(
                f"Element quantifier on {path!r} requires an array or jsonb-array "
                "column on the 'postgres' backend.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        elem_pred = self._render_elem_inner(inner, col, t, path.split("."), depth=0)

        return self._quantify(quantifier, self._elem_source(col, t, 0), elem_pred)

    # ....................... #

    def _resolve_column_and_type(
        self,
        path: str,
    ) -> tuple[sql.Composable, PostgresType | None]:
        segments = path.split(".")

        if len(segments) > 1:
            if self.types is None or self.model_type is None:
                raise exc.internal(
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
                raise exc.internal(f"Unknown column: {path!r}")

        else:
            t = None

        return self._top_column(path), t

    # ....................... #

    def _render_elem_inner(
        self,
        inner: QueryExpr,
        col: sql.Composable,
        t: PostgresType | None,
        model_path: list[str],
        *,
        depth: int,
    ) -> sql.Composable:
        if elem_inner_is_scalar(inner):
            return self._render_elem_scalar_inner(
                inner,
                t,
                depth=depth,
                decimal_elem=self._elem_may_hold_decimal(model_path),
                path=".".join(model_path),
            )

        return self._render_elem_object_inner(
            inner,
            model_path,
            depth=depth,
            # A native jsonb[] / json[] element can be a scalar too; other arrays cannot.
            elem_typeof=(
                {"jsonb": "jsonb_typeof", "json": "json_typeof"}.get(t.base)
                if t is not None and t.is_array
                else "jsonb_typeof"
            ),
        )

    # ....................... #

    def _elem_may_hold_decimal(self, model_path: list[str]) -> bool:
        """Whether the array at *model_path* is annotated to hold ``Decimal`` scalars."""

        if self.model_type is None:
            return False

        ann = walk_pydantic_path(self.model_type, model_path)

        return ann is not None and _scalar_ann_may_hold_decimal(ann)

    # ....................... #

    def _render_elem_scalar_inner(
        self,
        inner: QueryExpr,
        t: PostgresType | None,
        *,
        depth: int,
        decimal_elem: bool,
        path: str,
    ) -> sql.Composable:
        match inner:
            case QueryField(name, _, _) if name == ELEM_SCALAR_FIELD:
                fields = [inner]

            case QueryAnd(items):
                fields = [i for i in items if isinstance(i, QueryField)]

            case QueryOr(items):
                parts = [
                    self._render_elem_scalar_inner(
                        i, t, depth=depth, decimal_elem=decimal_elem, path=path
                    )
                    for i in items
                ]

                if len(parts) == 1:
                    return parts[0]
                return sql.SQL("(") + sql.SQL(" OR ").join(parts) + sql.SQL(")")

            case _:
                raise exc.internal(f"Invalid scalar element inner: {inner!r}")

        elem = sql.Identifier(self._elem_alias(depth))

        if t is None or t.base in ("json", "jsonb"):
            # A JSON array, or a native jsonb[] / json[], yields jsonb elements of any JSON
            # type; compare each in jsonb space by the operand's type, as a nested scalar
            # sub-array does (Decimal operands and elements take the numeric path there).
            jsonb = (
                sql.SQL("({})::jsonb").format(elem)
                if t is not None and t.is_array and t.base == "json"
                else elem
            )

            if t is not None and t.is_array:
                # A native element can be SQL NULL as well as JSON null; the evaluator reads
                # both as None, so SQL NULL compares as JSON null, not as a missing field.
                jsonb = sql.SQL("COALESCE({}, 'null'::jsonb)").format(jsonb)

            parts = [
                self._render_jsonb_scalar_field(
                    jsonb, f.op, f.value, decimal_elem=decimal_elem, path=path
                )  # type: ignore[arg-type]
                for f in fields
            ]

        else:
            elem_t = PostgresType(base=t.base, is_array=False, not_null=True)
            parts = [self._render_native_elem_field(elem, f.op, f.value, t=elem_t) for f in fields]  # type: ignore[arg-type]

        if len(parts) == 1:
            return parts[0]

        return sql.SQL("(") + sql.SQL(" AND ").join(parts) + sql.SQL(")")

    # ....................... #

    def _render_native_elem_field(
        self,
        elem: sql.Composable,
        op: str,
        value: Any,
        *,
        t: PostgresType,
    ) -> sql.Composable:
        """Compare a native array element; a ``None`` ``$in`` / ``$nin`` member is its NULL.

        ``= ANY`` never matches NULL, where the in-memory evaluator matches a NULL element
        to a ``None`` member, so the member is taken out and tested with ``IS NULL``.
        """

        if op in ("$in", "$nin") and isinstance(value, (list, tuple, set)) and None in value:
            known = self._render_memb(
                elem,
                "$in",
                [v for v in value if v is not None],  # pyright: ignore[reportUnknownVariableType]
                t=t,
            )

            if op == "$in":
                return sql.SQL("({} IS NULL OR {})").format(elem, known)

            return sql.SQL("({} IS NOT NULL AND NOT ({}))").format(elem, known)

        return self._render_field(elem, op, value, t=t)  # type: ignore[arg-type]

    # ....................... #

    def _render_elem_object_inner(
        self,
        inner: QueryExpr,
        model_path: list[str],
        *,
        depth: int,
        elem_typeof: str | None = "jsonb_typeof",
    ) -> sql.Composable:
        alias = sql.Identifier(self._elem_alias(depth))

        match inner:
            case QueryAnd(items):
                fields = [i for i in items if isinstance(i, QueryField)]
                nested = [i for i in items if isinstance(i, QueryElem)]

                unsupported = [i for i in items if not isinstance(i, (QueryField, QueryElem))]
                if unsupported:
                    raise exc.internal(
                        f"Unsupported node in object element predicate: {unsupported[0]!r}",
                    )

            case QueryField() as f:
                fields = [f]
                nested = []

            case QueryElem() as qe:
                fields = []
                nested = [qe]

            case QueryOr(items):
                or_parts = [
                    self._render_elem_object_inner(
                        i, model_path, depth=depth, elem_typeof=elem_typeof
                    )
                    for i in items
                ]

                if len(or_parts) == 1:
                    return or_parts[0]

                return sql.SQL("(") + sql.SQL(" OR ").join(or_parts) + sql.SQL(")")

            case _:
                raise exc.internal(f"Invalid object element inner: {inner!r}")

        parts: list[sql.Composable] = []

        for f in fields:
            segments = f.name.split(".")
            full_segments = [*model_path, *segments]
            path = ".".join(full_segments)
            leaf_t = None

            if self.model_type is not None:
                # Read model first, then ``nested_field_hints[path]`` — the hint is what
                # types a field the model leaves untyped (``list[dict]``).
                try:
                    leaf_t = resolve_leaf_python_type(
                        model_type=self.model_type,
                        path=path,
                        segments=full_segments,
                        nested_field_hints=self.nested_field_hints,
                    )
                except exc:
                    leaf_t = None

            pg_t = self._python_ann_to_postgres_type(leaf_t) if leaf_t else None

            if len(segments) == 1:
                arrows, key = ("->", "->>"), sql.Literal(segments[0])

            else:
                arrows, key = ("#>", "#>>"), sql.Literal("{" + ",".join(segments) + "}")

            if pg_t is None and self._untyped_compares_as_jsonb(f.op, f.value, path):
                json_expr = sql.SQL("({} {} {})").format(alias, sql.SQL(arrows[0]), key)

                # A native json[] element yields json, which has no jsonb comparisons.
                if elem_typeof == "json_typeof":
                    json_expr = sql.SQL("({})::jsonb").format(json_expr)

                parts.append(
                    self._render_jsonb_scalar_field(
                        json_expr, f.op, f.value, decimal_elem=False, path=path
                    )
                )
                continue

            field_expr = sql.SQL("({} {} {})").format(alias, sql.SQL(arrows[1]), key)

            if pg_t is not None:
                cast = cast_sql_for_column_type(pg_t)

                if cast is not None:
                    field_expr = sql.SQL("({})::{}").format(field_expr, cast)

            parts.append(self._render_field(field_expr, f.op, f.value, t=pg_t))

        # A quantifier over the element itself (an array of arrays) reads no field of it.
        arrays = [qe for qe in nested if qe.path == ELEM_SCALAR_FIELD]
        parts.extend(
            self._render_nested_elem(qe, model_path, depth=depth)
            for qe in nested
            if qe.path != ELEM_SCALAR_FIELD
        )

        # A field of a scalar or null element reads as NULL. Only $neq, $nin and a nested
        # $all / $none can hold on that, so only they pay for the per-element gate.
        holds_on_null = any(f.op in ("$neq", "$nin") for f in fields) or any(
            qe.quantifier in ("$all", "$none") for qe in nested if qe.path != ELEM_SCALAR_FIELD
        )

        if parts and elem_typeof is not None and holds_on_null:
            parts = [self._object_elements_only(self._and(parts), depth, elem_typeof)]

        parts.extend(self._render_nested_elem(qe, model_path, depth=depth) for qe in arrays)

        return self._and(parts)

    # ....................... #

    @staticmethod
    def _and(parts: list[sql.Composable]) -> sql.Composable:
        if len(parts) == 1:
            return parts[0]

        return sql.SQL("(") + sql.SQL(" AND ").join(parts) + sql.SQL(")")

    # ....................... #

    def _render_nested_elem(
        self,
        node: QueryElem,
        model_path: list[str],
        *,
        depth: int,
    ) -> sql.Composable:
        """Render a quantifier nested inside another element's object predicate.

        The outer element is bound to ``_fz_elem[_depth]`` (a ``jsonb`` object); its
        sub-array ``node.path`` is iterated by a fresh inner alias one level deeper.
        Scalar sub-arrays compare element-to-value in ``jsonb`` space (``to_jsonb``),
        sidestepping per-element type resolution; object sub-arrays recurse through
        :meth:`_render_elem_object_inner`, which can nest again.
        """

        outer = sql.Identifier(self._elem_alias(depth))
        next_depth = depth + 1

        if node.path == ELEM_SCALAR_FIELD:
            # Scalar array-of-arrays: the outer element is itself the sub-array.
            base = sql.SQL("{}").format(outer)
            sub_model_path = model_path

        else:
            sub_segments = node.path.split(".")

            if len(sub_segments) == 1:
                base = sql.SQL("({} -> {})").format(outer, sql.Literal(sub_segments[0]))

            else:
                base = sql.SQL("({} #> {})").format(
                    outer,
                    sql.Literal("{" + ",".join(sub_segments) + "}"),
                )

            sub_model_path = [*model_path, *sub_segments]

        if elem_inner_is_scalar(node.inner):
            elem_pred = self._render_jsonb_scalar_inner(
                node.inner,
                depth=next_depth,
                decimal_elem=self._elem_may_hold_decimal(sub_model_path),
                path=".".join(sub_model_path),
            )

        else:
            elem_pred = self._render_elem_object_inner(node.inner, sub_model_path, depth=next_depth)

        return self._quantify(node.quantifier, self._elem_source(base, None, next_depth), elem_pred)

    # ....................... #

    def _render_jsonb_scalar_inner(
        self,
        inner: QueryExpr,
        *,
        depth: int,
        decimal_elem: bool,
        path: str,
    ) -> sql.Composable:
        """Element predicate for a *nested* scalar sub-array (``jsonb`` elements).

        Unlike :meth:`_render_elem_scalar_inner` (native array, typed scalar element),
        the element here is a ``jsonb`` value produced by ``jsonb_array_elements``;
        comparisons stay in ``jsonb`` space so no per-element type resolution is needed —
        except numeric comparisons against Decimal-annotated elements (*decimal_elem*),
        which extract-and-cast because stored decimals are JSON strings.
        """

        match inner:
            case QueryField(name, _, _) if name == ELEM_SCALAR_FIELD:
                fields = [inner]

            case QueryAnd(items):
                fields = [i for i in items if isinstance(i, QueryField)]

            case QueryOr(items):
                parts = [
                    self._render_jsonb_scalar_inner(
                        i, depth=depth, decimal_elem=decimal_elem, path=path
                    )
                    for i in items
                ]

                if len(parts) == 1:
                    return parts[0]
                return sql.SQL("(") + sql.SQL(" OR ").join(parts) + sql.SQL(")")

            case _:
                raise exc.internal(f"Invalid scalar element inner: {inner!r}")

        alias = sql.Identifier(self._elem_alias(depth))
        parts = [
            self._render_jsonb_scalar_field(  # type: ignore[arg-type]
                alias, f.op, f.value, decimal_elem=decimal_elem, path=path
            )
            for f in fields
        ]

        if len(parts) == 1:
            return parts[0]

        return sql.SQL("(") + sql.SQL(" AND ").join(parts) + sql.SQL(")")

    # ....................... #

    def _render_jsonb_scalar_field(
        self,
        elem: sql.Composable,
        op: str,
        value: Any,
        *,
        decimal_elem: bool,
        path: str,
    ) -> sql.Composable:
        """Compare a single ``jsonb`` scalar — a sub-array element, or an untyped object
        element's field — to *value* without a typed cast.

        Equality / ordering / membership compare in ``jsonb`` space via ``to_jsonb`` on
        the bound value (ordering only a value of the operand's JSON type); text operators
        extract the element text with ``#>> '{}'`` first. Numeric operands go through the
        numeric extract-and-cast path when the operand is a Decimal or the element
        annotation can hold one (*decimal_elem*) — stored decimals are JSON strings, so
        ``jsonb`` space would rank them above every number.
        """

        self._refuse_without_json_form(value, path)

        if op not in ("$in", "$nin") and _compares_numerically(value, decimal_elem=decimal_elem):
            return self._render_jsonb_scalar_numeric(elem, op, value)

        if op in ("$eq", "$neq"):
            sql_op = sql.SQL("=") if op == "$eq" else sql.SQL("IS DISTINCT FROM")

            return sql.SQL("{} {} {}").format(elem, sql_op, self._jsonb_bound(value))

        if op in ("$gt", "$gte", "$lt", "$lte"):
            if value is None:
                # The parser refuses it; the evaluator orders None against nothing.
                return sql.SQL("FALSE")

            ord_map = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}

            # jsonb orders across types (a string below every number, a boolean above), so
            # a range orders only values of the operand's own JSON type. The evaluator would
            # still order a bool against a number (Python's True > 0); here that never matches.
            return sql.SQL("(jsonb_typeof({e}) = {jt} AND {e} {op} {b})").format(
                e=elem,
                jt=sql.Literal(_jsonb_operand_type(value)[1]),
                op=sql.SQL(ord_map[op]),  # pyright: ignore[reportArgumentType]
                b=self._jsonb_bound(value),
            )

        if op in ("$in", "$nin"):
            values = list(value) if isinstance(value, (list, tuple, set)) else [value]  # type: ignore[arg-type]

            if not values:
                return sql.SQL("FALSE") if op == "$in" else sql.SQL("TRUE")

            # Each member compares as it would under $eq: a numeric one by value, any other
            # (a string, a boolean, None as JSON null) as itself in jsonb space.
            numeric = [v for v in values if _compares_numerically(v, decimal_elem=decimal_elem)]  # pyright: ignore[reportUnknownVariableType]
            plain = [v for v in values if not _compares_numerically(v, decimal_elem=decimal_elem)]  # pyright: ignore[reportUnknownVariableType]
            parts: list[sql.Composable] = []

            if numeric:
                parts.append(
                    sql.SQL("{} IN ({})").format(
                        self._jsonb_numeric_value(elem),
                        sql.SQL(", ").join(
                            self.binder.add(self.coercer.caster.as_decimal(v))
                            for v in numeric  # pyright: ignore[reportUnknownVariableType]
                        ),
                    )
                )

            if plain:
                parts.append(
                    sql.SQL("{} IN ({})").format(
                        elem,
                        sql.SQL(", ").join(self._jsonb_bound(v) for v in plain),  # pyright: ignore[reportUnknownVariableType]
                    )
                )

            in_expr = (
                parts[0] if len(parts) == 1 else sql.SQL("({})").format(sql.SQL(" OR ").join(parts))
            )

            if op == "$in":
                return in_expr

            if not numeric:
                # A missing object field is SQL NULL here, and satisfies $nin as it does $neq.
                return sql.SQL("({} IS NULL OR NOT ({}))").format(elem, in_expr)

            # An element with no numeric value (a non-numeric string, an object) is in no
            # numeric list, so its NULL there holds $nin.
            return sql.SQL("({}) IS NOT TRUE").format(in_expr)

        if op in ("$like", "$ilike", "$regex"):
            text_map = {"$like": "LIKE", "$ilike": "ILIKE", "$regex": "~"}
            text_elem = sql.SQL("({} #>> '{{}}')").format(elem)

            return sql.SQL("{} {} {}").format(
                text_elem,
                sql.SQL(text_map[op]),  # pyright: ignore[reportArgumentType]
                self.binder.add(value),
            )

        raise exc.internal(f"Unsupported nested scalar element operator: {op!r}")

    # ....................... #

    def _render_jsonb_scalar_numeric(
        self,
        elem: sql.Composable,
        op: str,
        value: Any,
    ) -> sql.Composable:
        """Compare a ``jsonb`` scalar element to a ``Decimal`` operand numerically.

        Stored decimals are JSON *strings* (see the jsonb write default) while other
        writers may store JSON numbers; extracting the element text and casting
        ``::numeric`` compares both exactly, where a jsonb-space ``to_jsonb`` comparison
        would treat ``"10.5"`` and ``10.5`` as different JSON types and never match.
        """

        num_elem = self._jsonb_numeric_value(elem)
        caster = self.coercer.caster

        if op in ("$eq", "$neq"):
            sql_op = sql.SQL("=") if op == "$eq" else sql.SQL("IS DISTINCT FROM")

            return sql.SQL("{} {} {}").format(
                num_elem,
                sql_op,
                self.binder.add(caster.as_decimal(value)),
            )

        if op in ("$gt", "$gte", "$lt", "$lte"):
            ord_map = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}

            return sql.SQL("{} {} {}").format(
                num_elem,
                sql.SQL(ord_map[op]),  # pyright: ignore[reportArgumentType]
                self.binder.add(caster.as_decimal(value)),
            )

        raise exc.internal(f"Unsupported nested scalar element operator: {op!r}")

    # ....................... #

    @staticmethod
    def _jsonb_numeric_value(elem: sql.Composable) -> sql.Composable:
        """*elem*'s numeric value: a JSON number, or a string ``::numeric`` accepts; else NULL.

        Stored decimals are JSON strings, so both forms are read. Only text that matches
        :data:`_NUMERIC_TEXT_PATTERN` is cast, so a non-numeric string or an object is
        NULL, never a cast error.
        """

        return sql.SQL(
            "(CASE WHEN jsonb_typeof({e}) = 'number' THEN ({e})::numeric "
            "WHEN jsonb_typeof({e}) = 'string' AND length({e} #>> '{{}}') <= {max_length} "
            "AND ({e} #>> '{{}}') ~ {pattern} THEN ({e} #>> '{{}}')::numeric END)"
        ).format(
            e=elem,
            max_length=sql.Literal(_NUMERIC_TEXT_MAX_LENGTH),
            pattern=sql.Literal(_NUMERIC_TEXT_PATTERN),
        )

    # ....................... #

    def _jsonb_bound(self, value: Any) -> sql.Composable:
        """Bind *value* and wrap as ``jsonb`` for element-space comparison.

        A bare ``to_jsonb($1)`` fails — a parameter arrives as ``unknown`` and
        ``to_jsonb`` is polymorphic with nothing to infer from — so the placeholder
        is cast to a concrete type chosen from the Python value first.
        """

        # Decimal never lands here: stored decimals are JSON strings, so a ``to_jsonb``
        # comparison cannot represent them — those operands are routed through
        # :meth:`_render_jsonb_scalar_numeric`, or refused on an untyped object field.
        if value is None:
            # A ``$in`` / ``$nin`` member: the evaluator matches it against a stored null.
            return sql.SQL("'null'::jsonb")

        cast = _jsonb_operand_type(value)[0]
        # A date or UUID binds its own text: ``$1::text`` of a date follows DateStyle.
        bound = str(value) if cast == "text" and not isinstance(value, str) else value

        return sql.SQL("to_jsonb({}::{})").format(self.binder.add(bound), sql.SQL(cast))

    # ....................... #

    @staticmethod
    def _refuse_without_json_form(value: Any, path: str) -> None:
        """Refuse an operand with no single JSON form to compare a jsonb value with.

        A non-finite float has none, and a datetime's text depends on who wrote it
        (``Z`` or ``+00:00``, with or without microseconds), so neither can match the
        in-memory evaluator, which compares the typed values.
        """

        members = list(value) if isinstance(value, (list, tuple, set)) else [value]  # type: ignore[arg-type]  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

        if any(isinstance(v, float) and not math.isfinite(v) for v in members):  # pyright: ignore[reportUnknownVariableType]
            raise exc.precondition(
                f"Filter on {path!r}: a non-finite float has no JSON form to compare with a "
                "JSON element on the 'postgres' backend.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        if any(isinstance(v, datetime) for v in members):  # pyright: ignore[reportUnknownVariableType]
            raise exc.precondition(
                f"Filter on {path!r}: a datetime has no single JSON text to compare with a "
                "JSON array element on the 'postgres' backend; store it in a timestamptz "
                "column or array.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

    # ....................... #

    @staticmethod
    def _untyped_compares_as_jsonb(op: str, value: Any, path: str) -> bool:
        """Whether an element field with no resolved type compares in ``jsonb`` space.

        ``->>`` yields text, which has no operator for a boolean or a number, so those
        operands compare against their JSON encoding instead, and a ``None`` list member
        against JSON null; an all-string operand keeps its text comparison. An operand
        JSON has no type for (a ``Decimal``, a date or time, a UUID) is refused: such a
        value is stored as a JSON string, which the in-memory evaluator compares as the
        typed value, so no SQL comparison agrees. So is a non-finite float.

        :param op: The field operator.
        :param value: The operand, a list for ``$in`` / ``$nin``.
        :param path: The field's full dotted path, named in a refusal.
        :returns: ``True`` to compare in ``jsonb`` space, ``False`` to keep the text path.
        :raises CoreException: ``query_feature_unsupported`` for an operand JSON cannot hold.
        """

        if op not in _JSONB_COMPARE_OPS:
            return False

        members = list(value) if isinstance(value, (list, tuple, set)) else [value]  # type: ignore[arg-type]  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

        if all(isinstance(v, str) for v in members):  # pyright: ignore[reportUnknownVariableType]
            return False

        odd = [v for v in members if v is not None and not isinstance(v, (str, int, float))]  # pyright: ignore[reportUnknownVariableType]

        if odd:
            raise exc.precondition(
                f"Filter on {path!r}: the read model gives this element field no type, so a "
                f"{type(odd[0]).__name__} operand cannot be compared with it on the 'postgres' "
                "backend. Annotate the array's element type on the read model (for example "
                f"list[ItemModel]) or set nested_field_hints[{path!r}].",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        return True

    # ....................... #

    @staticmethod
    def _python_ann_to_postgres_type(ann: Any) -> PostgresType | None:
        if ann is None:
            return None

        if ann is UUID:
            return PostgresType(base="uuid", is_array=False, not_null=False)

        if ann is bool:
            return PostgresType(base="bool", is_array=False, not_null=False)

        if ann is int:
            return PostgresType(base="int8", is_array=False, not_null=False)

        if ann is float:
            return PostgresType(base="float8", is_array=False, not_null=False)

        if ann is Decimal:
            return PostgresType(base="numeric", is_array=False, not_null=False)

        if ann is datetime:
            return PostgresType(base="timestamptz", is_array=False, not_null=False)

        if ann is date:
            return PostgresType(base="date", is_array=False, not_null=False)

        if ann is str:
            return PostgresType(base="text", is_array=False, not_null=False)

        pg = python_type_to_postgres_scalar(ann)

        if pg is not None:
            # Covers what the identity checks above cannot: subclasses and unions —
            # notably ``Decimal`` and mixed numeric unions like ``Decimal | int``,
            # which would otherwise fall back to lexical text comparison.
            return PostgresType(base=pg.base, is_array=False, not_null=False)

        return None
