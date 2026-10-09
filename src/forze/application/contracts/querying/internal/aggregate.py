"""Aggregate expression parsing and validation."""

import re
from collections.abc import Mapping
from datetime import UTC, datetime, tzinfo
from decimal import Decimal
from typing import Any, Literal, cast, get_args

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc

from ..capabilities import UNSUPPORTED_QUERY_FEATURE_CODE
from ..expressions import (
    AggregateFunction,
    AggregatesExpression,
    QueryFilterExpression,
    QuerySortExpression,
    QuerySortValue,
)
from ..field_types import (
    _resolve_annotation,  # pyright: ignore[reportPrivateUsage]
    classify_field_type,
    coerce_query_ord_operands,
)
from ..sort_resolution.value import (
    _tiebreaker_direction,  # pyright: ignore[reportPrivateUsage]
    parse_sort_value,
)
from .cast import QueryValueCaster
from .nodes import (
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)
from .parse import QueryFilterExpressionParser
from .time_bucket import (
    ResolvedTimeBucketTimezone,
    parse_aggregate_timezone,
    tzinfo_from_resolved,
)

# ----------------------- #

_DEFAULT_FILTER_PARSER = QueryFilterExpressionParser()

_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FUNCTIONS: frozenset[str] = frozenset(get_args(AggregateFunction))
_UNITS: frozenset[str] = frozenset(("hour", "day", "week", "month"))
_VALUE_PRESERVING: frozenset[str] = frozenset(("$min", "$max"))
"""Measures whose output takes its field's type; every other measure is a number."""
_NON_NUMERIC_OPS: frozenset[str] = frozenset(
    (
        "$like",
        "$ilike",
        "$regex",
        "$superset",
        "$subset",
        "$disjoint",
        "$overlaps",
        "$empty",
        "$descendant_of",
        "$ancestor_of",
    )
)
"""Operators no number supports: text patterns, set relations, emptiness, hierarchy."""
_GROUP_OPS: frozenset[str] = frozenset(("$trunc",))
_INSTANT_OPS: frozenset[str] = frozenset(
    ("$eq", "$neq", "$gt", "$gte", "$lt", "$lte", "$in", "$nin")
)
"""Operators whose operand a time bucket compares as an instant."""

# ....................... #


def _non_numeric_uses(expr: QueryExpr) -> frozenset[str]:
    """Aliases a ``$having`` AST applies a non-numeric operator or an element quantifier to."""

    used: set[str] = set()

    def _walk(node: QueryExpr) -> None:
        match node:
            case QueryAnd(items) | QueryOr(items):
                for item in items:
                    _walk(item)

            case QueryNot(item):
                _walk(item)

            case QueryField(name, op, _) if op in _NON_NUMERIC_OPS:
                used.add(name.split(".", 1)[0])

            case QueryElem(path, _, _):
                used.add(path.split(".", 1)[0])

            case _:
                pass

    _walk(expr)

    return frozenset(used)


# ....................... #


def _bucket_instants(
    expr: QueryExpr,
    zones: Mapping[str, ResolvedTimeBucketTimezone],
) -> QueryExpr:
    """*expr* with each operand on a time bucket made the instant it names.

    A bucket is the instant it starts at: an aware operand is that instant, and a naive one is
    wall time in the bucket's zone, as the bucket itself was cut. Every store then compares
    instants, whatever its session zone and however it represents the bucket.
    """

    def _instant(value: Any, tz: tzinfo) -> Any:
        if value is None:
            return None

        dt = QueryValueCaster.parse_datetime(value)

        if dt.utcoffset() is not None:
            return dt

        # A wall time the zone repeats or skips names two instants; take the later one, as
        # Postgres reads a bucket back into its zone. Compared in UTC: two datetimes sharing a
        # tzinfo compare by wall clock and ignore ``fold``.
        return max(dt.replace(tzinfo=tz, fold=fold).astimezone(UTC) for fold in (0, 1))

    def _walk(node: QueryExpr) -> QueryExpr:
        match node:
            case QueryAnd(items):
                return QueryAnd(tuple(_walk(item) for item in items))

            case QueryOr(items):
                return QueryOr(tuple(_walk(item) for item in items))

            case QueryNot(item):
                return QueryNot(_walk(item))

            case QueryField(name, op, value) if name in zones and op in _INSTANT_OPS:
                tz = tzinfo_from_resolved(zones[name])

                if isinstance(value, (list, tuple)):
                    instants = [_instant(v, tz) for v in value]  # pyright: ignore[reportUnknownVariableType]
                    coerced: Any = tuple(instants) if isinstance(value, tuple) else instants

                else:
                    coerced = _instant(value, tz)

                return attrs.evolve(node, value=coerced)

            case _:
                return node

    return _walk(expr) if zones else expr


# ....................... #


def _having_field_roots(expr: QueryExpr) -> frozenset[str]:
    """Top-level field names a ``$having`` AST references (for alias validation)."""

    roots: set[str] = set()

    def _walk(node: QueryExpr) -> None:
        match node:
            case QueryAnd(items) | QueryOr(items):
                for item in items:
                    _walk(item)

            case QueryNot(item):
                _walk(item)

            case QueryField(name, _, _):
                roots.add(name.split(".", 1)[0])

            case QueryCompare(left, _, right):
                roots.add(left.split(".", 1)[0])
                roots.add(right.split(".", 1)[0])

            case QueryElem(path, _, _):
                roots.add(path.split(".", 1)[0])

            case _:
                pass

    _walk(expr)

    return frozenset(roots)


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class GroupField:
    """Group by a document field path."""

    field: str
    """Source field path."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class GroupTrunc:
    """Calendar bucket dimension derived from a timestamp field."""

    field: str
    """Source field path."""

    unit: Literal["hour", "day", "week", "month"]
    """Bucket width."""

    timezone: ResolvedTimeBucketTimezone
    """Resolved IANA or fixed-offset timezone."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class GroupKey:
    """One aggregate group dimension with its output alias."""

    alias: str
    """Output field alias."""

    expr: GroupField | GroupTrunc
    """Group dimension expression."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class AggregateComputedField:
    """Computed aggregate selected into an aggregate result row."""

    alias: str
    """Output field alias."""

    function: AggregateFunction
    """Aggregate function name."""

    field: str | None
    """Source field path, or ``None`` for row-count aggregates."""

    filter: QueryFilterExpression | None = None  # type: ignore[valid-type]
    """Optional row filter applied only to this aggregate."""

    parsed_filter: QueryExpr | None = None
    """Parsed AST for :attr:`filter`, set when the aggregate expression is validated."""

    p: float | None = None
    """Quantile in ``[0, 1]`` for ``$percentile``; ``None`` for every other function."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class ParsedAggregates:
    """Validated aggregate expression."""

    groups: tuple[GroupKey, ...]
    """Group dimensions in wire declaration order."""

    computed_fields: tuple[AggregateComputedField, ...]
    """Computed aggregate fields."""

    having: QueryExpr | None = None
    """Optional post-group filter (``$having``) over the output aliases, or ``None``."""

    # ....................... #

    @property
    def aliases(self) -> frozenset[str]:
        """All output aliases declared by the expression."""

        keys = [group.alias for group in self.groups] + [
            field.alias for field in self.computed_fields
        ]

        return frozenset(keys)


# ....................... #


_AGGREGATES_KEYS = frozenset({"$groups", "$computed", "$having"})


class AggregatesExpressionParser:
    """Parser for :class:`~forze.application.contracts.querying.AggregatesExpression`."""

    @classmethod
    def parse(
        cls,
        expr: AggregatesExpression,
        *,
        filter_parser: QueryFilterExpressionParser | None = None,
        model_type: type[BaseModel] | None = None,
    ) -> ParsedAggregates:
        """Validate and parse an aggregate expression.

        :param filter_parser: Parses each metric ``filter`` and ``$having``; the default limits
            when omitted. Pass the spec's, so they get the bounds its other filters get.
        :param model_type: The read model the outputs are measured over. Given it, ``$having``
            knows the type of a field group and a ``$min`` / ``$max`` as well: an operator no
            number or time supports is refused on one of those too, and a string bound in it or
            in a metric ``filter`` is cast to the field's type as a filter casts it
            (:func:`coerce_query_ord_operands`).
        """

        parser = filter_parser or _DEFAULT_FILTER_PARSER

        # Dropped, a mistyped ``$having`` would return every group.
        if unknown := set(expr) - _AGGREGATES_KEYS:
            raise exc.precondition(
                f"Unknown aggregates key {', '.join(sorted(map(str, unknown)))}: an "
                "aggregates expression takes $groups, $computed and $having",
            )

        raw_computed_obj: object = expr.get("$computed", {})

        if not isinstance(raw_computed_obj, Mapping):
            raise exc.precondition(
                "Invalid aggregate $computed: expected an object mapping aliases to metrics, "
                f"got {type(raw_computed_obj).__name__}",
            )

        raw_computed = cast(Mapping[Any, Any], raw_computed_obj)  # type: ignore[redundant-cast]

        groups_obj: object = expr.get("$groups", {})
        groups = cls._group_keys(groups_obj)
        computed_fields = tuple(
            cls._computed(alias, spec, parser) for alias, spec in raw_computed.items()
        )

        if not computed_fields:
            raise exc.precondition("Aggregates expression requires $computed")

        if model_type is not None:
            # A metric filter is a filter over the documents, so it casts as one does.
            computed_fields = tuple(
                attrs.evolve(
                    computed,
                    parsed_filter=coerce_query_ord_operands(computed.parsed_filter, model_type),
                )
                if computed.parsed_filter is not None
                else computed
                for computed in computed_fields
            )

        aliases = [group.alias for group in groups] + [field.alias for field in computed_fields]
        duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})

        if duplicates:
            raise exc.precondition(f"Duplicate aggregate aliases: {duplicates}")

        kinds = cls._output_kinds(groups, computed_fields, model_type)
        scalar = frozenset(
            alias
            for alias, kind in kinds.items()
            if classify_field_type(kind) in ("number", "temporal")
        )
        having = cls._having(expr.get("$having"), frozenset(aliases), parser, scalar=scalar)

        if having is not None:
            zones = {
                group.alias: group.expr.timezone
                for group in groups
                if isinstance(group.expr, GroupTrunc)
            }
            having = _bucket_instants(having, zones)

        if having is not None and model_type is not None:
            having = coerce_query_ord_operands(having, model_type, field_type_hints=kinds)

        return ParsedAggregates(
            groups=groups,
            computed_fields=computed_fields,
            having=having,
        )

    # ....................... #

    @classmethod
    def _having(
        cls,
        raw: QueryFilterExpression | None,
        aliases: frozenset[str],
        parser: QueryFilterExpressionParser,
        *,
        scalar: frozenset[str] = frozenset(),
    ) -> QueryExpr | None:
        """Parse and validate the ``$having`` filter over the output aliases.

        An operator no number or time supports is refused on a *scalar* output (a number, a
        time bucket, a ``$min`` / ``$max`` of a date) here, on every backend, rather than
        stringified by one and failed by another's server.
        """

        if not raw:
            return None

        expr = parser.parse_filter(raw)
        referenced = _having_field_roots(expr)
        unknown = sorted(referenced - aliases)

        if unknown:
            raise exc.precondition(
                f"$having may only reference aggregate output aliases "
                f"({sorted(aliases)}); unknown: {unknown}.",
            )

        if misused := sorted(_non_numeric_uses(expr) & scalar):
            raise exc.precondition(
                f"$having applies an operator no number or time supports to the output(s) "
                f"{misused}.",
                code=UNSUPPORTED_QUERY_FEATURE_CODE,
            )

        return expr

    # ....................... #

    @staticmethod
    def _output_kinds(
        groups: tuple[GroupKey, ...],
        computed_fields: tuple[AggregateComputedField, ...],
        model_type: type[BaseModel] | None,
    ) -> dict[str, Any]:
        """The Python type of each output alias: a bucket is a ``datetime``, a measure other
        than ``$min`` / ``$max`` a number, and the rest take their field's annotation from
        *model_type* (``Any`` without one, which nothing is checked or cast against)."""

        def _field(field: str) -> Any:
            if model_type is None:
                return Any

            return _resolve_annotation(model_type, field.split("."), {})

        kinds: dict[str, Any] = {}

        for group in groups:
            expr = group.expr
            kinds[group.alias] = _field(expr.field) if isinstance(expr, GroupField) else datetime

        for computed in computed_fields:
            if computed.function in _VALUE_PRESERVING and computed.field is not None:
                kinds[computed.alias] = _field(computed.field)

            else:
                kinds[computed.alias] = Decimal

        return kinds

    # ....................... #

    @classmethod
    def _group_keys(cls, raw: object) -> tuple[GroupKey, ...]:
        if isinstance(raw, Mapping):
            mapping = cast(Mapping[Any, Any], raw)  # type: ignore[redundant-cast]

            return tuple(
                GroupKey(
                    alias=cls._alias(alias),
                    expr=cls._parse_group_value(raw_value),
                )
                for alias, raw_value in mapping.items()
            )

        if isinstance(raw, (list, tuple)):
            seq = cast(list[Any] | tuple[Any, ...], raw)  # type: ignore[redundant-cast]

            return tuple(
                GroupKey(
                    alias=cls._alias(name),
                    expr=GroupField(field=cls._field(name)),
                )
                for name in seq
            )

        raise exc.precondition(
            "Invalid aggregate $groups: expected an object mapping aliases to dimensions or a "
            f"list of field paths, got {type(raw).__name__}",
        )

    # ....................... #

    @classmethod
    def _parse_group_value(cls, raw: object) -> GroupField | GroupTrunc:
        if isinstance(raw, str):
            return GroupField(field=cls._field(raw))

        if not isinstance(raw, Mapping):
            raise exc.precondition(
                "Invalid $groups map value: expected a field path or an operator map, got "
                f"{type(raw).__name__}",
            )

        spec = cast(Mapping[Any, Any], raw)  # type: ignore[redundant-cast]

        if len(spec) != 1:
            raise exc.precondition(
                f"$groups map value must declare exactly one operator, got {list(spec)!r}",
            )

        op, inner = next(iter(spec.items()))

        if op not in _GROUP_OPS:
            raise exc.precondition(f"Invalid $groups operator: {op!r}")

        if op == "$trunc":
            return cls._parse_trunc(inner)

        # Unreachable: ``op`` is already validated against ``_GROUP_OPS`` above, so a
        # caller can never reach this. A defensive guard over already-validated data —
        # internal (a bug) if it ever fires, not a caller-facing precondition.
        raise exc.internal(f"Invalid $groups operator: {op!r}")

    # ....................... #

    @classmethod
    def _parse_trunc(cls, raw: object) -> GroupTrunc:
        if not isinstance(raw, Mapping):
            raise exc.precondition(
                f"Invalid $trunc spec: expected an object, got {type(raw).__name__}"
            )

        spec = cast(Mapping[Any, Any], raw)  # type: ignore[redundant-cast]
        allowed = {"field", "unit", "timezone"}
        extra = set(spec) - allowed

        if extra:
            raise exc.precondition(f"Invalid $trunc keys: {sorted(extra)}")

        field = spec.get("field")
        unit = spec.get("unit")

        if not isinstance(field, str) or not field.strip():
            raise exc.precondition("$trunc.field must be a non-empty string")

        if not isinstance(unit, str) or unit not in _UNITS:
            raise exc.precondition(
                f"$trunc.unit must be one of {sorted(_UNITS)}",
            )

        tz_raw = spec.get("timezone")
        if tz_raw is not None and not isinstance(tz_raw, str):
            raise exc.precondition(f"$trunc.timezone must be a string, got {type(tz_raw).__name__}")

        resolved = parse_aggregate_timezone(tz_raw)

        return GroupTrunc(
            field=cls._field(field),
            unit=cast(Literal["hour", "day", "week", "month"], unit),
            timezone=resolved,
        )

    # ....................... #

    @staticmethod
    def _alias(alias: object) -> str:
        if not isinstance(alias, str) or not _ALIAS_RE.fullmatch(alias):
            raise exc.precondition(
                f"Invalid aggregate alias {alias!r}"
                if isinstance(alias, str)
                else "Invalid aggregate alias"
            )

        return alias

    # ....................... #

    @staticmethod
    def _field(field: object) -> str:
        if not isinstance(field, str) or not field.strip():
            raise exc.precondition(
                f"Invalid aggregate field path: expected a non-empty string, got {type(field).__name__}",
            )

        return field

    # ....................... #

    @classmethod
    def _computed(
        cls, alias: str, spec: object, parser: QueryFilterExpressionParser
    ) -> AggregateComputedField:
        alias = cls._alias(alias)

        if not isinstance(spec, Mapping):
            raise exc.precondition(
                f"Invalid aggregate computed field spec for {alias}: expected an object, got "
                f"{type(spec).__name__}",
            )

        raw_spec: Mapping[Any, Any] = spec  # type: ignore[assignment]

        if len(raw_spec) != 1:
            raise exc.precondition(
                f"Aggregate computed field {alias!r} must declare exactly one function",
            )

        function, field = next(iter(raw_spec.items()))

        if function not in _FUNCTIONS:
            raise exc.precondition(f"Invalid aggregate function: {function!r}")

        field_path, filter_expr, parsed_filter, p = cls._function_arg(function, field, parser)

        return AggregateComputedField(
            alias=alias,
            function=function,  # type: ignore[arg-type]
            field=field_path,
            filter=filter_expr,
            parsed_filter=parsed_filter,
            p=p,
        )

    # ....................... #

    @classmethod
    def _function_arg(
        cls,
        function: object,
        raw: object,
        parser: QueryFilterExpressionParser,
    ) -> tuple[str | None, QueryFilterExpression | None, QueryExpr | None, float | None]:  # type: ignore[valid-type]
        fieldless = function == "$count"  # only plain count takes no field
        needs_p = function == "$percentile"

        if not isinstance(raw, Mapping):
            if needs_p:
                raise exc.precondition(
                    "$percentile requires the {field, p} form; no scalar shorthand",
                )

            field_path = cls._field(raw) if raw is not None else None  # type: ignore[arg-type]
            cls._check_field_presence(function, field_path, fieldless=fieldless)
            return field_path, None, None, None

        raw_spec: Mapping[Any, Any] = raw  # type: ignore[assignment]
        field = raw_spec.get("field")
        filter_expr = raw_spec.get("filter")
        p = raw_spec.get("p")
        allowed: set[str] = {"field", "filter"} | ({"p"} if needs_p else set())
        extra = sorted(str(key) for key in set(raw_spec) - allowed)

        if extra:
            raise exc.precondition(f"Invalid aggregate function keys: {extra}")

        cls._check_field_presence(function, field, fieldless=fieldless)

        resolved_p = cls._quantile(p) if needs_p else None

        parsed_filter: QueryExpr | None = None
        if filter_expr is not None:
            parsed_filter = parser.parse_filter(filter_expr)  # type: ignore[arg-type]

        return (
            cls._field(field) if field is not None else None,
            filter_expr,  # type: ignore[return-value]
            parsed_filter,
            resolved_p,
        )

    @classmethod
    def _check_field_presence(
        cls,
        function: object,
        field: object,
        *,
        fieldless: bool,
    ) -> None:
        if fieldless and field is not None:
            raise exc.precondition("$count aggregate expects no field")

        if not fieldless and field is None:
            raise exc.precondition(f"{function} aggregate requires a field")

    @classmethod
    def _quantile(cls, p: object) -> float:
        if p is None:
            raise exc.precondition("$percentile requires a 'p' quantile")

        if isinstance(p, bool) or not isinstance(p, (int, float)) or not 0 <= p <= 1:
            raise exc.precondition("$percentile 'p' must be a number in [0, 1]")

        return float(p)


# ....................... #


def with_group_tiebreakers(
    aggregates: AggregatesExpression,
    sorts: QuerySortExpression | None,
) -> QuerySortExpression | None:
    """*sorts* with the aggregate's group keys appended, the order a batched read needs.

    Each output row is one group, so its keys are unique to it and break every tie the
    caller's sort leaves: read in batches, no group repeats or goes missing. They take the
    sort's direction when every key shares one, else ``asc``. Without groups there is one row,
    and *sorts* is returned as given.
    """

    groups = AggregatesExpressionParser._group_keys(  # pyright: ignore[reportPrivateUsage]
        aggregates.get("$groups", {})
    )

    if not groups:
        return sorts

    out: dict[str, QuerySortValue] = dict(sorts or {})
    direction = _tiebreaker_direction(
        [parse_sort_value(value, field=field)[0] for field, value in out.items()]
    )
    tie: Literal["asc", "desc"] = "desc" if direction == "desc" else "asc"

    for group in groups:
        out.setdefault(group.alias, tie)

    return out
