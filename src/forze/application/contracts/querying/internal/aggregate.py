"""Aggregate expression parsing and validation."""

import re
from collections.abc import Mapping
from typing import Any, Literal, cast, get_args

import attrs

from forze.base.exceptions import exc

from ..expressions import AggregateFunction, AggregatesExpression, QueryFilterExpression
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
from .time_bucket import ResolvedTimeBucketTimezone, parse_aggregate_timezone

# ----------------------- #

_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FUNCTIONS: frozenset[str] = frozenset(get_args(AggregateFunction))
_UNITS: frozenset[str] = frozenset(("hour", "day", "week", "month"))
_GROUP_OPS: frozenset[str] = frozenset(("$trunc",))

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


class AggregatesExpressionParser:
    """Parser for :class:`~forze.application.contracts.querying.AggregatesExpression`."""

    @classmethod
    def parse(cls, expr: AggregatesExpression) -> ParsedAggregates:
        """Validate and parse an aggregate expression."""

        raw_computed_obj: object = expr.get("$computed", {})

        if not isinstance(raw_computed_obj, Mapping):
            raise exc.precondition(f"Invalid aggregate $computed: {raw_computed_obj!r}")

        raw_computed = cast(Mapping[Any, Any], raw_computed_obj)  # type: ignore[redundant-cast]

        groups_obj: object = expr.get("$groups", {})
        groups = cls._group_keys(groups_obj)
        computed_fields = tuple(cls._computed(alias, spec) for alias, spec in raw_computed.items())

        if not computed_fields:
            raise exc.precondition("Aggregates expression requires $computed")

        aliases = [group.alias for group in groups] + [field.alias for field in computed_fields]
        duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})

        if duplicates:
            raise exc.precondition(f"Duplicate aggregate aliases: {duplicates}")

        having = cls._having(expr.get("$having"), frozenset(aliases))

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
    ) -> QueryExpr | None:
        """Parse and validate the ``$having`` filter over the output aliases."""

        if not raw:
            return None

        expr = QueryFilterExpressionParser.parse(raw)
        referenced = _having_field_roots(expr)
        unknown = sorted(referenced - aliases)

        if unknown:
            raise exc.precondition(
                f"$having may only reference aggregate output aliases "
                f"({sorted(aliases)}); unknown: {unknown}.",
            )

        return expr

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

        raise exc.precondition(f"Invalid aggregate $groups: {raw!r}")

    # ....................... #

    @classmethod
    def _parse_group_value(cls, raw: object) -> GroupField | GroupTrunc:
        if isinstance(raw, str):
            return GroupField(field=cls._field(raw))

        if not isinstance(raw, Mapping):
            raise exc.precondition(f"Invalid $groups map value: {raw!r}")

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
            raise exc.precondition(f"Invalid $trunc spec: {raw!r}")

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
            raise exc.precondition(f"$trunc.timezone must be a string, got {tz_raw!r}")

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
            raise exc.precondition(f"Invalid aggregate alias: {alias!r}")

        return alias

    # ....................... #

    @staticmethod
    def _field(field: object) -> str:
        if not isinstance(field, str) or not field.strip():
            raise exc.precondition(f"Invalid aggregate field path: {field!r}")

        return field

    # ....................... #

    @classmethod
    def _computed(cls, alias: str, spec: object) -> AggregateComputedField:
        alias = cls._alias(alias)

        if not isinstance(spec, Mapping):
            raise exc.precondition(f"Invalid aggregate computed field spec: {spec!r}")

        raw_spec: Mapping[Any, Any] = spec  # type: ignore[assignment]

        if len(raw_spec) != 1:
            raise exc.precondition(
                f"Aggregate computed field {alias!r} must declare exactly one function",
            )

        function, field = next(iter(raw_spec.items()))

        if function not in _FUNCTIONS:
            raise exc.precondition(f"Invalid aggregate function: {function!r}")

        field_path, filter_expr, parsed_filter, p = cls._function_arg(function, field)

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
            parsed_filter = QueryFilterExpressionParser.parse(filter_expr)  # type: ignore[arg-type]

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
            raise exc.precondition(f"$percentile 'p' must be a number in [0, 1], got {p!r}")

        return float(p)
