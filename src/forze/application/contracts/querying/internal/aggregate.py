"""Aggregate expression parsing and validation."""

import re
from collections.abc import Mapping
from typing import Any, Literal, cast, get_args

import attrs

from forze.base.exceptions import exc

from ..expressions import AggregateFunction, AggregatesExpression, QueryFilterExpression
from .nodes import QueryExpr
from .parse import QueryFilterExpressionParser
from .time_bucket import ResolvedTimeBucketTimezone, parse_aggregate_timezone

# ----------------------- #

_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FUNCTIONS: frozenset[str] = frozenset(get_args(AggregateFunction))
_UNITS: frozenset[str] = frozenset(("hour", "day", "week", "month"))
_GROUP_OPS: frozenset[str] = frozenset(("$trunc",))

# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class GroupRef:
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

    expr: GroupRef | GroupTrunc
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


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class ParsedAggregates:
    """Validated aggregate expression."""

    groups: tuple[GroupKey, ...]
    """Group dimensions in wire declaration order."""

    computed_fields: tuple[AggregateComputedField, ...]
    """Computed aggregate fields."""

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
            raise exc.internal(f"Invalid aggregate $computed: {raw_computed_obj!r}")

        raw_computed = cast(Mapping[Any, Any], raw_computed_obj)  # type: ignore[redundant-cast]

        groups_obj: object = expr.get("$groups", {})
        groups = cls._group_keys(groups_obj)
        computed_fields = tuple(
            cls._computed(alias, spec) for alias, spec in raw_computed.items()
        )

        if not computed_fields:
            raise exc.internal("Aggregates expression requires $computed")

        aliases = [group.alias for group in groups] + [
            field.alias for field in computed_fields
        ]
        duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})

        if duplicates:
            raise exc.internal(f"Duplicate aggregate aliases: {duplicates}")

        return ParsedAggregates(
            groups=groups,
            computed_fields=computed_fields,
        )

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
                    expr=GroupRef(field=cls._field(name)),
                )
                for name in seq
            )

        raise exc.internal(f"Invalid aggregate $groups: {raw!r}")

    # ....................... #

    @classmethod
    def _parse_group_value(cls, raw: object) -> GroupRef | GroupTrunc:
        if isinstance(raw, str):
            return GroupRef(field=cls._field(raw))

        if not isinstance(raw, Mapping):
            raise exc.internal(f"Invalid $groups map value: {raw!r}")

        spec = cast(Mapping[Any, Any], raw)  # type: ignore[redundant-cast]

        if len(spec) != 1:
            raise exc.internal(
                f"$groups map value must declare exactly one operator, got {list(spec)!r}",
            )

        op, inner = next(iter(spec.items()))

        if op not in _GROUP_OPS:
            raise exc.internal(f"Invalid $groups operator: {op!r}")

        if op == "$trunc":
            return cls._parse_trunc(inner)

        raise exc.internal(f"Invalid $groups operator: {op!r}")

    # ....................... #

    @classmethod
    def _parse_trunc(cls, raw: object) -> GroupTrunc:
        if not isinstance(raw, Mapping):
            raise exc.internal(f"Invalid $trunc spec: {raw!r}")

        spec = cast(Mapping[Any, Any], raw)  # type: ignore[redundant-cast]
        allowed = {"field", "unit", "timezone"}
        extra = set(spec) - allowed

        if extra:
            raise exc.internal(f"Invalid $trunc keys: {sorted(extra)}")

        field = spec.get("field")
        unit = spec.get("unit")

        if not isinstance(field, str) or not field.strip():
            raise exc.internal("$trunc.field must be a non-empty string")

        if not isinstance(unit, str) or unit not in _UNITS:
            raise exc.internal(
                f"$trunc.unit must be one of {sorted(_UNITS)}",
            )

        tz_raw = spec.get("timezone")
        if tz_raw is not None and not isinstance(tz_raw, str):
            raise exc.internal(f"$trunc.timezone must be a string, got {tz_raw!r}")

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
            raise exc.internal(f"Invalid aggregate alias: {alias!r}")

        return alias

    # ....................... #

    @staticmethod
    def _field(field: object) -> str:
        if not isinstance(field, str) or not field.strip():
            raise exc.internal(f"Invalid aggregate field path: {field!r}")

        return field

    # ....................... #

    @classmethod
    def _computed(cls, alias: str, spec: object) -> AggregateComputedField:
        alias = cls._alias(alias)

        if not isinstance(spec, Mapping):
            raise exc.internal(f"Invalid aggregate computed field spec: {spec!r}")

        raw_spec: Mapping[Any, Any] = spec  # type: ignore[assignment]

        if len(raw_spec) != 1:
            raise exc.internal(
                f"Aggregate computed field {alias!r} must declare exactly one function",
            )

        function, field = next(iter(raw_spec.items()))

        if function not in _FUNCTIONS:
            raise exc.internal(f"Invalid aggregate function: {function!r}")

        field_path, filter_expr, parsed_filter = cls._function_arg(function, field)

        if function == "$count":
            if field_path is not None:
                raise exc.internal("$count aggregate expects no field")

            return AggregateComputedField(
                alias=alias,
                function=function,
                field=None,
                filter=filter_expr,
                parsed_filter=parsed_filter,
            )  # type: ignore[arg-type]

        return AggregateComputedField(
            alias=alias,
            function=function,  # type: ignore[arg-type]
            field=cls._field(field_path),
            filter=filter_expr,
            parsed_filter=parsed_filter,
        )

    # ....................... #

    @classmethod
    def _function_arg(
        cls,
        function: object,
        raw: object,
    ) -> tuple[str | None, QueryFilterExpression | None, QueryExpr | None]:  # type: ignore[valid-type]
        if not isinstance(raw, Mapping):
            return raw, None, None  # type: ignore[return-value]

        raw_spec: Mapping[Any, Any] = raw  # type: ignore[assignment]
        field = raw_spec.get("field")
        filter_expr = raw_spec.get("filter")
        allowed = {"field", "filter"}
        extra = sorted(str(key) for key in set(raw_spec) - allowed)

        if extra:
            raise exc.internal(f"Invalid aggregate function keys: {extra}")

        if function == "$count" and field is not None:
            raise exc.internal("$count aggregate expects no field")

        if function != "$count" and field is None:
            raise exc.internal(f"{function} aggregate requires a field")

        parsed_filter: QueryExpr | None = None
        if filter_expr is not None:
            parsed_filter = QueryFilterExpressionParser.parse(filter_expr)  # type: ignore[arg-type]

        return (
            cls._field(field) if field is not None else None,
            filter_expr,  # type: ignore[return-value]
            parsed_filter,
        )
