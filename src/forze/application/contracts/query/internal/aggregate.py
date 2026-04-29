"""Aggregate expression parsing and validation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, get_args

import attrs

from ..expressions import AggregateFunction, AggregatesExpression, QueryFilterExpression
from .parse import QueryFilterExpressionParser

# ----------------------- #

_ALIAS_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FUNCTIONS: frozenset[str] = frozenset(get_args(AggregateFunction))


@attrs.define(slots=True, frozen=True, match_args=True)
class AggregateField:
    """Group key selected into an aggregate result row."""

    alias: str
    """Output field alias."""

    field: str
    """Source field path."""


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


@attrs.define(slots=True, frozen=True, match_args=True)
class ParsedAggregates:
    """Validated aggregate expression."""

    fields: tuple[AggregateField, ...]
    """Group key fields."""

    computed_fields: tuple[AggregateComputedField, ...]
    """Computed aggregate fields."""

    @property
    def aliases(self) -> frozenset[str]:
        """All output aliases declared by the expression."""

        return frozenset(
            [field.alias for field in self.fields]
            + [field.alias for field in self.computed_fields],
        )


class AggregatesExpressionParser:
    """Parser for :class:`~forze.application.contracts.query.AggregatesExpression`."""

    @classmethod
    def parse(cls, expr: AggregatesExpression) -> ParsedAggregates:
        """Validate and parse an aggregate expression."""

        raw_fields = expr.get("fields", {})
        raw_computed_fields = expr.get("computed_fields", {})

        fields = tuple(
            AggregateField(alias=cls._alias(alias), field=cls._field(field))
            for alias, field in raw_fields.items()
        )
        computed_fields = tuple(
            cls._computed(alias, spec) for alias, spec in raw_computed_fields.items()
        )

        if not computed_fields:
            raise ValueError("Aggregates expression requires computed_fields")

        aliases = [field.alias for field in fields] + [
            field.alias for field in computed_fields
        ]
        duplicates = sorted({alias for alias in aliases if aliases.count(alias) > 1})

        if duplicates:
            raise ValueError(f"Duplicate aggregate aliases: {duplicates}")

        return ParsedAggregates(fields=fields, computed_fields=computed_fields)

    # ....................... #

    @staticmethod
    def _alias(alias: object) -> str:
        if not isinstance(alias, str) or not _ALIAS_RE.fullmatch(alias):
            raise ValueError(f"Invalid aggregate alias: {alias!r}")
        return alias

    # ....................... #

    @staticmethod
    def _field(field: object) -> str:
        if not isinstance(field, str) or not field.strip():
            raise ValueError(f"Invalid aggregate field path: {field!r}")
        return field

    # ....................... #

    @classmethod
    def _computed(cls, alias: str, spec: object) -> AggregateComputedField:
        alias = cls._alias(alias)

        if not isinstance(spec, Mapping):
            raise ValueError(f"Invalid aggregate computed field spec: {spec!r}")

        raw_spec: Mapping[Any, Any] = spec  # type: ignore[assignment]

        if len(raw_spec) != 1:
            raise ValueError(
                f"Aggregate computed field {alias!r} must declare exactly one function",
            )

        function, field = next(iter(raw_spec.items()))

        if function not in _FUNCTIONS:
            raise ValueError(f"Invalid aggregate function: {function!r}")

        field_path, filter_expr = cls._function_arg(function, field)

        if function == "$count":
            if field_path is not None:
                raise ValueError("$count aggregate expects no field")
            return AggregateComputedField(
                alias=alias,
                function=function,
                field=None,
                filter=filter_expr,
            )  # type: ignore[arg-type]

        return AggregateComputedField(
            alias=alias,
            function=function,  # type: ignore[arg-type]
            field=cls._field(field_path),
            filter=filter_expr,
        )

    # ....................... #

    @classmethod
    def _function_arg(
        cls,
        function: object,
        raw: object,
    ) -> tuple[str | None, QueryFilterExpression | None]:  # type: ignore[valid-type]
        if not isinstance(raw, Mapping):
            return raw, None  # type: ignore[return-value]

        raw_spec: Mapping[Any, Any] = raw  # type: ignore[assignment]
        field = raw_spec.get("field")
        filter_expr = raw_spec.get("filter")
        allowed = {"field", "filter"}
        extra = sorted(str(key) for key in set(raw_spec) - allowed)

        if extra:
            raise ValueError(f"Invalid aggregate function keys: {extra}")

        if function == "$count" and field is not None:
            raise ValueError("$count aggregate expects no field")

        if function != "$count" and field is None:
            raise ValueError(f"{function} aggregate requires a field")

        if filter_expr is not None:
            QueryFilterExpressionParser.parse(filter_expr)  # type: ignore[arg-type]

        return cls._field(field) if field is not None else None, filter_expr  # type: ignore[return-value]
