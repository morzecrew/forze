"""Filter/sort request field types with empty-mapping normalization."""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any

from pydantic import BeforeValidator, ValidationError, ValidatorFunctionWrapHandler, WrapValidator

from forze.application.contracts.querying import (
    AggregatesExpression,
    AggregatesExpressionParser,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QuerySortExpression,
)
from forze.base.exceptions import CoreException

# ----------------------- #


def empty_mapping_to_none(value: Any) -> Any:
    """Normalize a bare empty mapping (``{}``) to ``None`` (no filter/sort).

    A fully-empty mapping carries no predicates, so it unambiguously means "no
    constraint" — the same as omitting the field. This lets clients that
    serialize an absent filter as ``{}`` reach the handler without a violation.

    A structured-but-empty envelope (e.g. ``{"$values": {}}``) is left
    untouched, so the strict filter parser still rejects it as a probable
    dropped-predicate bug.
    """

    return None if value == {} else value


# ....................... #


def _named_by(parse: Callable[[Any], Any]) -> WrapValidator:
    """Validate as the type does; when that fails, let *parse* say why.

    An expression is a union of closed shapes, so a validation error lists every shape it did
    not fit. The parser that reads it behind the port names the one key or value at fault, and
    a request is refused with that instead.
    """

    def validate(value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
        try:
            return handler(value)

        except ValidationError as invalid:
            try:
                parse(value)

            except CoreException as error:
                raise ValueError(error.summary) from None

            # Whatever else the parser makes of it, the type's refusal stands.
            except Exception:
                raise invalid from None

            raise

    return WrapValidator(validate)


# ....................... #


OptionalFilterExpression = Annotated[
    QueryFilterExpression | None,  # type: ignore[valid-type]
    BeforeValidator(empty_mapping_to_none),
    _named_by(QueryFilterExpressionParser.parse),
]
"""Optional filter expression; a bare ``{}`` is coerced to ``None``."""

NamedAggregatesExpression = Annotated[
    AggregatesExpression,  # type: ignore[valid-type]
    _named_by(AggregatesExpressionParser.parse),
]
"""Aggregates expression whose refusal names the key or value at fault."""

OptionalSortExpression = Annotated[
    QuerySortExpression | None,
    BeforeValidator(empty_mapping_to_none),
]
"""Optional sort expression; a bare ``{}`` is coerced to ``None``."""
