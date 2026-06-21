"""Filter/sort request field types with empty-mapping normalization."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BeforeValidator

from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
)

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


OptionalFilterExpression = Annotated[
    QueryFilterExpression | None,  # type: ignore[valid-type]
    BeforeValidator(empty_mapping_to_none),
]
"""Optional filter expression; a bare ``{}`` is coerced to ``None``."""

OptionalSortExpression = Annotated[
    QuerySortExpression | None,
    BeforeValidator(empty_mapping_to_none),
]
"""Optional sort expression; a bare ``{}`` is coerced to ``None``."""
