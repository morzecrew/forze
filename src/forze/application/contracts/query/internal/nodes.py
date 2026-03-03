"""AST nodes for parsed filter expressions."""

import attrs

from ..types import Array, Op, Scalar

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class QueryExpr:
    """Base class for all filter AST nodes."""


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryAnd(QueryExpr):
    """Conjunction of child expressions (logical AND)."""

    items: tuple[QueryExpr, ...]
    """Child expressions."""


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryOr(QueryExpr):
    """Disjunction of child expressions (logical OR)."""

    items: tuple[QueryExpr, ...]
    """Child expressions."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryField(QueryExpr):
    """Leaf node: field name, operator, and value."""

    name: str
    """Field name."""

    op: Op
    """Operator (e.g. ``$eq``, ``$in``)."""

    value: Scalar | Array
    """Operand value."""
