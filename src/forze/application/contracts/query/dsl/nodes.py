"""AST nodes for parsed filter expressions."""

import attrs

from ..types import Array, Op, Scalar

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class Expr:
    """Base class for all filter AST nodes."""


@attrs.define(slots=True, frozen=True, match_args=True)
class And(Expr):
    """Conjunction of child expressions (logical AND)."""

    items: tuple[Expr, ...]
    """Child expressions."""


@attrs.define(slots=True, frozen=True, match_args=True)
class Or(Expr):
    """Disjunction of child expressions (logical OR)."""

    items: tuple[Expr, ...]
    """Child expressions."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class Field(Expr):
    """Leaf node: field name, operator, and value."""

    name: str
    """Field name."""

    op: Op
    """Operator (e.g. ``$eq``, ``$in``)."""

    value: Scalar | Array
    """Operand value."""
