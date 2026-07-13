"""AST nodes for parsed filter expressions."""

import attrs

from ..types import Array, CompareOp, Op, QueryElementQuantifier, Scalar

# Sentinel field name for scalar array element predicates in :class:`QueryField`.
ELEM_SCALAR_FIELD = "$"

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class QueryExpr:
    """Base class for all filter AST nodes."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryAnd(QueryExpr):
    """Conjunction of child expressions (logical AND)."""

    items: tuple[QueryExpr, ...]
    """Child expressions."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryOr(QueryExpr):
    """Disjunction of child expressions (logical OR)."""

    items: tuple[QueryExpr, ...]
    """Child expressions."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryNot(QueryExpr):
    """Negation of a single child expression."""

    item: QueryExpr
    """Child expression."""


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


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryCompare(QueryExpr):
    """Leaf node: compare left field to right field with an operator."""

    left: str
    """Left-hand field path."""

    op: CompareOp
    """Compare operator (equality or ordering)."""

    right: str
    """Right-hand field path."""


# ....................... #


@attrs.define(slots=True, frozen=True, match_args=True)
class QueryElem(QueryExpr):
    """Array element quantifier over a document field path."""

    path: str
    """Document field path (e.g. ``tags``, ``line_items``)."""

    quantifier: QueryElementQuantifier
    """``$any``, ``$all``, or ``$none``."""

    inner: QueryExpr
    """Element predicate (often :class:`QueryAnd` of element-relative :class:`QueryField` nodes)."""


# ....................... #


def elem_inner_is_scalar(inner: QueryExpr) -> bool:
    """Whether an element-quantifier's inner predicate targets the scalar element itself.

    A scalar inner predicate references only the sentinel :data:`ELEM_SCALAR_FIELD`
    (a primitive array element, e.g. ``tags`` of strings) rather than sub-fields of an
    object element. Backend renderers branch on this to choose scalar-vs-object element
    SQL/operator shapes; kept here (with the AST nodes) so every backend shares one
    definition.

    The branches are intentionally asymmetric: ``QueryAnd`` accepts only **flat**
    scalar-element leaves (the parser only ever emits direct ``$`` ``QueryField``
    conjunctions for scalar element predicates), whereas ``QueryOr`` recurses to allow
    nested disjunctions. This matches the original per-backend renderer behavior; do
    not widen the ``QueryAnd`` case to recurse without re-validating element-quantifier
    rendering across backends.
    """

    match inner:
        case QueryField(name, _, _):
            return name == ELEM_SCALAR_FIELD

        case QueryAnd(items):
            return all(isinstance(i, QueryField) and i.name == ELEM_SCALAR_FIELD for i in items)

        case QueryOr(items):
            return all(elem_inner_is_scalar(i) for i in items)

        case _:
            return False
