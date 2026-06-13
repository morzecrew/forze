"""Per-backend query capabilities + a validator that fails cleanly, not at render time.

The filter DSL presents one surface, but backends diverge: a document store does
array element quantifiers and regex; a search engine or a key-value document store
may do neither. Historically each renderer discovered the gap deep in its own code
and raised :func:`~forze.base.exceptions.exc.internal` (a 500) — opaque to the caller
and untestable as a contract.

:class:`QueryCapabilities` makes the supported surface **declarative**: each backend
publishes what its renderer can compile, and :func:`validate_query_capabilities`
checks a parsed filter against it *before* rendering, raising a clean
:func:`~forze.base.exceptions.exc.precondition` (code ``query_feature_unsupported``)
naming the feature and backend. The in-memory mock is the canonical superset
(:data:`FULL_QUERY_CAPABILITIES`); a cross-backend parity suite uses each backend's
capabilities to decide which cases it must reproduce.

Scope: this validates **AST-determinable** features (which operators, in which
context — top-level vs inside an element quantifier — plus negation, field-to-field
comparison, and quantifier support). Gaps that depend on a column's storage shape
(e.g. set operators on a *nested JSON* path vs a native array column) cannot be seen
from the AST alone; those stay backend-internal but should raise the same
``query_feature_unsupported`` code rather than ``internal``.
"""

from typing import Final

import attrs

from forze.base.exceptions import exc

from .internal.nodes import (
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)

# ----------------------- #

UNSUPPORTED_QUERY_FEATURE_CODE: Final[str] = "query_feature_unsupported"
"""Error code raised when a filter uses a feature the target backend cannot compile."""


# Runtime operator sets (the ``types.py`` aliases are ``Literal`` types — not iterable
# at runtime — so the canonical sets are spelled out here, the single source for the
# full surface a backend may advertise).

ALL_VALUE_OPS: Final[frozenset[str]] = frozenset(
    {
        "$eq",
        "$neq",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$in",
        "$nin",
        "$null",
        "$empty",
        "$superset",
        "$subset",
        "$disjoint",
        "$overlaps",
        "$like",
        "$ilike",
        "$regex",
    }
)
"""Every operator the DSL allows on a top-level field predicate (the full ``Op`` set)."""

ALL_ELEMENT_OPS: Final[frozenset[str]] = frozenset(
    {
        "$eq",
        "$neq",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$like",
        "$ilike",
        "$regex",
        "$in",
        "$nin",
    }
)
"""Every operator the DSL allows inside an element quantifier (the full ``ElementOp`` set)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QueryCapabilities:
    """What a backend's filter renderer can compile, declared per backend.

    A query that stays within these is guaranteed (by the renderer's contract) to
    compile; one that strays is rejected up front by :func:`validate_query_capabilities`
    with a clean error, instead of a render-time ``internal`` failure.
    """

    value_ops: frozenset[str] = ALL_VALUE_OPS
    """Operators supported on a top-level field predicate."""

    element_ops: frozenset[str] = ALL_ELEMENT_OPS
    """Operators supported on a field predicate *inside* an element quantifier."""

    supports_quantifiers: bool = True
    """Whether array element quantifiers (``$any`` / ``$all`` / ``$none``) compile."""

    supports_nested_quantifiers: bool = True
    """Whether a quantifier may appear *inside* another quantifier's element predicate
    (e.g. ``orders $any {items $any ...}`` over nested object arrays)."""

    supports_negation: bool = True
    """Whether the ``$not`` combinator compiles."""

    supports_field_compare: bool = True
    """Whether field-to-field comparison (``$fields`` → :class:`QueryCompare`) compiles."""


# ....................... #

FULL_QUERY_CAPABILITIES: Final[QueryCapabilities] = QueryCapabilities()
"""The canonical full surface — every operator and feature the DSL defines.

The in-memory mock evaluates all of it, so it is both the reference semantics and the
capability superset every other backend is a subset of.
"""


# ....................... #


def validate_query_capabilities(
    expr: QueryExpr,
    caps: QueryCapabilities,
    *,
    backend: str,
) -> None:
    """Raise if *expr* uses a feature *caps* does not advertise (clean, not ``internal``).

    Walks the parsed filter AST; the first unsupported feature raises
    :func:`~forze.base.exceptions.exc.precondition` with code
    :data:`UNSUPPORTED_QUERY_FEATURE_CODE`, naming the feature and *backend*. Call it at
    the top of a renderer's filter entry, before compiling to the backend dialect.
    """

    def _fail(feature: str) -> None:
        raise exc.precondition(
            f"Query feature {feature} is not supported by the {backend!r} backend.",
            code=UNSUPPORTED_QUERY_FEATURE_CODE,
        )

    def _walk(node: QueryExpr, *, in_element: bool) -> None:
        match node:
            case QueryAnd(items) | QueryOr(items):
                for item in items:
                    _walk(item, in_element=in_element)

            case QueryNot(item):
                if not caps.supports_negation:
                    _fail("negation ($not)")

                _walk(item, in_element=in_element)

            case QueryField(_, op, _):
                allowed = caps.element_ops if in_element else caps.value_ops

                if op not in allowed:
                    where = " inside element quantifiers" if in_element else ""
                    _fail(f"operator {op!r}{where}")

            case QueryCompare(_, _, _):
                if not caps.supports_field_compare:
                    _fail("field-to-field comparison ($fields)")

            case QueryElem(_, quantifier, inner):
                if not caps.supports_quantifiers:
                    _fail(f"element quantifier {quantifier!r}")

                if in_element and not caps.supports_nested_quantifiers:
                    _fail("nested element quantifiers")

                _walk(inner, in_element=True)

            case _:
                pass

    _walk(expr, in_element=False)
